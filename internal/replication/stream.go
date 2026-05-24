package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/sudesh856/walveil/internal/wal"
	"go.uber.org/zap"
)

type Dispatcher interface {
	DispatchAndWait(batch []*wal.ChangeEvent) error
}

type Stream struct {
	conn         *Conn
	decoder      *Decoder
	dispatcher   Dispatcher
	slotName     string
	pubName      string
	confirmedLSN pglogrepl.LSN
	standbyFreq  time.Duration
	log          *zap.Logger
}

func NewStream(
	conn *Conn,
	decoder *Decoder,
	dispatcher Dispatcher,
	slotName string,
	pubName string,
	startLSN wal.LSN,
	log *zap.Logger,
) *Stream {

	return &Stream{
		conn:         conn,
		decoder:      decoder,
		dispatcher:   dispatcher,
		slotName:     slotName,
		pubName:      pubName,
		confirmedLSN: pglogrepl.LSN(startLSN.Uint64()),
		standbyFreq:  10 * time.Second,
		log:          log,
	}
}

func (s *Stream) ConfirmedLSN() wal.LSN {
	return wal.LSN(s.confirmedLSN)
}

func (s *Stream) Start(ctx context.Context) error {

	for {
		err := s.runOnce(ctx)

		if ctx.Err() != nil {
			return nil
		}

		if err != nil {
			s.log.Warn("stream error, will reconnect",
				zap.Error(err),
				zap.String("confirmed_lsn", s.confirmedLSN.String()),
			)
		}

		if reconnErr := s.conn.Reconnect(ctx); reconnErr != nil {
			return fmt.Errorf("reconnect: %w", reconnErr)
		}
	}

}

func (s *Stream) runOnce(ctx context.Context) error {

	if err := s.startReplication(ctx); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	s.log.Info("replication stream started",
		zap.String("slot", s.slotName),
		zap.String("confirmed_lsn", s.confirmedLSN.String()),
	)

	nextStandby := time.Now().Add(s.standbyFreq)

	for {
		if time.Now().After(nextStandby) {
			if err := s.sendStandbyStatus(ctx); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}

			nextStandby = time.Now().Add(s.standbyFreq)
		}

		//set deadline here even when no WAL messages arrive

		deadline := time.Now().Add(s.standbyFreq)
		if err := s.conn.Raw().Conn().SetDeadline(deadline); err != nil {
			return fmt.Errorf("set deadline: %w", err)
		}

		msg, err := pglogrepl.ReceiveMessage(ctx, s.conn.Raw())
		if err != nil {
			if pglogrepl.IsDeadlineTimeout(err) {
				continue
			}

			return fmt.Errorf("receive message: %w", err)
		}

		switch m := msg.(type) {
		case pglogrepl.PrimaryKeepaliveMessage:
			if m.ReplyRequested {
				if err := s.sendStandbyStatus(ctx); err != nil {
					return fmt.Errorf("keepalive reply: %w", err)
				}
				nextStandby = time.Now().Add(s.standbyFreq)
			}

		case pglogrepl.XLogData:
			if err := s.processXLogData(ctx, m); err != nil {
				return fmt.Errorf("process xlog: %w", err)
			}

		default:
			s.log.Debug("unhandled replication message",
			zap.String("type", fmt.Sprintf("%T", msg)),
		)
		}

		select {
		case <-ctx.Done():
			return nil
		default:
			continue //can be kept empty too (for myself!!)

		}
	}

}


func(s *Stream) processXLogData(ctx context.Context, msg pglogrepl.XLogData) error {
		batch, err := s.decoder.Decode(msg.WALData)
		if err != nil {
			return fmt.Errorf("decode WAL: %w",err)
		}
		if len(batch) == 0 {
			return nil

		}

		if err := 	s.dispatcher.DispatchAndWait(batch); err != nil {
			s.log.Error("dispatch failed -- LSN not advanced, will retry on restart",
			zap.Error(err),
			zap.String("wal_start", msg.WALStart.String()),
			zap.Int("batch_size", len(batch)),
			
			
			)
			return err
		}

		s.confirmedLSN = msg.WALStart
		s.log.Debug("LSN advanced",
		zap.String("lsn", s.confirmedLSN.String()),
		zap.Int("batch_size", len(batch)),
	)
	return nil
}

func (s *Stream) sendStandbyStatus(ctx context.Context) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn.Raw(),
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: s.confirmedLSN,
			WALFlushPosition: s.confirmedLSN,
			WALApplyPosition: s.confirmedLSN,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		},
	)
	if err != nil {
		return fmt.Errorf("send standby status: %w", err)
	}
	return nil
}

func (s *Stream) startReplication(ctx context.Context) error {
	return pglogrepl.StartReplication(
		ctx,
		s.conn.Raw(),
		s.slotName,
		s.confirmedLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", s.pubName),
			},
		},
	)
}
