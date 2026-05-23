package replication

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/sudesh856/walveil/internal/config"
	"github.com/sudesh856/walveil/internal/wal"
	"go.uber.org/zap"
)

type Slot struct {
	conn *Conn
	cfg  config.SourceConfig
	log  *zap.Logger
}

func NewSlot(conn *Conn, cfg config.SourceConfig, log *zap.Logger) *Slot {
	return &Slot{conn: conn, cfg: cfg, log: log}
}

func (s *Slot) EnsureSlot(ctx context.Context) (wal.LSN, error) {
	existing, err := s.querySlot(ctx)
	if err != nil {
		return wal.ZeroLSN, fmt.Errorf("slot query: %w", err)
	}

	if existing != nil {
		s.log.Info("replication slot exists, resuming",
			zap.String("slot", s.cfg.SlotName),
			zap.String("conmfirmed_flush_lsn", existing.String()),
		)
		return *existing, nil
	}

	if !s.cfg.Slot.CreateIfMissing {
		return wal.ZeroLSN, fmt.Errorf(
			"replication slot %q doesn't exist and create_if_missing is false",
			s.cfg.SlotName,
		)
	}

	return s.createSlot(ctx)
}

func (s *Slot) Drop(ctx context.Context) error {
	s.log.Warn("dropping replication slot",
		zap.String("slot", s.cfg.SlotName),
	)
	_, err := s.conn.Raw().Exec(ctx,
		fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", s.cfg.SlotName),
	).ReadAll()
	if err != nil {
		return fmt.Errorf("drop slot %q: %w", s.cfg.SlotName, err)
	}
	s.log.Info("replication slot dropped", zap.String("slot", s.cfg.SlotName))
	return nil
}

func (s *Slot) querySlot(ctx context.Context) (*wal.LSN, error) {
	results, err := s.conn.Raw().Exec(ctx, fmt.Sprintf(
		`SELECT confirmed_flush_lsn::text
		FROM pg_replication_slots
		WHERE slot_name = '%s' AND slot_type = 'logical'`,
		s.cfg.SlotName,
	)).ReadAll()
	if err != nil {
		return nil, err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}
	raw := string(results[0].Rows[0][0])
	lsn, err := wal.ParseLSN(raw)
	if err != nil {
		return nil, fmt.Errorf("parse confirmed_flush_lsn %q: %w", raw, err)
	}
	return &lsn, nil
}

func (s *Slot) createSlot(ctx context.Context) (wal.LSN, error) {
	s.log.Info("creating replication slot",
		zap.String("slot", s.cfg.SlotName),
	)
	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		s.conn.Raw(),
		s.cfg.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		return wal.ZeroLSN, fmt.Errorf("create replication slot %q: %w", s.cfg.SlotName, err)
	}
	lsn, err := wal.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return wal.ZeroLSN, fmt.Errorf("parse slot LSN %q: %w", result.ConsistentPoint, err)
	}

	s.log.Info("replication slot created",
		zap.String("slot", s.cfg.SlotName),
		zap.String("consistent_point", result.ConsistentPoint),
	)
	return lsn, nil
}
