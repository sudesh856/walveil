package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pglogrepl"
	"github.com/sudesh856/walveil/internal/config"

	"go.uber.org/zap"
)

type Conn struct {
	dsn            config.SecretRef
	conn           *pgconn.PgConn
	storedSystemID string
	log            *zap.Logger
}

func NewConn(dsn config.SecretRef, log *zap.Logger) *Conn {
	return &Conn{
		dsn: dsn,
		log: log,
	}
}

func (c *Conn) Connect(ctx context.Context) error {
	return c.reconnect(ctx)
}

func (c *Conn) Raw() *pgconn.PgConn {
	return c.conn
}

func (c *Conn) SystemID() string {
	return c.storedSystemID
}

func (c *Conn) Close(ctx context.Context) {
	if c.conn != nil {
		c.conn.Close(ctx)
	}
}

func (c *Conn) Reconnect(ctx context.Context) error {
	return c.reconnect(ctx)
}

func (c *Conn) reconnect(ctx context.Context) error {
	backoff := newExponentialBackoff(1*time.Second, 60*time.Second)

	for {
		conn, err := pgconn.Connect(ctx, c.dsn.Value())
		if err != nil {
			c.log.Warn("reconnect failed, backing off",
				zap.Error(err),
				zap.Duration("next_attempt", backoff.Current()),
			)
			select {
			case <-time.After(backoff.Next()):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		sysID, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			c.log.Warn("IDENTIFY_SYSTEM failed, retrying", zap.Error(err))
			conn.Close(ctx)
			continue
		}
		if c.storedSystemID != "" && sysID.SystemID != c.storedSystemID {
			conn.Close(ctx)
			return fmt.Errorf(
				"FATAL: system ID mismatch: expected %s, got %s"+
					" --possible failover to a different cluster,"+
					" manual intervention required",
				c.storedSystemID, sysID.SystemID,
			)
		}

		c.storedSystemID = sysID.SystemID
		c.conn = conn
		backoff.Reset()

		c.log.Info("replication conncetion established",
			zap.String("system_id", sysID.SystemID),
			zap.String("timeline", fmt.Sprintf("%d", sysID.Timeline)),
		)
		return nil
	}
}

type exponentialBackoff struct {
	current time.Duration
	max     time.Duration
}

func newExponentialBackoff(initial, max time.Duration) *exponentialBackoff {
	return &exponentialBackoff{current: initial, max: max}
}

func (b *exponentialBackoff) Next() time.Duration {
	d := b.current
	b.current *= 2
	if b.current > b.max {
		b.current = b.max
	}
	return d
}

func (b *exponentialBackoff) Current() time.Duration {
	return b.current
}

func (b *exponentialBackoff) Reset() {
	b.current = 1 * time.Second
}
