package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sudesh856/walveil/internal/wal"
)

type record struct {
	LSN string `json:"lsn"`
	CRC string `json:"crc"`
	TS  string `json:"ts"`
}

type Store struct {
	path            string
	flushIntervalMs int

	mu sync.Mutex
}

func NewStore(path string, flushIntervalMs int) *Store {
	return &Store{
		path:            path,
		flushIntervalMs: flushIntervalMs,
	}
}

func (s *Store) FlushInterval() time.Duration {

	return time.Duration(s.flushIntervalMs) * time.Millisecond
}

func (s *Store) Save(lsn wal.LSN) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lsnStr := lsn.String()
	crc := crc32.ChecksumIEEE([]byte(lsnStr))

	rec := record{
		LSN: lsnStr,
		CRC: fmt.Sprintf("%08x", crc),
		TS:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("checkpoint marshal: %w", err)
	}

	tmp := s.path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("checkpoint open tmp:  %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint write tmp: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint fsync: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("checkpoint close tmp: %w", err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("checkpoint rename: %w", err)
	}

	return nil

}

func (s *Store) Load() (wal.LSN, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return wal.ZeroLSN, nil
		}
		return wal.ZeroLSN, fmt.Errorf("checkpoint read: %w", err)
	}

	var rec record
	if err := json.Unmarshal(data, &rec); err != nil {
		return wal.ZeroLSN, fmt.Errorf(
			"checkpoint corrupt (unmarshal failed): %w — "+
				"delete %s to reset (events will re-deliver from slot LSN)",
			err, s.path,
		)
	}

	expected := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(rec.LSN)))
	if rec.CRC != expected {
		return wal.ZeroLSN, fmt.Errorf(
			"checkpoint CRC mismatch: file=%s expected=%s path=%s — "+
				"file may be corrupted or tampered with. "+
				"Delete %s to reset (events will re-deliver from slot LSN)",
			rec.CRC, expected, s.path, s.path,
		)
	}

	lsn, err := wal.ParseLSN(rec.LSN)
	if err != nil {
		return wal.ZeroLSN, fmt.Errorf(
			"checkpoint LSN parse failed %q: %w", rec.LSN, err,
		)
	}

	return lsn, nil
}

type LSNProvider func() wal.LSN

func (s *Store) RunFlusher(ctx context.Context, lsnFn LSNProvider, log func(error)) error {
	ticker := time.NewTicker(s.FlushInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.Save(lsnFn()); err != nil {
				if log != nil {
					log(err)
				}
			}
		case <-ctx.Done():

			return s.Save(lsnFn())
		}
	}
}

func (s *Store) CleanStaleTmp() error {
	tmp := s.path + ".tmp"
	err := os.Remove(tmp)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cleanup stale tmp %s: %w", tmp, err)
	}
	return nil
}

func (s *Store) Dir() string {
	return filepath.Dir(s.path)
}
