package wal

import (
	"fmt"
	"strconv"
	"strings"
)

type LSN uint64

const ZeroLSN LSN = 0

func ParseLSN(s string) (LSN, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %q", s)
	}

	hi, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN high part %q: %w", parts[0], err)
	}

	lo, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN low part %q: %w", parts[1], err)
	}

	return LSN((hi << 32) | lo), nil
}

func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(l>>32), uint(l))
}

func (l LSN) Uint64() uint64 {
	return uint64(l)
}

func (l LSN) After(other LSN) bool {
	return l > other
}

func (l LSN) Before(other LSN) bool {
	return l < other
}

func (l LSN) IsZero() bool {
	return l == ZeroLSN
}

//lets marshaltext

func (l LSN) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *LSN) UnmarshalText(b []byte) error {
	v, err := ParseLSN(string(b))
	if err != nil {
		return err
	}

	*l = v
	return nil
}
