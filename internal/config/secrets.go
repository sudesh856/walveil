package config

import (
	"fmt"
	"os"
	"strings"
)

type SecretRef struct {
	value string
}

func (s SecretRef) String() string                { return "[REDACTED]" }
func (s SecretRef) M