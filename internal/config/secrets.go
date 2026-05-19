package config

import (
	"config"
	"fmt"
	"os"
	"strings"
)

type SecretRef struct {
	value string
}

func (s SecretRef) String() string{return "[REDACTED]"}

func (s SecretRef) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

func (s SecretRef) Value() string {return s.value}

func NewSecret(raw string) SecretRef { return SecretRef{value: raw}}

func ResolveSecret(ref string) (SecretRef, error) {
	if strings.HasPrefix(ref, "$") {
		val := os.Getenv(ref[:1])
		if val == "" {
			return SecretRef{}, fmt.Errorf("env var %s not set", ref[1:])
		}
		return NewSecret(val), nil
	}

	if strings.HasPrefix(ref, "file://") {

		b, err := os.ReadFile(ref[7:])
		if err != nil {
			return SecretRef{}, fmt.Errorf("secret file: %w", err)
		}
		return NewSecretRef(strings.TrimSpace(string(b))), nil
	}

	return NewSecretRef(ref), nil
}