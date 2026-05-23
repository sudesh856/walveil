package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/jackc/pgx/v5/pgconn"
)

type Config struct {
	Source     SourceConfig     `toml:"source"`
	Filter     FilterConfig     `toml:"filter"`
	Sinks      []SinkConfig     `toml:"sinks"`
	Checkpoint CheckpointConfig `toml:"checkpoint"`
	Server     ServerConfig     `toml:"server"`
	Log        LogConfig        `toml:"log"`
}

type SourceConfig struct {
	DSN             string     `toml:"dsn"`
	SlotName        string     `toml:"slot_name"`
	PublicationName string     `toml:"publication_name"`
	SSLMode         string     `toml:"ssl_mode"`
	SSLRootCert     string     `toml:"ssl_root_cert"`
	Slot            SlotConfig `toml:"slot"`

	dsnSecret SecretRef
}

type SlotConfig struct {
	CreateIfMissing bool  `toml:"create_if_missing"`
	MaxLagBytes     int64 `toml:"max_lag_bytes"`
	MaxLagSeconds   int64 `toml:"max_lag_seconds"`
}

type FilterConfig struct {
	Tables        []string `toml:"tables"`
	Events        []string `toml:"events"`
	RedactColumns []string `toml:"redact_columns"`
}

type SinkConfig struct {
	Type         string `toml:"type"`
	URL          string `toml:"url"`
	Secret       string `toml:"secret"`
	TimeoutMs    int    `toml:"timeout_ms"`
	MaxRetries   int    `toml:"max_retries"`
	RateLimitRPS int    `toml:"rate_limit_rps"`
	Addr         string `toml:"addr"`
	StreamName   string `toml:"stream_name"`
	MaxLen       int64  `toml:"max_len"`

	SecretRef SecretRef
}

type CheckpointConfig struct {
	Path            string `toml:"path"`
	FlushIntervalMs int    `toml:"flush_interval_ms"`
}

type ServerConfig struct {
	Addr string `toml:"addr"`
}

type LogConfig struct {
	Level  string `toml:"level"`
	Format string `toml:"format"`
}

func (s *SourceConfig) DSNValue() string {
	return s.dsnSecret.Value()

}

func Load(path string) (*Config, error) {
	var cfg Config
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, fmt.Errorf("config decode: %w", err)
	}

	if unknown := meta.Undecoded(); len(unknown) > 0 {
		keys := make([]string, len(unknown))
		for i, k := range unknown {
			keys[i] = k.String()
		}
		return nil, fmt.Errorf("config: unknown keys: %s", strings.Join(keys, ", "))
	}

	if err := cfg.resolveSecrets(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) resolveSecrets() error {
	var errs []string

	dsn, err := ResolveSecret(c.Source.DSN)
	if err != nil {
		errs = append(errs, fmt.Sprintf("source.dsn secret: %v", err))
	} else {
		c.Source.dsnSecret = dsn
	}

	for i := range c.Sinks {
		if c.Sinks[i].Secret != "" {
			ref, err := ResolveSecret(c.Sinks[i].Secret)
			if err != nil {
				errs = append(errs, fmt.Sprintf("sinks[%d].secret: %v", i, err))
			} else {
				c.Sinks[i].SecretRef = ref
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("secret resolution failed:\n - %s", strings.Join(errs, "\n - "))
	}
	return nil
}

func (c *Config) Validate() error {
	var errs []string

	if c.Source.DSN == "" {
		errs = append(errs, "source.dsn: required")
	} else if _, err := pgconn.ParseConfig(c.Source.DSNValue()); err != nil {
		errs = append(errs, fmt.Sprintf("source.dsn: %v", err))
	}

	if c.Source.SlotName == "" {
		errs = append(errs, "source.slot_name: required")
	}
	if c.Source.PublicationName == "" {
		errs = append(errs, "source.publication_name: required")
	}
	if c.Checkpoint.Path == "" {
		errs = append(errs, "checkpoint.path: required")
	}
	if c.Server.Addr == "" {
		errs = append(errs, "server.addr: required")
	}

	for i, sink := range c.Sinks {
		switch sink.Type {
		case "webhook":
			if sink.URL == "" {
				errs = append(errs, fmt.Sprintf("sinks[%d].url: required for webhook sink", i))
			}

		case "redis":
			if sink.Addr == "" {
				errs = append(errs, fmt.Sprintf("sinks[%d].addr: required for redis sink", i))
			}
			if sink.StreamName == "" {
				errs = append(errs, fmt.Sprintf("sinks[%d].stream_name: required for redis sink", i))
			}
		case "":
			errs = append(errs, fmt.Sprintf("sinks[%d].type: required", i))
		default:
			errs = append(errs, fmt.Sprintf("sinks[%d].type: unknown sink type %q", sink.Type))
		}
	}

	if c.Checkpoint.Path != "" {
		dir := filepath.Dir(c.Checkpoint.Path)
		if err := checkDirWritable(dir); err != nil {
			errs = append(errs, fmt.Sprintf("checkpoint.path: %v", err))
		}
	}

	if c.Source.SSLRootCert != "" {
		if _, err := os.Stat(c.Source.SSLRootCert); err != nil {
			errs = append(errs, fmt.Sprintf("source.ssl_root_cert: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n -%s", strings.Join(errs, "\n - "))
	}
	return nil
}

func checkDirWritable(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("directory %q doesn't exist: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%q is not a directory", dir)
	}

	tmp, err := os.CreateTemp(dir, ".walveil-write-check-*")
	if err != nil {
		return fmt.Errorf("directory %q is not writable: %w", dir, err)
	}

	tmp.Close()
	os.Remove(tmp.Name())
	return nil

}
