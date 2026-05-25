package filter

import (
	"fmt"
	"strings"

	"github.com/sudesh856/walveil/internal/wal"
)

const RedactPlaceholder = "[REDACTED]"

type Filter struct {
	allowedTables map[string]struct{}

	allowedOps map[string]struct{}

	redactRules []redactRule
}

type redactRule struct {
	schema string
	table  string
	column string
}

type Config struct {
	Tables []string
	Events []string

	RedactColumns []string
}

func New(cfg Config) (*Filter, error) {
	f := &Filter{
		allowedTables: make(map[string]struct{}, len(cfg.Tables)),
		allowedOps:    make(map[string]struct{}, len(cfg.Events)),
	}

	for _, t := range cfg.Tables {
		f.allowedTables[strings.ToLower(t)] = struct{}{}
	}

	for _, op := range cfg.Events {
		norm := strings.ToLower(strings.TrimSpace(op))
		switch norm {
		case "insert", "update", "delete", "truncate":
			f.allowedOps[norm] = struct{}{}
		default:
			return nil, fmt.Errorf("filter: unknown event type %q (want insert|update|delete|truncate)", op)
		}
	}

	for _, raw := range cfg.RedactColumns {
		rule, err := parseRedactRule(raw)
		if err != nil {
			return nil, fmt.Errorf("filter: bad readct_column %q: %w", raw, err)
		}

		f.redactRules = append(f.redactRules, rule)
	}

	return f, nil
}

func parseRedactRule(raw string) (redactRule, error) {

	parts := strings.Split(strings.ToLower(strings.TrimSpace(raw)), ".")
	switch len(parts) {
	case 3:
		return redactRule{schema: parts[0], table: parts[1], column: parts[2]}, nil
	case 2:
		return redactRule{schema: "*", table: parts[0], column: parts[1]}, nil
	default:
		return redactRule{}, fmt.Errorf("must be schema.table.column or table.column, got %d parts", len(parts))

	}
}

func (f *Filter) Match(event *wal.ChangeEvent) bool {
	if event == nil {
		return false
	}

	if len(f.allowedTables) > 0 {
		key := strings.ToLower(event.Schema + "." + event.Table)
		if _, ok := f.allowedTables[key]; !ok {
			return false
		}
	}

	return true
}

func (f *Filter) Redact(event *wal.ChangeEvent) {
	if event == nil || len(f.redactRules) == 0 {
		return
	}
	schema := strings.ToLower(event.Schema)
	table := strings.ToLower(event.Table)

	var cols []string

	for _, rule := range f.redactRules {
		if (rule.schema == "*" || rule.schema == schema) &&
			(rule.table == "*" || rule.table == table) {
			cols = append(cols, rule.column)
		}
	}

	if len(cols) == 0 {
		return

	}

	if event.Before != nil {
		event.Before = copyAndRedact(event.Before, cols)
	}

	if event.After != nil {
		event.After = copyAndRedact(event.After, cols)
	}
}

func copyAndRedact(m map[string]interface{}, cols []string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}

	for _, col := range cols {
		if _, exists := out[col]; exists {
			out[col] = RedactPlaceholder
		}
	}

	return out
}
