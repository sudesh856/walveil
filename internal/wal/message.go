package wal

import "time"

type Op string

const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
)

type ChangeEvent struct {
	ID        string         `json:"id"`
	Source    EventSource    `json:"source"`
	Op        Op             `json:"op"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	LSN       string         `json:"lsn"`
	XID       uint32         `json:"xid"`
	TimeStamp time.Time      `json:"timestamp"`
	PK        map[string]any `json:"pk,omitempty"`
	Before    map[string]any `json:"before,omitempty"`
	After     map[string]any `json:"after,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type EventSource struct {
	Connector string `json:"connector"`
	Version   string `json:"version"`
	DB        string `json:"db"`
	SystemID  string `json:"system_id"`
	SlotName  string `json:"slot_name"`
}

type WalMessage struct {
	Type      byte
	LSN       uint64
	XID       uint32
	Timestamp time.Time
	Data      []byte
}
