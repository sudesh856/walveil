package replication

import (
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/sudesh856/walveil/internal/wal"
	"go.uber.org/zap"
)

type txnBuffer struct {
	xid       uint32
	commitLSN pglogrepl.LSN
	events    []*wal.ChangeEvent
	sizeBytes int64
	order     int
}

type Decoder struct {
	relations   *wal.RelationCache
	txns        map[uint32]*txnBuffer
	currentXID  uint32
	txnCounter  int
	maxBytes    int64
	usedBytes   int64
	maxOpenTxns int
	source      wal.EventSource
	log         *zap.Logger
}

func NewDecoder(
	relations *wal.RelationCache,
	source wal.EventSource,
	maxBytes int64,
	maxOpenTxns int,
	log *zap.Logger,

) *Decoder {
	if maxOpenTxns <= 0 {
		maxOpenTxns = 1024
	}

	return &Decoder{
		relations:   relations,
		txns:        make(map[uint32]*txnBuffer),
		maxBytes:    maxBytes,
		maxOpenTxns: maxOpenTxns,
		source:      source,
		log:         log,
	}
}

func (d *Decoder) Decode(data []byte) ([]*wal.ChangeEvent, error) {
	if len(data) == 0 {
		return nil, nil
	}
	msgType := data[0]
	switch msgType {
	case 'B':
		msg, err := pglogrepl.ParseBeginMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Begin: %w", err)
		}

		if len(d.txns) >= d.maxOpenTxns {
			d.evictOldest()
		}

		d.txnCounter++
		d.currentXID = msg.Xid
		d.txns[msg.Xid] = &txnBuffer{xid: msg.Xid, order: d.txnCounter}
		return nil, nil

	case 'R':
		msg, err := pglogrepl.ParseRelationMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Relation: %w", err)
		}

		rel := &wal.Relation{
			OID:             msg.RelationID,
			Schema:          msg.Namespace,
			Table:           msg.RelationName,
			ReplicaIdentity: msg.ReplicaIdentity,
		}

		for _, col := range msg.Columns {
			rel.Columns = append(rel.Columns, wal.Column{
				Name:    col.Name,
				TypeOID: col.DataType,
				IsKey:   col.Flags == 1,
			})
		}

		d.relations.Set(rel)
		return nil, nil

	case 'I':
		msg, err := pglogrepl.ParseInsertMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Insert: %w", err)
		}

		rel := d.relations.Get(msg.RelationID)
		if rel == nil {
			return nil, fmt.Errorf("unknown relation OID  %d for Insert", msg.RelationID)
		}

		event, err := d.buildEvent(wal.OpInsert, rel, nil, msg.Tuple, d.currentXID)
		if err != nil {
			return nil, err
		}
		return nil, d.bufferEvent(d.currentXID, event)

	case 'U':
		msg, err := pglogrepl.ParseUpdateMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Update: %w", err)
		}

		rel := d.relations.Get(msg.RelationID)
		if rel == nil {
			return nil, fmt.Errorf("unknown relation OID %d for Update", msg.RelationID)
		}

		event, err := d.buildEvent(wal.OpUpdate, rel, msg.OldTuple, msg.NewTuple, d.currentXID)
		if err != nil {
			return nil, err
		}
		return nil, d.bufferEvent(d.currentXID, event)

	case 'D':
		msg, err := pglogrepl.ParseDeleteMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Delete: %w", err)
		}

		rel := d.relations.Get(msg.RelationID)
		if rel == nil {
			return nil, fmt.Errorf("unknown relation OID %d for Delete", msg.RelationID)
		}

		event, err := d.buildEvent(wal.OpDelete, rel, msg.OldTuple, nil, d.currentXID)
		if err != nil {
			return nil, err
		}
		return nil, d.bufferEvent(d.currentXID, event)

	case 'T':
		msg, err := pglogrepl.ParseTruncateMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Truncate: %w", err)
		}
		for _, oid := range msg.RelationIDs {
			rel := d.relations.Get(oid)
			if rel == nil {
				continue
			}
			event := &wal.ChangeEvent{
				Op:     wal.OpTruncate,
				Schema: rel.Schema,
				Table:  rel.Table,
				XID:    d.currentXID,
				Source: d.source,
			}
			if err := d.bufferEvent(d.currentXID, event); err != nil {
				return nil, err
			}
		}
		return nil, nil

	case 'C':
		msg, err := pglogrepl.ParseCommitMessage(data[1:])
		if err != nil {
			return nil, fmt.Errorf("parse Commit: %w", err)
		}
		buf, ok := d.txns[msg.TransactionID]
		if !ok {
			return nil, nil
		}
		buf.commitLSN = msg.CommitLSN
		events := buf.events
		for _, e := range events {
			e.CommitLSN = buf.commitLSN
		}
		d.usedBytes -= buf.sizeBytes
		delete(d.txns, msg.TransactionID)
		if d.currentXID == msg.TransactionID {
			d.currentXID = 0
		}
		return events, nil

	case 'O':
		return nil, nil

	case 'M':
		return nil, nil

	case 'Y':
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown pgoutput message type: %c", msgType)
	}
}

func (d *Decoder) bufferEvent(xid uint32, event *wal.ChangeEvent) error {
	buf, ok := d.txns[xid]
	if !ok {
		d.log.Warn("bufferEvent: no open txn for XID — discarding event",
			zap.Uint32("xid", xid),
			zap.String("table", event.Table),
			zap.String("op", string(event.Op)),
		)
		return nil
	}

	size := estimateEventSize(event)
	if d.maxBytes > 0 && d.usedBytes+size > d.maxBytes {
		return fmt.Errorf(
			"txn buffer exceeded max %d bytes (used %d) — "+
				"pause WAL consumption and apply backpressure",
			d.maxBytes, d.usedBytes,
		)
	}

	buf.events = append(buf.events, event)
	buf.sizeBytes += size
	d.usedBytes += size
	return nil
}

func (d *Decoder) evictOldest() {
	var oldestXID uint32
	oldestOrder := int(^uint(0) >> 1) // max int
	for xid, buf := range d.txns {
		if buf.order < oldestOrder {
			oldestOrder = buf.order
			oldestXID = xid
		}
	}
	if oldestXID == 0 {
		return
	}
	buf := d.txns[oldestXID]
	d.usedBytes -= buf.sizeBytes
	delete(d.txns, oldestXID)
	d.log.Warn("evicted stale txn buffer — likely a rollback or very long-running transaction",
		zap.Uint32("xid", oldestXID),
		zap.Int("events_lost", len(buf.events)),
		zap.Int64("bytes_freed", buf.sizeBytes),
	)
}

func (d *Decoder) buildEvent(
	op wal.Op,
	rel *wal.Relation,
	oldTuple *pglogrepl.TupleData,
	newTuple *pglogrepl.TupleData,
	xid uint32,
) (*wal.ChangeEvent, error) {
	event := &wal.ChangeEvent{
		Op:     op,
		Schema: rel.Schema,
		Table:  rel.Table,
		XID:    xid,
		Source: d.source,
	}

	if newTuple != nil {
		after, err := tupleToMap(rel, newTuple)
		if err != nil {
			return nil, fmt.Errorf("decode new tuple: %w", err)
		}
		event.After = after
	}

	if oldTuple != nil {
		before, err := tupleToMap(rel, oldTuple)
		if err != nil {
			return nil, fmt.Errorf("decode old tuple: %w", err)
		}
		event.Before = before
	}

	return event, nil
}

func tupleToMap(rel *wal.Relation, tuple *pglogrepl.TupleData) (map[string]any, error) {
	if tuple == nil {
		return nil, nil
	}
	m := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case 'n':
			m[name] = nil
		case 'u':
			m[name] = "__unchanged_toast__"
		case 't':
			m[name] = string(col.Data)
		default:

			m[name] = string(col.Data)
		}
	}
	return m, nil
}

func estimateEventSize(e *wal.ChangeEvent) int64 {
	size := int64(512)
	size += int64(len(e.Schema) + len(e.Table) + len(e.Op))
	size += int64(len(e.Source.SystemID) + len(e.Source.SlotName) + len(e.Source.DB))

	for k, v := range e.After {
		size += int64(len(k)) + 8
		if s, ok := v.(string); ok {
			size += int64(len(s))
		}
	}
	for k, v := range e.Before {
		size += int64(len(k)) + 8
		if s, ok := v.(string); ok {
			size += int64(len(s))
		}
	}
	return size
}
