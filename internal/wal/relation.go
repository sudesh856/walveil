package wal

import "sync"

type Column struct {
	Name string
	TypeOID uint32
	TypeName string
	IsKey bool
}

type Relation struct {
	OID uint32
	Schema string
	Table string
	Columns []Column
	ReplicaIdentity byte
}

type RelationCache struct {
	mu sync.RWMutex
	rels map[uint32]*Relation
}

func NewRelationCache() *RelationCache {
	return &RelationCache{
		rels: make(map[uint32]*Relation),
	}
}

func(c *RelationCache) Set(r *Relation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rels[r.OID] = r
}

func(c *RelationCache) Get(oid uint32) *Relation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rels[oid]
}

func(c *RelationCache) Delete(oid uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rels, oid)
}