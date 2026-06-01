package partition

import (
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

type RangeDescriptor struct {
	RangeID  uint64
	StartKey uint64            // inclusive
	EndKey   uint64            // exclusive
	Replicas map[uint64]string // nodeID → address
	LeaderID uint64
	Size     uint64 // approximate bytes, for split decisions
}

type RangeStats struct {
	KeyCount   uint64
	TotalBytes uint64
}

const (
	MaxKeysPerRange    = 1000
	MinRangeSplitBytes = 1 << 20 // 1MB
)

type RangeOp int

const (
	RangeOpScan RangeOp = iota
	RangeOpInsert
	RangeOpUpdate
	RangeOpDelete
)

type RangeRequest struct {
	Op        RangeOp
	TableID   uint32
	StartKey  uint64
	EndKey    uint64
	Key       uint64              // INSERT: specific key
	Fields    []btree.Field       // INSERT: row data; UPDATE: new value
	Where     sqllayer.Expression // SELECT/UPDATE/DELETE: filter
	Columns   []int               // SELECT: projection indices
	UpdateCol int                 // UPDATE: which column index
}

type ResultRow struct {
	Key    uint64
	Fields []btree.Field
}

type ResultSet struct {
	Columns  []string
	ColTypes []string
	Rows     []ResultRow
}

type RangeResponse struct {
	Rows  []ResultRow
	Error error
}
