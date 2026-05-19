package replication

import (
	"os"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

type ReplOp int

const (
	ReplPut ReplOp = iota
	ReplDelete
)

type ReplicationLogEntry struct {
	LSN    uint64
	Op     ReplOp
	Key    uint64
	Fields []btree.Field
}

type ReplicationManager struct {
	mu      sync.Mutex
	nextLSN uint64
	file    *os.File
}

const (
	//One time offset
	REPL_OffsetTotalLen = 0

	//Record Offsets
	REPL_OffsetLSN   = 0
	REPL_OffsetOp    = 8
	REPL_OffsetKey   = 9
	REPL_OffsetValue = 17
)
