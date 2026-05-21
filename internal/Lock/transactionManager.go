package lock

import (
	"log"
	"slices"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	replication "github.com/your-username/DistributedDatabaseSystem/internal/replication"
)

type UndoOp int

const (
	UndoInsert UndoOp = iota
	UndoDelete
	UndoUpdate
)

type TxnStatus int

const (
	TxnActive TxnStatus = iota
	TxnCommitted
	TxnAborted
)

type UndoEntry struct {
	Op     UndoOp
	Key    uint64
	Fields []btree.Field //nil for UndoInsert, old row info for the others
}

type Transaction struct {
	Id      uint64
	Status  TxnStatus
	UndoLog []UndoEntry
	RedoLog []replication.ReplicationLogEntry
}

type TransactionManager struct {
	mu     sync.Mutex
	nextId uint64
	active map[uint64]*Transaction
	lm     *LockManager
	bt     *btree.BTree
	rm     *replication.ReplicationManager
}

func NewTransactionManager(bt *btree.BTree, rm *replication.ReplicationManager) *TransactionManager {
	return &TransactionManager{
		nextId: 1,
		active: make(map[uint64]*Transaction),
		lm:     NewLockManager(),
		bt:     bt,
		rm:     rm,
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	t := &Transaction{
		Id:      tm.nextId,
		Status:  TxnActive,
		UndoLog: make([]UndoEntry, 0),
		RedoLog: make([]replication.ReplicationLogEntry, 0),
	}
	tm.active[tm.nextId] = t
	tm.nextId++
	tm.mu.Unlock()
	return t
}

func (tm *TransactionManager) Lock(txnId uint64, rowKey uint64, lockType LockType) error {
	return tm.lm.Lock(txnId, rowKey, lockType)
}

func (tm *TransactionManager) AppendUndo(txnId uint64, entry UndoEntry) {
	tm.mu.Lock()
	if t := tm.active[txnId]; t != nil {
		t.UndoLog = append(t.UndoLog, entry)
	}
	tm.mu.Unlock()
}

func (tm *TransactionManager) AppendRedo(txnId uint64, entry replication.ReplicationLogEntry) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if t := tm.active[txnId]; t != nil {
		t.RedoLog = append(t.RedoLog, entry)
	}
}

func (tm *TransactionManager) Commit(txnId uint64) error {
	tm.mu.Lock()
	t := tm.active[txnId]
	if t == nil {
		tm.mu.Unlock()
		return nil
	}

	if tm.rm != nil && len(t.RedoLog) > 0 {
		if err := tm.rm.Append(t.RedoLog); err != nil {
			tm.mu.Unlock()
			return err
		}
	}

	t.Status = TxnCommitted
	t.UndoLog = nil
	t.RedoLog = nil
	delete(tm.active, txnId)
	tm.mu.Unlock()
	tm.lm.UnlockAll(txnId)
	return nil
}

func (tm *TransactionManager) Rollback(txnId uint64) {
	tm.mu.Lock()
	t := tm.active[txnId]
	if t == nil {
		tm.mu.Unlock()
		return
	}
	undoLog := t.UndoLog
	t.UndoLog = nil
	t.Status = TxnAborted
	delete(tm.active, txnId)
	tm.mu.Unlock()

	for _, entry := range slices.Backward(undoLog) {
		switch entry.Op {
		case UndoInsert:
			if err := tm.bt.Delete(entry.Key); err != nil {
				log.Printf("rollback: undo insert for key %d: %v", entry.Key, err)
			}
		case UndoDelete:
			if err := tm.bt.Insert(entry.Key, entry.Fields); err != nil {
				log.Printf("rollback: undo delete for key %d: %v", entry.Key, err)
			}
		case UndoUpdate:
			if err := tm.bt.Insert(entry.Key, entry.Fields); err != nil {
				log.Printf("rollback: undo update for key %d: %v", entry.Key, err)
			}
		}
	}

	tm.lm.UnlockAll(txnId)
}
