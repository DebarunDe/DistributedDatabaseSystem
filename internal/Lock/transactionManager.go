package lock

import (
	"slices"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
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
}

type TransactionManager struct {
	mu     sync.Mutex
	nextId uint64
	active map[uint64]*Transaction
	lm     *LockManager
	bt     *btree.BTree
}

func NewTransactionManager(bt *btree.BTree) *TransactionManager {
	return &TransactionManager{
		nextId: 1,
		active: make(map[uint64]*Transaction),
		lm:     NewLockManager(),
		bt:     bt,
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	t := &Transaction{
		Id:      tm.nextId,
		Status:  TxnActive,
		UndoLog: make([]UndoEntry, 0),
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

func (tm *TransactionManager) Commit(txnId uint64) {
	tm.mu.Lock()
	t := tm.active[txnId]
	if t == nil {
		tm.mu.Unlock()
		return
	}
	t.Status = TxnCommitted
	t.UndoLog = nil
	delete(tm.active, txnId)
	tm.mu.Unlock()

	tm.lm.UnlockAll(txnId)
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
			_ = tm.bt.Delete(entry.Key)
		case UndoDelete:
			_ = tm.bt.Insert(entry.Key, entry.Fields)
		case UndoUpdate:
			_ = tm.bt.Insert(entry.Key, entry.Fields)
		}
	}

	tm.lm.UnlockAll(txnId)
}
