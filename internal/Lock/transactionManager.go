package lock

import (
	"slices"

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
	t := &Transaction{
		Id:      tm.nextId,
		Status:  TxnActive,
		UndoLog: make([]UndoEntry, 0),
	}
	tm.active[tm.nextId] = t
	tm.nextId++
	return t
}

func (tm *TransactionManager) Lock(txnId uint64, rowKey uint64, lockType LockType) error {
	return tm.lm.Lock(txnId, rowKey, lockType)
}

func (tm *TransactionManager) AppendUndo(txnId uint64, entry UndoEntry) {
	if t := tm.active[txnId]; t != nil {
		t.UndoLog = append(t.UndoLog, entry)
	}
}

func (tm *TransactionManager) Commit(txnId uint64) {
	t := tm.active[txnId]
	if t == nil {
		return
	}

	t.Status = TxnCommitted
	tm.lm.UnlockAll(txnId)
	t.UndoLog = nil
	delete(tm.active, txnId)
}

func (tm *TransactionManager) Rollback(txnId uint64) {
	t := tm.active[txnId]
	if t == nil {
		return
	}

	for _, entry := range slices.Backward(t.UndoLog) {
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
	t.Status = TxnAborted
	delete(tm.active, txnId)
}
