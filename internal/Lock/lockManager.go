package lock

import (
	"fmt"
	"slices"
	"sync"
)

// Deadlock Detector
type WaitForGraph = map[uint64][]uint64 //txnId → list of txnIds it waits for

func buildWaitForGraph(rowLocks map[uint64]*RowLock) WaitForGraph {
	graph := make(map[uint64][]uint64)

	for rowKey := range rowLocks {
		for _, waiter := range rowLocks[rowKey].waitQueue {
			graph[waiter.txnId] = append(graph[waiter.txnId], rowLocks[rowKey].holders...)
		}
	}

	return graph
}

func detectCycle(graph WaitForGraph, startTxnId uint64) bool {
	visited := make(map[uint64]bool)

	return dfs(graph, startTxnId, visited)
}

func dfs(graph WaitForGraph, txnId uint64, visited map[uint64]bool) bool {
	visited[txnId] = true
	for _, nei := range graph[txnId] {
		if visited[nei] {
			return true
		}
		if dfs(graph, nei, visited) {
			return true
		}
	}
	visited[txnId] = false
	return false
}

// Lock Manager
type LockType int

const (
	LockNone LockType = iota
	LockShared
	LockExclusive
)

type WaitRequest struct {
	txnId    uint64
	lockType LockType
	wake     chan struct{}
}

type RowLock struct {
	lockType  LockType
	holders   []uint64
	waitQueue []WaitRequest
}

type TransactionLocks struct {
	rows map[uint64]LockType // row key to lock type held by transaction performing operation on row
}

type LockManager struct {
	mu               sync.Mutex
	rowLocks         map[uint64]*RowLock          //for given row, which transactions hold or are waiting for locks
	transactionLocks map[uint64]*TransactionLocks //transactions holding which rows, and which lock type for row
}

func NewLockManager() *LockManager {
	return &LockManager{
		rowLocks:         make(map[uint64]*RowLock),
		transactionLocks: make(map[uint64]*TransactionLocks),
	}
}

func (lm *LockManager) Lock(txnId uint64, rowKey uint64, lockType LockType) error {
	lm.mu.Lock()

	if txnLocks := lm.transactionLocks[txnId]; txnLocks != nil {
		if existing, already := txnLocks.rows[rowKey]; already {
			if existing >= lockType {
				lm.mu.Unlock()
				return nil
			}
			// Upgrade: shared → exclusive. Strip this txn from the holders
			// so the normal grant/wait path below re-acquires at the new strength.
			row := lm.rowLocks[rowKey]
			row.holders = slices.DeleteFunc(row.holders, func(n uint64) bool {
				return n == txnId
			})
			delete(lm.transactionLocks[txnId].rows, rowKey)
			if len(row.holders) == 0 {
				row.lockType = LockNone
			}
		}
	}

	row, ok := lm.rowLocks[rowKey]
	if !ok {
		row = &RowLock{lockType: LockNone}
		lm.rowLocks[rowKey] = row
	}
	granted := false
	switch row.lockType {
	case LockNone:
		granted = true
	case LockShared:
		if lockType == LockShared && len(row.waitQueue) == 0 {
			granted = true
		}
	}

	if granted {
		row.holders = append(row.holders, txnId)
		row.lockType = lockType
		if lm.transactionLocks[txnId] == nil {
			lm.transactionLocks[txnId] = &TransactionLocks{rows: make(map[uint64]LockType)}
		}
		lm.transactionLocks[txnId].rows[rowKey] = lockType
	} else {
		wr := &WaitRequest{
			txnId:    txnId,
			lockType: lockType,
			wake:     make(chan struct{}, 1),
		}
		row.waitQueue = append(row.waitQueue, *wr)

		//determine if deadlock occurs
		graph := buildWaitForGraph(lm.rowLocks)
		if detectCycle(graph, wr.txnId) {
			row.waitQueue = slices.Delete(row.waitQueue, len(row.waitQueue)-1, len(row.waitQueue))
			lm.mu.Unlock()
			return fmt.Errorf("deadlock detected, rejecting wait request for txn: %d", wr.txnId)
		}

		lm.mu.Unlock()
		<-wr.wake
		lm.mu.Lock()
	}

	lm.mu.Unlock()
	return nil
}

func (lm *LockManager) UnlockAll(txnId uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	transaction, ok := lm.transactionLocks[txnId]
	if !ok {
		return
	}

	for rowKey := range transaction.rows {
		rowLock := lm.rowLocks[rowKey]
		rowLock.holders = slices.DeleteFunc(rowLock.holders, func(n uint64) bool {
			return n == txnId
		})

		if len(rowLock.holders) == 0 {
			//process wait queue
			if len(rowLock.waitQueue) == 0 {
				delete(lm.rowLocks, rowKey)
			} else {
				waiter := rowLock.waitQueue[0]
				if waiter.lockType == LockExclusive {
					rowLock.lockType = LockExclusive
					rowLock.holders = append(rowLock.holders, waiter.txnId)
					if lm.transactionLocks[waiter.txnId] == nil {
						lm.transactionLocks[waiter.txnId] = &TransactionLocks{rows: make(map[uint64]LockType)}
					}
					lm.transactionLocks[waiter.txnId].rows[rowKey] = LockExclusive
					close(waiter.wake)
					rowLock.waitQueue = slices.Delete(rowLock.waitQueue, 0, 1)
				} else {
					rowLock.lockType = LockShared
					for len(rowLock.waitQueue) > 0 && rowLock.waitQueue[0].lockType != LockExclusive {
						waiter := rowLock.waitQueue[0]
						rowLock.holders = append(rowLock.holders, waiter.txnId)
						if lm.transactionLocks[waiter.txnId] == nil {
							lm.transactionLocks[waiter.txnId] = &TransactionLocks{rows: make(map[uint64]LockType)}
						}
						lm.transactionLocks[waiter.txnId].rows[rowKey] = LockShared
						close(waiter.wake)
						rowLock.waitQueue = slices.Delete(rowLock.waitQueue, 0, 1)
					}
				}
			}
		}
	}

	delete(lm.transactionLocks, txnId)
}
