package lock

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// --- helpers ---

func mustLock(t *testing.T, lm *LockManager, txnId, rowKey uint64, lockType LockType) {
	t.Helper()
	if err := lm.Lock(txnId, rowKey, lockType); err != nil {
		t.Errorf("Lock(%d, %d) unexpected error: %v", txnId, rowKey, err)
	}
}

func assertLockType(t *testing.T, lm *LockManager, rowKey uint64, want LockType) {
	t.Helper()
	lm.mu.Lock()
	defer lm.mu.Unlock()
	row := lm.rowLocks[rowKey]
	if row == nil {
		if want != LockNone {
			t.Errorf("rowLocks[%d] absent, want lockType %d", rowKey, want)
		}
		return
	}
	if row.lockType != want {
		t.Errorf("rowLocks[%d].lockType = %d, want %d", rowKey, row.lockType, want)
	}
}

func assertHolders(t *testing.T, lm *LockManager, rowKey uint64, want []uint64) {
	t.Helper()
	lm.mu.Lock()
	defer lm.mu.Unlock()
	row := lm.rowLocks[rowKey]
	if row == nil {
		if len(want) != 0 {
			t.Errorf("rowLocks[%d] absent, want holders %v", rowKey, want)
		}
		return
	}
	got := make(map[uint64]bool, len(row.holders))
	for _, h := range row.holders {
		got[h] = true
	}
	wantMap := make(map[uint64]bool, len(want))
	for _, h := range want {
		wantMap[h] = true
	}
	for h := range wantMap {
		if !got[h] {
			t.Errorf("rowLocks[%d] missing holder %d, holders=%v", rowKey, h, row.holders)
		}
	}
	for h := range got {
		if !wantMap[h] {
			t.Errorf("rowLocks[%d] unexpected holder %d, holders=%v", rowKey, h, row.holders)
		}
	}
}

func assertRowAbsent(t *testing.T, lm *LockManager, rowKey uint64) {
	t.Helper()
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if _, ok := lm.rowLocks[rowKey]; ok {
		t.Errorf("rowLocks[%d] should have been deleted", rowKey)
	}
}

func assertTxnHolds(t *testing.T, lm *LockManager, txnId uint64, rowKey uint64, want LockType) {
	t.Helper()
	lm.mu.Lock()
	defer lm.mu.Unlock()
	txnLocks := lm.transactionLocks[txnId]
	if txnLocks == nil {
		t.Errorf("transactionLocks[%d] absent", txnId)
		return
	}
	got, ok := txnLocks.rows[rowKey]
	if !ok {
		t.Errorf("transactionLocks[%d].rows[%d] absent, want %d", txnId, rowKey, want)
		return
	}
	if got != want {
		t.Errorf("transactionLocks[%d].rows[%d] = %d, want %d", txnId, rowKey, got, want)
	}
}

func assertTxnAbsent(t *testing.T, lm *LockManager, txnId uint64) {
	t.Helper()
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if _, ok := lm.transactionLocks[txnId]; ok {
		t.Errorf("transactionLocks[%d] should have been deleted", txnId)
	}
}

func assertQueueLen(t *testing.T, lm *LockManager, rowKey uint64, want int) {
	t.Helper()
	lm.mu.Lock()
	defer lm.mu.Unlock()
	row := lm.rowLocks[rowKey]
	if row == nil {
		if want != 0 {
			t.Errorf("rowLocks[%d] absent, want queue len %d", rowKey, want)
		}
		return
	}
	if got := len(row.waitQueue); got != want {
		t.Errorf("rowLocks[%d].waitQueue len = %d, want %d", rowKey, got, want)
	}
}

// waitFor blocks until ch is closed or the timeout expires.
func waitFor(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for: %s", msg)
	}
}

// notDone asserts ch is NOT closed within the timeout.
func notDone(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("should not have completed: %s", msg)
	case <-time.After(timeout):
	}
}

// --- basic Lock tests ---

func TestLockExclusiveGranted(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)
	assertLockType(t, lm, 100, LockExclusive)
	assertHolders(t, lm, 100, []uint64{1})
	assertTxnHolds(t, lm, 1, 100, LockExclusive)
}

func TestLockSharedGranted(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)
	assertLockType(t, lm, 100, LockShared)
	assertHolders(t, lm, 100, []uint64{1})
	assertTxnHolds(t, lm, 1, 100, LockShared)
}

func TestMultipleSharedLocksGrantedConcurrently(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)
	mustLock(t, lm, 2, 100, LockShared)
	mustLock(t, lm, 3, 100, LockShared)

	assertLockType(t, lm, 100, LockShared)
	assertHolders(t, lm, 100, []uint64{1, 2, 3})
	assertTxnHolds(t, lm, 1, 100, LockShared)
	assertTxnHolds(t, lm, 2, 100, LockShared)
	assertTxnHolds(t, lm, 3, 100, LockShared)
}

func TestSameTxnMultipleRows(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)
	mustLock(t, lm, 1, 200, LockExclusive)

	assertTxnHolds(t, lm, 1, 100, LockShared)
	assertTxnHolds(t, lm, 1, 200, LockExclusive)
	assertHolders(t, lm, 100, []uint64{1})
	assertHolders(t, lm, 200, []uint64{1})
}

func TestDuplicateLockIsIdempotent(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	done := make(chan struct{})
	go func() {
		mustLock(t, lm, 1, 100, LockExclusive) // should return immediately, not deadlock
		close(done)
	}()

	waitFor(t, done, 100*time.Millisecond, "duplicate Lock should return immediately")
	assertHolders(t, lm, 100, []uint64{1}) // txn1 not added twice
}

// --- UnlockAll tests ---

func TestUnlockAllNoLocks(t *testing.T) {
	lm := NewLockManager()
	lm.UnlockAll(999) // must not panic
}

func TestUnlockAllCleansRowAndTransaction(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)
	lm.UnlockAll(1)

	assertRowAbsent(t, lm, 100)
	assertTxnAbsent(t, lm, 1)
}

func TestUnlockAllMultipleRows(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)
	mustLock(t, lm, 1, 200, LockShared)
	mustLock(t, lm, 1, 300, LockExclusive)
	lm.UnlockAll(1)

	assertRowAbsent(t, lm, 100)
	assertRowAbsent(t, lm, 200)
	assertRowAbsent(t, lm, 300)
	assertTxnAbsent(t, lm, 1)
}

func TestUnlockAllTwiceIsNoop(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)
	lm.UnlockAll(1)
	lm.UnlockAll(1) // second call must not panic
}

// --- blocking: exclusive vs exclusive ---

func TestExclusiveBlocksExclusive(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	txn2Done := make(chan struct{})
	go func() {
		mustLock(t, lm, 2, 100, LockExclusive)
		close(txn2Done)
	}()

	notDone(t, txn2Done, 50*time.Millisecond, "txn2 exclusive should block")
	assertQueueLen(t, lm, 100, 1)

	lm.UnlockAll(1)
	waitFor(t, txn2Done, 100*time.Millisecond, "txn2 should be granted after txn1 releases")

	assertLockType(t, lm, 100, LockExclusive)
	assertHolders(t, lm, 100, []uint64{2})
	assertTxnHolds(t, lm, 2, 100, LockExclusive)
	assertTxnAbsent(t, lm, 1)
}

// --- blocking: shared vs exclusive ---

func TestSharedBlocksExclusive(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)

	txn2Done := make(chan struct{})
	go func() {
		mustLock(t, lm, 2, 100, LockExclusive)
		close(txn2Done)
	}()

	notDone(t, txn2Done, 50*time.Millisecond, "exclusive should block while shared is held")

	lm.UnlockAll(1)
	waitFor(t, txn2Done, 100*time.Millisecond, "exclusive should be granted after shared released")

	assertLockType(t, lm, 100, LockExclusive)
	assertHolders(t, lm, 100, []uint64{2})
}

func TestExclusiveBlocksShared(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	txn2Done := make(chan struct{})
	go func() {
		mustLock(t, lm, 2, 100, LockShared)
		close(txn2Done)
	}()

	notDone(t, txn2Done, 50*time.Millisecond, "shared should block while exclusive is held")

	lm.UnlockAll(1)
	waitFor(t, txn2Done, 100*time.Millisecond, "shared should be granted after exclusive released")

	assertLockType(t, lm, 100, LockShared)
	assertHolders(t, lm, 100, []uint64{2})
}

// --- starvation prevention ---

func TestExclusiveWaiterBlocksNewShared(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)

	txn2Done := make(chan struct{})
	go func() {
		mustLock(t, lm, 2, 100, LockExclusive) // queues as exclusive waiter
		close(txn2Done)
	}()
	time.Sleep(30 * time.Millisecond) // let txn2 queue up

	txn3Done := make(chan struct{})
	go func() {
		mustLock(t, lm, 3, 100, LockShared) // must queue behind txn2, not bypass
		close(txn3Done)
	}()
	time.Sleep(30 * time.Millisecond) // let txn3 queue up

	lm.UnlockAll(1)

	waitFor(t, txn2Done, 100*time.Millisecond, "exclusive waiter should be granted")
	notDone(t, txn3Done, 30*time.Millisecond, "shared txn3 should wait behind exclusive txn2")

	lm.UnlockAll(2)
	waitFor(t, txn3Done, 100*time.Millisecond, "shared txn3 should be granted after exclusive released")
}

// --- queue processing: multiple shared waiters granted together ---

func TestMultipleSharedWaitersGrantedTogether(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	const n = 4
	var wg sync.WaitGroup
	for i := uint64(2); i <= n+1; i++ {
		wg.Add(1)
		go func(id uint64) {
			mustLock(t, lm, id, 100, LockShared)
			wg.Done()
		}(i)
	}
	time.Sleep(30 * time.Millisecond) // let all waiters queue

	lm.UnlockAll(1)

	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	waitFor(t, allDone, 200*time.Millisecond, "all shared waiters should be granted together")

	assertLockType(t, lm, 100, LockShared)
	assertHolders(t, lm, 100, []uint64{2, 3, 4, 5})
}

// --- queue processing: shared waiters stop at exclusive ---

func TestSharedWaitersStopAtExclusive(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	txn2Done := make(chan struct{})
	go func() { mustLock(t, lm, 2, 100, LockShared); close(txn2Done) }()
	time.Sleep(20 * time.Millisecond)

	txn3Done := make(chan struct{})
	go func() { mustLock(t, lm, 3, 100, LockExclusive); close(txn3Done) }()
	time.Sleep(20 * time.Millisecond)

	txn4Done := make(chan struct{})
	go func() { mustLock(t, lm, 4, 100, LockShared); close(txn4Done) }()
	time.Sleep(20 * time.Millisecond)

	// release txn1: txn2 (shared) granted, txn3 (exclusive) and txn4 (shared) still wait
	lm.UnlockAll(1)
	waitFor(t, txn2Done, 100*time.Millisecond, "txn2 shared should be granted")
	notDone(t, txn3Done, 30*time.Millisecond, "txn3 exclusive should wait for txn2")
	notDone(t, txn4Done, 30*time.Millisecond, "txn4 shared should wait behind txn3 exclusive")

	// release txn2: txn3 (exclusive) granted, txn4 still waits
	lm.UnlockAll(2)
	waitFor(t, txn3Done, 100*time.Millisecond, "txn3 exclusive should be granted")
	notDone(t, txn4Done, 30*time.Millisecond, "txn4 should wait for exclusive txn3")

	// release txn3: txn4 (shared) granted
	lm.UnlockAll(3)
	waitFor(t, txn4Done, 100*time.Millisecond, "txn4 shared should be granted")
}

// --- partial shared release ---

func TestSharedPartialReleaseDoesNotGrantExclusive(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)
	mustLock(t, lm, 2, 100, LockShared)

	txn3Done := make(chan struct{})
	go func() {
		mustLock(t, lm, 3, 100, LockExclusive)
		close(txn3Done)
	}()
	time.Sleep(30 * time.Millisecond)

	lm.UnlockAll(1) // only one of two shared holders releases

	notDone(t, txn3Done, 50*time.Millisecond, "exclusive should still wait while txn2 holds shared")
	assertHolders(t, lm, 100, []uint64{2})

	lm.UnlockAll(2)
	waitFor(t, txn3Done, 100*time.Millisecond, "exclusive should be granted after all shared released")
}

// --- multiple exclusive waiters serialised ---

func TestMultipleExclusiveWaitersSerialized(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	txn2Done := make(chan struct{})
	txn3Done := make(chan struct{})
	go func() { mustLock(t, lm, 2, 100, LockExclusive); close(txn2Done) }()
	time.Sleep(20 * time.Millisecond)
	go func() { mustLock(t, lm, 3, 100, LockExclusive); close(txn3Done) }()
	time.Sleep(20 * time.Millisecond)

	lm.UnlockAll(1)
	waitFor(t, txn2Done, 100*time.Millisecond, "txn2 should be granted")
	notDone(t, txn3Done, 30*time.Millisecond, "txn3 should still wait while txn2 holds")

	lm.UnlockAll(2)
	waitFor(t, txn3Done, 100*time.Millisecond, "txn3 should be granted after txn2 releases")
}

// --- concurrent stress ---

func TestConcurrentSharedLockStress(t *testing.T) {
	lm := NewLockManager()
	const goroutines = 50
	var wg sync.WaitGroup
	for i := uint64(1); i <= goroutines; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			mustLock(t, lm, id, 100, LockShared)
		}(i)
	}
	wg.Wait()

	assertLockType(t, lm, 100, LockShared)
	lm.mu.Lock()
	if got := len(lm.rowLocks[100].holders); got != goroutines {
		t.Errorf("holders count = %d, want %d", got, goroutines)
	}
	lm.mu.Unlock()
}

func TestConcurrentExclusiveLockStress(t *testing.T) {
	lm := NewLockManager()
	const goroutines = 20
	counter := 0
	var wg sync.WaitGroup

	for i := uint64(1); i <= goroutines; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			mustLock(t, lm, id, 100, LockExclusive)
			counter++ // only one goroutine should be in here at a time
			lm.UnlockAll(id)
		}(i)
	}
	wg.Wait()

	if counter != goroutines {
		t.Errorf("counter = %d, want %d (some locks may have been skipped)", counter, goroutines)
	}
	assertRowAbsent(t, lm, 100)
}

// --- graph helpers ---

func assertEdge(t *testing.T, graph WaitForGraph, from, to uint64) {
	t.Helper()
	for _, n := range graph[from] {
		if n == to {
			return
		}
	}
	t.Errorf("expected edge %d→%d in graph %v", from, to, graph)
}

func assertNoEdge(t *testing.T, graph WaitForGraph, from, to uint64) {
	t.Helper()
	for _, n := range graph[from] {
		if n == to {
			t.Errorf("unexpected edge %d→%d in graph %v", from, to, graph)
			return
		}
	}
}

// --- buildWaitForGraph tests ---

func TestBuildWaitForGraphEmpty(t *testing.T) {
	graph := buildWaitForGraph(map[uint64]*RowLock{})
	if len(graph) != 0 {
		t.Errorf("expected empty graph, got %v", graph)
	}
}

func TestBuildWaitForGraphSingleWaiterSingleHolder(t *testing.T) {
	rowLocks := map[uint64]*RowLock{
		100: {
			lockType: LockExclusive,
			holders:  []uint64{1},
			waitQueue: []WaitRequest{
				{txnId: 2, lockType: LockExclusive, wake: make(chan struct{}, 1)},
			},
		},
	}
	graph := buildWaitForGraph(rowLocks)
	assertEdge(t, graph, 2, 1)
	if len(graph) != 1 {
		t.Errorf("expected 1 graph entry, got %d", len(graph))
	}
}

func TestBuildWaitForGraphMultipleWaiters(t *testing.T) {
	rowLocks := map[uint64]*RowLock{
		100: {
			lockType: LockExclusive,
			holders:  []uint64{1},
			waitQueue: []WaitRequest{
				{txnId: 2, lockType: LockExclusive, wake: make(chan struct{}, 1)},
				{txnId: 3, lockType: LockExclusive, wake: make(chan struct{}, 1)},
			},
		},
	}
	graph := buildWaitForGraph(rowLocks)
	assertEdge(t, graph, 2, 1)
	assertEdge(t, graph, 3, 1)
}

func TestBuildWaitForGraphWaiterMultipleHolders(t *testing.T) {
	// shared lock held by txn1 and txn2, txn3 waiting for exclusive
	rowLocks := map[uint64]*RowLock{
		100: {
			lockType: LockShared,
			holders:  []uint64{1, 2},
			waitQueue: []WaitRequest{
				{txnId: 3, lockType: LockExclusive, wake: make(chan struct{}, 1)},
			},
		},
	}
	graph := buildWaitForGraph(rowLocks)
	assertEdge(t, graph, 3, 1)
	assertEdge(t, graph, 3, 2)
}

func TestBuildWaitForGraphNoWaiters(t *testing.T) {
	// holders only — no edges should appear
	rowLocks := map[uint64]*RowLock{
		100: {
			lockType:  LockExclusive,
			holders:   []uint64{1},
			waitQueue: []WaitRequest{},
		},
	}
	graph := buildWaitForGraph(rowLocks)
	if len(graph) != 0 {
		t.Errorf("expected no edges for row with no waiters, got %v", graph)
	}
}

func TestBuildWaitForGraphMultipleRows(t *testing.T) {
	// row100: txn1 holds, txn2 waits
	// row200: txn2 holds, txn1 waits → classic deadlock shape
	rowLocks := map[uint64]*RowLock{
		100: {
			lockType: LockExclusive,
			holders:  []uint64{1},
			waitQueue: []WaitRequest{
				{txnId: 2, lockType: LockExclusive, wake: make(chan struct{}, 1)},
			},
		},
		200: {
			lockType: LockExclusive,
			holders:  []uint64{2},
			waitQueue: []WaitRequest{
				{txnId: 1, lockType: LockExclusive, wake: make(chan struct{}, 1)},
			},
		},
	}
	graph := buildWaitForGraph(rowLocks)
	assertEdge(t, graph, 2, 1) // txn2 waits for txn1
	assertEdge(t, graph, 1, 2) // txn1 waits for txn2
	assertNoEdge(t, graph, 1, 1)
	assertNoEdge(t, graph, 2, 2)
}

// --- detectCycle / dfs tests ---

func TestDetectCycleEmptyGraph(t *testing.T) {
	graph := WaitForGraph{}
	if detectCycle(graph, 1) {
		t.Error("empty graph should have no cycle")
	}
}

func TestDetectCycleStartNotInGraph(t *testing.T) {
	graph := WaitForGraph{1: {2}, 2: {1}}
	if detectCycle(graph, 99) {
		t.Error("node with no edges should have no cycle")
	}
}

func TestDetectCycleNoCycleLinearChain(t *testing.T) {
	// 1→2→3→4
	graph := WaitForGraph{1: {2}, 2: {3}, 3: {4}}
	if detectCycle(graph, 1) {
		t.Error("linear chain should have no cycle")
	}
}

func TestDetectCycleTwoNode(t *testing.T) {
	// 1→2→1
	graph := WaitForGraph{1: {2}, 2: {1}}
	if !detectCycle(graph, 1) {
		t.Error("expected two-node cycle to be detected")
	}
}

func TestDetectCycleThreeNode(t *testing.T) {
	// 1→2→3→1
	graph := WaitForGraph{1: {2}, 2: {3}, 3: {1}}
	if !detectCycle(graph, 1) {
		t.Error("expected three-node cycle to be detected")
	}
}

func TestDetectCycleSelfLoop(t *testing.T) {
	graph := WaitForGraph{1: {1}}
	if !detectCycle(graph, 1) {
		t.Error("expected self-loop to be detected as cycle")
	}
}

func TestDetectCycleDiamondNoCycle(t *testing.T) {
	// 1→2, 1→3, 2→4, 3→4 (diamond — no cycle)
	graph := WaitForGraph{1: {2, 3}, 2: {4}, 3: {4}}
	if detectCycle(graph, 1) {
		t.Error("diamond shape should have no cycle")
	}
}

func TestDetectCycleCycleNotReachableFromStart(t *testing.T) {
	// 1→2 (no cycle), 3→4→3 (cycle, unreachable from 1)
	graph := WaitForGraph{1: {2}, 3: {4}, 4: {3}}
	if detectCycle(graph, 1) {
		t.Error("cycle unreachable from start node should not be detected")
	}
}

func TestDetectCycleLongCycle(t *testing.T) {
	// 1→2→3→4→5→1
	graph := WaitForGraph{1: {2}, 2: {3}, 3: {4}, 4: {5}, 5: {1}}
	if !detectCycle(graph, 1) {
		t.Error("expected long cycle to be detected")
	}
}

func TestDetectCycleMultiplePaths(t *testing.T) {
	// 1→2, 1→3, 2→4, 3→4, 4→1 (cycle via both paths)
	graph := WaitForGraph{1: {2, 3}, 2: {4}, 3: {4}, 4: {1}}
	if !detectCycle(graph, 1) {
		t.Error("expected cycle to be detected via multiple paths")
	}
}

// --- Lock deadlock integration tests ---

func TestLockDeadlockTwoTransactions(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive) // txn1 holds row100
	mustLock(t, lm, 2, 200, LockExclusive) // txn2 holds row200

	// txn1 waits for row200 (held by txn2)
	txn1Done := make(chan error, 1)
	go func() {
		txn1Done <- lm.Lock(1, 200, LockExclusive)
	}()
	time.Sleep(30 * time.Millisecond) // let txn1 queue

	// txn2 tries row100 → cycle: txn2→txn1→txn2
	err := lm.Lock(2, 100, LockExclusive)
	if err == nil {
		t.Fatal("expected deadlock error for txn2")
	}
	if !strings.Contains(err.Error(), "deadlock") {
		t.Errorf("error message should mention deadlock, got: %v", err)
	}

	// txn2's request was rejected — unlock txn2, txn1 should proceed
	lm.UnlockAll(2)
	select {
	case lockErr := <-txn1Done:
		if lockErr != nil {
			t.Errorf("txn1 should have acquired lock, got error: %v", lockErr)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("txn1 should have been granted lock after txn2 released")
	}
}

func TestLockDeadlockThreeTransactions(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive) // txn1 holds row100
	mustLock(t, lm, 2, 200, LockExclusive) // txn2 holds row200
	mustLock(t, lm, 3, 300, LockExclusive) // txn3 holds row300

	// txn1 waits for row200 (held by txn2)
	go func() { _ = lm.Lock(1, 200, LockExclusive) }()
	time.Sleep(20 * time.Millisecond)

	// txn2 waits for row300 (held by txn3)
	go func() { _ = lm.Lock(2, 300, LockExclusive) }()
	time.Sleep(20 * time.Millisecond)

	// txn3 tries row100 → cycle: txn3→txn1→txn2→txn3
	err := lm.Lock(3, 100, LockExclusive)
	if err == nil {
		t.Fatal("expected deadlock error for three-way cycle")
	}
}

func TestLockNoDeadlockOneWayWait(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)

	// txn2 waits for txn1 — no cycle, txn1 is not waiting for anything
	errCh := make(chan error, 1)
	go func() {
		errCh <- lm.Lock(2, 100, LockExclusive)
	}()
	time.Sleep(30 * time.Millisecond)

	lm.UnlockAll(1)

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("expected no deadlock error, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("txn2 should have been granted lock")
	}
}

func TestLockDeadlockQueueCleanup(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)
	mustLock(t, lm, 2, 200, LockExclusive)

	// txn1 waits for row200
	go func() { _ = lm.Lock(1, 200, LockExclusive) }()
	time.Sleep(30 * time.Millisecond)

	// txn2 tries row100 → deadlock → rejected
	if err := lm.Lock(2, 100, LockExclusive); err == nil {
		t.Fatal("expected deadlock error")
	}

	// txn2's request must have been removed from row100's queue
	assertQueueLen(t, lm, 100, 0)
	// txn1 is still waiting in row200's queue
	assertQueueLen(t, lm, 200, 1)
}

// --- lock upgrade: shared → exclusive ---

func TestLockUpgradeSharedToExclusiveOnlyHolder(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)

	// Upgrade: txn1 is the only shared holder, should be granted exclusive immediately.
	done := make(chan struct{})
	go func() {
		mustLock(t, lm, 1, 100, LockExclusive)
		close(done)
	}()

	waitFor(t, done, 100*time.Millisecond, "upgrade should be granted immediately when sole holder")
	assertLockType(t, lm, 100, LockExclusive)
	assertHolders(t, lm, 100, []uint64{1})
	assertTxnHolds(t, lm, 1, 100, LockExclusive)
}

func TestLockUpgradeSharedToExclusiveWithOtherHolders(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockShared)
	mustLock(t, lm, 2, 100, LockShared)

	// txn1 requests upgrade; must block while txn2 still holds shared.
	upgradeDone := make(chan struct{})
	go func() {
		mustLock(t, lm, 1, 100, LockExclusive)
		close(upgradeDone)
	}()

	notDone(t, upgradeDone, 50*time.Millisecond, "upgrade should block while another txn holds shared")

	// After txn2 releases, txn1's upgrade should be granted.
	lm.UnlockAll(2)
	waitFor(t, upgradeDone, 100*time.Millisecond, "upgrade should be granted after other shared holder releases")
	assertLockType(t, lm, 100, LockExclusive)
	assertHolders(t, lm, 100, []uint64{1})
	assertTxnHolds(t, lm, 1, 100, LockExclusive)
}

func TestLockDeadlockOtherWaitersUnaffected(t *testing.T) {
	lm := NewLockManager()
	mustLock(t, lm, 1, 100, LockExclusive)
	mustLock(t, lm, 2, 200, LockExclusive)

	// txn3 queues for row100 (no deadlock — txn3 holds nothing)
	txn3Done := make(chan error, 1)
	go func() { txn3Done <- lm.Lock(3, 100, LockExclusive) }()
	time.Sleep(20 * time.Millisecond)

	// txn1 waits for row200 (held by txn2)
	go func() { _ = lm.Lock(1, 200, LockExclusive) }()
	time.Sleep(20 * time.Millisecond)

	// txn2 tries row100 → deadlock → rejected
	if err := lm.Lock(2, 100, LockExclusive); err == nil {
		t.Fatal("expected deadlock error")
	}

	// txn3 is still in queue and should be granted when txn1 releases
	lm.UnlockAll(1)
	select {
	case err := <-txn3Done:
		if err != nil {
			t.Errorf("txn3 should have acquired lock cleanly, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("txn3 should have been granted lock after txn1 released")
	}
}
