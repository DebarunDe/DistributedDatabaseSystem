package lock

import (
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
