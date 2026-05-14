package lock

import (
	"testing"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

// ---- helpers ----

func newTMBTree(t *testing.T) *btree.BTree {
	t.Helper()
	pm, err := pagemanager.NewDB(t.TempDir() + "/tm.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	return btree.NewBTree(pm)
}

// newTestTM returns a TransactionManager and the BTree it owns.
func newTestTM(t *testing.T) (*TransactionManager, *btree.BTree) {
	t.Helper()
	bt := newTMBTree(t)
	return NewTransactionManager(bt), bt
}

// intFields builds a single-field slice with an INT value, used as a minimal row.
func intFields(v int64) []btree.Field {
	return []btree.Field{{Tag: 0, Value: btree.IntValue{V: v}}}
}

func assertBTreeKeyValue(t *testing.T, bt *btree.BTree, key uint64, want int64) {
	t.Helper()
	fields, ok, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if !ok {
		t.Errorf("key %d: expected to exist, not found", key)
		return
	}
	iv, ok2 := fields[0].Value.(btree.IntValue)
	if !ok2 {
		t.Fatalf("key %d: expected IntValue, got %T", key, fields[0].Value)
	}
	if iv.V != want {
		t.Errorf("key %d: value = %d, want %d", key, iv.V, want)
	}
}

func assertBTreeKeyAbsent(t *testing.T, bt *btree.BTree, key uint64) {
	t.Helper()
	_, ok, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if ok {
		t.Errorf("key %d: expected to be absent, found", key)
	}
}

func assertInActive(t *testing.T, tm *TransactionManager, txnId uint64) {
	t.Helper()
	if _, ok := tm.active[txnId]; !ok {
		t.Errorf("txn %d: expected in active map", txnId)
	}
}

func assertNotInActive(t *testing.T, tm *TransactionManager, txnId uint64) {
	t.Helper()
	if _, ok := tm.active[txnId]; ok {
		t.Errorf("txn %d: expected not in active map", txnId)
	}
}

// ---- Begin ----

func TestBegin_FirstIdIsOne(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if txn.Id != 1 {
		t.Errorf("first txn ID: got %d, want 1", txn.Id)
	}
}

func TestBegin_IdsIncrementMonotonically(t *testing.T) {
	tm, _ := newTestTM(t)
	ids := make([]uint64, 5)
	for i := range ids {
		ids[i] = tm.Begin().Id
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			t.Errorf("ID not incremented at index %d: %d → %d", i, ids[i-1], ids[i])
		}
	}
}

func TestBegin_RegisteredInActiveMap(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	assertInActive(t, tm, txn.Id)
}

func TestBegin_ActiveMapEntryMatchesReturnedPointer(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if tm.active[txn.Id] != txn {
		t.Error("active[txn.Id] should be the same *Transaction pointer returned by Begin")
	}
}

func TestBegin_StatusIsActive(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if txn.Status != TxnActive {
		t.Errorf("status: got %v, want TxnActive", txn.Status)
	}
}

func TestBegin_UndoLogInitialized(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if txn.UndoLog == nil {
		t.Error("UndoLog should be non-nil after Begin")
	}
	if len(txn.UndoLog) != 0 {
		t.Errorf("UndoLog should be empty, got len=%d", len(txn.UndoLog))
	}
}

func TestBegin_MultipleTransactionsHaveUniqueIds(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()
	t3 := tm.Begin()
	assertInActive(t, tm, t1.Id)
	assertInActive(t, tm, t2.Id)
	assertInActive(t, tm, t3.Id)
	if t1.Id == t2.Id || t2.Id == t3.Id || t1.Id == t3.Id {
		t.Errorf("duplicate IDs: %d, %d, %d", t1.Id, t2.Id, t3.Id)
	}
}

// ---- Lock delegation ----

func TestTMLock_ExclusiveGranted(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if err := tm.Lock(txn.Id, 100, LockExclusive); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTMLock_SharedGranted(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if err := tm.Lock(txn.Id, 100, LockShared); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTMLock_MultipleSharedOnSameRow(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()
	if err := tm.Lock(t1.Id, 100, LockShared); err != nil {
		t.Errorf("t1 shared: %v", err)
	}
	if err := tm.Lock(t2.Id, 100, LockShared); err != nil {
		t.Errorf("t2 shared: %v", err)
	}
}

func TestTMLock_DeadlockReturnsError(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	if err := tm.Lock(t1.Id, 100, LockExclusive); err != nil {
		t.Fatalf("t1 lock row 100: %v", err)
	}
	if err := tm.Lock(t2.Id, 200, LockExclusive); err != nil {
		t.Fatalf("t2 lock row 200: %v", err)
	}

	// t1 waits for row 200 (held by t2) — creates t1→t2 edge in wait-for graph
	go func() { _ = tm.Lock(t1.Id, 200, LockExclusive) }()
	time.Sleep(30 * time.Millisecond)

	// t2 tries row 100 (held by t1 which waits for t2) → cycle
	err := tm.Lock(t2.Id, 100, LockExclusive)
	if err == nil {
		t.Error("expected deadlock error, got nil")
	}
}

// ---- AppendUndo ----

func TestAppendUndo_SingleEntry(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: 100})
	if len(txn.UndoLog) != 1 {
		t.Fatalf("UndoLog len: got %d, want 1", len(txn.UndoLog))
	}
	e := txn.UndoLog[0]
	if e.Op != UndoInsert || e.Key != 100 {
		t.Errorf("entry: got {Op:%v Key:%d}, want {UndoInsert 100}", e.Op, e.Key)
	}
}

func TestAppendUndo_PreservesInsertionOrder(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: 1})
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoDelete, Key: 2, Fields: intFields(5)})
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoUpdate, Key: 3, Fields: intFields(9)})

	if len(txn.UndoLog) != 3 {
		t.Fatalf("UndoLog len: got %d, want 3", len(txn.UndoLog))
	}
	if txn.UndoLog[0].Op != UndoInsert || txn.UndoLog[0].Key != 1 {
		t.Errorf("entry[0]: got %+v", txn.UndoLog[0])
	}
	if txn.UndoLog[1].Op != UndoDelete || txn.UndoLog[1].Key != 2 {
		t.Errorf("entry[1]: got %+v", txn.UndoLog[1])
	}
	if txn.UndoLog[2].Op != UndoUpdate || txn.UndoLog[2].Key != 3 {
		t.Errorf("entry[2]: got %+v", txn.UndoLog[2])
	}
}

func TestAppendUndo_UnknownTxnIdIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	tm.AppendUndo(999, UndoEntry{Op: UndoInsert, Key: 1}) // must not panic
}

func TestAppendUndo_DoesNotCrossContaminateTxns(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	tm.AppendUndo(t1.Id, UndoEntry{Op: UndoInsert, Key: 100})
	tm.AppendUndo(t2.Id, UndoEntry{Op: UndoInsert, Key: 200})

	if len(tm.active[t1.Id].UndoLog) != 1 || tm.active[t1.Id].UndoLog[0].Key != 100 {
		t.Error("t1 undo log contaminated by t2's entry")
	}
	if len(tm.active[t2.Id].UndoLog) != 1 || tm.active[t2.Id].UndoLog[0].Key != 200 {
		t.Error("t2 undo log contaminated by t1's entry")
	}
}

// ---- Commit ----

func TestCommit_RemovesFromActiveMap(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Commit(txn.Id)
	assertNotInActive(t, tm, txn.Id)
}

func TestCommit_SetsStatusCommitted(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Commit(txn.Id)
	if txn.Status != TxnCommitted {
		t.Errorf("status: got %v, want TxnCommitted", txn.Status)
	}
}

func TestCommit_ClearsUndoLog(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: 100})
	tm.Commit(txn.Id)
	if txn.UndoLog != nil {
		t.Error("UndoLog should be nil after commit")
	}
}

func TestCommit_ReleasesLocks(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	if err := tm.Lock(t1.Id, 100, LockExclusive); err != nil {
		t.Fatalf("t1 lock: %v", err)
	}
	t2Locked := make(chan struct{})
	go func() {
		_ = tm.Lock(t2.Id, 100, LockExclusive)
		close(t2Locked)
	}()
	time.Sleep(30 * time.Millisecond)

	tm.Commit(t1.Id)
	waitFor(t, t2Locked, 100*time.Millisecond, "t2 should acquire lock after t1 commits")
}

func TestCommit_UnknownTxnIdIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	tm.Commit(999) // must not panic
}

func TestCommit_DoubleCommitIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Commit(txn.Id)
	tm.Commit(txn.Id) // must not panic
}

func TestCommit_DoesNotAffectOtherTransactions(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()
	tm.Commit(t1.Id)
	assertNotInActive(t, tm, t1.Id)
	assertInActive(t, tm, t2.Id)
}

// ---- Rollback ----

func TestRollback_UndoInsert_DeletesRow(t *testing.T) {
	tm, bt := newTestTM(t)
	txn := tm.Begin()

	if err := bt.Insert(100, intFields(42)); err != nil {
		t.Fatalf("bt.Insert: %v", err)
	}
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: 100})

	tm.Rollback(txn.Id)

	assertBTreeKeyAbsent(t, bt, 100)
}

func TestRollback_UndoDelete_ReInsertsRow(t *testing.T) {
	tm, bt := newTestTM(t)
	txn := tm.Begin()

	// The row was deleted during the txn; undo log holds the old fields.
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoDelete, Key: 100, Fields: intFields(42)})

	tm.Rollback(txn.Id)

	assertBTreeKeyValue(t, bt, 100, 42)
}

func TestRollback_UndoUpdate_RestoresOldFields(t *testing.T) {
	tm, bt := newTestTM(t)
	txn := tm.Begin()

	// Row 100 was updated to 99; undo log holds original value 42.
	if err := bt.Insert(100, intFields(99)); err != nil {
		t.Fatalf("bt.Insert: %v", err)
	}
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoUpdate, Key: 100, Fields: intFields(42)})

	tm.Rollback(txn.Id)

	assertBTreeKeyValue(t, bt, 100, 42)
}

func TestRollback_ReversesMultipleInserts(t *testing.T) {
	tm, bt := newTestTM(t)
	txn := tm.Begin()

	for _, key := range []uint64{100, 200, 300} {
		if err := bt.Insert(key, intFields(int64(key))); err != nil {
			t.Fatalf("bt.Insert(%d): %v", key, err)
		}
		tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: key})
	}

	tm.Rollback(txn.Id)

	assertBTreeKeyAbsent(t, bt, 100)
	assertBTreeKeyAbsent(t, bt, 200)
	assertBTreeKeyAbsent(t, bt, 300)
}

func TestRollback_LIFOOrder_InsertThenUpdate(t *testing.T) {
	// Sequence: INSERT row 100 (value 1), UPDATE row 100 (value 1 → 99).
	// LIFO rollback: undo UPDATE first (restore 1), then undo INSERT (delete).
	// Expected final state: row 100 absent.
	tm, bt := newTestTM(t)
	txn := tm.Begin()

	if err := bt.Insert(100, intFields(1)); err != nil {
		t.Fatalf("bt.Insert: %v", err)
	}
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: 100})

	if err := bt.Insert(100, intFields(99)); err != nil { // btree insert overwrites — simulates UPDATE
		t.Fatalf("bt.Insert: %v", err)
	}
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoUpdate, Key: 100, Fields: intFields(1)})

	tm.Rollback(txn.Id)

	assertBTreeKeyAbsent(t, bt, 100)
}

func TestRollback_LIFOOrder_DeleteThenReInsert(t *testing.T) {
	// Sequence: DELETE row 100 (old=42), INSERT row 100 (value 99).
	// LIFO rollback: undo INSERT (delete 100), then undo DELETE (re-insert 42).
	// Expected final state: row 100 exists with value 42.
	tm, bt := newTestTM(t)
	txn := tm.Begin()

	// Undo log entry for "DELETE" comes first in log order
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoDelete, Key: 100, Fields: intFields(42)})

	// Then an INSERT of key 100 with a new value
	if err := bt.Insert(100, intFields(99)); err != nil {
		t.Fatalf("bt.Insert: %v", err)
	}
	tm.AppendUndo(txn.Id, UndoEntry{Op: UndoInsert, Key: 100})

	tm.Rollback(txn.Id)

	assertBTreeKeyValue(t, bt, 100, 42)
}

func TestRollback_RemovesFromActiveMap(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Rollback(txn.Id)
	assertNotInActive(t, tm, txn.Id)
}

func TestRollback_SetsStatusAborted(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Rollback(txn.Id)
	if txn.Status != TxnAborted {
		t.Errorf("status: got %v, want TxnAborted", txn.Status)
	}
}

func TestRollback_ReleasesLocks(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	if err := tm.Lock(t1.Id, 100, LockExclusive); err != nil {
		t.Fatalf("t1 lock: %v", err)
	}
	t2Locked := make(chan struct{})
	go func() {
		_ = tm.Lock(t2.Id, 100, LockExclusive)
		close(t2Locked)
	}()
	time.Sleep(30 * time.Millisecond)

	tm.Rollback(t1.Id)
	waitFor(t, t2Locked, 100*time.Millisecond, "t2 should acquire lock after t1 rolls back")
}

func TestRollback_EmptyUndoLogIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Rollback(txn.Id) // no undo entries — just releases locks
	assertNotInActive(t, tm, txn.Id)
	if txn.Status != TxnAborted {
		t.Errorf("status: got %v, want TxnAborted", txn.Status)
	}
}

func TestRollback_UnknownTxnIdIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	tm.Rollback(999) // must not panic
}

func TestRollback_DoubleRollbackIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Rollback(txn.Id)
	tm.Rollback(txn.Id) // must not panic
}

func TestRollback_DoesNotAffectOtherTransactions(t *testing.T) {
	tm, bt := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	if err := bt.Insert(100, intFields(1)); err != nil {
		t.Fatalf("bt.Insert(100): %v", err)
	}
	if err := bt.Insert(200, intFields(2)); err != nil {
		t.Fatalf("bt.Insert(200): %v", err)
	}
	tm.AppendUndo(t1.Id, UndoEntry{Op: UndoInsert, Key: 100})
	tm.AppendUndo(t2.Id, UndoEntry{Op: UndoInsert, Key: 200})

	tm.Rollback(t1.Id)

	assertBTreeKeyAbsent(t, bt, 100)   // t1's row rolled back
	assertBTreeKeyValue(t, bt, 200, 2) // t2's row untouched
	assertNotInActive(t, tm, t1.Id)
	assertInActive(t, tm, t2.Id)
}

// ---- Multi-transaction scenarios ----

func TestMultiTxn_CommitOneRollbackOther(t *testing.T) {
	tm, bt := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	if err := bt.Insert(100, intFields(1)); err != nil {
		t.Fatalf("bt.Insert(100): %v", err)
	}
	if err := bt.Insert(200, intFields(2)); err != nil {
		t.Fatalf("bt.Insert(200): %v", err)
	}
	tm.AppendUndo(t1.Id, UndoEntry{Op: UndoInsert, Key: 100})
	tm.AppendUndo(t2.Id, UndoEntry{Op: UndoInsert, Key: 200})

	tm.Commit(t1.Id)   // row 100 persists
	tm.Rollback(t2.Id) // row 200 deleted

	assertBTreeKeyValue(t, bt, 100, 1)
	assertBTreeKeyAbsent(t, bt, 200)
	assertNotInActive(t, tm, t1.Id)
	assertNotInActive(t, tm, t2.Id)
}

func TestMultiTxn_IdsNotReusedAfterCommit(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	tm.Commit(t1.Id)
	t2 := tm.Begin()
	if t2.Id <= t1.Id {
		t.Errorf("new txn ID (%d) should be greater than committed txn ID (%d)", t2.Id, t1.Id)
	}
}

func TestMultiTxn_IdsNotReusedAfterRollback(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	tm.Rollback(t1.Id)
	t2 := tm.Begin()
	if t2.Id <= t1.Id {
		t.Errorf("new txn ID (%d) should be greater than rolled-back txn ID (%d)", t2.Id, t1.Id)
	}
}

func TestMultiTxn_UndoLogsAreIndependent(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	tm.AppendUndo(t1.Id, UndoEntry{Op: UndoInsert, Key: 100})
	tm.AppendUndo(t1.Id, UndoEntry{Op: UndoInsert, Key: 101})
	tm.AppendUndo(t2.Id, UndoEntry{Op: UndoDelete, Key: 200, Fields: intFields(7)})

	if len(tm.active[t1.Id].UndoLog) != 2 {
		t.Errorf("t1 UndoLog len: got %d, want 2", len(tm.active[t1.Id].UndoLog))
	}
	if len(tm.active[t2.Id].UndoLog) != 1 {
		t.Errorf("t2 UndoLog len: got %d, want 1", len(tm.active[t2.Id].UndoLog))
	}
}

func TestMultiTxn_ConcurrentLocksThenCommit(t *testing.T) {
	// Two transactions lock different rows — no blocking, both commit cleanly.
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	if err := tm.Lock(t1.Id, 100, LockExclusive); err != nil {
		t.Fatalf("t1 lock row 100: %v", err)
	}
	if err := tm.Lock(t2.Id, 200, LockExclusive); err != nil {
		t.Fatalf("t2 lock row 200: %v", err)
	}

	tm.Commit(t1.Id)
	tm.Commit(t2.Id)

	assertNotInActive(t, tm, t1.Id)
	assertNotInActive(t, tm, t2.Id)
}

func TestMultiTxn_ThreeTransactionsSerialised(t *testing.T) {
	// t1 holds an exclusive lock; t2 and t3 queue; each gets granted in turn.
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()
	t3 := tm.Begin()

	if err := tm.Lock(t1.Id, 100, LockExclusive); err != nil {
		t.Fatalf("t1: %v", err)
	}

	t2Done := make(chan struct{})
	go func() { _ = tm.Lock(t2.Id, 100, LockExclusive); close(t2Done) }()
	time.Sleep(20 * time.Millisecond)

	t3Done := make(chan struct{})
	go func() { _ = tm.Lock(t3.Id, 100, LockExclusive); close(t3Done) }()
	time.Sleep(20 * time.Millisecond)

	tm.Commit(t1.Id)
	waitFor(t, t2Done, 100*time.Millisecond, "t2 should be granted after t1 commits")
	notDone(t, t3Done, 30*time.Millisecond, "t3 should wait while t2 holds")

	tm.Commit(t2.Id)
	waitFor(t, t3Done, 100*time.Millisecond, "t3 should be granted after t2 commits")

	tm.Commit(t3.Id)
	assertNotInActive(t, tm, t1.Id)
	assertNotInActive(t, tm, t2.Id)
	assertNotInActive(t, tm, t3.Id)
}
