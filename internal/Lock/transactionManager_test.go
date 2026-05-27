package lock

import (
	"testing"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
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

// newTestTM returns a TransactionManager (no replication) and the BTree it owns.
// The applyFn writes committed RedoLog entries directly to the BTree.
func newTestTM(t *testing.T) (*TransactionManager, *btree.BTree) {
	t.Helper()
	bt := newTMBTree(t)
	applyFn := func(op raft.ReplOp, key uint64, fields []btree.Field) error {
		switch op {
		case raft.ReplPut:
			return bt.Insert(key, fields)
		case raft.ReplDelete:
			return bt.Delete(key)
		}
		return nil
	}
	return NewTransactionManager(bt, nil, applyFn), bt
}

func mustCommit(t *testing.T, tm *TransactionManager, txnId uint64) {
	t.Helper()
	if err := tm.Commit(txnId); err != nil {
		t.Fatalf("Commit(%d): %v", txnId, err)
	}
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

// ---- Begin (redo log init) ----

func TestBegin_RedoLogInitialized(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	if txn.RedoLog == nil {
		t.Error("RedoLog should be non-nil after Begin")
	}
	if len(txn.RedoLog) != 0 {
		t.Errorf("RedoLog should be empty, got len=%d", len(txn.RedoLog))
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

// ---- Commit ----

func TestCommit_RemovesFromActiveMap(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	mustCommit(t, tm, txn.Id)
	assertNotInActive(t, tm, txn.Id)
}

func TestCommit_SetsStatusCommitted(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	mustCommit(t, tm, txn.Id)
	if txn.Status != TxnCommitted {
		t.Errorf("status: got %v, want TxnCommitted", txn.Status)
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

	mustCommit(t, tm, t1.Id)
	waitFor(t, t2Locked, 100*time.Millisecond, "t2 should acquire lock after t1 commits")
}

func TestCommit_UnknownTxnIdIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	mustCommit(t, tm, 999) // must not panic
}

func TestCommit_DoubleCommitIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	mustCommit(t, tm, txn.Id)
	mustCommit(t, tm, txn.Id) // must not panic
}

func TestCommit_DoesNotAffectOtherTransactions(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()
	mustCommit(t, tm, t1.Id)
	assertNotInActive(t, tm, t1.Id)
	assertInActive(t, tm, t2.Id)
}

// ---- Rollback ----

// In the deferred-write design the executor buffers all writes in RedoLog and never
// touches the BTree before Commit. Rollback simply discards the RedoLog — there is
// nothing to undo in the BTree. The tests below verify this new semantics.

func TestRollback_PendingRedoInsert_NeverApplied(t *testing.T) {
	// A buffered ReplPut that is rolled back must never reach the BTree.
	tm, bt := newTestTM(t)
	txn := tm.Begin()
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(42)})
	tm.Rollback(txn.Id)
	assertBTreeKeyAbsent(t, bt, 100)
}

func TestRollback_PendingRedoDelete_DoesNotEraseCommittedRow(t *testing.T) {
	// Commit a row, then buffer a delete and roll back — the committed row must survive.
	tm, bt := newTestTM(t)
	txn1 := tm.Begin()
	tm.AppendRedo(txn1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(42)})
	mustCommit(t, tm, txn1.Id)
	assertBTreeKeyValue(t, bt, 100, 42)

	txn2 := tm.Begin()
	tm.AppendRedo(txn2.Id, raft.RaftCommand{Op: raft.ReplDelete, Key: 100})
	tm.Rollback(txn2.Id)
	assertBTreeKeyValue(t, bt, 100, 42)
}

func TestRollback_PendingRedoUpdate_DoesNotModifyCommittedRow(t *testing.T) {
	// Commit a row (value 42), buffer an update to 99 and roll back — the original must remain.
	tm, bt := newTestTM(t)
	txn1 := tm.Begin()
	tm.AppendRedo(txn1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(42)})
	mustCommit(t, tm, txn1.Id)

	txn2 := tm.Begin()
	tm.AppendRedo(txn2.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(99)})
	tm.Rollback(txn2.Id)
	assertBTreeKeyValue(t, bt, 100, 42)
}

func TestRollback_PendingMultipleRedoInserts_NeverApplied(t *testing.T) {
	// Multiple buffered inserts all discarded by a single rollback.
	tm, bt := newTestTM(t)
	txn := tm.Begin()
	for _, key := range []uint64{100, 200, 300} {
		tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: key, Fields: intFields(int64(key))})
	}
	tm.Rollback(txn.Id)
	assertBTreeKeyAbsent(t, bt, 100)
	assertBTreeKeyAbsent(t, bt, 200)
	assertBTreeKeyAbsent(t, bt, 300)
}

func TestRollback_MultiPendingOps_AllDiscarded(t *testing.T) {
	// Commit row 100 (value 1). Buffer an update + a new insert, then roll back.
	// Row 100 must keep its committed value; row 200 must not appear.
	tm, bt := newTestTM(t)
	txn1 := tm.Begin()
	tm.AppendRedo(txn1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(1)})
	mustCommit(t, tm, txn1.Id)

	txn2 := tm.Begin()
	tm.AppendRedo(txn2.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(99)})
	tm.AppendRedo(txn2.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 200, Fields: intFields(200)})
	tm.Rollback(txn2.Id)

	assertBTreeKeyValue(t, bt, 100, 1)
	assertBTreeKeyAbsent(t, bt, 200)
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

func TestRollback_EmptyRedoLogIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.Rollback(txn.Id) // nothing buffered — just releases locks
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

	tm.AppendRedo(t1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(1)})
	tm.AppendRedo(t2.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 200, Fields: intFields(2)})

	tm.Rollback(t1.Id)

	assertBTreeKeyAbsent(t, bt, 100) // t1's pending write never applied
	assertNotInActive(t, tm, t1.Id)
	assertInActive(t, tm, t2.Id)

	// t2 can still commit cleanly
	mustCommit(t, tm, t2.Id)
	assertBTreeKeyValue(t, bt, 200, 2)
	assertNotInActive(t, tm, t2.Id)
}

// ---- Multi-transaction scenarios ----

func TestMultiTxn_CommitOneRollbackOther(t *testing.T) {
	tm, bt := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	tm.AppendRedo(t1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100, Fields: intFields(1)})
	tm.AppendRedo(t2.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 200, Fields: intFields(2)})

	mustCommit(t, tm, t1.Id) // row 100 applied via applyFn
	tm.Rollback(t2.Id)       // row 200 never applied

	assertBTreeKeyValue(t, bt, 100, 1)
	assertBTreeKeyAbsent(t, bt, 200)
	assertNotInActive(t, tm, t1.Id)
	assertNotInActive(t, tm, t2.Id)
}

func TestMultiTxn_IdsNotReusedAfterCommit(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	mustCommit(t, tm, t1.Id)
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

	mustCommit(t, tm, t1.Id)
	mustCommit(t, tm, t2.Id)

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

	mustCommit(t, tm, t1.Id)
	waitFor(t, t2Done, 100*time.Millisecond, "t2 should be granted after t1 commits")
	notDone(t, t3Done, 30*time.Millisecond, "t3 should wait while t2 holds")

	mustCommit(t, tm, t2.Id)
	waitFor(t, t3Done, 100*time.Millisecond, "t3 should be granted after t2 commits")

	mustCommit(t, tm, t3.Id)
	assertNotInActive(t, tm, t1.Id)
	assertNotInActive(t, tm, t2.Id)
	assertNotInActive(t, tm, t3.Id)
}

// ---- AppendRedo ----

func TestAppendRedo_SingleEntry(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100})
	if len(txn.RedoLog) != 1 {
		t.Fatalf("RedoLog len: got %d, want 1", len(txn.RedoLog))
	}
	e := txn.RedoLog[0]
	if e.Op != raft.ReplPut || e.Key != 100 {
		t.Errorf("entry: got {Op:%v Key:%d}, want {ReplPut 100}", e.Op, e.Key)
	}
}

func TestAppendRedo_PreservesInsertionOrder(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 1})
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplDelete, Key: 2})
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 3})

	if len(txn.RedoLog) != 3 {
		t.Fatalf("RedoLog len: got %d, want 3", len(txn.RedoLog))
	}
	if txn.RedoLog[0].Key != 1 || txn.RedoLog[1].Key != 2 || txn.RedoLog[2].Key != 3 {
		t.Errorf("insertion order not preserved: keys %d %d %d", txn.RedoLog[0].Key, txn.RedoLog[1].Key, txn.RedoLog[2].Key)
	}
}

func TestAppendRedo_UnknownTxnIdIsNoop(t *testing.T) {
	tm, _ := newTestTM(t)
	tm.AppendRedo(999, raft.RaftCommand{Op: raft.ReplPut, Key: 1}) // must not panic
}

func TestAppendRedo_DoesNotCrossContaminateTxns(t *testing.T) {
	tm, _ := newTestTM(t)
	t1 := tm.Begin()
	t2 := tm.Begin()

	tm.AppendRedo(t1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 100})
	tm.AppendRedo(t2.Id, raft.RaftCommand{Op: raft.ReplDelete, Key: 200})

	if len(tm.active[t1.Id].RedoLog) != 1 || tm.active[t1.Id].RedoLog[0].Key != 100 {
		t.Error("t1 redo log contaminated by t2's entry")
	}
	if len(tm.active[t2.Id].RedoLog) != 1 || tm.active[t2.Id].RedoLog[0].Key != 200 {
		t.Error("t2 redo log contaminated by t1's entry")
	}
}

// ---- Commit + raft ----

func TestCommit_ClearsRedoLog(t *testing.T) {
	tm, _ := newTestTM(t)
	txn := tm.Begin()
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 1})
	mustCommit(t, tm, txn.Id)
	if txn.RedoLog != nil {
		t.Error("RedoLog should be nil after commit")
	}
}

func TestCommit_NilRaftNode_DoesNotPanic(t *testing.T) {
	tm, _ := newTestTM(t) // rn=nil
	txn := tm.Begin()
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 1, Fields: intFields(42)})
	mustCommit(t, tm, txn.Id) // must not panic
}

func TestCommit_MultipleRedoEntries_AllCleared(t *testing.T) {
	tm, _ := newTestTM(t)

	t1 := tm.Begin()
	tm.AppendRedo(t1.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 1, Fields: intFields(1)})
	mustCommit(t, tm, t1.Id)

	t2 := tm.Begin()
	tm.AppendRedo(t2.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 2, Fields: intFields(2)})
	tm.AppendRedo(t2.Id, raft.RaftCommand{Op: raft.ReplDelete, Key: 1}) // key 1 exists from t1
	mustCommit(t, tm, t2.Id)

	if t1.RedoLog != nil {
		t.Error("t1 RedoLog should be nil after commit")
	}
	if t2.RedoLog != nil {
		t.Error("t2 RedoLog should be nil after commit")
	}
}

func TestRollback_DoesNotProposeToRaft(t *testing.T) {
	tm, _ := newTestTM(t) // rn=nil — if Propose were called it would panic
	txn := tm.Begin()
	tm.AppendRedo(txn.Id, raft.RaftCommand{Op: raft.ReplPut, Key: 1})
	tm.Rollback(txn.Id) // must not panic or call Propose
}
