package ap_test

// Unit tests for the AP consistency components:
//   TimestampStore  — Get, Set, CompareAndSet, Delete, Snapshot
//   APWriteLog      — Append, ReadFrom, recovery (nextLSN after reopen)
//   APSyncer        — LWW conflict resolution, per-peer cursor advance, SyncNow
//   ReplicaPicker   — round-robin distribution, empty map fallback
//   Integration     — two-node sync: writes on node A propagate to node B

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ap "github.com/your-username/DistributedDatabaseSystem/internal/AP"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func tmpLog(t *testing.T) *ap.APWriteLog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ap.log")
	l, err := ap.NewAPWriteLog(path)
	if err != nil {
		t.Fatalf("NewAPWriteLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func tmpLogAt(t *testing.T, path string) *ap.APWriteLog {
	t.Helper()
	l, err := ap.NewAPWriteLog(path)
	if err != nil {
		t.Fatalf("NewAPWriteLog(%s): %v", path, err)
	}
	return l
}

func newTestBTree(t *testing.T) *btree.BTree {
	t.Helper()
	dir := t.TempDir()
	pm, err := pagemanager.NewDB(filepath.Join(dir, "bt.db"))
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Close() })
	return btree.NewBTree(pm)
}

func intField(tag uint8, v int64) btree.Field {
	return btree.Field{Tag: tag, Value: btree.IntValue{V: v}}
}

func mustAppend(t *testing.T, l *ap.APWriteLog, op raft.ReplOp, key uint64, ts int64, fields []btree.Field) uint64 {
	t.Helper()
	lsn, err := l.Append(op, key, ts, fields)
	if err != nil {
		t.Fatalf("APWriteLog.Append: %v", err)
	}
	return lsn
}

func mustClose(t *testing.T, l *ap.APWriteLog) {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("APWriteLog.Close: %v", err)
	}
}

func mustBTInsert(t *testing.T, bt *btree.BTree, key uint64, fields []btree.Field) {
	t.Helper()
	if err := bt.Insert(key, fields); err != nil {
		t.Fatalf("BTree.Insert key=%d: %v", key, err)
	}
}

func strField(tag uint8, v string) btree.Field {
	return btree.Field{Tag: tag, Value: btree.StringValue{V: v}}
}

// ─── TimestampStore ───────────────────────────────────────────────────────────

func TestTimestampStore_GetReturnsZeroForUnknownKey(t *testing.T) {
	ts := ap.NewTimestampStore()
	if got := ts.Get(42); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestTimestampStore_SetAndGet(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 100)
	if got := ts.Get(1); got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
}

func TestTimestampStore_SetOverwrite(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 100)
	ts.Set(1, 200)
	if got := ts.Get(1); got != 200 {
		t.Fatalf("expected 200, got %d", got)
	}
}

func TestTimestampStore_CompareAndSet_RemoteWins(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 100)
	if !ts.CompareAndSet(1, 200) {
		t.Fatal("expected remote to win when remoteTs > localTs")
	}
	if got := ts.Get(1); got != 200 {
		t.Fatalf("expected timestamp updated to 200, got %d", got)
	}
}

func TestTimestampStore_CompareAndSet_LocalWins(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 200)
	if ts.CompareAndSet(1, 100) {
		t.Fatal("expected local to win when remoteTs < localTs")
	}
	if got := ts.Get(1); got != 200 {
		t.Fatalf("expected timestamp to remain 200, got %d", got)
	}
}

func TestTimestampStore_CompareAndSet_Equal_LocalWins(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 100)
	if ts.CompareAndSet(1, 100) {
		t.Fatal("equal timestamp should not update (local wins on tie)")
	}
}

func TestTimestampStore_CompareAndSet_NoEntry_RemoteWins(t *testing.T) {
	ts := ap.NewTimestampStore()
	// Key has no timestamp yet (0). Any positive remoteTs should win.
	if !ts.CompareAndSet(99, 50) {
		t.Fatal("expected remote to win when no local timestamp")
	}
	if got := ts.Get(99); got != 50 {
		t.Fatalf("expected 50, got %d", got)
	}
}

func TestTimestampStore_Delete(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 100)
	ts.Delete(1)
	if got := ts.Get(1); got != 0 {
		t.Fatalf("expected 0 after delete, got %d", got)
	}
}

func TestTimestampStore_Snapshot(t *testing.T) {
	ts := ap.NewTimestampStore()
	ts.Set(1, 10)
	ts.Set(2, 20)
	snap := ts.Snapshot()
	if len(snap) != 2 || snap[1] != 10 || snap[2] != 20 {
		t.Fatalf("unexpected snapshot: %v", snap)
	}
	// Modify snapshot — original must not change.
	snap[1] = 999
	if ts.Get(1) != 10 {
		t.Fatal("snapshot mutation affected original TimestampStore")
	}
}

func TestTimestampStore_Concurrent(t *testing.T) {
	ts := ap.NewTimestampStore()
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts.Set(uint64(i), int64(i*10))
			_ = ts.Get(uint64(i))
			_ = ts.CompareAndSet(uint64(i), int64(i*10+1))
		}(i)
	}
	wg.Wait()
}

// ─── APWriteLog ───────────────────────────────────────────────────────────────

func TestAPWriteLog_AppendAndReadFrom(t *testing.T) {
	l := tmpLog(t)
	fields := []btree.Field{intField(0, 42), strField(1, "hello")}
	lsn, err := l.Append(raft.ReplPut, 10, 1000, fields)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("expected first LSN=0, got %d", lsn)
	}
	entries, err := l.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.LSN != 0 || e.Key != 10 || e.Timestamp != 1000 || e.Op != raft.ReplPut {
		t.Fatalf("unexpected entry: %+v", e)
	}
	if len(e.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(e.Fields))
	}
}

func TestAPWriteLog_MultipleEntries_OrderPreserved(t *testing.T) {
	l := tmpLog(t)
	for i := 0; i < 5; i++ {
		if _, err := l.Append(raft.ReplPut, uint64(i), int64(i*100), []btree.Field{intField(0, int64(i))}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	entries, err := l.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.LSN != uint64(i) {
			t.Fatalf("entry %d: expected LSN %d, got %d", i, i, e.LSN)
		}
	}
}

func TestAPWriteLog_ReadFromOffset(t *testing.T) {
	l := tmpLog(t)
	for i := 0; i < 5; i++ {
		if _, err := l.Append(raft.ReplPut, uint64(i), 0, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	entries, err := l.ReadFrom(3)
	if err != nil {
		t.Fatalf("ReadFrom(3): %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (LSN 3,4), got %d", len(entries))
	}
	if entries[0].LSN != 3 || entries[1].LSN != 4 {
		t.Fatalf("wrong LSNs: %d, %d", entries[0].LSN, entries[1].LSN)
	}
}

func TestAPWriteLog_DeleteOp(t *testing.T) {
	l := tmpLog(t)
	if _, err := l.Append(raft.ReplDelete, 7, 500, nil); err != nil {
		t.Fatalf("Append delete: %v", err)
	}
	entries, _ := l.ReadFrom(0)
	if len(entries) != 1 || entries[0].Op != raft.ReplDelete || entries[0].Fields != nil {
		t.Fatalf("unexpected delete entry: %+v", entries)
	}
}

func TestAPWriteLog_NextLSNIncrements(t *testing.T) {
	l := tmpLog(t)
	if l.NextLSN() != 0 {
		t.Fatalf("expected initial NextLSN=0")
	}
	mustAppend(t, l, raft.ReplPut, 1, 0, nil)
	if l.NextLSN() != 1 {
		t.Fatalf("expected NextLSN=1 after first append")
	}
	mustAppend(t, l, raft.ReplPut, 2, 0, nil)
	if l.NextLSN() != 2 {
		t.Fatalf("expected NextLSN=2 after second append")
	}
}

func TestAPWriteLog_Recovery_NextLSN(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ap.log")

	l1 := tmpLogAt(t, path)
	for i := 0; i < 3; i++ {
		mustAppend(t, l1, raft.ReplPut, uint64(i), int64(i), nil)
	}
	mustClose(t, l1)

	// Reopen and verify LSN is recovered.
	l2 := tmpLogAt(t, path)
	defer func() { _ = l2.Close() }()
	if l2.NextLSN() != 3 {
		t.Fatalf("expected NextLSN=3 after recovery, got %d", l2.NextLSN())
	}
	// Append a new entry — it should get LSN 3.
	lsn, _ := l2.Append(raft.ReplPut, 99, 0, nil)
	if lsn != 3 {
		t.Fatalf("expected LSN=3 for first append after recovery, got %d", lsn)
	}
}

func TestAPWriteLog_Recovery_EntriesIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ap.log")

	l1 := tmpLogAt(t, path)
	mustAppend(t, l1, raft.ReplPut, 10, 1001, []btree.Field{intField(0, 99)})
	mustAppend(t, l1, raft.ReplDelete, 11, 1002, nil)
	mustClose(t, l1)

	l2 := tmpLogAt(t, path)
	defer func() { _ = l2.Close() }()
	entries, err := l2.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom after recovery: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after recovery, got %d", len(entries))
	}
	if entries[0].Key != 10 || entries[0].Timestamp != 1001 {
		t.Fatalf("first entry wrong: %+v", entries[0])
	}
	if entries[1].Key != 11 || entries[1].Op != raft.ReplDelete {
		t.Fatalf("second entry wrong: %+v", entries[1])
	}
}

func TestAPWriteLog_EmptyLog_ReadFrom(t *testing.T) {
	l := tmpLog(t)
	entries, err := l.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom on empty log: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestAPWriteLog_Concurrent_Append(t *testing.T) {
	l := tmpLog(t)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := l.Append(raft.ReplPut, uint64(i), int64(i), nil); err != nil {
				t.Errorf("concurrent Append[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	entries, _ := l.ReadFrom(0)
	if len(entries) != n {
		t.Fatalf("expected %d entries, got %d", n, len(entries))
	}
}

// ─── APSyncer ─────────────────────────────────────────────────────────────────

// newSyncerNode creates a BTree + TimestampStore + APWriteLog triple
// that represents a single AP node.
type syncerNode struct {
	bt         *btree.BTree
	timestamps *ap.TimestampStore
	log        *ap.APWriteLog
}

func newSyncerNode(t *testing.T) *syncerNode {
	t.Helper()
	bt := newTestBTree(t)
	ts := ap.NewTimestampStore()
	l := tmpLog(t)
	return &syncerNode{bt: bt, timestamps: ts, log: l}
}

func TestAPSyncer_LWW_RemoteWins(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// NodeA has an older write for key 1.
	mustBTInsert(t, nodeA.bt, 1, []btree.Field{intField(0, 10)})
	nodeA.timestamps.Set(1, 100)
	mustAppend(t, nodeA.log, raft.ReplPut, 1, 100, []btree.Field{intField(0, 10)})

	// NodeB has a newer write for key 1.
	mustBTInsert(t, nodeB.bt, 1, []btree.Field{intField(0, 20)})
	nodeB.timestamps.Set(1, 200)
	mustAppend(t, nodeB.log, raft.ReplPut, 1, 200, []btree.Field{intField(0, 20)})

	// Syncer on nodeA pulls from nodeB — nodeB's write should overwrite nodeA's.
	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
	}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)
	syncer.SyncNow(context.Background())

	rows, _ := nodeA.bt.RangeScan(1, 1)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	v, ok := rows[0].Fields[0].Value.(btree.IntValue)
	if !ok || v.V != 20 {
		t.Fatalf("expected value 20 (nodeB's newer write), got %+v", rows[0].Fields)
	}
	// Timestamp must be updated.
	if nodeA.timestamps.Get(1) != 200 {
		t.Fatalf("expected timestamp 200, got %d", nodeA.timestamps.Get(1))
	}
}

func TestAPSyncer_LWW_LocalWins(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// NodeA has a NEWER write for key 1 — it should not be overwritten.
	mustBTInsert(t, nodeA.bt, 1, []btree.Field{intField(0, 99)})
	nodeA.timestamps.Set(1, 300)

	// NodeB has an older write.
	mustBTInsert(t, nodeB.bt, 1, []btree.Field{intField(0, 5)})
	nodeB.timestamps.Set(1, 100)
	mustAppend(t, nodeB.log, raft.ReplPut, 1, 100, []btree.Field{intField(0, 5)})

	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
	}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)
	syncer.SyncNow(context.Background())

	rows, _ := nodeA.bt.RangeScan(1, 1)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	v, ok := rows[0].Fields[0].Value.(btree.IntValue)
	if !ok || v.V != 99 {
		t.Fatalf("expected local value 99 to remain, got %+v", rows[0].Fields)
	}
	if nodeA.timestamps.Get(1) != 300 {
		t.Fatalf("expected local timestamp 300 to be preserved, got %d", nodeA.timestamps.Get(1))
	}
}

func TestAPSyncer_Delete_RemoteWins(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// NodeA has a row that nodeB deleted more recently.
	mustBTInsert(t, nodeA.bt, 5, []btree.Field{intField(0, 7)})
	nodeA.timestamps.Set(5, 50)

	// NodeB's delete has a newer timestamp.
	nodeB.timestamps.Set(5, 200)
	mustAppend(t, nodeB.log, raft.ReplDelete, 5, 200, nil)

	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
	}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)
	syncer.SyncNow(context.Background())

	rows, _ := nodeA.bt.RangeScan(5, 5)
	if len(rows) != 0 {
		t.Fatalf("expected row to be deleted, but found %d rows", len(rows))
	}
}

func TestAPSyncer_Delete_LocalWins(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// NodeA has a row that it inserted AFTER nodeB's delete.
	mustBTInsert(t, nodeA.bt, 5, []btree.Field{intField(0, 77)})
	nodeA.timestamps.Set(5, 300) // newer than nodeB's delete

	mustAppend(t, nodeB.log, raft.ReplDelete, 5, 100, nil) // older delete

	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
	}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)
	syncer.SyncNow(context.Background())

	rows, _ := nodeA.bt.RangeScan(5, 5)
	if len(rows) != 1 {
		t.Fatalf("expected row to survive (local wins), got %d rows", len(rows))
	}
}

func TestAPSyncer_CursorAdvances(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// Append 3 entries to nodeB.
	for i := 0; i < 3; i++ {
		mustAppend(t, nodeB.log, raft.ReplPut, uint64(i), int64(i+1), []btree.Field{intField(0, int64(i))})
	}

	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
	}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)

	// First sync — should pull all 3 entries.
	syncer.SyncNow(context.Background())
	rows, _ := nodeA.bt.RangeScan(0, 2)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows after first sync, got %d", len(rows))
	}

	// NodeB appends a 4th entry.
	mustAppend(t, nodeB.log, raft.ReplPut, 3, 4, []btree.Field{intField(0, 3)})

	// Second sync — cursor should be at LSN 3, so only the new entry is pulled.
	syncer.SyncNow(context.Background())
	rows, _ = nodeA.bt.RangeScan(0, 3)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows after second sync, got %d", len(rows))
	}
}

func TestAPSyncer_MultiPeer(t *testing.T) {
	central := newSyncerNode(t)
	nodeB := newSyncerNode(t)
	nodeC := newSyncerNode(t)

	// NodeB wrote key 10, nodeC wrote key 20.
	mustAppend(t, nodeB.log, raft.ReplPut, 10, 100, []btree.Field{intField(0, 10)})
	mustAppend(t, nodeC.log, raft.ReplPut, 20, 200, []btree.Field{intField(0, 20)})

	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
		3: ap.NewDirectPeerPuller(nodeC.log),
	}
	syncer := ap.NewAPSyncer(central.bt, central.timestamps, central.log, peers, nil)
	syncer.SyncNow(context.Background())

	rows, _ := central.bt.RangeScan(10, 10)
	if len(rows) != 1 {
		t.Fatalf("expected key 10 from nodeB, got %d rows", len(rows))
	}
	rows, _ = central.bt.RangeScan(20, 20)
	if len(rows) != 1 {
		t.Fatalf("expected key 20 from nodeC, got %d rows", len(rows))
	}
}

func TestAPSyncer_ConflictMetrics(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// NodeA has old timestamps; nodeB has newer ones for same keys.
	for i := uint64(0); i < 5; i++ {
		mustBTInsert(t, nodeA.bt, i, []btree.Field{intField(0, int64(i))})
		nodeA.timestamps.Set(i, 10)
		mustAppend(t, nodeB.log, raft.ReplPut, i, 100, []btree.Field{intField(0, int64(i+100))})
	}

	peers := map[uint64]ap.PeerPuller{
		2: ap.NewDirectPeerPuller(nodeB.log),
	}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)
	syncer.SyncNow(context.Background())

	snap := syncer.Metrics.Snapshot()
	if snap.ConflictsResolved != 5 {
		t.Fatalf("expected 5 resolved conflicts, got %d", snap.ConflictsResolved)
	}
}

func TestAPSyncer_EmptyPeerLog(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)
	// NodeB has no entries — sync should be a no-op and not panic.
	peers := map[uint64]ap.PeerPuller{2: ap.NewDirectPeerPuller(nodeB.log)}
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, peers, nil)
	syncer.SyncNow(context.Background())
	// No assertion needed — just must not panic or error.
}

func TestAPSyncer_Run_RespondsToCancel(t *testing.T) {
	nodeA := newSyncerNode(t)
	syncer := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		syncer.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("APSyncer.Run did not exit after context cancellation")
	}
}

// ─── ReplicaPicker ────────────────────────────────────────────────────────────

func TestReplicaPicker_RoundRobin(t *testing.T) {
	picker := &ap.ReplicaPicker{}
	replicas := map[uint64]string{1: "a", 2: "b", 3: "c"}

	chosen := make(map[uint64]int)
	for i := 0; i < 9; i++ {
		id := picker.Pick(replicas)
		chosen[id]++
	}
	// Each of the 3 nodes should be picked exactly 3 times out of 9.
	for id, count := range chosen {
		if count != 3 {
			t.Fatalf("node %d picked %d times, expected 3", id, count)
		}
	}
}

func TestReplicaPicker_SingleReplica(t *testing.T) {
	picker := &ap.ReplicaPicker{}
	replicas := map[uint64]string{7: "solo"}
	for i := 0; i < 5; i++ {
		if id := picker.Pick(replicas); id != 7 {
			t.Fatalf("expected node 7, got %d", id)
		}
	}
}

func TestReplicaPicker_EmptyMap_ReturnsZero(t *testing.T) {
	picker := &ap.ReplicaPicker{}
	if id := picker.Pick(nil); id != 0 {
		t.Fatalf("expected 0 for empty map, got %d", id)
	}
	if id := picker.Pick(map[uint64]string{}); id != 0 {
		t.Fatalf("expected 0 for empty map, got %d", id)
	}
}

func TestReplicaPicker_Concurrent(t *testing.T) {
	picker := &ap.ReplicaPicker{}
	replicas := map[uint64]string{1: "a", 2: "b"}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := picker.Pick(replicas)
			if id != 1 && id != 2 {
				t.Errorf("unexpected replica id: %d", id)
			}
		}()
	}
	wg.Wait()
}

// ─── Two-node integration ─────────────────────────────────────────────────────

// TestAPSync_TwoNode_WritePropagate verifies that a write on node A is
// visible on node B after a sync, and later updates obey LWW.
func TestAPSync_TwoNode_WritePropagate(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// Node A writes key 100 with timestamp 1000.
	mustBTInsert(t, nodeA.bt, 100, []btree.Field{intField(0, 42)})
	nodeA.timestamps.Set(100, 1000)
	mustAppend(t, nodeA.log, raft.ReplPut, 100, 1000, []btree.Field{intField(0, 42)})

	// Syncer on nodeB pulls from nodeA.
	syncerB := ap.NewAPSyncer(
		nodeB.bt, nodeB.timestamps, nodeB.log,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(nodeA.log)},
		nil,
	)
	syncerB.SyncNow(context.Background())

	rows, _ := nodeB.bt.RangeScan(100, 100)
	if len(rows) != 1 {
		t.Fatalf("expected key 100 on nodeB after sync, got %d rows", len(rows))
	}
	if nodeB.timestamps.Get(100) != 1000 {
		t.Fatalf("expected timestamp 1000 on nodeB, got %d", nodeB.timestamps.Get(100))
	}

	// Node A overwrites key 100 with a newer timestamp.
	mustBTInsert(t, nodeA.bt, 100, []btree.Field{intField(0, 99)})
	nodeA.timestamps.Set(100, 2000)
	mustAppend(t, nodeA.log, raft.ReplPut, 100, 2000, []btree.Field{intField(0, 99)})

	// Node B concurrently writes key 100 with a timestamp between the two.
	mustBTInsert(t, nodeB.bt, 100, []btree.Field{intField(0, 55)})
	nodeB.timestamps.Set(100, 1500)
	mustAppend(t, nodeB.log, raft.ReplPut, 100, 1500, []btree.Field{intField(0, 55)})

	// Second sync: nodeA's write (ts=2000) should win over nodeB's (ts=1500).
	syncerB.SyncNow(context.Background())
	rows, _ = nodeB.bt.RangeScan(100, 100)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after second sync, got %d", len(rows))
	}
	v, ok := rows[0].Fields[0].Value.(btree.IntValue)
	if !ok || v.V != 99 {
		t.Fatalf("expected value 99 from nodeA's newer write, got %+v", rows[0].Fields)
	}
}

// TestAPSync_TwoNode_BidirectionalSync verifies that two nodes converge when
// each runs a syncer pulling from the other.
func TestAPSync_TwoNode_BidirectionalSync(t *testing.T) {
	nodeA := newSyncerNode(t)
	nodeB := newSyncerNode(t)

	// Each node writes a different key.
	mustBTInsert(t, nodeA.bt, 1, []btree.Field{intField(0, 10)})
	nodeA.timestamps.Set(1, 100)
	mustAppend(t, nodeA.log, raft.ReplPut, 1, 100, []btree.Field{intField(0, 10)})

	mustBTInsert(t, nodeB.bt, 2, []btree.Field{intField(0, 20)})
	nodeB.timestamps.Set(2, 200)
	mustAppend(t, nodeB.log, raft.ReplPut, 2, 200, []btree.Field{intField(0, 20)})

	syncerA := ap.NewAPSyncer(nodeA.bt, nodeA.timestamps, nodeA.log,
		map[uint64]ap.PeerPuller{2: ap.NewDirectPeerPuller(nodeB.log)}, nil)
	syncerB := ap.NewAPSyncer(nodeB.bt, nodeB.timestamps, nodeB.log,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(nodeA.log)}, nil)

	syncerA.SyncNow(context.Background())
	syncerB.SyncNow(context.Background())

	// Both nodes should now have keys 1 and 2.
	for _, bt := range []*btree.BTree{nodeA.bt, nodeB.bt} {
		rows, _ := bt.RangeScan(1, 2)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows on node, got %d", len(rows))
		}
	}
}

// TestAPSync_NoTempFileLeak verifies that temp files from the log are cleaned up.
func TestAPSync_LogFilePresent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ap.log")
	l := tmpLogAt(t, path)
	mustAppend(t, l, raft.ReplPut, 1, 1, nil)
	mustClose(t, l)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("log file should exist after close: %v", err)
	}
}
