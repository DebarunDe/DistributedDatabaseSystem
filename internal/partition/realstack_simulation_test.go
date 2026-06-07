package partition

// realstack_simulation_test.go — comprehensive real-stack simulation tests.
//
// Every test in this file exercises the actual RangeServer/Gateway stack (NOT the
// simRangeServer mock). Tests are grouped into six categories:
//
//   TestSim_CP_        — CP-mode happy-path CRUD
//   TestSim_CP_Edge_   — CP-mode edge cases
//   TestSim_AP_        — AP-mode happy-path writes + log verification
//   TestSim_AP_Edge_   — AP-mode edge cases
//   TestSim_AP_TwoNode_— two-node AP sync (LWW, idempotency, delete propagation)
//   TestSim_Stress_    — stress / concurrency tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	ap "github.com/your-username/DistributedDatabaseSystem/internal/AP"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
)

// =============================================================================
// 1. CP Mode — Happy Path
// =============================================================================

// TestSim_CP_FullCRUD inserts 5 rows, selects all, updates one by PK, deletes
// one by PK, and verifies the final count is 3.
func TestSim_CP_FullCRUD(t *testing.T) {
	c := newClusterT(t)
	c.createTable("crud", intIntCols...)

	// Insert 5 rows.
	for i := 1; i <= 5; i++ {
		c.insert("crud", n(i), n(i*10))
	}

	// Select all — expect 5.
	rs := c.query("crud", []string{"id", "v"}, nil)
	assertCount(t, rs, 5)

	// Update row id=3 → v=999.
	c.update("crud", "v", n(999), eq("id", "3"))
	row3 := c.query("crud", []string{"v"}, eq("id", "3"))
	assertCount(t, row3, 1)
	if getInt(t, row3, 0, 0) != 999 {
		t.Errorf("after update: v=%d, want 999", getInt(t, row3, 0, 0))
	}

	// Delete row id=5.
	c.delete("crud", eq("id", "5"))
	after := c.query("crud", []string{"id"}, nil)
	assertCount(t, after, 4)

	// Also delete row id=1 to get final count = 3.
	c.delete("crud", eq("id", "1"))
	assertCount(t, c.query("crud", []string{"id"}, nil), 3)
}

// TestSim_CP_SelectColumnProjection inserts rows and verifies that projecting a
// single column returns only that column.
func TestSim_CP_SelectColumnProjection(t *testing.T) {
	c := newClusterT(t)
	c.createTable("proj", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("proj", n(i), n(i*100))
	}

	rs := c.query("proj", []string{"v"}, nil)
	assertCount(t, rs, 5)
	assertColumns(t, rs, "v")
	// Each row must have exactly one field.
	for i, row := range rs.Rows {
		if len(row.Fields) != 1 {
			t.Errorf("row %d: fields=%d, want 1", i, len(row.Fields))
		}
	}
}

// TestSim_CP_RangeScan inserts 10 rows and queries with an AND range WHERE
// clause, expecting exactly the rows in [4, 7].
func TestSim_CP_RangeScan(t *testing.T) {
	c := newClusterT(t)
	c.createTable("rangescan", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("rangescan", n(i), n(i))
	}

	where := andE(gte("id", "4"), lte("id", "7"))
	rs := c.query("rangescan", []string{"id"}, where)
	assertCount(t, rs, 4)
	got := colInts(rs, 0)
	assertIntsEqual(t, got, []int64{4, 5, 6, 7})
}

// TestSim_CP_MultiUpdateSamePK updates the same PK five times and verifies the
// final value is the last write.
func TestSim_CP_MultiUpdateSamePK(t *testing.T) {
	c := newClusterT(t)
	c.createTable("multiupd", intIntCols...)
	c.insert("multiupd", n(1), n(0))

	for i := 1; i <= 5; i++ {
		c.update("multiupd", "v", n(i*11), eq("id", "1"))
	}

	rs := c.query("multiupd", []string{"v"}, eq("id", "1"))
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 55 {
		t.Errorf("final v=%d, want 55 (5×11)", getInt(t, rs, 0, 0))
	}
}

// TestSim_CP_InsertDeleteReinsertSamePK verifies that after delete+reinsert the
// count stays at 1 and the value is the new one.
func TestSim_CP_InsertDeleteReinsertSamePK(t *testing.T) {
	c := newClusterT(t)
	c.createTable("reins", intIntCols...)
	c.insert("reins", n(42), n(100))
	c.delete("reins", eq("id", "42"))
	c.insert("reins", n(42), n(200))

	rs := c.query("reins", []string{"id", "v"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 1) != 200 {
		t.Errorf("reinserted value=%d, want 200", getInt(t, rs, 0, 1))
	}
}

// =============================================================================
// 2. CP Mode — Edge Cases
// =============================================================================

// TestSim_CP_Edge_EmptyTableSelect verifies that SELECT on an empty table returns
// 0 rows and no error.
func TestSim_CP_Edge_EmptyTableSelect(t *testing.T) {
	c := newClusterT(t)
	c.createTable("empty", intIntCols...)
	rs := c.query("empty", []string{"id", "v"}, nil)
	assertCount(t, rs, 0)
}

// TestSim_CP_Edge_UpdateNoMatch verifies that UPDATE with a non-matching WHERE
// does not change row count or values.
func TestSim_CP_Edge_UpdateNoMatch(t *testing.T) {
	c := newClusterT(t)
	c.createTable("nomatch_upd", intIntCols...)
	c.insert("nomatch_upd", n(1), n(10))
	c.insert("nomatch_upd", n(2), n(20))

	// Update with a WHERE that matches nothing.
	mustExec(t, c.gw, &sqllayer.UpdateStatement{
		Table: "nomatch_upd", Column: "v", Value: n(999),
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(99)},
	})

	rs := c.query("nomatch_upd", []string{"v"}, nil)
	assertCount(t, rs, 2)
	for i := 0; i < 2; i++ {
		v := getInt(t, rs, i, 0)
		if v == 999 {
			t.Errorf("row %d was unexpectedly updated to 999", i)
		}
	}
}

// TestSim_CP_Edge_DeleteNoMatch verifies that DELETE with a non-matching WHERE
// leaves the table unchanged.
func TestSim_CP_Edge_DeleteNoMatch(t *testing.T) {
	c := newClusterT(t)
	c.createTable("nomatch_del", intIntCols...)
	for i := 1; i <= 3; i++ {
		c.insert("nomatch_del", n(i), n(0))
	}

	// Delete with WHERE that matches nothing.
	mustExec(t, c.gw, &sqllayer.DeleteStatement{
		Table: "nomatch_del",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(100)},
	})

	assertCount(t, c.query("nomatch_del", []string{"id"}, nil), 3)
}

// TestSim_CP_Edge_LargeTextValue inserts a ~1KB string and reads it back intact.
func TestSim_CP_Edge_LargeTextValue(t *testing.T) {
	c := newClusterT(t)
	c.createTable("largetxt", intStrCols...)

	// Build a 1024-char string.
	large := strings.Repeat("X", 1024)
	c.insert("largetxt", n(1), s(large))

	rs := c.query("largetxt", []string{"name"}, nil)
	assertCount(t, rs, 1)
	got := getStr(t, rs, 0, 0)
	if got != large {
		t.Errorf("large text roundtrip failed: len(got)=%d, want %d", len(got), len(large))
	}
}

// TestSim_CP_Edge_MultiTableIsolation inserts key=1 into two tables with the
// same PK, updates one, and verifies the other is unchanged.
func TestSim_CP_Edge_MultiTableIsolation(t *testing.T) {
	c := newClusterT(t)
	c.createTable("iso_a", intIntCols...)
	c.createTable("iso_b", intIntCols...)

	c.insert("iso_a", n(1), n(10))
	c.insert("iso_b", n(1), n(20))

	c.update("iso_a", "v", n(99), eq("id", "1"))

	rsA := c.query("iso_a", []string{"v"}, nil)
	rsB := c.query("iso_b", []string{"v"}, nil)
	if getInt(t, rsA, 0, 0) != 99 {
		t.Errorf("iso_a v=%d, want 99", getInt(t, rsA, 0, 0))
	}
	if getInt(t, rsB, 0, 0) != 20 {
		t.Errorf("iso_b v=%d, want 20 (must be unchanged)", getInt(t, rsB, 0, 0))
	}
}

// TestSim_CP_Edge_TwoNode2PC inserts rows on both sides of a split and then
// issues a 2PC UPDATE ALL. Both nodes must see the new value.
func TestSim_CP_Edge_TwoNode2PC(t *testing.T) {
	// Create table with single-node gateway first to get its tableId.
	single, sc := newSingleNodeRealGateway(t)
	mustExec(t, single, &sqllayer.CreateTableStatement{Table: "split2pc", Columns: intIntCols})
	tid := sc.FindTableSchema("split2pc").TableId

	splitKey := sqllayer.EncodeKey(tid, 51)
	gw, _ := newTwoNodeRealGateway(t, splitKey)
	// Recreate the schema on the two-node gateway.
	gw.schema = sc

	// Insert rows on both sides of the split.
	for i := 1; i <= 100; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "split2pc", Values: []sqllayer.Literal{n(i), n(1)},
		})
	}

	// 2PC UPDATE ALL → v=7.
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "split2pc", Column: "v", Value: n(7)})

	rs := mustSelect(t, gw, "split2pc", []string{"v"}, nil)
	assertCount(t, rs, 100)
	if sumCol(rs, 0) != 700 {
		t.Errorf("sum=%d after 2PC UPDATE ALL, want 700", sumCol(rs, 0))
	}
}

// =============================================================================
// 3. AP Mode — Happy Path
// =============================================================================

// TestSim_AP_FullCRUDWithLog performs a full AP CRUD cycle and verifies that
// each write operation appends correct ReplOp entries to the AP log.
func TestSim_AP_FullCRUDWithLog(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_crud", intIntCols...)

	// INSERT 3 rows.
	lsnBefore := node.apLog.NextLSN()
	for i := 1; i <= 3; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "ap_crud", Values: []sqllayer.Literal{n(i), n(i * 10)},
		})
	}
	// 3 ReplPut entries expected.
	entries, err := node.apLog.ReadFrom(lsnBefore)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	putCount := 0
	for _, e := range entries {
		if e.Op == raft.ReplPut {
			putCount++
		}
	}
	if putCount != 3 {
		t.Errorf("INSERT: want 3 ReplPut entries, got %d", putCount)
	}

	// UPDATE row id=2 → ReplPut entry.
	lsnAfterInserts := node.apLog.NextLSN()
	mustExec(t, gw, &sqllayer.UpdateStatement{
		Table: "ap_crud", Column: "v", Value: n(999),
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(2)},
	})
	updEntries, _ := node.apLog.ReadFrom(lsnAfterInserts)
	updPuts := 0
	for _, e := range updEntries {
		if e.Op == raft.ReplPut {
			updPuts++
		}
	}
	if updPuts < 1 {
		t.Errorf("UPDATE: want ≥1 ReplPut entry in AP log, got %d", updPuts)
	}

	// DELETE row id=1 → ReplDelete entry.
	lsnAfterUpd := node.apLog.NextLSN()
	mustExec(t, gw, &sqllayer.DeleteStatement{
		Table: "ap_crud",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(1)},
	})
	delEntries, _ := node.apLog.ReadFrom(lsnAfterUpd)
	delCount := 0
	for _, e := range delEntries {
		if e.Op == raft.ReplDelete {
			delCount++
		}
	}
	if delCount < 1 {
		t.Errorf("DELETE: want ≥1 ReplDelete entry in AP log, got %d", delCount)
	}

	// Final SELECT → 2 rows (id=2 and id=3).
	rs := mustSelect(t, gw, "ap_crud", []string{"id"}, nil)
	assertCount(t, rs, 2)
}

// TestSim_AP_TimestampAdvancesOnUpdates verifies that each update to the same
// key produces a strictly newer timestamp.
func TestSim_AP_TimestampAdvancesOnUpdates(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ts_adv", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "ts_adv", Values: []sqllayer.Literal{n(1), n(0)},
	})

	schema := node.sc.FindTableSchema("ts_adv")
	key := sqllayer.EncodeKey(schema.TableId, 1)

	var prevTS int64
	for i := 1; i <= 5; i++ {
		mustExec(t, gw, &sqllayer.UpdateStatement{
			Table: "ts_adv", Column: "v", Value: n(i),
			Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(1)},
		})
		ts := node.timestamps.Get(key)
		if ts <= prevTS {
			t.Errorf("update %d: timestamp did not advance (prev=%d, cur=%d)", i, prevTS, ts)
		}
		prevTS = ts
	}
}

// TestSim_AP_ConcurrentInsertLSNMonotonic inserts N rows concurrently and
// verifies that the LSNs in the AP log are all distinct (monotonically assigned).
func TestSim_AP_ConcurrentInsertLSNMonotonic(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "lsn_mono", intIntCols...)

	const N = 30
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errs[id-1] = gw.Execute(&sqllayer.InsertStatement{
				Table: "lsn_mono", Values: []sqllayer.Literal{n(id), n(id)},
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("INSERT[%d]: %v", i+1, err)
		}
	}

	entries, err := node.apLog.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	lsnSeen := make(map[uint64]bool, len(entries))
	for _, e := range entries {
		if lsnSeen[e.LSN] {
			t.Errorf("duplicate LSN %d in AP log", e.LSN)
		}
		lsnSeen[e.LSN] = true
	}
}

// TestSim_AP_SelectWithConsistencyOverrideEventual verifies that a CP table can
// be queried with a per-query EVENTUAL override.
func TestSim_AP_SelectWithConsistencyOverrideEventual(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	// CP table (no ALTER).
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "cp_over", Columns: intIntCols})
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "cp_over", Values: []sqllayer.Literal{n(1), n(7)}})

	mode := sqllayer.ConsistencyAP
	rs, err := gw.Execute(&sqllayer.SelectStatement{
		Table:               "cp_over",
		Columns:             []string{"*"},
		ConsistencyOverride: &mode,
	})
	if err != nil {
		t.Fatalf("SELECT WITH CONSISTENCY EVENTUAL on CP table: %v", err)
	}
	assertCount(t, rs, 1)
}

// TestSim_AP_SelectWithConsistencyOverrideStrong verifies that an AP table can
// be queried with a per-query STRONG override.
func TestSim_AP_SelectWithConsistencyOverrideStrong(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_strong", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "ap_strong", Values: []sqllayer.Literal{n(1), n(42)}})

	mode := sqllayer.ConsistencyCP
	rs, err := gw.Execute(&sqllayer.SelectStatement{
		Table:               "ap_strong",
		Columns:             []string{"*"},
		ConsistencyOverride: &mode,
	})
	if err != nil {
		t.Fatalf("SELECT WITH CONSISTENCY STRONG on AP table: %v", err)
	}
	assertCount(t, rs, 1)
}

// =============================================================================
// 4. AP Mode — Edge Cases
// =============================================================================

// TestSim_AP_Edge_EmptyTableSelect verifies that SELECT on an empty AP table
// returns 0 rows and no error.
func TestSim_AP_Edge_EmptyTableSelect(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_empty", intIntCols...)
	rs := mustSelect(t, gw, "ap_empty", []string{"*"}, nil)
	assertCount(t, rs, 0)
}

// TestSim_AP_Edge_UpdateNoMatch verifies that an AP UPDATE with a non-matching
// WHERE does not write any new log entries.
func TestSim_AP_Edge_UpdateNoMatch(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_nomatch_upd", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "ap_nomatch_upd", Values: []sqllayer.Literal{n(1), n(10)},
	})

	lsnBefore := node.apLog.NextLSN()
	mustExec(t, gw, &sqllayer.UpdateStatement{
		Table: "ap_nomatch_upd", Column: "v", Value: n(999),
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(99)},
	})
	lsnAfter := node.apLog.NextLSN()

	if lsnAfter != lsnBefore {
		t.Errorf("AP UPDATE with no match: NextLSN advanced from %d to %d (expected no-op)",
			lsnBefore, lsnAfter)
	}
}

// TestSim_AP_Edge_DeleteNoMatch verifies that an AP DELETE with a non-matching
// WHERE does not write any new log entries.
func TestSim_AP_Edge_DeleteNoMatch(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_nomatch_del", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "ap_nomatch_del", Values: []sqllayer.Literal{n(1), n(10)},
	})

	lsnBefore := node.apLog.NextLSN()
	mustExec(t, gw, &sqllayer.DeleteStatement{
		Table: "ap_nomatch_del",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(99)},
	})
	lsnAfter := node.apLog.NextLSN()

	if lsnAfter != lsnBefore {
		t.Errorf("AP DELETE with no match: NextLSN advanced from %d to %d (expected no-op)",
			lsnBefore, lsnAfter)
	}
}

// TestSim_AP_Edge_DeleteReinsertTimestamp verifies that after delete+reinsert,
// the new timestamp is strictly greater than the old timestamp, preventing
// phantom reads during sync.
func TestSim_AP_Edge_DeleteReinsertTimestamp(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_rein_ts", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "ap_rein_ts", Values: []sqllayer.Literal{n(5), n(100)},
	})

	schema := node.sc.FindTableSchema("ap_rein_ts")
	key := sqllayer.EncodeKey(schema.TableId, 5)
	oldTS := node.timestamps.Get(key)

	// Delete then reinsert.
	mustExec(t, gw, &sqllayer.DeleteStatement{
		Table: "ap_rein_ts",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(5)},
	})
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "ap_rein_ts", Values: []sqllayer.Literal{n(5), n(200)},
	})

	newTS := node.timestamps.Get(key)
	if newTS <= oldTS {
		t.Errorf("reinsert timestamp (%d) not > delete timestamp (%d)", newTS, oldTS)
	}

	// Final read must return the new value.
	rs := mustSelect(t, gw, "ap_rein_ts", []string{"v"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 200 {
		t.Errorf("reinserted value=%d, want 200", getInt(t, rs, 0, 0))
	}
}

// TestSim_AP_Edge_LargeTextValue inserts a ~1KB string via AP mode and reads it
// back intact.
func TestSim_AP_Edge_LargeTextValue(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ap_largetxt", intStrCols...)

	large := strings.Repeat("A", 1024)
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "ap_largetxt", Values: []sqllayer.Literal{n(1), s(large)},
	})

	rs := mustSelect(t, gw, "ap_largetxt", []string{"name"}, nil)
	assertCount(t, rs, 1)
	got := getStr(t, rs, 0, 0)
	if got != large {
		t.Errorf("AP large text roundtrip failed: len(got)=%d, want %d", len(got), len(large))
	}
}

// TestSim_AP_Edge_LogRecovery creates an APWriteLog, appends entries, closes it,
// reopens it at the same path, and verifies ReadFrom returns all entries.
func TestSim_AP_Edge_LogRecovery(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "recover.log")

	// Open, write 5 entries, close.
	log1, err := ap.NewAPWriteLog(logPath)
	if err != nil {
		t.Fatalf("NewAPWriteLog (first open): %v", err)
	}
	for i := 0; i < 5; i++ {
		fields := []btree.Field{{Tag: 0, Value: btree.IntValue{V: int64(i)}}}
		if _, err := log1.Append(raft.ReplPut, uint64(i+1), int64(i*1000), fields); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close log1: %v", err)
	}

	// Reopen at the same path.
	log2, err := ap.NewAPWriteLog(logPath)
	if err != nil {
		t.Fatalf("NewAPWriteLog (second open): %v", err)
	}
	t.Cleanup(func() {
		_ = log2.Close()
		_ = os.Remove(logPath)
	})

	entries, err := log2.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom after reopen: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("recovered %d entries, want 5", len(entries))
	}
	// Verify the recovered nextLSN is correct.
	if log2.NextLSN() != 5 {
		t.Errorf("recovered NextLSN=%d, want 5", log2.NextLSN())
	}
}

// TestSim_AP_Edge_ModeFlipUnderData inserts 3 rows in CP mode, flips to AP, then
// inserts 3 more. Total rows must be 6 and the AP log must have exactly 3 ReplPut
// entries (only the AP-mode writes).
func TestSim_AP_Edge_ModeFlipUnderData(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)

	// Create table in default CP mode.
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "flip_data", Columns: intIntCols})

	// Insert 3 CP rows — must NOT appear in AP log.
	for i := 1; i <= 3; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "flip_data", Values: []sqllayer.Literal{n(i), n(i)},
		})
	}

	lsnBeforeFlip := node.apLog.NextLSN()

	// Flip to AP.
	mustExec(t, gw, &sqllayer.AlterConsistencyStatement{Table: "flip_data", Mode: sqllayer.ConsistencyAP})

	// Insert 3 more AP rows.
	for i := 4; i <= 6; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "flip_data", Values: []sqllayer.Literal{n(i), n(i)},
		})
	}

	// Total rows = 6.
	rs := mustSelect(t, gw, "flip_data", []string{"id"}, nil)
	assertCount(t, rs, 6)

	// AP log must have exactly 3 ReplPut entries (from after the flip).
	entries, err := node.apLog.ReadFrom(lsnBeforeFlip)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	apPuts := 0
	for _, e := range entries {
		if e.Op == raft.ReplPut {
			apPuts++
		}
	}
	if apPuts != 3 {
		t.Errorf("AP log has %d ReplPut entries after flip, want 3", apPuts)
	}
}

// =============================================================================
// 5. Two-Node AP Sync
// =============================================================================

// TestSim_AP_TwoNode_BidirectionalSync verifies that after bidirectional sync,
// both nodes have rows written on either node.
func TestSim_AP_TwoNode_BidirectionalSync(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0) // put table 1 on node 1
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "bidir", intIntCols...)
	schema := n1.sc.FindTableSchema("bidir")

	// Row A on node 1.
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "bidir", Values: []sqllayer.Literal{n(1), n(10)}})
	keyA := sqllayer.EncodeKey(schema.TableId, 1)

	// Row B: write directly to node 2's BTree (simulates a write on n2 outside routing).
	keyB := sqllayer.EncodeKey(schema.TableId, 2)
	tsB := int64(1_000_000)
	_ = n2.bt.Insert(keyB, []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 2}},
		{Tag: 1, Value: btree.IntValue{V: 20}},
	})
	n2.timestamps.Set(keyB, tsB)
	if _, err := n2.apLog.Append(raft.ReplPut, keyB, tsB, []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 2}},
		{Tag: 1, Value: btree.IntValue{V: 20}},
	}); err != nil {
		t.Fatalf("n2.apLog.Append: %v", err)
	}

	// Sync n2 ← n1.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	// Sync n1 ← n2.
	syncer1 := ap.NewAPSyncer(n1.bt, n1.timestamps, n1.apLog,
		map[uint64]ap.PeerPuller{2: ap.NewDirectPeerPuller(n2.apLog)}, n1.sc)
	syncer1.SyncNow(context.Background())

	// Both nodes should have both rows.
	n1A, _ := n1.bt.RangeScan(keyA, keyA)
	n1B, _ := n1.bt.RangeScan(keyB, keyB)
	n2A, _ := n2.bt.RangeScan(keyA, keyA)
	n2B, _ := n2.bt.RangeScan(keyB, keyB)

	if len(n1A) != 1 {
		t.Errorf("n1 missing row A (len=%d)", len(n1A))
	}
	if len(n1B) != 1 {
		t.Errorf("n1 missing row B (len=%d)", len(n1B))
	}
	if len(n2A) != 1 {
		t.Errorf("n2 missing row A after sync (len=%d)", len(n2A))
	}
	if len(n2B) != 1 {
		t.Errorf("n2 missing row B (len=%d)", len(n2B))
	}
}

// TestSim_AP_TwoNode_LWW verifies that Last-Writer-Wins resolves conflicts
// correctly: n1's newer timestamp beats n2's older timestamp.
func TestSim_AP_TwoNode_LWW(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "lww_tbl", intIntCols...)
	schema := n1.sc.FindTableSchema("lww_tbl")

	// n1 writes key=1 with value=10 (will have a real recent timestamp).
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "lww_tbl", Values: []sqllayer.Literal{n(1), n(10)}})
	key := sqllayer.EncodeKey(schema.TableId, 1)
	n1TS := n1.timestamps.Get(key)

	// n2 writes key=1 with value=99 but with an older timestamp.
	oldTS := n1TS - 1_000_000
	_ = n2.bt.Insert(key, []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 99}},
	})
	n2.timestamps.Set(key, oldTS)
	if _, err := n2.apLog.Append(raft.ReplPut, key, oldTS, []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 99}},
	}); err != nil {
		t.Fatalf("n2.apLog.Append: %v", err)
	}

	// Sync n2 from n1 → n1's newer write must win.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	rows, _ := n2.bt.RangeScan(key, key)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after LWW sync, got %d", len(rows))
	}
	v, ok := rows[0].Fields[1].Value.(btree.IntValue)
	if !ok || v.V != 10 {
		t.Errorf("LWW result: got value=%v, want 10 (n1's newer write)", rows[0].Fields[1].Value)
	}
}

// TestSim_AP_TwoNode_SyncIdempotent verifies that running SyncNow twice does not
// double the row count.
func TestSim_AP_TwoNode_SyncIdempotent(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "idempotent", intIntCols...)
	schema := n1.sc.FindTableSchema("idempotent")

	mustExec(t, gw, &sqllayer.InsertStatement{Table: "idempotent", Values: []sqllayer.Literal{n(1), n(42)}})

	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())
	syncer2.SyncNow(context.Background()) // second sync must be a no-op

	key := sqllayer.EncodeKey(schema.TableId, 1)
	rows, _ := n2.bt.RangeScan(key, key)
	if len(rows) != 1 {
		t.Errorf("after 2 syncs: expected 1 row, got %d (idempotency violated)", len(rows))
	}
}

// TestSim_AP_TwoNode_DeletePropagates verifies that a delete on n1 propagates to
// n2 after sync.
func TestSim_AP_TwoNode_DeletePropagates(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "del_prop2", intIntCols...)
	schema := n1.sc.FindTableSchema("del_prop2")

	mustExec(t, gw, &sqllayer.InsertStatement{Table: "del_prop2", Values: []sqllayer.Literal{n(1), n(7)}})
	key := sqllayer.EncodeKey(schema.TableId, 1)

	// Sync n2 so it has the row.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	rowsBefore, _ := n2.bt.RangeScan(key, key)
	if len(rowsBefore) != 1 {
		t.Fatalf("setup: expected row on n2, got %d", len(rowsBefore))
	}

	// Delete on n1.
	mustExec(t, gw, &sqllayer.DeleteStatement{
		Table: "del_prop2",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(1)},
	})

	// Sync again.
	syncer2.SyncNow(context.Background())

	rowsAfter, _ := n2.bt.RangeScan(key, key)
	if len(rowsAfter) != 0 {
		t.Errorf("after delete propagation: expected 0 rows on n2, got %d", len(rowsAfter))
	}
}

// TestSim_AP_TwoNode_MultiRoundConvergence verifies that after bidirectional sync
// of disjoint write sets, each node ends up with all rows.
func TestSim_AP_TwoNode_MultiRoundConvergence(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "converge", intIntCols...)
	schema := n1.sc.FindTableSchema("converge")

	// n1 writes rows 1–10.
	for i := 1; i <= 10; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "converge", Values: []sqllayer.Literal{n(i), n(i)},
		})
	}

	// n2 writes rows 11–20 directly (bypassing the gateway routing).
	for i := 11; i <= 20; i++ {
		k := sqllayer.EncodeKey(schema.TableId, uint32(i))
		ts := int64(i * 1000)
		fields := []btree.Field{
			{Tag: 0, Value: btree.IntValue{V: int64(i)}},
			{Tag: 1, Value: btree.IntValue{V: int64(i)}},
		}
		_ = n2.bt.Insert(k, fields)
		n2.timestamps.Set(k, ts)
		if _, err := n2.apLog.Append(raft.ReplPut, k, ts, fields); err != nil {
			t.Fatalf("n2.apLog.Append: %v", err)
		}
	}

	// Bidirectional sync.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	syncer1 := ap.NewAPSyncer(n1.bt, n1.timestamps, n1.apLog,
		map[uint64]ap.PeerPuller{2: ap.NewDirectPeerPuller(n2.apLog)}, n1.sc)
	syncer1.SyncNow(context.Background())

	// Each node should now have all 20 rows.
	startKey := sqllayer.EncodeKey(schema.TableId, 1)
	endKey := sqllayer.EncodeKey(schema.TableId, 20)

	n1rows, _ := n1.bt.RangeScan(startKey, endKey)
	n2rows, _ := n2.bt.RangeScan(startKey, endKey)

	if len(n1rows) != 20 {
		t.Errorf("n1 has %d rows after convergence, want 20", len(n1rows))
	}
	if len(n2rows) != 20 {
		t.Errorf("n2 has %d rows after convergence, want 20", len(n2rows))
	}
}

// =============================================================================
// 6. Stress Tests
// =============================================================================

// TestSim_Stress_CPConcurrentInsertsThenSelect fires 100 goroutines each
// inserting a unique PK, then verifies exactly 100 rows are visible.
func TestSim_Stress_CPConcurrentInsertsThenSelect(t *testing.T) {
	c := newClusterT(t)
	c.createTable("stress_cp_ins", intIntCols...)

	const N = 100
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errs[id-1] = c.gw.Execute(&sqllayer.InsertStatement{
				Table: "stress_cp_ins", Values: []sqllayer.Literal{n(id), n(id)},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("CP INSERT[%d]: %v", i+1, err)
		}
	}
	assertCount(t, c.query("stress_cp_ins", []string{"id"}, nil), N)
}

// TestSim_Stress_APConcurrentInserts fires 200 goroutines each inserting a
// unique PK in AP mode. No errors, and the AP log must have 200 entries.
func TestSim_Stress_APConcurrentInserts(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "stress_ap_ins", intIntCols...)

	const N = 200
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errs[id-1] = gw.Execute(&sqllayer.InsertStatement{
				Table: "stress_ap_ins", Values: []sqllayer.Literal{n(id), n(id)},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("AP INSERT[%d]: %v", i+1, err)
		}
	}

	entries, _ := node.apLog.ReadFrom(0)
	putCount := 0
	for _, e := range entries {
		if e.Op == raft.ReplPut {
			putCount++
		}
	}
	if putCount != N {
		t.Errorf("AP log has %d ReplPut entries, want %d", putCount, N)
	}
}

// TestSim_Stress_APAndCPConcurrentInterleaved runs 2 goroutines writing to an
// AP table and 2 goroutines writing to a CP table concurrently. No cross-
// contamination; each table ends up with its correct row count.
func TestSim_Stress_APAndCPConcurrentInterleaved(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "interleave_cp", Columns: intIntCols})
	createAPTable(t, gw, "interleave_ap", intIntCols...)

	const rowsPerWorker = 25
	var wg sync.WaitGroup

	// 2 AP writers: goroutine 0 uses IDs 1–25, goroutine 1 uses 26–50.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			base := worker * rowsPerWorker
			for i := 1; i <= rowsPerWorker; i++ {
				_, _ = gw.Execute(&sqllayer.InsertStatement{
					Table:  "interleave_ap",
					Values: []sqllayer.Literal{n(base + i), n(base + i)},
				})
			}
		}(w)
	}

	// 2 CP writers: goroutine 0 uses IDs 1–25, goroutine 1 uses 26–50.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			base := worker * rowsPerWorker
			for i := 1; i <= rowsPerWorker; i++ {
				_, _ = gw.Execute(&sqllayer.InsertStatement{
					Table:  "interleave_cp",
					Values: []sqllayer.Literal{n(base + i), n(base + i)},
				})
			}
		}(w)
	}

	wg.Wait()

	rsAP := mustSelect(t, gw, "interleave_ap", []string{"id"}, nil)
	rsCP := mustSelect(t, gw, "interleave_cp", []string{"id"}, nil)

	if len(rsAP.Rows) != 50 {
		t.Errorf("AP table has %d rows, want 50", len(rsAP.Rows))
	}
	if len(rsCP.Rows) != 50 {
		t.Errorf("CP table has %d rows, want 50", len(rsCP.Rows))
	}
}

// TestSim_Stress_TwoNodeAPConcurrent fires 50 goroutines each inserting a unique
// row to n1, then runs SyncNow to n2. n2 must have all 50 rows.
func TestSim_Stress_TwoNodeAPConcurrent(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "stress_2node", intIntCols...)
	schema := n1.sc.FindTableSchema("stress_2node")

	const N = 50
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errs[id-1] = gw.Execute(&sqllayer.InsertStatement{
				Table: "stress_2node", Values: []sqllayer.Literal{n(id), n(id)},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("INSERT[%d]: %v", i+1, err)
		}
	}

	// Sync all entries from n1 to n2.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	startKey := sqllayer.EncodeKey(schema.TableId, 1)
	endKey := sqllayer.EncodeKey(schema.TableId, uint32(N))
	rows, _ := n2.bt.RangeScan(startKey, endKey)
	if len(rows) != N {
		t.Errorf("n2 has %d rows after sync, want %d", len(rows), N)
	}
}

// TestSim_Stress_APConcurrentUpdatesSameKey fires 20 goroutines all updating
// key=1 concurrently. No panics/errors, and exactly 1 row exists afterwards.
func TestSim_Stress_APConcurrentUpdatesSameKey(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "stress_same_key", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{
		Table: "stress_same_key", Values: []sqllayer.Literal{n(1), n(0)},
	})

	const W = 20
	var wg sync.WaitGroup
	errs := make([]error, W)
	for i := 0; i < W; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = gw.Execute(&sqllayer.UpdateStatement{
				Table:  "stress_same_key",
				Column: "v",
				Value:  n(idx + 1),
				Where: &sqllayer.ComparisonExpr{
					Column: "id", Operator: "=", Value: n(1),
				},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("AP concurrent UPDATE[%d]: %v", i, err)
		}
	}

	// Exactly 1 row must exist with some value.
	rs := mustSelect(t, gw, "stress_same_key", []string{"id", "v"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 1 {
		t.Errorf("key=%d, want 1", getInt(t, rs, 0, 0))
	}
}

// =============================================================================
// Compile-time import check — keeps fmt in scope.
// =============================================================================
var _ = fmt.Sprintf
