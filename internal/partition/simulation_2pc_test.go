package partition

// Extensive 2PC simulation tests.
//
// Each test uses newMultiNodeCluster (distinct BTree per node) or newSimCluster
// (single node, multiple co-located ranges) to build a real in-process cluster
// and exercises the full Gateway → ExecuteDistributed → simRangeServer pipeline.
//
// Test groups:
//   A. Atomicity — partial failures leave data unchanged
//   B. Multi-range configurations — 2, 3, 4, 5+ ranges
//   C. Failure injection — which node fails drives which abort path runs
//   D. WHERE-clause edge cases
//   E. Sequential dependent transactions
//   F. Concurrent stress
//   G. Large transactions
//   H. Special scenarios

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"

	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

// ---------------------------------------------------------------------------
// Simulation helpers
// ---------------------------------------------------------------------------

// setupTable creates a table on the cluster and returns its tableId.
func setupTable(t *testing.T, c *simCluster, name string, extraCols []string, extraTypes []string) uint32 {
	t.Helper()
	c.addTable(name, "id", extraCols, extraTypes)
	return c.sc.FindTableSchema(name).TableId
}

// insertN inserts rows id=1..n with the given extra literal into a table.
func insertN(t *testing.T, c *simCluster, table string, n int, val sqllayer.Literal) {
	t.Helper()
	for i := 1; i <= n; i++ {
		c.insert(table, []sqllayer.Literal{intLitS(strconv.Itoa(i)), val})
	}
}

// sumIntCol sums the integer value at colIdx (0-based field index) across all
// rows in a ResultSet.
func sumIntCol(rs *ResultSet, colIdx int) int64 {
	var total int64
	for _, row := range rs.Rows {
		if colIdx < len(row.Fields) {
			if iv, ok := row.Fields[colIdx].Value.(btree.IntValue); ok {
				total += iv.V
			}
		}
	}
	return total
}

// strColValues returns the set of distinct string values at colIdx.
func strColValues(rs *ResultSet, colIdx int) map[string]int {
	m := make(map[string]int)
	for _, row := range rs.Rows {
		if colIdx < len(row.Fields) {
			if sv, ok := row.Fields[colIdx].Value.(btree.StringValue); ok {
				m[sv.V]++
			}
		}
	}
	return m
}

// sortedPKs returns sorted primary key integers from a ResultSet.
func sortedPKs(rs *ResultSet) []uint32 {
	keys := make([]uint32, len(rs.Rows))
	for i, row := range rs.Rows {
		keys[i] = pkVal(row.Key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// scanNodeTable scans all rows for tableId from a node's BTree directly.
func scanNodeTable(t *testing.T, node *simRangeServer, tableId uint32) []struct {
	Key    uint64
	Fields []btree.Field
} {
	t.Helper()
	node.mu.Lock()
	defer node.mu.Unlock()
	rows, err := node.bt.RangeScan(
		sqllayer.EncodeKey(tableId, 0),
		sqllayer.EncodeKey(tableId, ^uint32(0)),
	)
	if err != nil {
		t.Fatalf("scanNodeTable: %v", err)
	}
	return rows
}

// setup2NodeSplit creates a 2-node cluster with the given table split at pk=splitPK.
// Node 1 owns [0, splitPK), node 2 owns [splitPK, ∞).
func setup2NodeSplit(t *testing.T, tableName string, splitPK int) (*simCluster, uint32) {
	t.Helper()
	c := newMultiNodeCluster(t, 2)
	tableId := setupTable(t, c, tableName, []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, uint32(splitPK)), 1, 2)
	return c, tableId
}

// setup3NodeSplit creates a 3-node cluster with the table split at pk=lo and pk=hi.
// Node 1 owns [0,lo), node 2 owns [lo,hi), node 3 owns [hi,∞).
func setup3NodeSplit(t *testing.T, tableName string, lo, hi int) (*simCluster, uint32) {
	t.Helper()
	c := newMultiNodeCluster(t, 3)
	tableId := setupTable(t, c, tableName, []string{"val"}, []string{"INT"})
	split1 := sqllayer.EncodeKey(tableId, uint32(lo))
	split2 := sqllayer.EncodeKey(tableId, uint32(hi))
	c.splitAtKey(split1, 1, 1) // put upper half on node 1 temporarily
	c.splitAtKey(split2, 2, 3) // re-assign upper portions
	return c, tableId
}

// execUpdate runs a multi-range UPDATE; returns the error (nil = success).
func execUpdate(c *simCluster, table, col, val string, where sqllayer.Expression) error {
	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  table,
		Column: col,
		Value:  sqllayer.Literal{Value: val, Type: sqllayer.TOKEN_NUMBER},
		Where:  where,
	})
	return err
}

func execUpdateStr(c *simCluster, table, col, val string, where sqllayer.Expression) error {
	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  table,
		Column: col,
		Value:  sqllayer.Literal{Value: val, Type: sqllayer.TOKEN_STRING},
		Where:  where,
	})
	return err
}

func execDelete(c *simCluster, table string, where sqllayer.Expression) error {
	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: table, Where: where})
	return err
}

// ---------------------------------------------------------------------------
// A. Atomicity
// ---------------------------------------------------------------------------

// A1: update spanning 2 nodes — all rows get the new value after commit.
func TestSim2PC_A1_TwoNode_UpdateAll_Committed(t *testing.T) {
	c, _ := setup2NodeSplit(t, "at1", 50)
	insertN(t, c, "at1", 100, intLitS("1"))

	if err := execUpdate(c, "at1", "val", "99", nil); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	rs := c.selectRows("at1", []string{"id", "val"}, nil)
	if len(rs.Rows) != 100 {
		t.Fatalf("row count=%d, want 100", len(rs.Rows))
	}
	if bad, key := func() (bool, uint64) {
		for _, row := range rs.Rows {
			if len(row.Fields) < 2 {
				return true, row.Key
			}
			if iv, ok := row.Fields[1].Value.(btree.IntValue); !ok || iv.V != 99 {
				return true, row.Key
			}
		}
		return false, 0
	}(); bad {
		t.Errorf("row pk=%d: val != 99 after commit", pkVal(key))
	}
}

// A2: when one node's Prepare fails, NO rows on either node are changed.
func TestSim2PC_A2_PrepareFailure_DataUnchanged(t *testing.T) {
	c, _ := setup2NodeSplit(t, "at2", 50)
	insertN(t, c, "at2", 100, intLitS("7"))

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	err := execUpdate(c, "at2", "val", "99", nil)

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	if err == nil {
		t.Fatal("expected error from failed Prepare, got nil")
	}
	rs := c.selectRows("at2", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 700 { // 100 rows × 7
		t.Errorf("sum after aborted update=%d, want 700 (no rows changed)", sum)
	}
}

// A3: delete-all atomicity — either all rows are gone or all remain.
func TestSim2PC_A3_DeleteAll_AtomicCommit(t *testing.T) {
	c, _ := setup2NodeSplit(t, "at3", 50)
	insertN(t, c, "at3", 100, intLitS("0"))

	if err := execDelete(c, "at3", nil); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	rs := c.selectRows("at3", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("rows after DELETE=%d, want 0", len(rs.Rows))
	}
}

// A4: delete-all with Prepare failure — all rows preserved.
func TestSim2PC_A4_DeleteAll_PrepareFailure_RowsPreserved(t *testing.T) {
	c, _ := setup2NodeSplit(t, "at4", 50)
	insertN(t, c, "at4", 100, intLitS("0"))

	c.nodes[1].mu.Lock()
	c.nodes[1].failPrepare = true
	c.nodes[1].mu.Unlock()

	err := execDelete(c, "at4", nil)

	c.nodes[1].mu.Lock()
	c.nodes[1].failPrepare = false
	c.nodes[1].mu.Unlock()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	rs := c.selectRows("at4", []string{"id"}, nil)
	if len(rs.Rows) != 100 {
		t.Errorf("rows after aborted DELETE=%d, want 100", len(rs.Rows))
	}
}

// A5: sum invariant — failed UPDATE preserves the sum of all values.
func TestSim2PC_A5_SumInvariant_FailedUpdate(t *testing.T) {
	c, _ := setup2NodeSplit(t, "at5", 50)
	// Insert 100 rows with val = id (sum = 1+2+…+100 = 5050)
	for i := 1; i <= 100; i++ {
		c.insert("at5", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
	}
	wantSum := int64(5050)

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	_ = execUpdate(c, "at5", "val", "0", nil) // try to zero everything

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	rs := c.selectRows("at5", []string{"id", "val"}, nil)
	if got := sumIntCol(rs, 1); got != wantSum {
		t.Errorf("sum after failed UPDATE=%d, want %d (abort preserved original)", got, wantSum)
	}
}

// ---------------------------------------------------------------------------
// B. Multi-range configurations
// ---------------------------------------------------------------------------

// B1: 3 ranges — update all rows across 3 nodes.
func TestSim2PC_B1_ThreeRanges_UpdateAll(t *testing.T) {
	c, tableId := setup3NodeSplit(t, "br1", 34, 67)
	for i := 1; i <= 99; i++ {
		c.insert("br1", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("1")})
	}

	if err := execUpdate(c, "br1", "val", "2", nil); err != nil {
		t.Fatalf("3-range UPDATE: %v", err)
	}

	for nodeID, node := range c.nodes {
		rows := scanNodeTable(t, node, tableId)
		for _, row := range rows {
			if len(row.Fields) >= 2 {
				if iv, ok := row.Fields[1].Value.(btree.IntValue); ok && iv.V != 2 {
					t.Errorf("node%d row pk=%d: val=%d, want 2", nodeID, pkVal(row.Key), iv.V)
				}
			}
		}
	}
}

// B2: 3 ranges — delete all rows.
func TestSim2PC_B2_ThreeRanges_DeleteAll(t *testing.T) {
	c, _ := setup3NodeSplit(t, "br2", 34, 67)
	for i := 1; i <= 99; i++ {
		c.insert("br2", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	if err := execDelete(c, "br2", nil); err != nil {
		t.Fatalf("3-range DELETE: %v", err)
	}
	rs := c.selectRows("br2", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("rows after 3-range DELETE=%d, want 0", len(rs.Rows))
	}
}

// B3: 4 ranges — update all rows.
func TestSim2PC_B3_FourRanges_UpdateAll(t *testing.T) {
	c := newMultiNodeCluster(t, 4)
	tableId := setupTable(t, c, "br3", []string{"val"}, []string{"INT"})
	s1 := sqllayer.EncodeKey(tableId, 25)
	s2 := sqllayer.EncodeKey(tableId, 50)
	s3 := sqllayer.EncodeKey(tableId, 75)
	c.splitAtKey(s1, 1, 1)
	c.splitAtKey(s2, 2, 2)
	c.splitAtKey(s3, 3, 4)

	for i := 1; i <= 100; i++ {
		c.insert("br3", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	if err := execUpdate(c, "br3", "val", "5", nil); err != nil {
		t.Fatalf("4-range UPDATE: %v", err)
	}

	rs := c.selectRows("br3", []string{"id", "val"}, nil)
	if len(rs.Rows) != 100 {
		t.Fatalf("row count=%d, want 100", len(rs.Rows))
	}
	if sum := sumIntCol(rs, 1); sum != 500 {
		t.Errorf("sum=%d, want 500 (100 rows × 5)", sum)
	}
}

// B4: 5 ranges, all on separate nodes — full table update succeeds.
func TestSim2PC_B4_FiveRanges_UpdateAll(t *testing.T) {
	c := newMultiNodeCluster(t, 5)
	tableId := setupTable(t, c, "br4", []string{"val"}, []string{"INT"})
	splits := []int{20, 40, 60, 80}
	nodes := []uint64{1, 1, 2, 3, 4}
	for i, sp := range splits {
		key := sqllayer.EncodeKey(tableId, uint32(sp))
		c.splitAtKey(key, nodes[i], nodes[i+1])
	}

	for i := 1; i <= 100; i++ {
		c.insert("br4", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("3")})
	}

	if err := execUpdate(c, "br4", "val", "7", nil); err != nil {
		t.Fatalf("5-range UPDATE: %v", err)
	}

	rs := c.selectRows("br4", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 700 {
		t.Errorf("sum=%d, want 700 (100 rows × 7)", sum)
	}
}

// B5: both ranges on same node (co-located) — commit is idempotent.
func TestSim2PC_B5_ColocatedRanges_UpdateAll(t *testing.T) {
	c := newSimCluster(t) // single node
	tableId := setupTable(t, c, "br5", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 1)

	insertN(t, c, "br5", 100, intLitS("10"))

	if err := execUpdate(c, "br5", "val", "20", nil); err != nil {
		t.Fatalf("co-located UPDATE: %v", err)
	}

	rs := c.selectRows("br5", []string{"id", "val"}, nil)
	if len(rs.Rows) != 100 {
		t.Fatalf("row count=%d, want 100", len(rs.Rows))
	}
	if sum := sumIntCol(rs, 1); sum != 2000 {
		t.Errorf("sum=%d, want 2000 (100 rows × 20)", sum)
	}
}

// B6: 3 co-located ranges — commit idempotency handles all 3 Commit calls.
func TestSim2PC_B6_ThreeColocatedRanges_UpdateAll(t *testing.T) {
	c := newSimCluster(t)
	tableId := setupTable(t, c, "br6", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 33), 1, 1)
	c.splitAtKey(sqllayer.EncodeKey(tableId, 66), 1, 1)

	for i := 1; i <= 99; i++ {
		c.insert("br6", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("1")})
	}

	if err := execUpdate(c, "br6", "val", "3", nil); err != nil {
		t.Fatalf("3-colocated UPDATE: %v", err)
	}

	rs := c.selectRows("br6", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 297 { // 99 × 3
		t.Errorf("sum=%d, want 297", sum)
	}
}

// ---------------------------------------------------------------------------
// C. Failure injection
// ---------------------------------------------------------------------------

// C1: first range's Prepare fails — no ranges need aborting (nothing prepared).
func TestSim2PC_C1_FirstRangeFails_ZeroAborts(t *testing.T) {
	c, _ := setup2NodeSplit(t, "ci1", 50)
	insertN(t, c, "ci1", 100, intLitS("1"))

	c.nodes[1].mu.Lock()
	c.nodes[1].failPrepare = true
	c.nodes[1].mu.Unlock()

	err := execUpdate(c, "ci1", "val", "99", nil)

	c.nodes[1].mu.Lock()
	c.nodes[1].failPrepare = false
	c.nodes[1].mu.Unlock()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Data must be unchanged.
	rs := c.selectRows("ci1", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 100 {
		t.Errorf("sum=%d, want 100 (unchanged)", sum)
	}
}

// C2: second range's Prepare fails — first range's staged ops are aborted.
func TestSim2PC_C2_SecondRangeFails_FirstAborted(t *testing.T) {
	c, _ := setup2NodeSplit(t, "ci2", 50)
	insertN(t, c, "ci2", 100, intLitS("5"))

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	err := execUpdate(c, "ci2", "val", "99", nil)

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Node 1's pending was aborted — its data must also be unchanged.
	rows1 := scanNodeTable(t, c.nodes[1], c.sc.FindTableSchema("ci2").TableId)
	for _, row := range rows1 {
		if len(row.Fields) >= 2 {
			if iv, ok := row.Fields[1].Value.(btree.IntValue); ok && iv.V != 5 {
				t.Errorf("node1 row pk=%d: val=%d, want 5 (must be aborted)", pkVal(row.Key), iv.V)
			}
		}
	}
}

// C3: middle range (of 3) fails — first and third are aborted.
func TestSim2PC_C3_MiddleRangeFails_OthersTwoAborted(t *testing.T) {
	c, _ := setup3NodeSplit(t, "ci3", 34, 67)
	for i := 1; i <= 99; i++ {
		c.insert("ci3", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("1")})
	}

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	err := execUpdate(c, "ci3", "val", "9", nil)

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	if err == nil {
		t.Fatal("expected error")
	}
	rs := c.selectRows("ci3", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 99 { // all rows val=1, unchanged
		t.Errorf("sum=%d, want 99 (all ranges aborted)", sum)
	}
}

// C4: last range fails — all previously prepared ranges aborted.
func TestSim2PC_C4_LastRangeFails_AllPreparedAborted(t *testing.T) {
	c, _ := setup3NodeSplit(t, "ci4", 34, 67)
	for i := 1; i <= 99; i++ {
		c.insert("ci4", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("2")})
	}

	c.nodes[3].mu.Lock()
	c.nodes[3].failPrepare = true
	c.nodes[3].mu.Unlock()

	err := execUpdate(c, "ci4", "val", "8", nil)

	c.nodes[3].mu.Lock()
	c.nodes[3].failPrepare = false
	c.nodes[3].mu.Unlock()

	if err == nil {
		t.Fatal("expected error")
	}
	rs := c.selectRows("ci4", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 198 { // 99 × 2
		t.Errorf("sum=%d, want 198 (no rows updated)", sum)
	}
}

// C5: sequential prepare failures on the same cluster — each failed txn is
// independent and doesn't corrupt subsequent successful transactions.
func TestSim2PC_C5_FailThenSucceed_SubsequentTransactionClean(t *testing.T) {
	c, _ := setup2NodeSplit(t, "ci5", 50)
	insertN(t, c, "ci5", 100, intLitS("1"))

	// First attempt fails.
	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	_ = execUpdate(c, "ci5", "val", "99", nil)

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	// Second attempt must succeed.
	if err := execUpdate(c, "ci5", "val", "42", nil); err != nil {
		t.Fatalf("second UPDATE (should succeed): %v", err)
	}

	rs := c.selectRows("ci5", []string{"id", "val"}, nil)
	if len(rs.Rows) != 100 {
		t.Fatalf("row count=%d, want 100", len(rs.Rows))
	}
	if sum := sumIntCol(rs, 1); sum != 4200 {
		t.Errorf("sum=%d, want 4200 (100 rows × 42)", sum)
	}
}

// C6: alternating success/failure on 4 nodes.
func TestSim2PC_C6_FourNodeAlternatingFailure(t *testing.T) {
	c := newMultiNodeCluster(t, 4)
	tableId := setupTable(t, c, "ci6", []string{"val"}, []string{"INT"})
	s1 := sqllayer.EncodeKey(tableId, 25)
	s2 := sqllayer.EncodeKey(tableId, 50)
	s3 := sqllayer.EncodeKey(tableId, 75)
	c.splitAtKey(s1, 1, 1)
	c.splitAtKey(s2, 2, 2)
	c.splitAtKey(s3, 3, 4)

	for i := 1; i <= 100; i++ {
		c.insert("ci6", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	// Fail node 3.
	c.nodes[3].mu.Lock()
	c.nodes[3].failPrepare = true
	c.nodes[3].mu.Unlock()

	err := execUpdate(c, "ci6", "val", "1", nil)

	c.nodes[3].mu.Lock()
	c.nodes[3].failPrepare = false
	c.nodes[3].mu.Unlock()

	if err == nil {
		t.Fatal("expected error from node 3 failure")
	}

	// All rows still 0.
	rs := c.selectRows("ci6", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 0 {
		t.Errorf("sum=%d after failed UPDATE, want 0", sum)
	}
}

// ---------------------------------------------------------------------------
// D. WHERE-clause edge cases
// ---------------------------------------------------------------------------

// D1: WHERE matches zero rows across all ranges — transaction succeeds with no
// data change.
func TestSim2PC_D1_WhereMatchesNoRows_TransactionSucceeds(t *testing.T) {
	c, _ := setup2NodeSplit(t, "d1", 50)
	insertN(t, c, "d1", 100, intLitS("5"))

	// WHERE id = 999 matches nothing.
	if err := execUpdate(c, "d1", "val", "0", eqWhere("id", "999")); err != nil {
		t.Fatalf("UPDATE (no-match WHERE): %v", err)
	}

	rs := c.selectRows("d1", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 500 {
		t.Errorf("sum=%d, want 500 (no rows changed)", sum)
	}
}

// D2: WHERE narrows exactly to one range — that range runs 2PC, the other gets
// an empty prepare (no rows matched).
func TestSim2PC_D2_WhereNarrowsToOneRange_OtherRangeEmpty(t *testing.T) {
	c, _ := setup2NodeSplit(t, "d2", 50)
	insertN(t, c, "d2", 100, intLitS("1"))

	// WHERE id <= 40 only touches the lower range (ids 1-49 are on node 1).
	where := &sqllayer.ComparisonExpr{Column: "id", Operator: "<=", Value: intLitS("40")}
	if err := execUpdate(c, "d2", "val", "9", where); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	rs := c.selectRows("d2", []string{"id", "val"}, nil)
	// Rows 1-40 should be 9; rows 41-100 should be 1.
	for _, row := range rs.Rows {
		pk := pkVal(row.Key)
		if len(row.Fields) < 2 {
			continue
		}
		iv, ok := row.Fields[1].Value.(btree.IntValue)
		if !ok {
			continue
		}
		if pk <= 40 && iv.V != 9 {
			t.Errorf("pk=%d: val=%d, want 9", pk, iv.V)
		}
		if pk > 40 && iv.V != 1 {
			t.Errorf("pk=%d: val=%d, want 1", pk, iv.V)
		}
	}
}

// D3: WHERE spans the range boundary (straddles the split key).
func TestSim2PC_D3_WhereSpansBoundary(t *testing.T) {
	c, _ := setup2NodeSplit(t, "d3", 50)
	insertN(t, c, "d3", 100, intLitS("1"))

	// WHERE 45 <= id <= 55 spans the split at pk=50.
	where := rangeWhere("id", "45", "55")
	if err := execUpdate(c, "d3", "val", "99", where); err != nil {
		t.Fatalf("UPDATE spanning boundary: %v", err)
	}

	rs := c.selectRows("d3", []string{"id", "val"}, nil)
	for _, row := range rs.Rows {
		pk := pkVal(row.Key)
		if len(row.Fields) < 2 {
			continue
		}
		iv, ok := row.Fields[1].Value.(btree.IntValue)
		if !ok {
			continue
		}
		inRange := pk >= 45 && pk <= 55
		if inRange && iv.V != 99 {
			t.Errorf("pk=%d in [45,55]: val=%d, want 99", pk, iv.V)
		}
		if !inRange && iv.V != 1 {
			t.Errorf("pk=%d outside [45,55]: val=%d, want 1", pk, iv.V)
		}
	}
}

// D4: rows at exact split key (pk=splitPK is the first key of the upper range).
func TestSim2PC_D4_RowAtExactSplitKey(t *testing.T) {
	c, _ := setup2NodeSplit(t, "d4", 50)
	// Insert exactly at the split key.
	c.insert("d4", []sqllayer.Literal{intLitS("50"), intLitS("1")})
	c.insert("d4", []sqllayer.Literal{intLitS("49"), intLitS("1")})
	c.insert("d4", []sqllayer.Literal{intLitS("51"), intLitS("1")})

	// Update pk=50 exactly.
	if err := execUpdate(c, "d4", "val", "99", eqWhere("id", "50")); err != nil {
		t.Fatalf("UPDATE at split key: %v", err)
	}

	rs := c.selectRows("d4", []string{"id", "val"}, nil)
	if len(rs.Rows) != 3 {
		t.Fatalf("row count=%d, want 3", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		pk := pkVal(row.Key)
		if len(row.Fields) < 2 {
			continue
		}
		iv, ok := row.Fields[1].Value.(btree.IntValue)
		if !ok {
			continue
		}
		if pk == 50 && iv.V != 99 {
			t.Errorf("pk=50 (at split key): val=%d, want 99", iv.V)
		}
		if pk != 50 && iv.V != 1 {
			t.Errorf("pk=%d: val=%d, want 1 (should not change)", pk, iv.V)
		}
	}
}

// D5: DELETE WHERE spans boundary.
func TestSim2PC_D5_DeleteWhereSpansBoundary(t *testing.T) {
	c, _ := setup2NodeSplit(t, "d5", 50)
	insertN(t, c, "d5", 100, intLitS("0"))

	// DELETE rows 40-60, spanning the split at 50.
	if err := execDelete(c, "d5", rangeWhere("id", "40", "60")); err != nil {
		t.Fatalf("DELETE spanning boundary: %v", err)
	}

	rs := c.selectRows("d5", []string{"id"}, nil)
	pks := sortedPKs(rs)
	if len(pks) != 79 { // 100 - 21 rows deleted (40-60 inclusive)
		t.Errorf("row count=%d, want 79", len(pks))
	}
	for _, pk := range pks {
		if pk >= 40 && pk <= 60 {
			t.Errorf("pk=%d should have been deleted", pk)
		}
	}
}

// D6: AND-WHERE across ranges.
func TestSim2PC_D6_AndWhereAcrossRanges(t *testing.T) {
	c, _ := setup2NodeSplit(t, "d6", 50)
	insertN(t, c, "d6", 100, intLitS("1"))

	// WHERE id >= 30 AND id <= 70 spans both ranges.
	where := rangeWhere("id", "30", "70")
	if err := execUpdate(c, "d6", "val", "5", where); err != nil {
		t.Fatalf("AND-WHERE UPDATE: %v", err)
	}

	rs := c.selectRows("d6", []string{"id", "val"}, nil)
	for _, row := range rs.Rows {
		pk := pkVal(row.Key)
		if len(row.Fields) < 2 {
			continue
		}
		iv, ok := row.Fields[1].Value.(btree.IntValue)
		if !ok {
			continue
		}
		inRange := pk >= 30 && pk <= 70
		if inRange && iv.V != 5 {
			t.Errorf("pk=%d: val=%d, want 5", pk, iv.V)
		}
		if !inRange && iv.V != 1 {
			t.Errorf("pk=%d: val=%d, want 1", pk, iv.V)
		}
	}
}

// D7: DELETE with AND-WHERE on 3 ranges — only middle range rows removed.
func TestSim2PC_D7_ThreeRanges_DeleteMiddle(t *testing.T) {
	c, _ := setup3NodeSplit(t, "d7", 34, 67)
	for i := 1; i <= 99; i++ {
		c.insert("d7", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	// Delete rows 34-66 (all in the middle range [34,67)).
	if err := execDelete(c, "d7", rangeWhere("id", "34", "66")); err != nil {
		t.Fatalf("DELETE middle: %v", err)
	}

	rs := c.selectRows("d7", []string{"id"}, nil)
	pks := sortedPKs(rs)
	for _, pk := range pks {
		if pk >= 34 && pk <= 66 {
			t.Errorf("pk=%d should have been deleted", pk)
		}
	}
	// 99 total - 33 deleted = 66 remaining
	if len(pks) != 66 {
		t.Errorf("remaining row count=%d, want 66", len(pks))
	}
}

// D8: string column update via 2PC.
func TestSim2PC_D8_StringColumnUpdate(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	tableId := setupTable(t, c, "d8", []string{"name"}, []string{"TEXT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)

	for i := 1; i <= 100; i++ {
		c.insert("d8", []sqllayer.Literal{intLitS(strconv.Itoa(i)),
			sqllayer.Literal{Value: "old", Type: sqllayer.TOKEN_STRING}})
	}

	if err := execUpdateStr(c, "d8", "name", "new", nil); err != nil {
		t.Fatalf("string UPDATE: %v", err)
	}

	rs := c.selectRows("d8", []string{"id", "name"}, nil)
	vals := strColValues(rs, 1)
	if vals["new"] != 100 || vals["old"] != 0 {
		t.Errorf("string values after UPDATE: %v, want all 'new'", vals)
	}
}

// ---------------------------------------------------------------------------
// E. Sequential dependent transactions
// ---------------------------------------------------------------------------

// E1: insert via single-range Execute, then update via 2PC, then select.
func TestSim2PC_E1_InsertThenUpdate_ValueVisible(t *testing.T) {
	c, _ := setup2NodeSplit(t, "e1", 50)

	// Phase 1: insert.
	for i := 1; i <= 50; i++ {
		c.insert("e1", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("10")})
	}
	for i := 51; i <= 100; i++ {
		c.insert("e1", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("20")})
	}

	// Phase 2: 2PC update — set all to 15.
	if err := execUpdate(c, "e1", "val", "15", nil); err != nil {
		t.Fatalf("phase-2 UPDATE: %v", err)
	}

	// Phase 3: verify.
	rs := c.selectRows("e1", []string{"id", "val"}, nil)
	if len(rs.Rows) != 100 {
		t.Fatalf("row count=%d, want 100", len(rs.Rows))
	}
	if sum := sumIntCol(rs, 1); sum != 1500 {
		t.Errorf("sum=%d, want 1500 (100 × 15)", sum)
	}
}

// E2: multiple sequential updates accumulate correctly.
func TestSim2PC_E2_SequentialUpdates_AccumulateCorrectly(t *testing.T) {
	c, _ := setup2NodeSplit(t, "e2", 50)
	insertN(t, c, "e2", 100, intLitS("0"))

	for round, val := range []string{"1", "2", "3", "4", "5"} {
		if err := execUpdate(c, "e2", "val", val, nil); err != nil {
			t.Fatalf("round %d UPDATE: %v", round, err)
		}
	}

	rs := c.selectRows("e2", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 500 { // 100 rows × final value 5
		t.Errorf("sum after 5 sequential updates=%d, want 500", sum)
	}
}

// E3: insert, 2PC delete of half, verify remaining rows.
func TestSim2PC_E3_InsertDeleteHalf_HalfRemains(t *testing.T) {
	c, _ := setup2NodeSplit(t, "e3", 50)
	insertN(t, c, "e3", 100, intLitS("1"))

	// Delete even-numbered rows across both ranges.
	// Even rows: 2, 4, 6, ..., 100 → 50 rows.
	// But WHERE on id % 2 is not supported directly; use two passes or rangeWhere.
	// Instead, DELETE rows 1-50 (lower range).
	if err := execDelete(c, "e3", rangeWhere("id", "1", "50")); err != nil {
		t.Fatalf("DELETE lower half: %v", err)
	}

	rs := c.selectRows("e3", []string{"id"}, nil)
	if len(rs.Rows) != 50 {
		t.Fatalf("row count=%d, want 50", len(rs.Rows))
	}
	for _, pk := range sortedPKs(rs) {
		if pk <= 50 {
			t.Errorf("pk=%d should have been deleted", pk)
		}
	}
}

// E4: update then immediately read — reads the committed state.
func TestSim2PC_E4_UpdateReadConsistency(t *testing.T) {
	c, _ := setup2NodeSplit(t, "e4", 50)
	insertN(t, c, "e4", 100, intLitS("100"))

	if err := execUpdate(c, "e4", "val", "200", nil); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	// Immediately read; no intermediate state should be visible.
	rs := c.selectRows("e4", []string{"id", "val"}, nil)
	for _, row := range rs.Rows {
		if len(row.Fields) >= 2 {
			if iv, ok := row.Fields[1].Value.(btree.IntValue); ok && iv.V != 200 {
				t.Errorf("pk=%d: val=%d, want 200 (stale read)", pkVal(row.Key), iv.V)
			}
		}
	}
}

// E5: chain — insert across 3 ranges, update with WHERE, delete remainder.
func TestSim2PC_E5_InsertUpdateDelete_Chain(t *testing.T) {
	c, _ := setup3NodeSplit(t, "e5", 34, 67)

	for i := 1; i <= 99; i++ {
		c.insert("e5", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("1")})
	}

	// Step 2: update rows 1-50 to val=2.
	if err := execUpdate(c, "e5", "val", "2", rangeWhere("id", "1", "50")); err != nil {
		t.Fatalf("step-2 UPDATE: %v", err)
	}
	// Step 3: delete rows val=1 (i.e., rows 51-99 still have val=1).
	// We can't filter by val in WHERE (only PK), so delete by range instead.
	if err := execDelete(c, "e5", rangeWhere("id", "51", "99")); err != nil {
		t.Fatalf("step-3 DELETE: %v", err)
	}

	rs := c.selectRows("e5", []string{"id", "val"}, nil)
	if len(rs.Rows) != 50 {
		t.Fatalf("final row count=%d, want 50", len(rs.Rows))
	}
	if sum := sumIntCol(rs, 1); sum != 100 { // 50 rows × 2
		t.Errorf("sum=%d, want 100", sum)
	}
}

// ---------------------------------------------------------------------------
// F. Concurrent stress tests
// ---------------------------------------------------------------------------

// F1: N independent 2PC update transactions on N distinct tables — all succeed.
func TestSim2PC_F1_ConcurrentIndependentTables_AllSucceed(t *testing.T) {
	const N = 20
	c := newMultiNodeCluster(t, 2)

	// Create N tables, each with a split at pk=50.
	tableIDs := make([]uint32, N)
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("f1t%d", i)
		tableIDs[i] = setupTable(t, c, name, []string{"val"}, []string{"INT"})
		c.splitAtKey(sqllayer.EncodeKey(tableIDs[i], 50), 1, 2)
		insertN(t, c, name, 10, intLitS("0"))
	}

	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("f1t%d", idx)
			errs[idx] = execUpdate(c, name, "val", strconv.Itoa(idx+1), nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("table f1t%d: %v", i, err)
		}
	}
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("f1t%d", i)
		rs := c.selectRows(name, []string{"id", "val"}, nil)
		wantVal := int64(i + 1)
		for _, row := range rs.Rows {
			if len(row.Fields) >= 2 {
				if iv, ok := row.Fields[1].Value.(btree.IntValue); ok && iv.V != wantVal {
					t.Errorf("table %s pk=%d: val=%d, want %d", name, pkVal(row.Key), iv.V, wantVal)
				}
			}
		}
	}
}

// F2: N concurrent DELETEs on separate tables — all succeed.
func TestSim2PC_F2_ConcurrentDeletes_AllSucceed(t *testing.T) {
	const N = 15
	c := newMultiNodeCluster(t, 2)

	for i := 0; i < N; i++ {
		name := fmt.Sprintf("f2t%d", i)
		tableId := setupTable(t, c, name, []string{"val"}, []string{"INT"})
		c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)
		insertN(t, c, name, 20, intLitS("1"))
	}

	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = execDelete(c, fmt.Sprintf("f2t%d", idx), nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("f2t%d DELETE: %v", i, err)
		}
	}
	for i := 0; i < N; i++ {
		rs := c.selectRows(fmt.Sprintf("f2t%d", i), []string{"id"}, nil)
		if len(rs.Rows) != 0 {
			t.Errorf("f2t%d: %d rows after DELETE, want 0", i, len(rs.Rows))
		}
	}
}

// F3: 30 concurrent updates on the same table (last-write-wins) — no errors,
// correct row count preserved.
func TestSim2PC_F3_ConcurrentUpdates_SameTable_NoErrors(t *testing.T) {
	const N = 30
	c := newMultiNodeCluster(t, 2)
	tableId := setupTable(t, c, "f3", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)
	insertN(t, c, "f3", 100, intLitS("0"))

	var errs [N]error
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = execUpdate(c, "f3", "val", strconv.Itoa(idx), nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent UPDATE[%d]: %v", i, err)
		}
	}
	// Row count must still be exactly 100.
	rs := c.selectRows("f3", []string{"id"}, nil)
	if len(rs.Rows) != 100 {
		t.Errorf("row count=%d after concurrent updates, want 100", len(rs.Rows))
	}
}

// F4: interleaved concurrent updates and selects — reads always see 100 rows.
func TestSim2PC_F4_ConcurrentUpdatesAndSelects_RowCountStable(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	tableId := setupTable(t, c, "f4", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)
	insertN(t, c, "f4", 100, intLitS("1"))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(v int) {
			defer wg.Done()
			_ = execUpdate(c, "f4", "val", strconv.Itoa(v), nil)
		}(i)
		go func() {
			defer wg.Done()
			rs := c.selectRows("f4", []string{"id"}, nil)
			// Row count should always be 100; updates don't insert or delete.
			if len(rs.Rows) != 100 {
				t.Errorf("concurrent read: row count=%d, want 100", len(rs.Rows))
			}
		}()
	}
	wg.Wait()
}

// F5: mixed concurrent ops on separate key partitions — updates and deletes on
// non-overlapping rows are both visible after all goroutines finish.
func TestSim2PC_F5_ConcurrentMixedOps_NonOverlapping(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	tableId := setupTable(t, c, "f5", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)
	insertN(t, c, "f5", 100, intLitS("1"))

	var wg sync.WaitGroup
	// Goroutine A: update rows 1-30 to val=99.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = execUpdate(c, "f5", "val", "99", rangeWhere("id", "1", "30"))
	}()
	// Goroutine B: delete rows 71-100.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = execDelete(c, "f5", rangeWhere("id", "71", "100"))
	}()
	wg.Wait()

	rs := c.selectRows("f5", []string{"id", "val"}, nil)
	// After both: rows 31-70 remain with val=1; rows 1-30 with val=99;
	// rows 71-100 gone.
	for _, row := range rs.Rows {
		pk := pkVal(row.Key)
		if pk >= 71 && pk <= 100 {
			t.Errorf("pk=%d should have been deleted", pk)
		}
	}
}

// F6: stress — 50 goroutines each update a separate table spanning 2 ranges,
// verify all 50 tables end with the correct data.
func TestSim2PC_F6_Stress50Tables_AllCorrect(t *testing.T) {
	const N = 50
	c := newMultiNodeCluster(t, 2)

	// Pre-create all tables to avoid concurrent schema mutations.
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("f6t%d", i)
		tableId := setupTable(t, c, name, []string{"score"}, []string{"INT"})
		c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)
		insertN(t, c, name, 10, intLitS("0"))
	}

	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("f6t%d", idx)
			errs[idx] = execUpdate(c, name, "score", strconv.Itoa(100+idx), nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("f6t%d: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		name := fmt.Sprintf("f6t%d", i)
		rs := c.selectRows(name, []string{"id", "score"}, nil)
		want := int64(100 + i)
		for _, row := range rs.Rows {
			if len(row.Fields) >= 2 {
				if iv, ok := row.Fields[1].Value.(btree.IntValue); ok && iv.V != want {
					t.Errorf("%s pk=%d: score=%d, want %d", name, pkVal(row.Key), iv.V, want)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// G. Large transactions
// ---------------------------------------------------------------------------

// G1: 500 rows across 3 ranges — bulk update.
func TestSim2PC_G1_BulkUpdate_500Rows_ThreeRanges(t *testing.T) {
	c, _ := setup3NodeSplit(t, "g1", 167, 334)

	for i := 1; i <= 500; i++ {
		c.insert("g1", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("1")})
	}

	if err := execUpdate(c, "g1", "val", "2", nil); err != nil {
		t.Fatalf("bulk UPDATE: %v", err)
	}

	rs := c.selectRows("g1", []string{"id", "val"}, nil)
	if len(rs.Rows) != 500 {
		t.Fatalf("row count=%d, want 500", len(rs.Rows))
	}
	if sum := sumIntCol(rs, 1); sum != 1000 {
		t.Errorf("sum=%d, want 1000 (500 × 2)", sum)
	}
}

// G2: 500 rows across 2 ranges — bulk delete.
func TestSim2PC_G2_BulkDelete_500Rows_TwoRanges(t *testing.T) {
	c, _ := setup2NodeSplit(t, "g2", 250)

	for i := 1; i <= 500; i++ {
		c.insert("g2", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	if err := execDelete(c, "g2", nil); err != nil {
		t.Fatalf("bulk DELETE: %v", err)
	}

	rs := c.selectRows("g2", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("rows after bulk DELETE=%d, want 0", len(rs.Rows))
	}
}

// G3: high row count with WHERE — only matching rows changed, rest intact.
func TestSim2PC_G3_LargeTable_SparseWhere(t *testing.T) {
	c, _ := setup2NodeSplit(t, "g3", 500)

	for i := 1; i <= 1000; i++ {
		c.insert("g3", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("1")})
	}

	// Update only rows 490-510 (spanning the split at pk=500).
	if err := execUpdate(c, "g3", "val", "7", rangeWhere("id", "490", "510")); err != nil {
		t.Fatalf("sparse WHERE UPDATE: %v", err)
	}

	rs := c.selectRows("g3", []string{"id", "val"}, nil)
	if len(rs.Rows) != 1000 {
		t.Fatalf("row count=%d, want 1000", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		pk := pkVal(row.Key)
		if len(row.Fields) < 2 {
			continue
		}
		iv, ok := row.Fields[1].Value.(btree.IntValue)
		if !ok {
			continue
		}
		inRange := pk >= 490 && pk <= 510
		if inRange && iv.V != 7 {
			t.Errorf("pk=%d: val=%d, want 7", pk, iv.V)
		}
		if !inRange && iv.V != 1 {
			t.Errorf("pk=%d: val=%d, want 1", pk, iv.V)
		}
	}
}

// ---------------------------------------------------------------------------
// H. Special scenarios
// ---------------------------------------------------------------------------

// H1: two independent tables coexist on the same nodes — operations on one do
// not affect the other.
func TestSim2PC_H1_TwoTableIsolation(t *testing.T) {
	c := newMultiNodeCluster(t, 2)

	idA := setupTable(t, c, "ta", []string{"val"}, []string{"INT"})
	idB := setupTable(t, c, "tb", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(idA, 50), 1, 2)
	c.splitAtKey(sqllayer.EncodeKey(idB, 50), 1, 2)

	insertN(t, c, "ta", 100, intLitS("1"))
	insertN(t, c, "tb", 100, intLitS("2"))

	// Update only table A.
	if err := execUpdate(c, "ta", "val", "9", nil); err != nil {
		t.Fatalf("UPDATE ta: %v", err)
	}

	// Table B must be unchanged.
	rsB := c.selectRows("tb", []string{"id", "val"}, nil)
	if sum := sumIntCol(rsB, 1); sum != 200 { // 100 × 2
		t.Errorf("tb sum=%d after UPDATE ta, want 200 (table isolation violated)", sum)
	}
}

// H2: DropTable across 2 ranges — all data deleted from both nodes' BTrees.
// The simulation's schema catalog has a separate BTree from the data BTrees, so
// schema removal is not verified here; the 2PC contract is "all data deleted".
func TestSim2PC_H2_DropTable_TwoRanges(t *testing.T) {
	c, tableId := setup2NodeSplit(t, "h2", 50)
	insertN(t, c, "h2", 100, intLitS("1"))

	if _, err := c.gw.Execute(&sqllayer.DropTableStatement{Table: "h2"}); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	// Verify all data rows deleted from each node's BTree.
	for nodeID, node := range c.nodes {
		rows := scanNodeTable(t, node, tableId)
		if len(rows) != 0 {
			t.Errorf("node%d: %d data rows remain after DropTable, want 0", nodeID, len(rows))
		}
	}
}

// H3: DropTable on a 3-range table — all data gone from all 3 nodes.
func TestSim2PC_H3_DropTable_ThreeRanges(t *testing.T) {
	c, tableId := setup3NodeSplit(t, "h3", 34, 67)
	for i := 1; i <= 99; i++ {
		c.insert("h3", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	if _, err := c.gw.Execute(&sqllayer.DropTableStatement{Table: "h3"}); err != nil {
		t.Fatalf("DropTable (3 ranges): %v", err)
	}

	for nodeID, node := range c.nodes {
		rows := scanNodeTable(t, node, tableId)
		if len(rows) != 0 {
			t.Errorf("node%d: %d data rows remain after 3-range DropTable, want 0", nodeID, len(rows))
		}
	}
}

// H4: empty table — 2PC update and delete succeed with no side-effects.
func TestSim2PC_H4_EmptyTable_UpdateAndDelete_Succeed(t *testing.T) {
	c, _ := setup2NodeSplit(t, "h4", 50)
	// No rows inserted.

	if err := execUpdate(c, "h4", "val", "5", nil); err != nil {
		t.Fatalf("UPDATE empty table: %v", err)
	}
	if err := execDelete(c, "h4", nil); err != nil {
		t.Fatalf("DELETE empty table: %v", err)
	}

	rs := c.selectRows("h4", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("rows=%d after ops on empty table, want 0", len(rs.Rows))
	}
}

// H5: fail, retry, then commit — the retry correctly re-applies all ops.
func TestSim2PC_H5_FailRetryCommit_DataCorrect(t *testing.T) {
	c, _ := setup2NodeSplit(t, "h5", 50)
	insertN(t, c, "h5", 100, intLitS("1"))

	// First try fails (node 2 rejects).
	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	if err := execUpdate(c, "h5", "val", "99", nil); err == nil {
		t.Fatal("first attempt should fail")
	}

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	// Retry succeeds.
	if err := execUpdate(c, "h5", "val", "99", nil); err != nil {
		t.Fatalf("retry UPDATE: %v", err)
	}

	rs := c.selectRows("h5", []string{"id", "val"}, nil)
	if sum := sumIntCol(rs, 1); sum != 9900 {
		t.Errorf("sum=%d after retry commit, want 9900 (100 × 99)", sum)
	}
}

// H6: concurrent 2PC transactions on the same table with some failing —
// row count is consistent (no phantom rows or lost rows).
func TestSim2PC_H6_ConcurrentSomeFailSomeSucceed_RowCountConsistent(t *testing.T) {
	const N = 20
	c := newMultiNodeCluster(t, 2)
	tableId := setupTable(t, c, "h6", []string{"val"}, []string{"INT"})
	c.splitAtKey(sqllayer.EncodeKey(tableId, 50), 1, 2)
	insertN(t, c, "h6", 100, intLitS("0"))

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Even-indexed goroutines trigger failure on node 2.
			if idx%2 == 0 {
				c.nodes[2].mu.Lock()
				c.nodes[2].failPrepare = true
				c.nodes[2].mu.Unlock()
				_ = execUpdate(c, "h6", "val", strconv.Itoa(idx), nil)
				c.nodes[2].mu.Lock()
				c.nodes[2].failPrepare = false
				c.nodes[2].mu.Unlock()
			} else {
				_ = execUpdate(c, "h6", "val", strconv.Itoa(idx), nil)
			}
		}(i)
	}
	wg.Wait()

	// Row count must still be exactly 100 regardless of which transactions
	// succeeded or failed.
	rs := c.selectRows("h6", []string{"id"}, nil)
	if len(rs.Rows) != 100 {
		t.Errorf("row count=%d after concurrent mixed-failure transactions, want 100", len(rs.Rows))
	}
}
