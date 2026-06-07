package partition

// Full-stack tests: Gateway → gRPC (bufconn) → real RangeServer → BTree.
//
// Every test uses partition.NewRangeServer as the gRPC server — no test doubles.
// This is the class of test that catches bugs invisible to simRangeServer-backed
// tests: unimplemented RPCs, proto encoding gaps, missing column headers, etc.
//
// Organisation:
//   Infra    — cluster helpers, assertion helpers
//   CRUD     — CREATE TABLE, INSERT, SELECT, UPDATE, DELETE fundamentals
//   Select   — column projection, SELECT *, column headers, ColTypes
//   Where    — all operators (=,!=,<,<=,>,>=), AND/OR, non-PK scan
//   Schema   — DDL edge cases, schema visibility, error paths
//   Types    — INT and TEXT edge values
//   Isolation — multi-table, independent data
//   Regression — one test per bug we've already shipped
//   2PC      — multi-range with shared schema; atomicity; 2- and 3-node
//   Stress   — concurrent operations under the real stack

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// =============================================================================
// Infrastructure
// =============================================================================

type realNode struct {
	bt      *btree.BTree
	sc      *sqllayer.SchemaCatalog
	records *TxnRecordStore
	srv     *RangeServer
}

// newRealNode creates one self-contained node whose single BTree stores both
// schema entries and data rows (matching production layout).  The applyFn
// reloads the SchemaCatalog after every schema-key write so CREATE TABLE is
// immediately visible.
func newRealNode(t *testing.T) *realNode {
	t.Helper()
	return newRealNodeWithSchema(t, nil)
}

// newRealNodeWithSchema creates a node that uses the supplied SchemaCatalog (or
// creates its own if sc == nil).  Sharing a catalog across nodes lets 2PC
// WHERE expressions resolve column names on every node.
func newRealNodeWithSchema(t *testing.T, sc *sqllayer.SchemaCatalog) *realNode {
	t.Helper()
	bt := newTestBTreeGateway(t)
	if sc == nil {
		sc = sqllayer.NewSchemaCatalog(bt)
	}
	records := NewTxnRecordStore()

	var applyFn lock.ApplyFn = func(op raft.ReplOp, key uint64, fields []btree.Field) error {
		switch op {
		case raft.ReplTxnRecord:
			records.Store(DecodeTxnRecord(key, fields))
		case raft.ReplPut:
			if err := bt.Insert(key, fields); err != nil {
				return err
			}
		case raft.ReplDelete:
			if err := bt.Delete(key); err != nil {
				return err
			}
		}
		if key>>32 == 0 { // schema-table key: reload catalog
			return sc.LoadSchemas()
		}
		return nil
	}

	tm := lock.NewTransactionManager(bt, nil, applyFn)
	srv := NewRangeServer(bt, tm, sc, records, nil, nil)
	return &realNode{bt: bt, sc: sc, records: records, srv: srv}
}

func startRealNode(t *testing.T, node *realNode) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	rs.RegisterRangeServiceServer(grpcSrv, node.srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// newSingleNodeRealGateway returns a Gateway backed by one real RangeServer.
func newSingleNodeRealGateway(t *testing.T) (*Gateway, *sqllayer.SchemaCatalog) {
	t.Helper()
	node := newRealNode(t)
	conn := startRealNode(t, node)

	coord := NewCoordinator(map[uint64]string{1: "node1"})
	coord.UpdateLeader(1, 1)
	router, err := NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	gw := NewGateway(router, node.sc)
	gw.AddConn(1, conn)
	return gw, node.sc
}

// newTwoNodeRealGateway returns a Gateway split at splitKey.
// Both nodes share one SchemaCatalog so WHERE expressions work on either node.
func newTwoNodeRealGateway(t *testing.T, splitKey uint64) (*Gateway, *sqllayer.SchemaCatalog) {
	t.Helper()
	n1 := newRealNode(t)
	n2 := newRealNodeWithSchema(t, n1.sc) // share catalog
	conn1 := startRealNode(t, n1)
	conn2 := startRealNode(t, n2)

	coord := NewCoordinator(map[uint64]string{1: "n1", 2: "n2"})
	if err := coord.RequestSplit(1, splitKey); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	for _, rd := range coord.GetAllRanges() {
		if rd.EndKey == splitKey {
			coord.UpdateLeader(rd.RangeID, 1)
		} else {
			coord.UpdateLeader(rd.RangeID, 2)
		}
	}
	router, err := NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	gw := NewGateway(router, n1.sc)
	gw.AddConn(1, conn1)
	gw.AddConn(2, conn2)
	return gw, n1.sc
}

// newThreeNodeRealGateway returns a Gateway split at lo and hi across 3 nodes.
func newThreeNodeRealGateway(t *testing.T, lo, hi uint64) (*Gateway, *sqllayer.SchemaCatalog) {
	t.Helper()
	n1 := newRealNode(t)
	n2 := newRealNodeWithSchema(t, n1.sc)
	n3 := newRealNodeWithSchema(t, n1.sc)
	conn1 := startRealNode(t, n1)
	conn2 := startRealNode(t, n2)
	conn3 := startRealNode(t, n3)

	coord := NewCoordinator(map[uint64]string{1: "n1", 2: "n2", 3: "n3"})
	if err := coord.RequestSplit(1, lo); err != nil {
		t.Fatalf("split1: %v", err)
	}
	// Assign leaders after first split
	for _, rd := range coord.GetAllRanges() {
		if rd.EndKey == lo {
			coord.UpdateLeader(rd.RangeID, 1)
		} else {
			coord.UpdateLeader(rd.RangeID, 1) // temporarily all on node 1
		}
	}
	// Find the upper range and split it again
	upper, err := coord.LookupKey(lo), error(nil)
	_ = err
	if upper == nil {
		t.Fatal("upper range not found after first split")
	}
	if err := coord.RequestSplit(upper.RangeID, hi); err != nil {
		t.Fatalf("split2: %v", err)
	}
	for _, rd := range coord.GetAllRanges() {
		switch {
		case rd.EndKey == lo:
			coord.UpdateLeader(rd.RangeID, 1)
		case rd.StartKey == lo && rd.EndKey == hi:
			coord.UpdateLeader(rd.RangeID, 2)
		default:
			coord.UpdateLeader(rd.RangeID, 3)
		}
	}
	router, err2 := NewRouter(coord)
	if err2 != nil {
		t.Fatalf("NewRouter: %v", err2)
	}

	gw := NewGateway(router, n1.sc)
	gw.AddConn(1, conn1)
	gw.AddConn(2, conn2)
	gw.AddConn(3, conn3)
	return gw, n1.sc
}

// ---------------------------------------------------------------------------
// clusterT — thin wrapper for cleaner test code
// ---------------------------------------------------------------------------

type clusterT struct {
	t  *testing.T
	gw *Gateway
	sc *sqllayer.SchemaCatalog
}

func newClusterT(t *testing.T) *clusterT {
	gw, sc := newSingleNodeRealGateway(t)
	return &clusterT{t: t, gw: gw, sc: sc}
}

func (c *clusterT) createTable(name string, cols ...sqllayer.ColumnDef) {
	c.t.Helper()
	mustExec(c.t, c.gw, &sqllayer.CreateTableStatement{Table: name, Columns: cols})
}

func (c *clusterT) insert(table string, vals ...sqllayer.Literal) {
	c.t.Helper()
	mustExec(c.t, c.gw, &sqllayer.InsertStatement{Table: table, Values: vals})
}

func (c *clusterT) update(table, col string, val sqllayer.Literal, where sqllayer.Expression) {
	c.t.Helper()
	mustExec(c.t, c.gw, &sqllayer.UpdateStatement{Table: table, Column: col, Value: val, Where: where})
}

func (c *clusterT) delete(table string, where sqllayer.Expression) {
	c.t.Helper()
	mustExec(c.t, c.gw, &sqllayer.DeleteStatement{Table: table, Where: where})
}

func (c *clusterT) query(table string, cols []string, where sqllayer.Expression) *ResultSet {
	c.t.Helper()
	return mustSelect(c.t, c.gw, table, cols, where)
}

func (c *clusterT) execErr(stmt sqllayer.Statement) error {
	c.t.Helper()
	_, err := c.gw.Execute(stmt)
	return err
}

// ---------------------------------------------------------------------------
// Assertion and value helpers
// ---------------------------------------------------------------------------

func assertCount(t *testing.T, rs *ResultSet, want int) {
	t.Helper()
	if len(rs.Rows) != want {
		t.Fatalf("row count=%d, want %d", len(rs.Rows), want)
	}
}

func assertColumns(t *testing.T, rs *ResultSet, want ...string) {
	t.Helper()
	if len(rs.Columns) != len(want) {
		t.Fatalf("Columns=%v, want %v", rs.Columns, want)
	}
	for i, w := range want {
		if rs.Columns[i] != w {
			t.Errorf("Columns[%d]=%q, want %q", i, rs.Columns[i], w)
		}
	}
}

func assertColTypes(t *testing.T, rs *ResultSet, want ...string) {
	t.Helper()
	if len(rs.ColTypes) != len(want) {
		t.Fatalf("ColTypes=%v, want %v", rs.ColTypes, want)
	}
	for i, w := range want {
		if rs.ColTypes[i] != w {
			t.Errorf("ColTypes[%d]=%q, want %q", i, rs.ColTypes[i], w)
		}
	}
}

func getInt(t *testing.T, rs *ResultSet, row, col int) int64 {
	t.Helper()
	if row >= len(rs.Rows) {
		t.Fatalf("row %d out of range (have %d)", row, len(rs.Rows))
	}
	fields := rs.Rows[row].Fields
	if col >= len(fields) {
		t.Fatalf("col %d out of range for row %d (have %d)", col, row, len(fields))
	}
	iv, ok := fields[col].Value.(btree.IntValue)
	if !ok {
		t.Fatalf("[%d][%d]: not IntValue, got %T", row, col, fields[col].Value)
	}
	return iv.V
}

func getStr(t *testing.T, rs *ResultSet, row, col int) string {
	t.Helper()
	if row >= len(rs.Rows) {
		t.Fatalf("row %d out of range (have %d)", row, len(rs.Rows))
	}
	fields := rs.Rows[row].Fields
	if col >= len(fields) {
		t.Fatalf("col %d out of range for row %d (have %d)", col, row, len(fields))
	}
	sv, ok := fields[col].Value.(btree.StringValue)
	if !ok {
		t.Fatalf("[%d][%d]: not StringValue, got %T", row, col, fields[col].Value)
	}
	return sv.V
}

// colInts returns all integer values in column colIdx, sorted ascending.
func colInts(rs *ResultSet, colIdx int) []int64 {
	out := make([]int64, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if colIdx < len(row.Fields) {
			if iv, ok := row.Fields[colIdx].Value.(btree.IntValue); ok {
				out = append(out, iv.V)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sumCol(rs *ResultSet, colIdx int) int64 {
	var s int64
	for _, row := range rs.Rows {
		if colIdx < len(row.Fields) {
			if iv, ok := row.Fields[colIdx].Value.(btree.IntValue); ok {
				s += iv.V
			}
		}
	}
	return s
}

func assertIntsEqual(t *testing.T, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("int slice length: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d]: got %d, want %d", i, got[i], want[i])
		}
	}
}

// Literal shortcuts
func n(v int) sqllayer.Literal { return intLit(itoa(v)) }
func s(v string) sqllayer.Literal {
	return sqllayer.Literal{Value: v, Type: sqllayer.TOKEN_STRING}
}

// Expression shortcuts
func eq(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: "=", Value: intLit(val)}
}
func neq(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: "!=", Value: intLit(val)}
}
func lt(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: "<", Value: intLit(val)}
}
func lte(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: "<=", Value: intLit(val)}
}
func gt(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: ">", Value: intLit(val)}
}
func gte(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: ">=", Value: intLit(val)}
}
func eqStr(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: "=",
		Value: sqllayer.Literal{Value: val, Type: sqllayer.TOKEN_STRING}}
}
func andE(l, r sqllayer.Expression) sqllayer.Expression {
	return &sqllayer.LogicalExpr{Operator: "AND", Left: l, Right: r}
}
func orE(l, r sqllayer.Expression) sqllayer.Expression {
	return &sqllayer.LogicalExpr{Operator: "OR", Left: l, Right: r}
}

// Standard table defs
var intIntCols = []sqllayer.ColumnDef{{Name: "id", DataType: "INT"}, {Name: "v", DataType: "INT"}}
var intStrCols = []sqllayer.ColumnDef{{Name: "id", DataType: "INT"}, {Name: "name", DataType: "TEXT"}}

// mustExec runs stmt and fatals on error.
func mustExec(t *testing.T, gw *Gateway, stmt sqllayer.Statement) {
	t.Helper()
	if _, err := gw.Execute(stmt); err != nil {
		t.Fatalf("%T: %v", stmt, err)
	}
}

// mustSelect runs a SELECT and fatals on error.
func mustSelect(t *testing.T, gw *Gateway, table string, cols []string, where sqllayer.Expression) *ResultSet {
	t.Helper()
	rs, err := gw.Execute(&sqllayer.SelectStatement{Table: table, Columns: cols, Where: where})
	if err != nil {
		t.Fatalf("SELECT from %q: %v", table, err)
	}
	return rs
}

func itoa(n int) string { return strconv.Itoa(n) }

// =============================================================================
// CRUD Fundamentals
// =============================================================================

func TestFS_CRUD_CreateTable_SchemaVisibleImmediately(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	if c.sc.FindTableSchema("tbl") == nil {
		t.Fatal("schema not visible after CREATE TABLE (applyFn must call LoadSchemas)")
	}
}

func TestFS_CRUD_Insert_RowReturnedBySelect(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(42))

	rs := c.query("tbl", []string{"id", "v"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 1 || getInt(t, rs, 0, 1) != 42 {
		t.Errorf("row=[%d,%d], want [1,42]", getInt(t, rs, 0, 0), getInt(t, rs, 0, 1))
	}
}

func TestFS_CRUD_InsertMultiple_AllRowsVisible(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 20; i++ {
		c.insert("tbl", n(i), n(i*10))
	}
	assertCount(t, c.query("tbl", []string{"id"}, nil), 20)
}

func TestFS_CRUD_Update_SingleRow(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(10))
	c.update("tbl", "v", n(99), eq("id", "1"))

	rs := c.query("tbl", []string{"v"}, eq("id", "1"))
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 99 {
		t.Errorf("v=%d, want 99", getInt(t, rs, 0, 0))
	}
}

func TestFS_CRUD_Update_AllRows(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(1))
	}
	c.update("tbl", "v", n(7), nil)

	rs := c.query("tbl", []string{"v"}, nil)
	assertCount(t, rs, 10)
	if sumCol(rs, 0) != 70 {
		t.Errorf("sum=%d, want 70 (all rows × 7)", sumCol(rs, 0))
	}
}

func TestFS_CRUD_Delete_SingleRow(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("tbl", n(i), n(0))
	}
	c.delete("tbl", eq("id", "3"))
	assertCount(t, c.query("tbl", []string{"id"}, nil), 4)

	rs := c.query("tbl", []string{"id"}, eq("id", "3"))
	assertCount(t, rs, 0)
}

func TestFS_CRUD_Delete_AllRows(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(0))
	}
	c.delete("tbl", nil)
	assertCount(t, c.query("tbl", []string{"id"}, nil), 0)
}

func TestFS_CRUD_Delete_PartialRows(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(0))
	}
	c.delete("tbl", gte("id", "6")) // delete 6-10
	rs := c.query("tbl", []string{"id"}, nil)
	assertCount(t, rs, 5)
	for _, v := range colInts(rs, 0) {
		if v >= 6 {
			t.Errorf("row id=%d should have been deleted", v)
		}
	}
}

func TestFS_CRUD_InsertUpdateDelete_Chain(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(10))
	c.update("tbl", "v", n(20), eq("id", "1"))
	c.delete("tbl", eq("id", "1"))
	assertCount(t, c.query("tbl", []string{"id"}, nil), 0)
}

func TestFS_CRUD_ReinsertAfterDelete_KeyReuse(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(5), n(100))
	c.delete("tbl", eq("id", "5"))
	c.insert("tbl", n(5), n(999))

	rs := c.query("tbl", []string{"id", "v"}, eq("id", "5"))
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 1) != 999 {
		t.Errorf("v=%d after re-insert, want 999", getInt(t, rs, 0, 1))
	}
}

func TestFS_CRUD_UpdateSameValue_NoOp(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(5))
	c.update("tbl", "v", n(5), eq("id", "1")) // same value

	rs := c.query("tbl", []string{"v"}, eq("id", "1"))
	if getInt(t, rs, 0, 0) != 5 {
		t.Errorf("v=%d, want 5 (same-value update must not corrupt)", getInt(t, rs, 0, 0))
	}
}

func TestFS_CRUD_MultipleUpdates_LastValueWins(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(0))
	for i := 1; i <= 5; i++ {
		c.update("tbl", "v", n(i), eq("id", "1"))
	}
	rs := c.query("tbl", []string{"v"}, eq("id", "1"))
	if getInt(t, rs, 0, 0) != 5 {
		t.Errorf("v=%d after 5 updates, want 5", getInt(t, rs, 0, 0))
	}
}

// =============================================================================
// SELECT Correctness
// =============================================================================

func TestFS_Select_ColumnNamesPopulated(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(9))

	rs := c.query("tbl", []string{"id", "v"}, nil)
	assertColumns(t, rs, "id", "v")
}

func TestFS_Select_ColTypesPopulated(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(9))

	rs := c.query("tbl", []string{"id", "v"}, nil)
	assertColTypes(t, rs, "INT", "INT")
}

func TestFS_Select_Star_ExpandsToAllColumns(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(42))

	rs := c.query("tbl", []string{"*"}, nil)
	assertCount(t, rs, 1)
	assertColumns(t, rs, "id", "v")
	if getInt(t, rs, 0, 0) != 1 || getInt(t, rs, 0, 1) != 42 {
		t.Errorf("SELECT * values wrong: [%d,%d], want [1,42]",
			getInt(t, rs, 0, 0), getInt(t, rs, 0, 1))
	}
}

func TestFS_Select_Star_TextTable(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intStrCols...)
	c.insert("tbl", n(1), s("alice"))

	rs := c.query("tbl", []string{"*"}, nil)
	assertColumns(t, rs, "id", "name")
	assertColTypes(t, rs, "INT", "TEXT")
	if getStr(t, rs, 0, 1) != "alice" {
		t.Errorf("name=%q, want alice", getStr(t, rs, 0, 1))
	}
}

func TestFS_Select_ColumnProjection_SingleColumn(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(7), n(99))

	rs := c.query("tbl", []string{"v"}, nil)
	assertColumns(t, rs, "v")
	if len(rs.Rows[0].Fields) != 1 {
		t.Errorf("fields=%d, want 1 (single-column projection)", len(rs.Rows[0].Fields))
	}
	if getInt(t, rs, 0, 0) != 99 {
		t.Errorf("v=%d, want 99", getInt(t, rs, 0, 0))
	}
}

func TestFS_Select_EmptyTable_ZeroRows(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	rs := c.query("tbl", []string{"id"}, nil)
	assertCount(t, rs, 0)
	// Columns should still be populated even for empty result sets.
	assertColumns(t, rs, "id")
}

func TestFS_Select_AfterDelete_RowGone(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(0))
	c.insert("tbl", n(2), n(0))
	c.delete("tbl", eq("id", "1"))

	rs := c.query("tbl", []string{"id"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 2 {
		t.Errorf("remaining id=%d, want 2", getInt(t, rs, 0, 0))
	}
}

func TestFS_Select_Values_ExactlyMatch_Inserted(t *testing.T) {
	// Verifies the full encode→proto→decode round-trip preserves values.
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	cases := [][2]int{{1, 0}, {2, -1}, {3, 1000000}, {4, -9999}}
	for _, tc := range cases {
		c.insert("tbl", n(tc[0]), n(tc[1]))
	}
	rs := c.query("tbl", []string{"id", "v"}, nil)
	assertCount(t, rs, 4)
	for _, tc := range cases {
		where := eq("id", itoa(tc[0]))
		row := mustSelect(t, c.gw, "tbl", []string{"v"}, where)
		if getInt(t, row, 0, 0) != int64(tc[1]) {
			t.Errorf("id=%d: v=%d, want %d", tc[0], getInt(t, row, 0, 0), tc[1])
		}
	}
}

// =============================================================================
// WHERE Clause Coverage
// =============================================================================

func TestFS_Where_Equal_Match(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("tbl", n(i), n(i*10))
	}
	rs := c.query("tbl", []string{"id"}, eq("id", "3"))
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 3 {
		t.Errorf("id=%d, want 3", getInt(t, rs, 0, 0))
	}
}

func TestFS_Where_Equal_NoMatch(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(0))
	assertCount(t, c.query("tbl", []string{"id"}, eq("id", "99")), 0)
}

func TestFS_Where_NotEqual(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("tbl", n(i), n(0))
	}
	rs := c.query("tbl", []string{"id"}, neq("id", "3"))
	assertCount(t, rs, 4)
	for _, v := range colInts(rs, 0) {
		if v == 3 {
			t.Error("row id=3 should be excluded by !=")
		}
	}
}

func TestFS_Where_LessThan(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(0))
	}
	rs := c.query("tbl", []string{"id"}, lt("id", "5"))
	assertIntsEqual(t, colInts(rs, 0), []int64{1, 2, 3, 4})
}

func TestFS_Where_LessOrEqual(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("tbl", n(i), n(0))
	}
	rs := c.query("tbl", []string{"id"}, lte("id", "3"))
	assertIntsEqual(t, colInts(rs, 0), []int64{1, 2, 3})
}

func TestFS_Where_GreaterThan(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(0))
	}
	rs := c.query("tbl", []string{"id"}, gt("id", "7"))
	assertIntsEqual(t, colInts(rs, 0), []int64{8, 9, 10})
}

func TestFS_Where_GreaterOrEqual(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("tbl", n(i), n(0))
	}
	rs := c.query("tbl", []string{"id"}, gte("id", "4"))
	assertIntsEqual(t, colInts(rs, 0), []int64{4, 5})
}

func TestFS_Where_And_RangeInclusive(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(0))
	}
	where := andE(gte("id", "3"), lte("id", "6"))
	rs := c.query("tbl", []string{"id"}, where)
	assertIntsEqual(t, colInts(rs, 0), []int64{3, 4, 5, 6})
}

func TestFS_Where_Or_TwoValues(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(0))
	}
	where := orE(eq("id", "2"), eq("id", "8"))
	rs := c.query("tbl", []string{"id"}, where)
	assertIntsEqual(t, colInts(rs, 0), []int64{2, 8})
}

func TestFS_Where_NonPK_Column_FullScan(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 10; i++ {
		c.insert("tbl", n(i), n(i%3)) // values: 1,2,0,1,2,0,1,2,0,1
	}
	where := &sqllayer.ComparisonExpr{Column: "v", Operator: "=", Value: intLit("0")}
	rs := c.query("tbl", []string{"id"}, where)
	// ids 3, 6, 9 have v=0
	assertCount(t, rs, 3)
}

func TestFS_Where_String_Equal(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intStrCols...)
	c.insert("tbl", n(1), s("alice"))
	c.insert("tbl", n(2), s("bob"))
	c.insert("tbl", n(3), s("alice"))

	rs := c.query("tbl", []string{"id"}, eqStr("name", "alice"))
	assertIntsEqual(t, colInts(rs, 0), []int64{1, 3})
}

func TestFS_Where_BoundaryValue_AtExactKey(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(0))
	c.insert("tbl", n(2), n(0))
	c.insert("tbl", n(3), n(0))

	rs := c.query("tbl", []string{"id"}, lte("id", "2"))
	assertIntsEqual(t, colInts(rs, 0), []int64{1, 2})
}

func TestFS_Where_Update_NonPK(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	for i := 1; i <= 6; i++ {
		c.insert("tbl", n(i), n(i%2)) // v alternates 1,0,1,0,1,0
	}
	// UPDATE WHERE v = 1 → rows 1,3,5
	where := &sqllayer.ComparisonExpr{Column: "v", Operator: "=", Value: intLit("1")}
	c.update("tbl", "v", n(9), where)

	// Rows with original v=1 should now be 9
	rs := c.query("tbl", []string{"id", "v"}, nil)
	for _, row := range rs.Rows {
		id := row.Fields[0].Value.(btree.IntValue).V
		v := row.Fields[1].Value.(btree.IntValue).V
		if id%2 == 1 && v != 9 { // odd ids had v=1
			t.Errorf("id=%d: v=%d, want 9", id, v)
		}
		if id%2 == 0 && v != 0 { // even ids had v=0, should be unchanged
			t.Errorf("id=%d: v=%d, want 0", id, v)
		}
	}
}

// =============================================================================
// Schema Edge Cases
// =============================================================================

func TestFS_Schema_DuplicateCreate_ReturnsError(t *testing.T) {
	c := newClusterT(t)
	c.createTable("dup", intIntCols...)
	err := c.execErr(&sqllayer.CreateTableStatement{Table: "dup", Columns: intIntCols})
	if err == nil {
		t.Fatal("expected error on duplicate CREATE TABLE, got nil")
	}
}

func TestFS_Schema_InsertUnknownTable_Error(t *testing.T) {
	c := newClusterT(t)
	err := c.execErr(&sqllayer.InsertStatement{Table: "ghost", Values: []sqllayer.Literal{n(1), n(1)}})
	if err == nil {
		t.Fatal("expected error for INSERT into unknown table")
	}
}

func TestFS_Schema_SelectUnknownTable_Error(t *testing.T) {
	c := newClusterT(t)
	_, err := c.gw.Execute(&sqllayer.SelectStatement{Table: "ghost", Columns: []string{"id"}})
	if err == nil {
		t.Fatal("expected error for SELECT from unknown table")
	}
}

func TestFS_Schema_SelectUnknownColumn_Error(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	_, err := c.gw.Execute(&sqllayer.SelectStatement{Table: "tbl", Columns: []string{"nonexistent"}})
	if err == nil {
		t.Fatal("expected error for unknown column in SELECT")
	}
}

func TestFS_Schema_UpdateUnknownColumn_Error(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	err := c.execErr(&sqllayer.UpdateStatement{Table: "tbl", Column: "ghost", Value: n(1)})
	if err == nil {
		t.Fatal("expected error for UPDATE of unknown column")
	}
}

func TestFS_Schema_PKOnlyTable(t *testing.T) {
	c := newClusterT(t)
	c.createTable("pkonly", sqllayer.ColumnDef{Name: "id", DataType: "INT"})
	c.insert("pkonly", n(1))
	rs := c.query("pkonly", []string{"id"}, nil)
	assertCount(t, rs, 1)
}

func TestFS_Schema_MultiColumnTable(t *testing.T) {
	cols := []sqllayer.ColumnDef{
		{Name: "id", DataType: "INT"},
		{Name: "a", DataType: "INT"},
		{Name: "b", DataType: "INT"},
		{Name: "c", DataType: "TEXT"},
	}
	c := newClusterT(t)
	c.createTable("big", cols...)
	c.insert("big", n(1), n(2), n(3), s("hello"))

	rs := c.query("big", []string{"id", "a", "b", "c"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 1) != 2 || getInt(t, rs, 0, 2) != 3 || getStr(t, rs, 0, 3) != "hello" {
		t.Error("multi-column values mismatch")
	}
}

func TestFS_Schema_CreateTableListValueRoundTrip(t *testing.T) {
	// ListValue fields in the schema proto must survive the proto encode/decode
	// round-trip (bytes_val). This was the bug that caused CREATE TABLE to fail
	// with "unsupported field value type: <nil>" on the real RangeServer.
	c := newClusterT(t)
	c.createTable("lt", []sqllayer.ColumnDef{
		{Name: "id", DataType: "INT"},
		{Name: "col1", DataType: "INT"},
		{Name: "col2", DataType: "TEXT"},
	}...)
	// If ListValue is corrupted the schema won't load and INSERT will fail.
	c.insert("lt", n(1), n(2), s("ok"))
	rs := c.query("lt", []string{"*"}, nil)
	assertCount(t, rs, 1)
}

// =============================================================================
// Data Type Edge Cases
// =============================================================================

func TestFS_Types_IntZero(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(0))
	if getInt(t, c.query("tbl", []string{"v"}, eq("id", "1")), 0, 0) != 0 {
		t.Error("int 0 not stored/retrieved correctly")
	}
}

func TestFS_Types_IntNegative(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), intLit("-500"))
	rs := c.query("tbl", []string{"v"}, nil)
	if getInt(t, rs, 0, 0) != -500 {
		t.Errorf("v=%d, want -500", getInt(t, rs, 0, 0))
	}
}

func TestFS_Types_TextEmptyString(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intStrCols...)
	c.insert("tbl", n(1), s(""))
	rs := c.query("tbl", []string{"name"}, nil)
	if getStr(t, rs, 0, 0) != "" {
		t.Errorf("name=%q, want empty string", getStr(t, rs, 0, 0))
	}
}

func TestFS_Types_TextLongString(t *testing.T) {
	long := ""
	for i := 0; i < 200; i++ {
		long += "abcdefghij"
	}
	c := newClusterT(t)
	c.createTable("tbl", intStrCols...)
	c.insert("tbl", n(1), s(long))
	rs := c.query("tbl", []string{"name"}, nil)
	if getStr(t, rs, 0, 0) != long {
		t.Error("long string not stored/retrieved correctly")
	}
}

func TestFS_Types_TextSpecialChars(t *testing.T) {
	val := "hello\tworld\nnewline"
	c := newClusterT(t)
	c.createTable("tbl", intStrCols...)
	c.insert("tbl", n(1), s(val))
	rs := c.query("tbl", []string{"name"}, nil)
	if getStr(t, rs, 0, 0) != val {
		t.Errorf("name=%q, want %q", getStr(t, rs, 0, 0), val)
	}
}

func TestFS_Types_UpdateIntToNegative(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intIntCols...)
	c.insert("tbl", n(1), n(100))
	c.update("tbl", "v", intLit("-42"), eq("id", "1"))
	rs := c.query("tbl", []string{"v"}, nil)
	if getInt(t, rs, 0, 0) != -42 {
		t.Errorf("v=%d, want -42", getInt(t, rs, 0, 0))
	}
}

func TestFS_Types_UpdateTextColumn(t *testing.T) {
	c := newClusterT(t)
	c.createTable("tbl", intStrCols...)
	c.insert("tbl", n(1), s("old"))
	c.update("tbl", "name", s("new"), eq("id", "1"))
	rs := c.query("tbl", []string{"name"}, nil)
	if getStr(t, rs, 0, 0) != "new" {
		t.Errorf("name=%q, want new", getStr(t, rs, 0, 0))
	}
}

// =============================================================================
// Multi-Table Isolation
// =============================================================================

func TestFS_Isolation_TwoTablesIndependent(t *testing.T) {
	c := newClusterT(t)
	c.createTable("a", intIntCols...)
	c.createTable("b", intIntCols...)
	c.insert("a", n(1), n(10))
	c.insert("b", n(1), n(20))

	ra := c.query("a", []string{"v"}, nil)
	rb := c.query("b", []string{"v"}, nil)
	if getInt(t, ra, 0, 0) != 10 || getInt(t, rb, 0, 0) != 20 {
		t.Error("table isolation violated")
	}
}

func TestFS_Isolation_UpdateOneTableDoesNotAffectOther(t *testing.T) {
	c := newClusterT(t)
	c.createTable("a", intIntCols...)
	c.createTable("b", intIntCols...)
	for i := 1; i <= 5; i++ {
		c.insert("a", n(i), n(1))
		c.insert("b", n(i), n(1))
	}
	c.update("a", "v", n(9), nil) // update only a

	rb := c.query("b", []string{"v"}, nil)
	if sumCol(rb, 0) != 5 {
		t.Errorf("b.sum=%d, want 5 (table a update must not affect b)", sumCol(rb, 0))
	}
}

func TestFS_Isolation_DeleteFromOneTable(t *testing.T) {
	c := newClusterT(t)
	c.createTable("a", intIntCols...)
	c.createTable("b", intIntCols...)
	for i := 1; i <= 3; i++ {
		c.insert("a", n(i), n(0))
		c.insert("b", n(i), n(0))
	}
	c.delete("a", nil) // clear a

	assertCount(t, c.query("a", []string{"id"}, nil), 0)
	assertCount(t, c.query("b", []string{"id"}, nil), 3)
}

func TestFS_Isolation_SamePKDifferentTables(t *testing.T) {
	c := newClusterT(t)
	c.createTable("x", intIntCols...)
	c.createTable("y", intIntCols...)
	c.insert("x", n(1), n(100))
	c.insert("y", n(1), n(200))

	rx := c.query("x", []string{"v"}, eq("id", "1"))
	ry := c.query("y", []string{"v"}, eq("id", "1"))
	if getInt(t, rx, 0, 0) != 100 || getInt(t, ry, 0, 0) != 200 {
		t.Error("same PK in different tables should be independent")
	}
}

// =============================================================================
// Regression Tests (one test per class of bug we shipped)
// =============================================================================

// RangeServer.Execute was unimplemented — every SQL call failed Unimplemented.
func TestFS_Regression_ExecuteRPC_NotUnimplemented(t *testing.T) {
	c := newClusterT(t)
	c.createTable("r1", intIntCols...)
	c.insert("r1", n(1), n(1)) // would panic/error before the fix
}

// fieldsToProto silently dropped ListValue (schema column-name arrays),
// causing bt.Insert to fail with "unsupported field value type: <nil>".
func TestFS_Regression_ListValueProtoRoundTrip(t *testing.T) {
	c := newClusterT(t)
	// Schema fields contain ListValue for column name/type arrays.
	c.createTable("r2", intIntCols...)
	// Successful INSERT proves schema row was stored without encoding error.
	c.insert("r2", n(1), n(0))
}

// result.Columns was never set in the gateway SELECT path, so the client
// always printed "OK" instead of showing rows.
func TestFS_Regression_SelectColumnsPopulated(t *testing.T) {
	c := newClusterT(t)
	c.createTable("r3", intIntCols...)
	c.insert("r3", n(1), n(9))

	rs := c.query("r3", []string{"id", "v"}, nil)
	if len(rs.Columns) == 0 {
		t.Fatal("Columns is empty — client would print OK instead of rows")
	}
}

// SELECT * raised "column '*' not found" because the gateway tried to
// resolve "*" as a literal column name.
func TestFS_Regression_SelectStarExpanded(t *testing.T) {
	c := newClusterT(t)
	c.createTable("r4", intIntCols...)
	c.insert("r4", n(2), n(5))
	rs := c.query("r4", []string{"*"}, nil)
	assertCount(t, rs, 1)
	assertColumns(t, rs, "id", "v")
}

// coordinator.UpdateLeader was never called → LeaderID stayed 0 → every
// sendToLeader failed "no connection for leader node 0".
func TestFS_Regression_LeaderIDZero_RoutingFails(t *testing.T) {
	// If LeaderID is 0, any Execute call would fail. This test passes only if
	// the coordinator is correctly initialised with a non-zero leader.
	gw, _ := newSingleNodeRealGateway(t)
	mustExec(t, gw, &sqllayer.CreateTableStatement{
		Table: "r5", Columns: intIntCols,
	})
}

// router.Refresh was never called after coordinator.UpdateLeader, so the
// router cache kept LeaderID=0 even after the hook ran.
func TestFS_Regression_RouterCacheStaleAfterLeaderChange(t *testing.T) {
	// Simulate a "leader change" by creating a gateway, manually wiping the
	// leader, then setting it again and refreshing the router.
	node := newRealNode(t)
	conn := startRealNode(t, node)

	coord := NewCoordinator(map[uint64]string{1: "node1"})
	coord.UpdateLeader(1, 1)
	router, _ := NewRouter(coord)
	gw := NewGateway(router, node.sc)
	gw.AddConn(1, conn)

	// Simulate a "leader lost then re-elected" cycle.
	coord.UpdateLeader(1, 0) // clear
	coord.UpdateLeader(1, 1) // re-elect
	if err := router.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "r6", Columns: intIntCols})
}

// UPDATE results must be visible in the next SELECT.
func TestFS_Regression_UpdateVisibleInSelect(t *testing.T) {
	c := newClusterT(t)
	c.createTable("r7", intIntCols...)
	c.insert("r7", n(1), n(0))
	c.update("r7", "v", n(99), nil)

	rs := c.query("r7", []string{"v"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 99 {
		t.Errorf("v=%d after UPDATE, want 99 (UPDATE must be visible immediately)", getInt(t, rs, 0, 0))
	}
}

// scatterGather for multi-range SELECT must also populate Columns.
func TestFS_Regression_ScatterGatherColumnsPopulated(t *testing.T) {
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 50))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "r8", Columns: intIntCols})
	for i := 1; i <= 4; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "r8", Values: []sqllayer.Literal{n(i * 30), n(i)}})
	}
	rs := mustSelect(t, gw, "r8", []string{"id", "v"}, nil)
	if len(rs.Columns) == 0 {
		t.Fatal("scatterGather result: Columns is empty (client would print OK)")
	}
}

// =============================================================================
// 2PC Multi-Range Tests
// =============================================================================

func TestFS_2PC_UpdateAllRows_TwoNodes(t *testing.T) {
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 51))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t2pc", Columns: intIntCols})
	for i := 1; i <= 100; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "t2pc", Values: []sqllayer.Literal{n(i), n(1)}})
	}
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "t2pc", Column: "v", Value: n(7)})

	rs := mustSelect(t, gw, "t2pc", []string{"v"}, nil)
	assertCount(t, rs, 100)
	if sumCol(rs, 0) != 700 {
		t.Errorf("sum=%d, want 700 (100 rows × 7)", sumCol(rs, 0))
	}
}

func TestFS_2PC_DeleteAllRows_TwoNodes(t *testing.T) {
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 51))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t2pcd", Columns: intIntCols})
	for i := 1; i <= 100; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "t2pcd", Values: []sqllayer.Literal{n(i), n(0)}})
	}
	mustExec(t, gw, &sqllayer.DeleteStatement{Table: "t2pcd"})
	assertCount(t, mustSelect(t, gw, "t2pcd", []string{"id"}, nil), 0)
}

func TestFS_2PC_UpdateWithWhere_PKRange(t *testing.T) {
	// The first table in a fresh cluster always receives tableId=1, so we can
	// compute the split key upfront without a separate bootstrap step.
	// Both nodes share the same SchemaCatalog so WHERE can resolve column names.
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 51))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t2pcw", Columns: intIntCols})

	for i := 1; i <= 100; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "t2pcw", Values: []sqllayer.Literal{n(i), n(1)}})
	}

	// WHERE spans the split: update rows 40-60 only.
	where := andE(gte("id", "40"), lte("id", "60"))
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "t2pcw", Column: "v", Value: n(9), Where: where})

	rs := mustSelect(t, gw, "t2pcw", []string{"id", "v"}, nil)
	for _, row := range rs.Rows {
		id := row.Fields[0].Value.(btree.IntValue).V
		v := row.Fields[1].Value.(btree.IntValue).V
		inRange := id >= 40 && id <= 60
		if inRange && v != 9 {
			t.Errorf("id=%d: v=%d, want 9", id, v)
		}
		if !inRange && v != 1 {
			t.Errorf("id=%d: v=%d, want 1", id, v)
		}
	}
}

func TestFS_2PC_DeleteWithWhere_NonPKColumn(t *testing.T) {
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 51))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t2pcnpk", Columns: intIntCols})

	// Two groups: v=1 (odd IDs) and v=0 (even IDs) spread across both nodes.
	for i := 1; i <= 100; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "t2pcnpk", Values: []sqllayer.Literal{n(i), n(i % 2)},
		})
	}
	// DELETE WHERE v = 0 (even IDs).
	where := &sqllayer.ComparisonExpr{Column: "v", Operator: "=", Value: intLit("0")}
	mustExec(t, gw, &sqllayer.DeleteStatement{Table: "t2pcnpk", Where: where})

	rs := mustSelect(t, gw, "t2pcnpk", []string{"id", "v"}, nil)
	assertCount(t, rs, 50)
	for _, row := range rs.Rows {
		if v := row.Fields[1].Value.(btree.IntValue).V; v != 1 {
			t.Errorf("remaining row has v=%d, want 1 (only odd-id rows should remain)", v)
		}
	}
}

func TestFS_2PC_Select_AfterUpdate_AllNodesVisible(t *testing.T) {
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 51))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t2pcsg", Columns: intIntCols})
	for i := 1; i <= 60; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "t2pcsg", Values: []sqllayer.Literal{n(i), n(0)}})
	}
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "t2pcsg", Column: "v", Value: n(3)})

	rs := mustSelect(t, gw, "t2pcsg", []string{"id", "v"}, nil)
	assertCount(t, rs, 60)
	for _, row := range rs.Rows {
		if v := row.Fields[1].Value.(btree.IntValue).V; v != 3 {
			t.Errorf("id=%d: v=%d, want 3 — 2PC update must be visible on both nodes",
				row.Fields[0].Value.(btree.IntValue).V, v)
		}
	}
}

func TestFS_2PC_Atomicity_PartialPrepareFailure(t *testing.T) {
	// Use the simRangeServer-backed cluster to inject a Prepare failure on node 2,
	// then verify node 1's data is unchanged (abort was issued).
	c := newMultiNodeCluster(t, 2)
	c.addTable("atomic", "id", []string{"v"}, []string{"INT"})
	schema := c.sc.FindTableSchema("atomic")
	splitKey := sqllayer.EncodeKey(schema.TableId, 51)
	c.splitAtKey(splitKey, 1, 2)

	for i := 1; i <= 100; i++ {
		c.insert("atomic", []sqllayer.Literal{intLitS(itoa(i)), intLitS("5")})
	}

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = true
	c.nodes[2].mu.Unlock()

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table: "atomic", Column: "v",
		Value: sqllayer.Literal{Value: "99", Type: sqllayer.TOKEN_NUMBER},
	})
	if err == nil {
		t.Fatal("expected error from partial Prepare failure")
	}

	c.nodes[2].mu.Lock()
	c.nodes[2].failPrepare = false
	c.nodes[2].mu.Unlock()

	rs := c.selectRows("atomic", []string{"id", "v"}, nil)
	if sumIntCol(rs, 1) != 500 {
		t.Errorf("sum=%d after aborted 2PC, want 500 (all rows unchanged)", sumIntCol(rs, 1))
	}
}

func TestFS_2PC_ThreeNodes_UpdateAll(t *testing.T) {
	lo := sqllayer.EncodeKey(1, 34)
	hi := sqllayer.EncodeKey(1, 67)
	gw, _ := newThreeNodeRealGateway(t, lo, hi)
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t3n", Columns: intIntCols})
	for i := 1; i <= 99; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "t3n", Values: []sqllayer.Literal{n(i), n(1)}})
	}
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "t3n", Column: "v", Value: n(2)})

	rs := mustSelect(t, gw, "t3n", []string{"v"}, nil)
	assertCount(t, rs, 99)
	if sumCol(rs, 0) != 198 {
		t.Errorf("sum=%d, want 198 (99 rows × 2)", sumCol(rs, 0))
	}
}

func TestFS_2PC_EmptyRangeOnOneNode(t *testing.T) {
	// All rows are in node 1's range (pk 1-10, split at pk=51).
	// Node 2's Prepare gets no rows but must still succeed.
	gw, _ := newTwoNodeRealGateway(t, sqllayer.EncodeKey(1, 51))
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "t2pce", Columns: intIntCols})
	for i := 1; i <= 10; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "t2pce", Values: []sqllayer.Literal{n(i), n(3)}})
	}
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "t2pce", Column: "v", Value: n(9)})

	rs := mustSelect(t, gw, "t2pce", []string{"v"}, nil)
	assertCount(t, rs, 10)
	if sumCol(rs, 0) != 90 {
		t.Errorf("sum=%d, want 90", sumCol(rs, 0))
	}
}

// =============================================================================
// Concurrent / Stress Tests
// =============================================================================

func TestFS_Concurrent_Inserts_AllVisible(t *testing.T) {
	c := newClusterT(t)
	c.createTable("conc", intIntCols...)

	const N = 50
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errs[id] = c.gw.Execute(&sqllayer.InsertStatement{
				Table:  "conc",
				Values: []sqllayer.Literal{n(id + 1), n(id)},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("INSERT[%d]: %v", i, err)
		}
	}
	assertCount(t, c.query("conc", []string{"id"}, nil), N)
}

func TestFS_Concurrent_Selects_NoErrors(t *testing.T) {
	c := newClusterT(t)
	c.createTable("csel", intIntCols...)
	for i := 1; i <= 20; i++ {
		c.insert("csel", n(i), n(i))
	}

	var wg sync.WaitGroup
	errs := make([]error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = c.gw.Execute(&sqllayer.SelectStatement{
				Table: "csel", Columns: []string{"id", "v"},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("SELECT[%d]: %v", i, err)
		}
	}
}

func TestFS_Concurrent_Updates_SameTable_NoErrors(t *testing.T) {
	c := newClusterT(t)
	c.createTable("cupd", intIntCols...)
	for i := 1; i <= 50; i++ {
		c.insert("cupd", n(i), n(0))
	}

	const W = 20
	var wg sync.WaitGroup
	errs := make([]error, W)
	for i := 0; i < W; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = c.gw.Execute(&sqllayer.UpdateStatement{
				Table: "cupd", Column: "v",
				Value: n(idx),
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("UPDATE[%d]: %v", i, err)
		}
	}
	// Row count must be unchanged.
	assertCount(t, c.query("cupd", []string{"id"}, nil), 50)
}

func TestFS_Concurrent_MixedOps_RowCountConsistent(t *testing.T) {
	c := newClusterT(t)
	c.createTable("cmix", intIntCols...)
	for i := 1; i <= 100; i++ {
		c.insert("cmix", n(i), n(1))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(v int) {
			defer wg.Done()
			_, _ = c.gw.Execute(&sqllayer.UpdateStatement{
				Table: "cmix", Column: "v", Value: n(v),
			})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = c.gw.Execute(&sqllayer.SelectStatement{
				Table: "cmix", Columns: []string{"id"},
			})
		}()
	}
	wg.Wait()

	// Row count must never change — no INSERTs or DELETEs ran.
	assertCount(t, c.query("cmix", []string{"id"}, nil), 100)
}

func TestFS_Stress_Sequential_LargeDataset(t *testing.T) {
	c := newClusterT(t)
	c.createTable("large", intIntCols...)
	const N = 500
	for i := 1; i <= N; i++ {
		c.insert("large", n(i), n(i))
	}
	assertCount(t, c.query("large", []string{"id"}, nil), N)

	c.update("large", "v", n(0), gte("id", "251"))
	rs := c.query("large", []string{"id", "v"}, nil)
	assertCount(t, rs, N)

	var lowSum, highSum int64
	for _, row := range rs.Rows {
		id := row.Fields[0].Value.(btree.IntValue).V
		v := row.Fields[1].Value.(btree.IntValue).V
		if id <= 250 {
			lowSum += v
		} else {
			highSum += v
		}
	}
	// Rows 1–250 untouched: sum = 1+2+…+250 = 31375
	if lowSum != 31375 {
		t.Errorf("low sum=%d, want 31375", lowSum)
	}
	// Rows 251–500 set to 0
	if highSum != 0 {
		t.Errorf("high sum=%d, want 0", highSum)
	}
}

func TestFS_Stress_ConcurrentInsertsThenSelect(t *testing.T) {
	c := newClusterT(t)
	c.createTable("ci", intIntCols...)

	const N = 100
	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, _ = c.gw.Execute(&sqllayer.InsertStatement{
				Table: "ci", Values: []sqllayer.Literal{n(id), n(id * 2)},
			})
		}(i)
	}
	wg.Wait()

	rs := c.query("ci", []string{"id"}, nil)
	if len(rs.Rows) != N {
		t.Errorf("rows=%d after %d concurrent inserts, want %d", len(rs.Rows), N, N)
	}
}

func TestFS_Stress_Concurrent2PC_IndependentTables(t *testing.T) {
	const N = 10
	type setup struct {
		gw      *Gateway
		sc      *sqllayer.SchemaCatalog
		tableID uint32
		split   uint64
	}

	// Build N independent 2-node setups.
	setups := make([]setup, N)
	for i := range setups {
		gwS, scS := newSingleNodeRealGateway(t)
		name := fmt.Sprintf("stress%d", i)
		mustExec(t, gwS, &sqllayer.CreateTableStatement{Table: name, Columns: intIntCols})
		tid := scS.FindTableSchema(name).TableId
		sp := sqllayer.EncodeKey(tid, 51)
		gw2, _ := newTwoNodeRealGateway(t, sp)
		gw2.schema = scS
		for j := 1; j <= 20; j++ {
			mustExec(t, gw2, &sqllayer.InsertStatement{
				Table: name, Values: []sqllayer.Literal{n(j), n(1)},
			})
		}
		setups[i] = setup{gw: gw2, sc: scS, tableID: tid, split: sp}
	}

	var wg sync.WaitGroup
	errs := make([]error, N)
	for i, s := range setups {
		wg.Add(1)
		go func(idx int, st setup) {
			defer wg.Done()
			name := fmt.Sprintf("stress%d", idx)
			_, errs[idx] = st.gw.Execute(&sqllayer.UpdateStatement{
				Table: name, Column: "v", Value: n(idx + 1),
			})
		}(i, s)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("2PC UPDATE[%d]: %v", i, err)
		}
	}

	// Verify each table has the correct sum.
	for i, s := range setups {
		name := fmt.Sprintf("stress%d", i)
		rs := mustSelect(t, s.gw, name, []string{"v"}, nil)
		want := int64(20 * (i + 1)) // 20 rows × (i+1)
		if sumCol(rs, 0) != want {
			t.Errorf("stress%d sum=%d, want %d", i, sumCol(rs, 0), want)
		}
	}
}
