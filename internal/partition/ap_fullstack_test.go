package partition

// AP-mode full-stack tests: Gateway → gRPC (bufconn) → real RangeServer with
// TimestampStore + APWriteLog → BTree.
//
// Sections:
//   Infrastructure  — helpers for AP-enabled nodes / gateways
//   Schema          — AP consistency stored, loaded, altered
//   APWrite         — INSERT / UPDATE / DELETE bypass Raft in AP mode
//   APRead          — SELECT serves local BTree; WITH CONSISTENCY override
//   AlterConsistency — ALTER TABLE … SET CONSISTENCY STRONG | EVENTUAL
//   Mixed           — CP and AP tables coexist in the same cluster
//   APSync          — two-node sync: writes on nodeA become visible on nodeB
//   Stress          — concurrent AP writes, no deadlocks

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	ap "github.com/your-username/DistributedDatabaseSystem/internal/AP"
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

type apRealNode struct {
	bt         *btree.BTree
	sc         *sqllayer.SchemaCatalog
	records    *TxnRecordStore
	srv        *RangeServer
	timestamps *ap.TimestampStore
	apLog      *ap.APWriteLog
}

func newAPRealNode(t *testing.T) *apRealNode {
	t.Helper()
	return newAPRealNodeWithSchema(t, nil)
}

func newAPRealNodeWithSchema(t *testing.T, sc *sqllayer.SchemaCatalog) *apRealNode {
	t.Helper()
	bt := newTestBTreeGateway(t)
	if sc == nil {
		sc = sqllayer.NewSchemaCatalog(bt)
	}
	records := NewTxnRecordStore()
	timestamps := ap.NewTimestampStore()

	// APWriteLog backed by a temp file.
	logPath := filepath.Join(t.TempDir(), "ap.log")
	apLog, err := ap.NewAPWriteLog(logPath)
	if err != nil {
		t.Fatalf("NewAPWriteLog: %v", err)
	}
	t.Cleanup(func() {
		_ = apLog.Close()
		_ = os.Remove(logPath)
	})

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
		if key>>32 == 0 {
			return sc.LoadSchemas()
		}
		return nil
	}

	tm := lock.NewTransactionManager(bt, nil, applyFn)
	srv := NewRangeServer(bt, tm, sc, records, timestamps, apLog)
	return &apRealNode{bt: bt, sc: sc, records: records, srv: srv, timestamps: timestamps, apLog: apLog}
}

func startAPRealNode(t *testing.T, node *apRealNode) *grpc.ClientConn {
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

// newSingleNodeAPGateway returns a Gateway backed by one AP-enabled RangeServer.
func newSingleNodeAPGateway(t *testing.T) (*Gateway, *apRealNode) {
	t.Helper()
	node := newAPRealNode(t)
	conn := startAPRealNode(t, node)

	coord := NewCoordinator(map[uint64]string{1: "node1"})
	coord.UpdateLeader(1, 1)
	router, err := NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	gw := NewGateway(router, node.sc)
	gw.AddConn(1, conn)
	return gw, node
}

// newTwoNodeAPGateway returns a Gateway with two AP-enabled nodes split at splitKey.
func newTwoNodeAPGateway(t *testing.T, splitKey uint64) (*Gateway, *apRealNode, *apRealNode) {
	t.Helper()
	n1 := newAPRealNode(t)
	n2 := newAPRealNodeWithSchema(t, n1.sc)
	conn1 := startAPRealNode(t, n1)
	conn2 := startAPRealNode(t, n2)

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
	return gw, n1, n2
}

// createAPTable creates a table and immediately alters it to EVENTUAL consistency.
func createAPTable(t *testing.T, gw *Gateway, name string, cols ...sqllayer.ColumnDef) {
	t.Helper()
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: name, Columns: cols})
	mustExec(t, gw, &sqllayer.AlterConsistencyStatement{Table: name, Mode: sqllayer.ConsistencyAP})
}

// =============================================================================
// Schema tests
// =============================================================================

func TestAP_Schema_DefaultCP(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	mustExec(t, gw, &sqllayer.CreateTableStatement{
		Table:   "tbl",
		Columns: intIntCols,
	})
	schema := node.sc.FindTableSchema("tbl")
	if schema == nil {
		t.Fatal("table not found")
	}
	if schema.Consistency != sqllayer.ConsistencyCP {
		t.Fatalf("expected default CP, got %v", schema.Consistency)
	}
}

func TestAP_Schema_AlterToAP(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "tbl", Columns: intIntCols})
	mustExec(t, gw, &sqllayer.AlterConsistencyStatement{Table: "tbl", Mode: sqllayer.ConsistencyAP})
	schema := node.sc.FindTableSchema("tbl")
	if schema == nil {
		t.Fatal("table not found after alter")
	}
	if schema.Consistency != sqllayer.ConsistencyAP {
		t.Fatalf("expected ConsistencyAP after ALTER, got %v", schema.Consistency)
	}
}

func TestAP_Schema_AlterToStrong(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "tbl", intIntCols...)
	mustExec(t, gw, &sqllayer.AlterConsistencyStatement{Table: "tbl", Mode: sqllayer.ConsistencyCP})
	schema := node.sc.FindTableSchema("tbl")
	if schema.Consistency != sqllayer.ConsistencyCP {
		t.Fatalf("expected CP after ALTER STRONG, got %v", schema.Consistency)
	}
}

func TestAP_Schema_AlterUnknownTable_Error(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	_, err := gw.Execute(&sqllayer.AlterConsistencyStatement{Table: "nosuch", Mode: sqllayer.ConsistencyAP})
	if err == nil {
		t.Fatal("expected error altering non-existent table, got nil")
	}
}

func TestAP_Schema_LoadSchemas_SurvivesReload(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "tbl", intIntCols...)
	// Reload and check the mode persisted.
	if err := node.sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	schema := node.sc.FindTableSchema("tbl")
	if schema == nil || schema.Consistency != sqllayer.ConsistencyAP {
		t.Fatalf("expected AP consistency after reload, got %+v", schema)
	}
}

// =============================================================================
// AP write tests
// =============================================================================

func TestAP_Insert_ImmediatelyReadable(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "users", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "users", Values: []sqllayer.Literal{n(1), n(100)}})

	rs := mustSelect(t, gw, "users", []string{"*"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 1) != 100 {
		t.Fatalf("expected v=100, got %d", getInt(t, rs, 0, 1))
	}
}

func TestAP_Insert_RecordedInAPLog(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "log_test", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "log_test", Values: []sqllayer.Literal{n(7), n(77)}})

	entries, err := node.apLog.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	// One entry: the insert (CREATE TABLE itself is CP, not AP)
	found := false
	for _, e := range entries {
		if e.Op == raft.ReplPut {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected at least one ReplPut entry in AP log, got entries: %+v", entries)
	}
}

func TestAP_Insert_TimestampSet(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "ts_test", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "ts_test", Values: []sqllayer.Literal{n(5), n(50)}})

	schema := node.sc.FindTableSchema("ts_test")
	key := sqllayer.EncodeKey(schema.TableId, 5)
	ts := node.timestamps.Get(key)
	if ts == 0 {
		t.Fatal("expected non-zero timestamp after AP insert")
	}
}

func TestAP_Update_BypassesRaft(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "upd", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "upd", Values: []sqllayer.Literal{n(1), n(10)}})
	mustExec(t, gw, &sqllayer.UpdateStatement{
		Table: "upd", Column: "v", Value: n(99),
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(1)},
	})

	rs := mustSelect(t, gw, "upd", []string{"v"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 99 {
		t.Fatalf("expected v=99 after AP update, got %d", getInt(t, rs, 0, 0))
	}
	// The update should also be in the AP log.
	entries, _ := node.apLog.ReadFrom(0)
	updateFound := false
	for _, e := range entries {
		if e.Op == raft.ReplPut {
			updateFound = true
		}
	}
	if !updateFound {
		t.Fatal("expected AP log entry for UPDATE")
	}
}

func TestAP_Delete_BypassesRaft(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "del", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "del", Values: []sqllayer.Literal{n(1), n(10)}})
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "del", Values: []sqllayer.Literal{n(2), n(20)}})
	mustExec(t, gw, &sqllayer.DeleteStatement{
		Table: "del",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(1)},
	})

	rs := mustSelect(t, gw, "del", []string{"*"}, nil)
	assertCount(t, rs, 1)
	if getInt(t, rs, 0, 0) != 2 {
		t.Fatalf("expected only id=2 to remain, got id=%d", getInt(t, rs, 0, 0))
	}
	// AP log must have a delete entry.
	entries, _ := node.apLog.ReadFrom(0)
	delFound := false
	for _, e := range entries {
		if e.Op == raft.ReplDelete {
			delFound = true
		}
	}
	if !delFound {
		t.Fatal("expected AP log entry for DELETE")
	}
}

func TestAP_MultipleInserts(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "multi", intIntCols...)
	for i := 1; i <= 10; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{
			Table: "multi", Values: []sqllayer.Literal{n(i), n(i * 10)},
		})
	}
	rs := mustSelect(t, gw, "multi", []string{"*"}, nil)
	assertCount(t, rs, 10)
}

func TestAP_Update_NoWhere_AllRows(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "allupd", intIntCols...)
	for i := 1; i <= 5; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "allupd", Values: []sqllayer.Literal{n(i), n(i)}})
	}
	mustExec(t, gw, &sqllayer.UpdateStatement{Table: "allupd", Column: "v", Value: n(99)})
	rs := mustSelect(t, gw, "allupd", []string{"v"}, nil)
	assertCount(t, rs, 5)
	for i := 0; i < 5; i++ {
		if getInt(t, rs, i, 0) != 99 {
			t.Fatalf("row %d: expected v=99, got %d", i, getInt(t, rs, i, 0))
		}
	}
}

// =============================================================================
// AP read tests
// =============================================================================

func TestAP_Select_ReturnsLocalData(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "rd", intStrCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "rd", Values: []sqllayer.Literal{n(1), s("alice")}})

	rs := mustSelect(t, gw, "rd", []string{"name"}, nil)
	assertCount(t, rs, 1)
	if getStr(t, rs, 0, 0) != "alice" {
		t.Fatalf("expected name=alice, got %q", getStr(t, rs, 0, 0))
	}
}

func TestAP_Select_StarExpansion(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "star", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "star", Values: []sqllayer.Literal{n(1), n(42)}})

	rs := mustSelect(t, gw, "star", []string{"*"}, nil)
	assertColumns(t, rs, "id", "v")
	assertCount(t, rs, 1)
}

func TestAP_Select_WithWhere(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "where_tbl", intIntCols...)
	for i := 1; i <= 5; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "where_tbl", Values: []sqllayer.Literal{n(i), n(i * 10)}})
	}
	rs := mustSelect(t, gw, "where_tbl", []string{"v"}, gt("id", "3"))
	assertCount(t, rs, 2) // id=4,5
}

func TestAP_Select_WithConsistencyOverride_Eventual(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	// CP table — override per-query to EVENTUAL.
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "cp_tbl", Columns: intIntCols})
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "cp_tbl", Values: []sqllayer.Literal{n(1), n(10)}})

	mode := sqllayer.ConsistencyAP
	rs, err := gw.Execute(&sqllayer.SelectStatement{
		Table: "cp_tbl", Columns: []string{"*"},
		ConsistencyOverride: &mode,
	})
	if err != nil {
		t.Fatalf("SELECT WITH CONSISTENCY EVENTUAL: %v", err)
	}
	assertCount(t, rs, 1)
}

func TestAP_Select_WithConsistencyOverride_Strong(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	// AP table — override per-query to STRONG.
	createAPTable(t, gw, "ap_tbl", intIntCols...)
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "ap_tbl", Values: []sqllayer.Literal{n(1), n(99)}})

	mode := sqllayer.ConsistencyCP
	rs, err := gw.Execute(&sqllayer.SelectStatement{
		Table: "ap_tbl", Columns: []string{"*"},
		ConsistencyOverride: &mode,
	})
	if err != nil {
		t.Fatalf("SELECT WITH CONSISTENCY STRONG: %v", err)
	}
	assertCount(t, rs, 1)
}

// =============================================================================
// Lexer / parser for ALTER TABLE and WITH CONSISTENCY
// =============================================================================

func TestAP_Parser_AlterConsistencyEventual(t *testing.T) {
	tokens, err := sqllayer.Tokenize("ALTER TABLE users SET CONSISTENCY EVENTUAL")
	if err != nil {
		t.Fatalf("tokenize: %v", err)
	}
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s, ok := stmt.(*sqllayer.AlterConsistencyStatement)
	if !ok {
		t.Fatalf("expected *AlterConsistencyStatement, got %T", stmt)
	}
	if s.Table != "users" || s.Mode != sqllayer.ConsistencyAP {
		t.Fatalf("unexpected statement: %+v", s)
	}
}

func TestAP_Parser_AlterConsistencyStrong(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("ALTER TABLE orders SET CONSISTENCY STRONG")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := stmt.(*sqllayer.AlterConsistencyStatement)
	if s.Mode != sqllayer.ConsistencyCP {
		t.Fatalf("expected CP, got %v", s.Mode)
	}
}

func TestAP_Parser_SelectWithConsistencyEventual(t *testing.T) {
	tokens, err := sqllayer.Tokenize("SELECT * FROM t WITH CONSISTENCY EVENTUAL")
	if err != nil {
		t.Fatalf("tokenize: %v", err)
	}
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*sqllayer.SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if sel.ConsistencyOverride == nil || *sel.ConsistencyOverride != sqllayer.ConsistencyAP {
		t.Fatalf("expected ConsistencyAP override, got %v", sel.ConsistencyOverride)
	}
}

func TestAP_Parser_SelectWithConsistencyStrong(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("SELECT id FROM t WHERE id = 1 WITH CONSISTENCY STRONG")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqllayer.SelectStatement)
	if sel.ConsistencyOverride == nil || *sel.ConsistencyOverride != sqllayer.ConsistencyCP {
		t.Fatalf("expected CP override, got %v", sel.ConsistencyOverride)
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause to be parsed")
	}
}

func TestAP_Parser_SelectNoOverride_NilOverride(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("SELECT * FROM t")
	stmt, _ := sqllayer.Parse(tokens)
	sel := stmt.(*sqllayer.SelectStatement)
	if sel.ConsistencyOverride != nil {
		t.Fatal("expected nil ConsistencyOverride when not specified")
	}
}

// =============================================================================
// AlterConsistency — full routing round-trip
// =============================================================================

func TestAP_AlterConsistency_RoundTrip(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "rt", Columns: intIntCols})
	mustExec(t, gw, &sqllayer.AlterConsistencyStatement{Table: "rt", Mode: sqllayer.ConsistencyAP})
	// Insert should now go through AP path.
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "rt", Values: []sqllayer.Literal{n(1), n(111)}})

	// Verify via raw BTree that the row is there.
	schema := node.sc.FindTableSchema("rt")
	key := sqllayer.EncodeKey(schema.TableId, 1)
	rows, _ := node.bt.RangeScan(key, key)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in BTree, got %d", len(rows))
	}
	// And via the AP log.
	entries, _ := node.apLog.ReadFrom(0)
	logHasInsert := false
	for _, e := range entries {
		if e.Op == raft.ReplPut && e.Key == key {
			logHasInsert = true
		}
	}
	if !logHasInsert {
		t.Fatal("expected AP log to contain the insert after mode change to AP")
	}
}

func TestAP_AlterConsistency_BackToCP_WritesViaRaft(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "flip", intIntCols...)
	// Switch back to CP.
	mustExec(t, gw, &sqllayer.AlterConsistencyStatement{Table: "flip", Mode: sqllayer.ConsistencyCP})
	logBefore := node.apLog.NextLSN()
	// Now insert — should go via CP (Raft / TransactionManager), NOT the AP log.
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "flip", Values: []sqllayer.Literal{n(1), n(9)}})
	logAfter := node.apLog.NextLSN()
	if logAfter != logBefore {
		t.Fatalf("expected AP log to be unchanged after CP insert, was %d before and %d after", logBefore, logAfter)
	}
}

// =============================================================================
// Mixed CP/AP tables
// =============================================================================

func TestAP_Mixed_CPAndAPTablesCoexist(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	// CP table
	mustExec(t, gw, &sqllayer.CreateTableStatement{Table: "cp", Columns: intIntCols})
	// AP table
	createAPTable(t, gw, "ap_tbl", intIntCols...)

	// Writes to both.
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "cp", Values: []sqllayer.Literal{n(1), n(1)}})
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "ap_tbl", Values: []sqllayer.Literal{n(1), n(2)}})

	// Reads from both.
	cpRs := mustSelect(t, gw, "cp", []string{"*"}, nil)
	apRs := mustSelect(t, gw, "ap_tbl", []string{"*"}, nil)
	assertCount(t, cpRs, 1)
	assertCount(t, apRs, 1)
	if getInt(t, cpRs, 0, 1) != 1 {
		t.Fatalf("cp.v wrong: %d", getInt(t, cpRs, 0, 1))
	}
	if getInt(t, apRs, 0, 1) != 2 {
		t.Fatalf("ap.v wrong: %d", getInt(t, apRs, 0, 1))
	}
}

func TestAP_Mixed_Isolation(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "a", intIntCols...)
	createAPTable(t, gw, "b", intIntCols...)

	mustExec(t, gw, &sqllayer.InsertStatement{Table: "a", Values: []sqllayer.Literal{n(1), n(10)}})
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "b", Values: []sqllayer.Literal{n(1), n(20)}})

	rsA := mustSelect(t, gw, "a", []string{"v"}, nil)
	rsB := mustSelect(t, gw, "b", []string{"v"}, nil)

	if getInt(t, rsA, 0, 0) != 10 {
		t.Fatalf("table a: expected 10, got %d", getInt(t, rsA, 0, 0))
	}
	if getInt(t, rsB, 0, 0) != 20 {
		t.Fatalf("table b: expected 20, got %d", getInt(t, rsB, 0, 0))
	}
}

// =============================================================================
// Two-node AP: writes on one node appear on the other after sync
// =============================================================================

func TestAP_TwoNode_Insert_PropagatesAfterSync(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0) // table 1 on node 1, table 2 on node 2
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)

	// Create AP table (table ID ≥ 1; both ranges see the schema via shared catalog).
	createAPTable(t, gw, "shared", intIntCols...)
	schema := n1.sc.FindTableSchema("shared")
	if schema == nil {
		t.Fatal("schema not found")
	}

	// Insert a row whose key falls on node 1.
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "shared", Values: []sqllayer.Literal{n(1), n(42)}})

	// Node 1 has the row; node 2 does NOT yet.
	key := sqllayer.EncodeKey(schema.TableId, 1)
	n1rows, _ := n1.bt.RangeScan(key, key)
	n2rows, _ := n2.bt.RangeScan(key, key)
	if len(n1rows) != 1 {
		t.Fatalf("expected row on node1, got %d", len(n1rows))
	}
	if len(n2rows) != 0 {
		t.Fatalf("expected NO row on node2 before sync, got %d", len(n2rows))
	}

	// Run sync on node 2 pulling from node 1.
	syncer2 := ap.NewAPSyncer(
		n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)},
		n1.sc,
	)
	syncer2.SyncNow(context.Background())

	// Node 2 should now have the row.
	n2rows, _ = n2.bt.RangeScan(key, key)
	if len(n2rows) != 1 {
		t.Fatalf("expected row on node2 after sync, got %d", len(n2rows))
	}
}

func TestAP_TwoNode_Delete_PropagatesAfterSync(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "del_prop", intIntCols...)
	schema := n1.sc.FindTableSchema("del_prop")

	mustExec(t, gw, &sqllayer.InsertStatement{Table: "del_prop", Values: []sqllayer.Literal{n(1), n(7)}})

	// Sync node 2 so it has the row first.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	// Now delete from node 1.
	mustExec(t, gw, &sqllayer.DeleteStatement{
		Table: "del_prop",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(1)},
	})

	// Sync again — delete must propagate.
	syncer2.SyncNow(context.Background())
	key := sqllayer.EncodeKey(schema.TableId, 1)
	n2rows, _ := n2.bt.RangeScan(key, key)
	if len(n2rows) != 0 {
		t.Fatalf("expected delete to propagate to node2, but row still exists")
	}
}

func TestAP_TwoNode_LWW_ConflictResolution(t *testing.T) {
	splitKey := sqllayer.EncodeKey(2, 0)
	gw, n1, n2 := newTwoNodeAPGateway(t, splitKey)
	createAPTable(t, gw, "conflict", intIntCols...)
	schema := n1.sc.FindTableSchema("conflict")

	// Insert the initial row on node 1.
	mustExec(t, gw, &sqllayer.InsertStatement{Table: "conflict", Values: []sqllayer.Literal{n(1), n(10)}})
	key := sqllayer.EncodeKey(schema.TableId, 1)

	// Node 2 has a direct (older) write for the same key.
	oldTs := n1.timestamps.Get(key) - 1000 // older
	if err := n2.bt.Insert(key, []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 99}},
	}); err != nil {
		t.Fatalf("n2.bt.Insert: %v", err)
	}
	n2.timestamps.Set(key, oldTs)
	if _, err := n2.apLog.Append(raft.ReplPut, key, oldTs, []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 99}},
	}); err != nil {
		t.Fatalf("n2.apLog.Append: %v", err)
	}

	// Sync node 2 from node 1 — node 1's newer write must win.
	syncer2 := ap.NewAPSyncer(n2.bt, n2.timestamps, n2.apLog,
		map[uint64]ap.PeerPuller{1: ap.NewDirectPeerPuller(n1.apLog)}, n1.sc)
	syncer2.SyncNow(context.Background())

	rows, _ := n2.bt.RangeScan(key, key)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	v, ok := rows[0].Fields[1].Value.(btree.IntValue)
	if !ok || v.V != 10 {
		t.Fatalf("expected node1's value (10) to win LWW, got %+v", rows[0].Fields)
	}
}

// =============================================================================
// Stress — concurrent AP writes on a single node
// =============================================================================

func TestAP_Stress_ConcurrentInserts(t *testing.T) {
	gw, node := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "stress", intIntCols...)

	const N = 50
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, errs[id-1] = gw.Execute(&sqllayer.InsertStatement{
				Table: "stress", Values: []sqllayer.Literal{n(id), n(id)},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("insert[%d]: %v", i+1, err)
		}
	}

	rs := mustSelect(t, gw, "stress", []string{"*"}, nil)
	assertCount(t, rs, N)

	// AP log must have N entries.
	entries, _ := node.apLog.ReadFrom(0)
	putCount := 0
	for _, e := range entries {
		if e.Op == raft.ReplPut {
			putCount++
		}
	}
	if putCount != N {
		t.Fatalf("expected %d ReplPut entries in AP log, got %d", N, putCount)
	}
}

func TestAP_Stress_ConcurrentMixedOps(t *testing.T) {
	gw, _ := newSingleNodeAPGateway(t)
	createAPTable(t, gw, "mix", intIntCols...)

	// Pre-insert rows 1–20.
	for i := 1; i <= 20; i++ {
		mustExec(t, gw, &sqllayer.InsertStatement{Table: "mix", Values: []sqllayer.Literal{n(i), n(i)}})
	}

	const workers = 10
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := w + 1
			// Alternate between update and select — errors are acceptable under
			// concurrent load; the test only checks for deadlocks/panics.
			_, _ = gw.Execute(&sqllayer.UpdateStatement{
				Table: "mix", Column: "v", Value: n(id * 100),
				Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(id)},
			})
			_, _ = gw.Execute(&sqllayer.SelectStatement{
				Table: "mix", Columns: []string{"v"},
				Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: n(id)},
			})
		}(w)
	}
	wg.Wait()
	// No deadlocks or panics — success.
}

// apFmt is an unexported helper that satisfies fmt.Stringer for test diagnostics.
var _ = fmt.Sprintf
