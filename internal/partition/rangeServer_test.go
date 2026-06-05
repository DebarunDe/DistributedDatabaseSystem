package partition

import (
	"context"
	"sync"
	"testing"
	"time"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

// newTestRangeServer spins up a RangeServer backed by real in-process dependencies
// in standalone mode (nil Raft node).  The applyFn wires ReplPut/ReplDelete
// directly to the BTree and ReplTxnRecord to the TxnRecordStore.
func newTestRangeServer(t *testing.T) (
	srv *RangeServer,
	bt *btree.BTree,
	sc *sqllayer.SchemaCatalog,
	records *TxnRecordStore,
	tm *lock.TransactionManager,
) {
	t.Helper()
	bt = newTestBTreeGateway(t)
	sc = sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	records = NewTxnRecordStore()

	var applyFn lock.ApplyFn = func(op raft.ReplOp, key uint64, fields []btree.Field) error {
		switch op {
		case raft.ReplTxnRecord:
			records.Store(DecodeTxnRecord(key, fields))
		case raft.ReplPut:
			return bt.Insert(key, fields)
		case raft.ReplDelete:
			return bt.Delete(key)
		}
		return nil
	}

	tm = lock.NewTransactionManager(bt, nil, applyFn)
	srv = NewRangeServer(bt, tm, sc, records)
	return
}

// mustCreateTable calls sc.CreateTable and fatals on error.
func mustCreateTable(t *testing.T, sc *sqllayer.SchemaCatalog, name, pkName, pkType string, cols []string, types []string) uint32 {
	t.Helper()
	if err := sc.CreateTable(name, pkName, pkType, cols, types); err != nil {
		t.Fatalf("CreateTable %q: %v", name, err)
	}
	return sc.FindTableSchema(name).TableId
}

// insertRaw inserts a row into bt directly (outside any transaction).
func insertRaw(t *testing.T, bt *btree.BTree, key uint64, fields []btree.Field) {
	t.Helper()
	if err := bt.Insert(key, fields); err != nil {
		t.Fatalf("bt.Insert(key=%d): %v", key, err)
	}
}

// intFields builds a slice of IntValue fields with consecutive tags starting at 0.
func intFields(vals ...int64) []btree.Field {
	out := make([]btree.Field, len(vals))
	for i, v := range vals {
		out[i] = btree.Field{Tag: uint8(i), Value: btree.IntValue{V: v}}
	}
	return out
}

// mkIntProtoField builds a proto Field with IntVal.
func mkIntProtoField(tag int, val int64) *rs.Field {
	return &rs.Field{
		Tag:   uint32(tag),
		Value: &rs.FieldValue{Value: &rs.FieldValue_IntVal{IntVal: val}},
	}
}

// mkCmp builds an rs.Expression for a single comparison.
func mkCmp(col, op, lit string) *rs.Expression {
	return &rs.Expression{Expr: &rs.Expression_Comparison{
		Comparison: &rs.ComparisonExpr{Column: col, Operator: op, LiteralValue: lit},
	}}
}

// mkAnd builds an AND expression over two sub-expressions.
func mkAnd(left, right *rs.Expression) *rs.Expression {
	return &rs.Expression{Expr: &rs.Expression_Logical{
		Logical: &rs.LogicalExpr{Operator: "AND", Left: left, Right: right},
	}}
}

// mkOr builds an OR expression over two sub-expressions.
func mkOr(left, right *rs.Expression) *rs.Expression {
	return &rs.Expression{Expr: &rs.Expression_Logical{
		Logical: &rs.LogicalExpr{Operator: "OR", Left: left, Right: right},
	}}
}

// testSchema is a reusable in-memory schema with two INT columns:
//
//	fields[0] = id (PK, INT)
//	fields[1] = amount (INT)
//	fields[2] = label (TEXT)
var testSchema = &sqllayer.TableSchemaValue{
	TableId:    1,
	PrimaryKey: sqllayer.ColumnDef{Name: "id", DataType: "INT"},
	Columns: []sqllayer.ColumnDef{
		{Name: "amount", DataType: "INT"},
		{Name: "label", DataType: "TEXT"},
	},
}

// scanBTree returns all rows in the BTree with keys in [lo, hi].
func scanBTree(t *testing.T, bt *btree.BTree, lo, hi uint64) []struct {
	Key    uint64
	Fields []btree.Field
} {
	t.Helper()
	rows, err := bt.RangeScan(lo, hi)
	if err != nil {
		t.Fatalf("RangeScan(%d,%d): %v", lo, hi, err)
	}
	return rows
}

// pendingFor safely reads a PendingTxn from the server's pending map.
func pendingFor(t *testing.T, srv *RangeServer, txnId uint64) *PendingTxn {
	t.Helper()
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.pending[txnId]
}

// ---------------------------------------------------------------------------
// compareProtoInt
// ---------------------------------------------------------------------------

func TestCompareProtoInt_Equal_True(t *testing.T) {
	if !compareProtoInt(5, 5, "=") {
		t.Error("5 = 5 should be true")
	}
}

func TestCompareProtoInt_Equal_False(t *testing.T) {
	if compareProtoInt(5, 6, "=") {
		t.Error("5 = 6 should be false")
	}
}

func TestCompareProtoInt_NotEqual(t *testing.T) {
	if !compareProtoInt(5, 6, "!=") {
		t.Error("5 != 6 should be true")
	}
	if compareProtoInt(7, 7, "!=") {
		t.Error("7 != 7 should be false")
	}
}

func TestCompareProtoInt_Less(t *testing.T) {
	if !compareProtoInt(3, 5, "<") {
		t.Error("3 < 5 should be true")
	}
	if compareProtoInt(5, 3, "<") {
		t.Error("5 < 3 should be false")
	}
	if compareProtoInt(5, 5, "<") {
		t.Error("5 < 5 should be false")
	}
}

func TestCompareProtoInt_LessOrEqual(t *testing.T) {
	if !compareProtoInt(5, 5, "<=") {
		t.Error("5 <= 5 should be true")
	}
	if !compareProtoInt(3, 5, "<=") {
		t.Error("3 <= 5 should be true")
	}
	if compareProtoInt(6, 5, "<=") {
		t.Error("6 <= 5 should be false")
	}
}

func TestCompareProtoInt_Greater(t *testing.T) {
	if !compareProtoInt(10, 5, ">") {
		t.Error("10 > 5 should be true")
	}
	if compareProtoInt(5, 5, ">") {
		t.Error("5 > 5 should be false")
	}
}

func TestCompareProtoInt_GreaterOrEqual(t *testing.T) {
	if !compareProtoInt(5, 5, ">=") {
		t.Error("5 >= 5 should be true")
	}
	if !compareProtoInt(10, 5, ">=") {
		t.Error("10 >= 5 should be true")
	}
	if compareProtoInt(4, 5, ">=") {
		t.Error("4 >= 5 should be false")
	}
}

func TestCompareProtoInt_NegativeValues(t *testing.T) {
	if !compareProtoInt(-10, -5, "<") {
		t.Error("-10 < -5 should be true")
	}
	if !compareProtoInt(-5, -10, ">") {
		t.Error("-5 > -10 should be true")
	}
}

func TestCompareProtoInt_UnknownOp_ReturnsFalse(t *testing.T) {
	for _, op := range []string{"LIKE", "IN", "", "??", "<>"} {
		if compareProtoInt(1, 1, op) {
			t.Errorf("op %q: expected false for unknown operator", op)
		}
	}
}

// ---------------------------------------------------------------------------
// compareProtoString
// ---------------------------------------------------------------------------

func TestCompareProtoString_Equal_True(t *testing.T) {
	if !compareProtoString("alice", "alice", "=") {
		t.Error(`"alice" = "alice" should be true`)
	}
}

func TestCompareProtoString_Equal_False(t *testing.T) {
	if compareProtoString("alice", "bob", "=") {
		t.Error(`"alice" = "bob" should be false`)
	}
}

func TestCompareProtoString_NotEqual(t *testing.T) {
	if !compareProtoString("a", "b", "!=") {
		t.Error(`"a" != "b" should be true`)
	}
	if compareProtoString("x", "x", "!=") {
		t.Error(`"x" != "x" should be false`)
	}
}

func TestCompareProtoString_LexLess(t *testing.T) {
	if !compareProtoString("apple", "banana", "<") {
		t.Error(`"apple" < "banana" should be true`)
	}
	if compareProtoString("z", "a", "<") {
		t.Error(`"z" < "a" should be false`)
	}
}

func TestCompareProtoString_LexGreater(t *testing.T) {
	if !compareProtoString("z", "a", ">") {
		t.Error(`"z" > "a" should be true`)
	}
	if compareProtoString("a", "z", ">") {
		t.Error(`"a" > "z" should be false`)
	}
}

func TestCompareProtoString_LessOrEqual(t *testing.T) {
	if !compareProtoString("a", "a", "<=") {
		t.Error(`"a" <= "a" should be true`)
	}
	if !compareProtoString("a", "b", "<=") {
		t.Error(`"a" <= "b" should be true`)
	}
}

func TestCompareProtoString_GreaterOrEqual(t *testing.T) {
	if !compareProtoString("b", "b", ">=") {
		t.Error(`"b" >= "b" should be true`)
	}
	if !compareProtoString("z", "a", ">=") {
		t.Error(`"z" >= "a" should be true`)
	}
}

func TestCompareProtoString_UnknownOp_ReturnsFalse(t *testing.T) {
	if compareProtoString("a", "a", "LIKE") {
		t.Error("LIKE: expected false for unknown operator")
	}
	if compareProtoString("a", "a", "") {
		t.Error("empty op: expected false for unknown operator")
	}
}

// ---------------------------------------------------------------------------
// matchProtoExpr
// ---------------------------------------------------------------------------

func TestMatchProtoExpr_NilExpr_ReturnsTrue(t *testing.T) {
	fields := intFields(42, 100)
	if !matchProtoExpr(nil, testSchema, fields) {
		t.Error("nil expression should match all rows")
	}
}

func TestMatchProtoExpr_IntComparison_EQ_Match(t *testing.T) {
	fields := intFields(42, 100)
	expr := mkCmp("id", "=", "42")
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error("id=42 should match row with id=42")
	}
}

func TestMatchProtoExpr_IntComparison_EQ_NoMatch(t *testing.T) {
	fields := intFields(42, 100)
	expr := mkCmp("id", "=", "99")
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("id=99 should not match row with id=42")
	}
}

func TestMatchProtoExpr_IntComparison_NonPKColumn(t *testing.T) {
	fields := intFields(1, 500)
	expr := mkCmp("amount", ">", "400")
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error("amount>400 should match row with amount=500")
	}
}

func TestMatchProtoExpr_StringComparison_EQ_Match(t *testing.T) {
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 0}},
		{Tag: 2, Value: btree.StringValue{V: "alice"}},
	}
	expr := mkCmp("label", "=", "alice")
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error(`label="alice" should match row with label="alice"`)
	}
}

func TestMatchProtoExpr_StringComparison_EQ_NoMatch(t *testing.T) {
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 0}},
		{Tag: 2, Value: btree.StringValue{V: "bob"}},
	}
	expr := mkCmp("label", "=", "alice")
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error(`label="alice" should not match row with label="bob"`)
	}
}

func TestMatchProtoExpr_UnknownColumn_ReturnsFalse(t *testing.T) {
	fields := intFields(1, 100)
	expr := mkCmp("nonexistent", "=", "1")
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("unknown column should not match")
	}
}

func TestMatchProtoExpr_InvalidLiteral_ReturnsFalse(t *testing.T) {
	fields := intFields(1, 100)
	expr := mkCmp("id", "=", "notanumber")
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("invalid literal for int column should not match")
	}
}

func TestMatchProtoExpr_ColumnIndexOutOfRange_ReturnsFalse(t *testing.T) {
	fields := intFields(1) // only one field, column "amount" at index 1 is out of range
	expr := mkCmp("amount", "=", "0")
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("column index out of range should not match")
	}
}

func TestMatchProtoExpr_AND_BothTrue_ReturnsTrue(t *testing.T) {
	fields := intFields(5, 200)
	expr := mkAnd(mkCmp("id", ">", "1"), mkCmp("amount", "<", "500"))
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error("AND(id>1, amount<500) should match row (5, 200)")
	}
}

func TestMatchProtoExpr_AND_OneFalse_ReturnsFalse(t *testing.T) {
	fields := intFields(5, 200)
	expr := mkAnd(mkCmp("id", ">", "1"), mkCmp("amount", ">", "500"))
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("AND(id>1, amount>500) should not match row (5, 200)")
	}
}

func TestMatchProtoExpr_OR_BothFalse_ReturnsFalse(t *testing.T) {
	fields := intFields(1, 100)
	expr := mkOr(mkCmp("id", ">", "10"), mkCmp("amount", ">", "500"))
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("OR(id>10, amount>500) should not match row (1, 100)")
	}
}

func TestMatchProtoExpr_OR_OneTrue_ReturnsTrue(t *testing.T) {
	fields := intFields(1, 100)
	expr := mkOr(mkCmp("id", "=", "1"), mkCmp("amount", ">", "500"))
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error("OR(id=1, amount>500) should match row (1, 100)")
	}
}

func TestMatchProtoExpr_Logical_CaseInsensitiveOperator(t *testing.T) {
	fields := intFields(5, 200)
	// "and" (lowercase) should behave the same as "AND"
	expr := &rs.Expression{Expr: &rs.Expression_Logical{
		Logical: &rs.LogicalExpr{
			Operator: "and",
			Left:     mkCmp("id", ">", "1"),
			Right:    mkCmp("amount", ">", "100"),
		},
	}}
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error("lowercase 'and' operator should work (case-insensitive)")
	}
}

func TestMatchProtoExpr_Logical_UnknownOp_ReturnsFalse(t *testing.T) {
	fields := intFields(5, 200)
	expr := &rs.Expression{Expr: &rs.Expression_Logical{
		Logical: &rs.LogicalExpr{
			Operator: "XOR",
			Left:     mkCmp("id", "=", "5"),
			Right:    mkCmp("id", "=", "5"),
		},
	}}
	if matchProtoExpr(expr, testSchema, fields) {
		t.Error("unknown logical operator should return false")
	}
}

func TestMatchProtoExpr_NestedLogical(t *testing.T) {
	// (id > 0 AND amount > 50) OR id = 99
	fields := intFields(3, 100)
	expr := mkOr(
		mkAnd(mkCmp("id", ">", "0"), mkCmp("amount", ">", "50")),
		mkCmp("id", "=", "99"),
	)
	if !matchProtoExpr(expr, testSchema, fields) {
		t.Error("nested (id>0 AND amount>50) OR id=99 should match row (3, 100)")
	}
}

// ---------------------------------------------------------------------------
// filterRows
// ---------------------------------------------------------------------------

func TestFilterRows_NilWhere_ReturnsAllRows(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	rows := []scanRow{
		{Key: 1, Fields: intFields(1, 10)},
		{Key: 2, Fields: intFields(2, 20)},
		{Key: 3, Fields: intFields(3, 30)},
	}
	got, err := srv.filterRows(0, rows, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("len=%d, want 3", len(got))
	}
}

func TestFilterRows_EmptyRows_ReturnsEmpty(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	got, err := srv.filterRows(0, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len=%d, want 0", len(got))
	}
}

func TestFilterRows_TableIDNotFound_ReturnsError(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	rows := []scanRow{{Key: 1, Fields: intFields(1, 10)}}
	expr := mkCmp("id", "=", "1")
	_, err := srv.filterRows(999, rows, expr)
	if err == nil {
		t.Fatal("expected error for unknown tableID, got nil")
	}
}

func TestFilterRows_WhereMatches_ReturnsSubset(t *testing.T) {
	srv, _, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "items", "id", "INT", []string{"price"}, []string{"INT"})

	rows := []scanRow{
		{Key: 1, Fields: intFields(1, 10)},
		{Key: 2, Fields: intFields(2, 50)},
		{Key: 3, Fields: intFields(3, 200)},
	}
	// WHERE price > 20 → rows 2 and 3
	expr := mkCmp("price", ">", "20")
	got, err := srv.filterRows(tableID, rows, expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len=%d, want 2", len(got))
	}
}

func TestFilterRows_WhereMatchesNone_ReturnsEmpty(t *testing.T) {
	srv, _, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "items2", "id", "INT", []string{"price"}, []string{"INT"})

	rows := []scanRow{
		{Key: 1, Fields: intFields(1, 5)},
		{Key: 2, Fields: intFields(2, 8)},
	}
	expr := mkCmp("price", ">", "1000")
	got, err := srv.filterRows(tableID, rows, expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len=%d, want 0", len(got))
	}
}

func TestFilterRows_WhereMatchesAll(t *testing.T) {
	srv, _, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "items3", "id", "INT", []string{"price"}, []string{"INT"})

	rows := []scanRow{
		{Key: 1, Fields: intFields(1, 100)},
		{Key: 2, Fields: intFields(2, 200)},
	}
	expr := mkCmp("price", ">", "0")
	got, err := srv.filterRows(tableID, rows, expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len=%d, want 2", len(got))
	}
}

// ---------------------------------------------------------------------------
// NewRangeServer
// ---------------------------------------------------------------------------

func TestNewRangeServer_NotNil(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	if srv == nil {
		t.Fatal("NewRangeServer returned nil")
	}
}

func TestNewRangeServer_PendingMapEmpty(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	srv.mu.Lock()
	n := len(srv.pending)
	srv.mu.Unlock()
	if n != 0 {
		t.Errorf("pending map len=%d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Prepare — INSERT
// ---------------------------------------------------------------------------

func TestPrepare_INSERT_ReturnsSuccess(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  1,
		Op:     rs.RangeOp_INSERT,
		Key:    100,
		Fields: []*rs.Field{mkIntProtoField(0, 42)},
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if !resp.Success {
		t.Errorf("Success=false, error=%q", resp.Error)
	}
}

func TestPrepare_INSERT_StoresPendingTxn(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  2,
		Op:     rs.RangeOp_INSERT,
		Key:    200,
		Fields: []*rs.Field{mkIntProtoField(0, 7)},
	})
	if p := pendingFor(t, srv, 2); p == nil {
		t.Fatal("pending txn should be stored after Prepare INSERT")
	}
}

func TestPrepare_INSERT_CommandHasReplPutOp(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  3,
		Op:     rs.RangeOp_INSERT,
		Key:    300,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	p := pendingFor(t, srv, 3)
	if p == nil {
		t.Fatal("pending txn not found")
	}
	if len(p.Commands) != 1 {
		t.Fatalf("commands len=%d, want 1", len(p.Commands))
	}
	if p.Commands[0].Op != raft.ReplPut {
		t.Errorf("command Op=%v, want ReplPut", p.Commands[0].Op)
	}
}

func TestPrepare_INSERT_CommandKeyMatchesRequest(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	const key = uint64(777)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  4,
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	p := pendingFor(t, srv, 4)
	if p == nil || p.Commands[0].Key != key {
		t.Errorf("command Key=%d, want %d", p.Commands[0].Key, key)
	}
}

func TestPrepare_INSERT_CommandFieldsFromRequest(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId: 5,
		Op:    rs.RangeOp_INSERT,
		Key:   500,
		Fields: []*rs.Field{
			mkIntProtoField(0, 99),
			mkIntProtoField(1, 200),
		},
	})
	p := pendingFor(t, srv, 5)
	if p == nil {
		t.Fatal("pending txn not found")
	}
	if len(p.Commands[0].Fields) != 2 {
		t.Fatalf("fields len=%d, want 2", len(p.Commands[0].Fields))
	}
	iv, ok := p.Commands[0].Fields[0].Value.(btree.IntValue)
	if !ok || iv.V != 99 {
		t.Errorf("fields[0]=%v, want IntValue{99}", p.Commands[0].Fields[0])
	}
}

func TestPrepare_INSERT_LockedKeyStoredInPending(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	const key = uint64(600)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  6,
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	p := pendingFor(t, srv, 6)
	if p == nil || len(p.LockedKeys) != 1 || p.LockedKeys[0] != key {
		t.Errorf("LockedKeys=%v, want [%d]", p.LockedKeys, key)
	}
}

// ---------------------------------------------------------------------------
// Prepare — UPDATE
// ---------------------------------------------------------------------------

func TestPrepare_UPDATE_NoRowsInRange_EmptyCommands(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	// BTree is empty; no rows in [1000, 2000]
	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:     10,
		Op:        rs.RangeOp_UPDATE,
		StartKey:  1000,
		EndKey:    2000,
		Fields:    []*rs.Field{mkIntProtoField(1, 999)},
		UpdateCol: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Success=false: %s", resp.Error)
	}
	p := pendingFor(t, srv, 10)
	if p == nil {
		t.Fatal("pending txn not found")
	}
	if len(p.Commands) != 0 {
		t.Errorf("commands len=%d, want 0 for empty range", len(p.Commands))
	}
}

func TestPrepare_UPDATE_MatchedRow_CreatesReplPutCommand(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 1001, intFields(1, 100))

	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:     11,
		Op:        rs.RangeOp_UPDATE,
		StartKey:  1000,
		EndKey:    1002,
		Fields:    []*rs.Field{mkIntProtoField(1, 999)},
		UpdateCol: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Success=false: %s", resp.Error)
	}
	p := pendingFor(t, srv, 11)
	if p == nil || len(p.Commands) != 1 {
		t.Fatalf("expected 1 command, got %v", p)
	}
	if p.Commands[0].Op != raft.ReplPut {
		t.Errorf("Op=%v, want ReplPut", p.Commands[0].Op)
	}
	if p.Commands[0].Key != 1001 {
		t.Errorf("Key=%d, want 1001", p.Commands[0].Key)
	}
}

func TestPrepare_UPDATE_AppliesNewFieldValue(t *testing.T) {
	// This tests the fix: req.Fields[0] at req.UpdateCol must be applied
	// to the new row, not just copy the old fields unchanged.
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 2000, intFields(1, 100)) // row: id=1, amount=100

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:     12,
		Op:        rs.RangeOp_UPDATE,
		StartKey:  2000,
		EndKey:    2000,
		Fields:    []*rs.Field{mkIntProtoField(1, 999)}, // set amount=999
		UpdateCol: 1,
	})
	p := pendingFor(t, srv, 12)
	if p == nil || len(p.Commands) != 1 {
		t.Fatalf("expected 1 command")
	}
	// After the fix: fields[1] should be 999, not the old value 100
	cmd := p.Commands[0]
	if len(cmd.Fields) < 2 {
		t.Fatalf("command fields len=%d, want ≥2", len(cmd.Fields))
	}
	iv, ok := cmd.Fields[1].Value.(btree.IntValue)
	if !ok {
		t.Fatalf("fields[1] not IntValue, got %T", cmd.Fields[1].Value)
	}
	if iv.V != 999 {
		t.Errorf("fields[1]=%d, want 999 (UPDATE fix: new value must be applied)", iv.V)
	}
}

func TestPrepare_UPDATE_MultipleRowsMatched_MultipleCommands(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 3000, intFields(1, 10))
	insertRaw(t, bt, 3001, intFields(2, 20))
	insertRaw(t, bt, 3002, intFields(3, 30))

	resp, _ := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:     13,
		Op:        rs.RangeOp_UPDATE,
		StartKey:  3000,
		EndKey:    3002,
		Fields:    []*rs.Field{mkIntProtoField(1, 99)},
		UpdateCol: 1,
	})
	if !resp.Success {
		t.Fatalf("Success=false: %s", resp.Error)
	}
	p := pendingFor(t, srv, 13)
	if p == nil || len(p.Commands) != 3 {
		t.Errorf("commands len=%d, want 3", len(p.Commands))
	}
}

func TestPrepare_UPDATE_WhereFilter_OnlyMatchedRows(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "upd_tbl", "id", "INT", []string{"score"}, []string{"INT"})

	insertRaw(t, bt, 4000, intFields(1, 10))
	insertRaw(t, bt, 4001, intFields(2, 50))
	insertRaw(t, bt, 4002, intFields(3, 80))

	// WHERE score > 30 → rows at 4001 and 4002
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:     14,
		Op:        rs.RangeOp_UPDATE,
		TableId:   tableID,
		StartKey:  4000,
		EndKey:    4002,
		Where:     mkCmp("score", ">", "30"),
		Fields:    []*rs.Field{mkIntProtoField(1, 0)},
		UpdateCol: 1,
	})
	p := pendingFor(t, srv, 14)
	if p == nil || len(p.Commands) != 2 {
		t.Errorf("commands len=%d, want 2 (score>30 filters row at 4000)", len(p.Commands))
	}
}

// ---------------------------------------------------------------------------
// Prepare — DELETE
// ---------------------------------------------------------------------------

func TestPrepare_DELETE_NoRowsInRange_EmptyCommands(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:    20,
		Op:       rs.RangeOp_DELETE,
		StartKey: 5000,
		EndKey:   6000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Success=false: %s", resp.Error)
	}
	p := pendingFor(t, srv, 20)
	if p == nil || len(p.Commands) != 0 {
		t.Errorf("commands len=%d, want 0 for empty range", len(p.Commands))
	}
}

func TestPrepare_DELETE_MatchedRow_CreatesReplDeleteCommand(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 5001, intFields(1, 10))

	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:    21,
		Op:       rs.RangeOp_DELETE,
		StartKey: 5000,
		EndKey:   5002,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Success=false: %s", resp.Error)
	}
	p := pendingFor(t, srv, 21)
	if p == nil || len(p.Commands) != 1 {
		t.Fatalf("commands len=%d, want 1", len(p.Commands))
	}
	if p.Commands[0].Op != raft.ReplDelete {
		t.Errorf("Op=%v, want ReplDelete", p.Commands[0].Op)
	}
	if p.Commands[0].Key != 5001 {
		t.Errorf("Key=%d, want 5001", p.Commands[0].Key)
	}
}

func TestPrepare_DELETE_MultipleRows_MultipleCommands(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 6000, intFields(1, 0))
	insertRaw(t, bt, 6001, intFields(2, 0))
	insertRaw(t, bt, 6002, intFields(3, 0))

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:    22,
		Op:       rs.RangeOp_DELETE,
		StartKey: 6000,
		EndKey:   6002,
	})
	p := pendingFor(t, srv, 22)
	if p == nil || len(p.Commands) != 3 {
		t.Errorf("commands len=%d, want 3", len(p.Commands))
	}
	for _, cmd := range p.Commands {
		if cmd.Op != raft.ReplDelete {
			t.Errorf("Op=%v, want ReplDelete", cmd.Op)
		}
	}
}

func TestPrepare_DELETE_WhereFilter_OnlyMatchedRows(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "del_tbl", "id", "INT", []string{"active"}, []string{"INT"})

	insertRaw(t, bt, 7000, intFields(1, 0)) // active=0
	insertRaw(t, bt, 7001, intFields(2, 1)) // active=1
	insertRaw(t, bt, 7002, intFields(3, 1)) // active=1

	// DELETE WHERE active = 1 → rows at 7001 and 7002
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:    23,
		Op:       rs.RangeOp_DELETE,
		TableId:  tableID,
		StartKey: 7000,
		EndKey:   7002,
		Where:    mkCmp("active", "=", "1"),
	})
	p := pendingFor(t, srv, 23)
	if p == nil || len(p.Commands) != 2 {
		t.Errorf("commands len=%d, want 2", len(p.Commands))
	}
}

func TestPrepare_DELETE_LockedKeysStoredInPending(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 8000, intFields(1, 0))
	insertRaw(t, bt, 8001, intFields(2, 0))

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:    24,
		Op:       rs.RangeOp_DELETE,
		StartKey: 8000,
		EndKey:   8001,
	})
	p := pendingFor(t, srv, 24)
	if p == nil || len(p.LockedKeys) != 2 {
		t.Errorf("LockedKeys=%v, want 2 keys", p.LockedKeys)
	}
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

func TestCommit_UnknownTxn_ReturnsFailure(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	resp, err := srv.Commit(context.Background(), &rs.CommitRequest{TxnId: 999})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.Success {
		t.Error("Commit of unknown txn should return Success=false")
	}
	if resp.Error != "transaction not found" {
		t.Errorf("Error=%q, want %q", resp.Error, "transaction not found")
	}
}

func TestCommit_INSERT_DataAppliedToBTree(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	const key = uint64(9001)

	// Prepare INSERT
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  30,
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 42)},
	})

	// Commit
	resp, err := srv.Commit(context.Background(), &rs.CommitRequest{TxnId: 30})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Commit failed: %s", resp.Error)
	}

	// Verify data is in the BTree
	rows := scanBTree(t, bt, key, key)
	if len(rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(rows))
	}
	iv, ok := rows[0].Fields[0].Value.(btree.IntValue)
	if !ok || iv.V != 42 {
		t.Errorf("field[0]=%v, want IntValue{42}", rows[0].Fields[0])
	}
}

func TestCommit_UPDATE_NewValueAppliedToBTree(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 9100, intFields(1, 100))

	// Prepare UPDATE: set field[1] = 999
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:     31,
		Op:        rs.RangeOp_UPDATE,
		StartKey:  9100,
		EndKey:    9100,
		Fields:    []*rs.Field{mkIntProtoField(1, 999)},
		UpdateCol: 1,
	})
	_, _ = srv.Commit(context.Background(), &rs.CommitRequest{TxnId: 31})

	rows := scanBTree(t, bt, 9100, 9100)
	if len(rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(rows))
	}
	iv, ok := rows[0].Fields[1].Value.(btree.IntValue)
	if !ok || iv.V != 999 {
		t.Errorf("after UPDATE commit: fields[1]=%v, want IntValue{999}", rows[0].Fields[1])
	}
}

func TestCommit_DELETE_RowRemovedFromBTree(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	insertRaw(t, bt, 9200, intFields(1, 5))

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:    32,
		Op:       rs.RangeOp_DELETE,
		StartKey: 9200,
		EndKey:   9200,
	})
	_, _ = srv.Commit(context.Background(), &rs.CommitRequest{TxnId: 32})

	rows := scanBTree(t, bt, 9200, 9200)
	if len(rows) != 0 {
		t.Errorf("rows len=%d after DELETE commit, want 0", len(rows))
	}
}

func TestCommit_RemovesPendingTxn(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  33,
		Op:     rs.RangeOp_INSERT,
		Key:    9300,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	_, _ = srv.Commit(context.Background(), &rs.CommitRequest{TxnId: 33})

	if p := pendingFor(t, srv, 33); p != nil {
		t.Error("pending txn should be removed after Commit")
	}
}

// ---------------------------------------------------------------------------
// Abort
// ---------------------------------------------------------------------------

func TestAbort_UnknownTxn_ReturnsSuccess(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	resp, err := srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 999})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if !resp.Success {
		t.Errorf("Abort of unknown txn should return Success=true (idempotent)")
	}
}

func TestAbort_KnownTxn_ReturnsSuccess(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  40,
		Op:     rs.RangeOp_INSERT,
		Key:    10000,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	resp, err := srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 40})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if !resp.Success {
		t.Errorf("Abort should return Success=true, got false")
	}
}

func TestAbort_RemovesPendingTxn(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  41,
		Op:     rs.RangeOp_INSERT,
		Key:    10001,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	_, _ = srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 41})

	if p := pendingFor(t, srv, 41); p != nil {
		t.Error("pending txn should be removed after Abort")
	}
}

func TestAbort_INSERT_DataNotWrittenToBTree(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	const key = uint64(10100)

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  42,
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 77)},
	})
	_, _ = srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 42})

	rows := scanBTree(t, bt, key, key)
	if len(rows) != 0 {
		t.Errorf("rows len=%d after Abort, want 0 (data must not be written)", len(rows))
	}
}

func TestAbort_Idempotent_SecondAbortSucceeds(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  43,
		Op:     rs.RangeOp_INSERT,
		Key:    10200,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	_, _ = srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 43})

	resp, err := srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 43})
	if err != nil || !resp.Success {
		t.Errorf("second Abort should succeed: err=%v success=%v", err, resp.Success)
	}
}

func TestAbort_ReleasesLocksForNextTxn(t *testing.T) {
	// After txn A aborts, txn B should be able to acquire the same key.
	srv, _, _, _, _ := newTestRangeServer(t)
	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  44,
		Op:     rs.RangeOp_INSERT,
		Key:    10300,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	_, _ = srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 44})

	// txn 45 should be able to lock the same key now
	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  45,
		Op:     rs.RangeOp_INSERT,
		Key:    10300,
		Fields: []*rs.Field{mkIntProtoField(0, 2)},
	})
	if err != nil || !resp.Success {
		t.Errorf("after Abort, next txn should acquire the key: err=%v success=%v", err, resp.Success)
	}
}

// ---------------------------------------------------------------------------
// WriteCommitRecord
// ---------------------------------------------------------------------------

func TestWriteCommitRecord_ReturnsSuccess(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)
	resp, err := srv.WriteCommitRecord(context.Background(), &rs.WriteCommitRecordRequest{
		TxnId:    50,
		Status:   uint32(DistTxnCommitted),
		RangeIds: []uint64{1, 2},
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if !resp.Success {
		t.Errorf("Success=false: %q", resp.Error)
	}
}

func TestWriteCommitRecord_StoresRecordViaApplyFn(t *testing.T) {
	srv, _, _, records, _ := newTestRangeServer(t)
	_, _ = srv.WriteCommitRecord(context.Background(), &rs.WriteCommitRecordRequest{
		TxnId:    51,
		Status:   uint32(DistTxnCommitted),
		RangeIds: []uint64{10, 20, 30},
	})

	got, ok := records.Get(51)
	if !ok {
		t.Fatal("record not stored in TxnRecordStore after WriteCommitRecord")
	}
	if got.TxnId != 51 {
		t.Errorf("TxnId=%d, want 51", got.TxnId)
	}
	if got.Status != DistTxnCommitted {
		t.Errorf("Status=%d, want DistTxnCommitted", got.Status)
	}
}

func TestWriteCommitRecord_AllRangeIDsPreserved(t *testing.T) {
	srv, _, _, records, _ := newTestRangeServer(t)
	wantIDs := []uint64{100, 200, 300}
	_, _ = srv.WriteCommitRecord(context.Background(), &rs.WriteCommitRecordRequest{
		TxnId:    52,
		Status:   uint32(DistTxnCommitted),
		RangeIds: wantIDs,
	})

	got, ok := records.Get(52)
	if !ok {
		t.Fatal("record not found")
	}
	if len(got.RangeIDs) != len(wantIDs) {
		t.Fatalf("RangeIDs len=%d, want %d", len(got.RangeIDs), len(wantIDs))
	}
	for i, id := range wantIDs {
		if got.RangeIDs[i] != id {
			t.Errorf("RangeIDs[%d]=%d, want %d", i, got.RangeIDs[i], id)
		}
	}
}

func TestWriteCommitRecord_StatusPreserved(t *testing.T) {
	srv, _, _, records, _ := newTestRangeServer(t)
	for _, status := range []DistTxnStatus{DistTxnPending, DistTxnPrepared, DistTxnAborted} {
		txnId := uint64(60 + status)
		_, _ = srv.WriteCommitRecord(context.Background(), &rs.WriteCommitRecordRequest{
			TxnId:  txnId,
			Status: uint32(status),
		})
		got, ok := records.Get(txnId)
		if !ok {
			t.Errorf("txnId=%d: record not found", txnId)
			continue
		}
		if got.Status != status {
			t.Errorf("txnId=%d: Status=%d, want %d", txnId, got.Status, status)
		}
	}
}

// ---------------------------------------------------------------------------
// Lock conflict — deadlock detected, Prepare returns failure
// ---------------------------------------------------------------------------

func TestPrepare_INSERT_DeadlockDetected_ReturnsFailure(t *testing.T) {
	srv, _, _, _, tm := newTestRangeServer(t)

	const (
		txn1 = uint64(100)
		txn2 = uint64(101)
		keyA = uint64(20000)
		keyB = uint64(20001)
	)

	// txn1 holds keyA; txn2 holds keyB
	if err := tm.Lock(txn1, keyA, lock.LockExclusive); err != nil {
		t.Fatalf("txn1 lock keyA: %v", err)
	}
	if err := tm.Lock(txn2, keyB, lock.LockExclusive); err != nil {
		t.Fatalf("txn2 lock keyB: %v", err)
	}

	// txn1 blocks waiting for keyB held by txn2.
	goroutineDone := make(chan error, 1)
	go func() {
		goroutineDone <- tm.Lock(txn1, keyB, lock.LockExclusive)
	}()

	// Allow the goroutine to enter the wait queue.
	time.Sleep(20 * time.Millisecond)

	// txn2 now tries to INSERT at keyA (held by txn1).
	// Graph: txn1→txn2, txn2→txn1 → cycle → deadlock.
	resp, err := srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  txn2,
		Op:     rs.RangeOp_INSERT,
		Key:    keyA,
		Fields: []*rs.Field{mkIntProtoField(0, 42)},
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.Success {
		t.Error("expected Prepare to fail on deadlock, got Success=true")
	}

	// Prepare's ReleaseAll(txn2) releases keyB, waking txn1's goroutine.
	select {
	case err := <-goroutineDone:
		// txn1 either acquired keyB or received an error; either way, it unblocked.
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("txn1 goroutine did not unblock after txn2 released keyB")
	}

	tm.ReleaseAll(txn1)
}

// ---------------------------------------------------------------------------
// Prepare → Commit / Abort lifecycle
// ---------------------------------------------------------------------------

func TestPrepareCommit_INSERT_EndToEnd(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	const key = uint64(30000)

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  70,
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 123), mkIntProtoField(1, 456)},
	})
	resp, _ := srv.Commit(context.Background(), &rs.CommitRequest{TxnId: 70})
	if !resp.Success {
		t.Fatalf("Commit failed: %s", resp.Error)
	}

	rows := scanBTree(t, bt, key, key)
	if len(rows) != 1 {
		t.Fatalf("rows after commit=%d, want 1", len(rows))
	}
	if rows[0].Key != key {
		t.Errorf("row key=%d, want %d", rows[0].Key, key)
	}
}

func TestPrepareAbort_INSERT_NoPersistence(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	const key = uint64(31000)

	_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
		TxnId:  71,
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 1)},
	})
	_, _ = srv.Abort(context.Background(), &rs.AbortRequest{TxnId: 71})

	rows := scanBTree(t, bt, key, key)
	if len(rows) != 0 {
		t.Errorf("rows after abort=%d, want 0", len(rows))
	}
}

// ---------------------------------------------------------------------------
// Concurrency — concurrent Prepare calls do not corrupt pending map
// ---------------------------------------------------------------------------

func TestPrepare_ConcurrentInserts_NoPanicOrCorruption(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)

	var wg sync.WaitGroup
	for i := uint64(0); i < 50; i++ {
		wg.Add(1)
		go func(txnId, key uint64) {
			defer wg.Done()
			_, _ = srv.Prepare(context.Background(), &rs.PrepareRequest{
				TxnId:  txnId,
				Op:     rs.RangeOp_INSERT,
				Key:    key,
				Fields: []*rs.Field{mkIntProtoField(0, int64(key))},
			})
		}(i+1000, i+40000)
	}
	wg.Wait()

	srv.mu.Lock()
	n := len(srv.pending)
	srv.mu.Unlock()

	if n != 50 {
		t.Errorf("pending map len=%d after 50 concurrent prepares, want 50", n)
	}
}

// ---------------------------------------------------------------------------
// Execute — single-range read/write (the gateway's non-2PC path)
// ---------------------------------------------------------------------------

func TestExecute_Insert_DataApplied(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)

	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:     rs.RangeOp_INSERT,
		Key:    50000,
		Fields: []*rs.Field{mkIntProtoField(0, 7), mkIntProtoField(1, 99)},
	})
	if err != nil {
		t.Fatalf("Execute INSERT: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("Execute INSERT error: %s", resp.Error)
	}

	rows := scanBTree(t, bt, 50000, 50000)
	if len(rows) != 1 {
		t.Fatalf("row count=%d, want 1", len(rows))
	}
	if iv, ok := rows[0].Fields[1].Value.(btree.IntValue); !ok || iv.V != 99 {
		t.Errorf("fields[1]=%v, want IntValue{99}", rows[0].Fields[1])
	}
}

func TestExecute_Scan_ReturnsMatchingRows(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex1", "id", "INT", []string{"val"}, []string{"INT"})
	insertRaw(t, bt, sqllayer.EncodeKey(tableID, 1), intFields(1, 10))
	insertRaw(t, bt, sqllayer.EncodeKey(tableID, 2), intFields(2, 20))
	insertRaw(t, bt, sqllayer.EncodeKey(tableID, 3), intFields(3, 30))

	start := sqllayer.EncodeKey(tableID, 0)
	end := sqllayer.EncodeKey(tableID, ^uint32(0))
	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:       rs.RangeOp_SCAN,
		TableId:  tableID,
		StartKey: start,
		EndKey:   end,
	})
	if err != nil {
		t.Fatalf("Execute SCAN: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("Execute SCAN error: %s", resp.Error)
	}
	if len(resp.Rows) != 3 {
		t.Errorf("rows=%d, want 3", len(resp.Rows))
	}
}

func TestExecute_Scan_WhereFilters(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex2", "id", "INT", []string{"val"}, []string{"INT"})
	for i := uint32(1); i <= 5; i++ {
		insertRaw(t, bt, sqllayer.EncodeKey(tableID, i), intFields(int64(i), int64(i*10)))
	}

	start := sqllayer.EncodeKey(tableID, 0)
	end := sqllayer.EncodeKey(tableID, ^uint32(0))
	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:       rs.RangeOp_SCAN,
		TableId:  tableID,
		StartKey: start,
		EndKey:   end,
		Where:    mkCmp("id", ">", "3"),
	})
	if err != nil {
		t.Fatalf("Execute SCAN with WHERE: %v", err)
	}
	if len(resp.Rows) != 2 { // rows 4 and 5
		t.Errorf("rows=%d, want 2 (id > 3)", len(resp.Rows))
	}
}

func TestExecute_Scan_ColumnProjection(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex3", "id", "INT", []string{"val"}, []string{"INT"})
	insertRaw(t, bt, sqllayer.EncodeKey(tableID, 1), intFields(1, 42))

	start := sqllayer.EncodeKey(tableID, 0)
	end := sqllayer.EncodeKey(tableID, ^uint32(0))
	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:       rs.RangeOp_SCAN,
		TableId:  tableID,
		StartKey: start,
		EndKey:   end,
		Columns:  []int32{1}, // only "val"
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute SCAN with Columns: err=%v resp.Error=%s", err, resp.Error)
	}
	if len(resp.Rows) != 1 || len(resp.Rows[0].Fields) != 1 {
		t.Fatalf("expected 1 row with 1 field, got %d rows", len(resp.Rows))
	}
	if got := resp.Rows[0].Fields[0].GetValue().GetIntVal(); got != 42 {
		t.Errorf("projected field value=%d, want 42", got)
	}
}

func TestExecute_Update_ModifiesData(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex4", "id", "INT", []string{"val"}, []string{"INT"})
	key := sqllayer.EncodeKey(tableID, 5)
	insertRaw(t, bt, key, intFields(5, 100))

	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:        rs.RangeOp_UPDATE,
		TableId:   tableID,
		StartKey:  key,
		EndKey:    key,
		Fields:    []*rs.Field{mkIntProtoField(1, 999)},
		UpdateCol: 1,
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute UPDATE: err=%v resp.Error=%s", err, resp.Error)
	}

	rows := scanBTree(t, bt, key, key)
	if len(rows) != 1 {
		t.Fatalf("row count=%d, want 1", len(rows))
	}
	if iv, ok := rows[0].Fields[1].Value.(btree.IntValue); !ok || iv.V != 999 {
		t.Errorf("val=%v, want 999 after UPDATE", rows[0].Fields[1])
	}
}

func TestExecute_Delete_RemovesRow(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex5", "id", "INT", []string{"val"}, []string{"INT"})
	key := sqllayer.EncodeKey(tableID, 7)
	insertRaw(t, bt, key, intFields(7, 0))

	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:       rs.RangeOp_DELETE,
		TableId:  tableID,
		StartKey: key,
		EndKey:   key,
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute DELETE: err=%v resp.Error=%s", err, resp.Error)
	}

	rows := scanBTree(t, bt, key, key)
	if len(rows) != 0 {
		t.Errorf("rows=%d after DELETE, want 0", len(rows))
	}
}

func TestExecute_Delete_WhereFilters(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex6", "id", "INT", []string{"val"}, []string{"INT"})
	for i := uint32(1); i <= 5; i++ {
		insertRaw(t, bt, sqllayer.EncodeKey(tableID, i), intFields(int64(i), 0))
	}

	start := sqllayer.EncodeKey(tableID, 0)
	end := sqllayer.EncodeKey(tableID, ^uint32(0))
	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:       rs.RangeOp_DELETE,
		TableId:  tableID,
		StartKey: start,
		EndKey:   end,
		Where:    mkCmp("id", "<=", "3"),
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute DELETE with WHERE: err=%v resp.Error=%s", err, resp.Error)
	}

	rows := scanBTree(t, bt, start, end)
	if len(rows) != 2 { // rows 4 and 5 remain
		t.Errorf("remaining rows=%d, want 2", len(rows))
	}
}

func TestExecute_Insert_DuplicateKey_Overwrites(t *testing.T) {
	srv, bt, _, _, _ := newTestRangeServer(t)
	const key = uint64(60000)
	insertRaw(t, bt, key, intFields(1, 10))

	// Insert at the same key with a different value — BTree upserts.
	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:     rs.RangeOp_INSERT,
		Key:    key,
		Fields: []*rs.Field{mkIntProtoField(0, 1), mkIntProtoField(1, 999)},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute INSERT overwrite: err=%v resp.Error=%s", err, resp.Error)
	}

	rows := scanBTree(t, bt, key, key)
	if len(rows) != 1 {
		t.Fatalf("row count=%d, want 1", len(rows))
	}
	if iv, ok := rows[0].Fields[1].Value.(btree.IntValue); !ok || iv.V != 999 {
		t.Errorf("val=%v, want 999 (overwrite)", rows[0].Fields[1])
	}
}

func TestExecute_Scan_EmptyRange_ReturnsEmpty(t *testing.T) {
	srv, _, _, _, _ := newTestRangeServer(t)

	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:       rs.RangeOp_SCAN,
		StartKey: 70000,
		EndKey:   71000,
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute SCAN empty: err=%v resp.Error=%s", err, resp.Error)
	}
	if len(resp.Rows) != 0 {
		t.Errorf("rows=%d, want 0 for empty range", len(resp.Rows))
	}
}

func TestExecute_Update_NoMatchingRows_Succeeds(t *testing.T) {
	srv, bt, sc, _, _ := newTestRangeServer(t)
	tableID := mustCreateTable(t, sc, "ex7", "id", "INT", []string{"val"}, []string{"INT"})
	insertRaw(t, bt, sqllayer.EncodeKey(tableID, 1), intFields(1, 5))

	resp, err := srv.Execute(context.Background(), &rs.RangeRequest{
		Op:        rs.RangeOp_UPDATE,
		TableId:   tableID,
		StartKey:  sqllayer.EncodeKey(tableID, 0),
		EndKey:    sqllayer.EncodeKey(tableID, ^uint32(0)),
		Where:     mkCmp("id", "=", "999"), // no match
		Fields:    []*rs.Field{mkIntProtoField(1, 0)},
		UpdateCol: 1,
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("Execute UPDATE no-match: err=%v resp.Error=%s", err, resp.Error)
	}
	// Original row unchanged.
	rows := scanBTree(t, bt, sqllayer.EncodeKey(tableID, 1), sqllayer.EncodeKey(tableID, 1))
	if iv, ok := rows[0].Fields[1].Value.(btree.IntValue); !ok || iv.V != 5 {
		t.Errorf("val=%v, want 5 (no-match UPDATE must not change data)", rows[0].Fields[1])
	}
}
