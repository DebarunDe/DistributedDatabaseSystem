package partition

import (
	"context"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// Fake gRPC server
// ---------------------------------------------------------------------------

type fakeRangeServer struct {
	rs.UnimplementedRangeServiceServer
	mu       sync.Mutex
	received []*rs.RangeRequest
	respFn   func(*rs.RangeRequest) (*rs.RangeResponse, error)

	// 2PC call tracking
	prepareRecv  []*rs.PrepareRequest
	commitRecv   []*rs.CommitRequest
	abortRecv    []*rs.AbortRequest
	writeRecRecv []*rs.WriteCommitRecordRequest

	// Configurable Prepare handler; nil → always succeed.
	prepareFn func(*rs.PrepareRequest) (*rs.PrepareResponse, error)
}

func (s *fakeRangeServer) Execute(_ context.Context, req *rs.RangeRequest) (*rs.RangeResponse, error) {
	s.mu.Lock()
	s.received = append(s.received, req)
	fn := s.respFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &rs.RangeResponse{}, nil
}

func (s *fakeRangeServer) Prepare(_ context.Context, req *rs.PrepareRequest) (*rs.PrepareResponse, error) {
	s.mu.Lock()
	s.prepareRecv = append(s.prepareRecv, req)
	fn := s.prepareFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &rs.PrepareResponse{Success: true}, nil
}

func (s *fakeRangeServer) Commit(_ context.Context, req *rs.CommitRequest) (*rs.CommitResponse, error) {
	s.mu.Lock()
	s.commitRecv = append(s.commitRecv, req)
	s.mu.Unlock()
	return &rs.CommitResponse{Success: true}, nil
}

func (s *fakeRangeServer) Abort(_ context.Context, req *rs.AbortRequest) (*rs.AbortResponse, error) {
	s.mu.Lock()
	s.abortRecv = append(s.abortRecv, req)
	s.mu.Unlock()
	return &rs.AbortResponse{Success: true}, nil
}

func (s *fakeRangeServer) WriteCommitRecord(_ context.Context, req *rs.WriteCommitRecordRequest) (*rs.WriteCommitRecordResponse, error) {
	s.mu.Lock()
	s.writeRecRecv = append(s.writeRecRecv, req)
	s.mu.Unlock()
	return &rs.WriteCommitRecordResponse{Success: true}, nil
}

func (s *fakeRangeServer) requests() []*rs.RangeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*rs.RangeRequest, len(s.received))
	copy(out, s.received)
	return out
}

func (s *fakeRangeServer) prepareRequests() []*rs.PrepareRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*rs.PrepareRequest, len(s.prepareRecv))
	copy(out, s.prepareRecv)
	return out
}

func (s *fakeRangeServer) commitRequests() []*rs.CommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*rs.CommitRequest, len(s.commitRecv))
	copy(out, s.commitRecv)
	return out
}

func (s *fakeRangeServer) abortRequests() []*rs.AbortRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*rs.AbortRequest, len(s.abortRecv))
	copy(out, s.abortRecv)
	return out
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

func newTestBTreeGateway(t *testing.T) *btree.BTree {
	t.Helper()
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	return btree.NewBTree(pm)
}

// newTestGateway wires up a Gateway with:
//   - a SchemaCatalog containing table "users"(id INT PK, name TEXT)
//   - a single-range Coordinator with leader=node 1
//   - one bufconn-backed gRPC connection to node 1 served by srv
func newTestGateway(t *testing.T, srv *fakeRangeServer) (*Gateway, *sqllayer.SchemaCatalog) {
	t.Helper()

	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	coord := NewCoordinator(map[uint64]string{1: "node1:8080"})
	coord.UpdateLeader(1, 1)
	router, err := NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	conn := startFakeServer(t, srv)
	gw := NewGateway(router, sc)
	gw.AddConn(1, conn)
	return gw, sc
}

// newSplitGateway creates a Gateway whose keyspace is split at splitKey.
// Both halves have leader=node 1, backed by the same fake server.
func newSplitGateway(t *testing.T, splitKey uint64, srv *fakeRangeServer) (*Gateway, *sqllayer.SchemaCatalog) {
	t.Helper()

	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	coord := NewCoordinator(map[uint64]string{1: "node1:8080"})
	if err := coord.RequestSplit(1, splitKey); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	coord.UpdateLeader(1, 1) // upper range (original id=1)
	coord.UpdateLeader(2, 1) // lower range (new id=2)

	router, err := NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	conn := startFakeServer(t, srv)
	gw := NewGateway(router, sc)
	gw.AddConn(1, conn)
	return gw, sc
}

// startFakeServer spins up an in-process gRPC server backed by bufconn.
func startFakeServer(t *testing.T, srv *fakeRangeServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	rs.RegisterRangeServiceServer(grpcSrv, srv)
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

// intLit returns an INT literal, matching sqllayer.TOKEN_NUMBER.
func intLit(v string) sqllayer.Literal {
	return sqllayer.Literal{Value: v, Type: sqllayer.TOKEN_NUMBER}
}

// strLit returns a TEXT literal.
func strLit(v string) sqllayer.Literal {
	return sqllayer.Literal{Value: v, Type: sqllayer.TOKEN_STRING}
}

// ---------------------------------------------------------------------------
// extractPKBounds
// ---------------------------------------------------------------------------

func TestExtractPKBounds_Nil(t *testing.T) {
	low, high := extractPKBounds(nil, "id")
	if low != 0 || high != math.MaxUint32 {
		t.Errorf("nil expr: got (%d,%d), want (0,MaxUint32)", low, high)
	}
}

func TestExtractPKBounds_EQ(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("42")}
	low, high := extractPKBounds(expr, "id")
	if low != 42 || high != 42 {
		t.Errorf("EQ 42: got (%d,%d), want (42,42)", low, high)
	}
}

func TestExtractPKBounds_GT(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: ">", Value: intLit("10")}
	low, high := extractPKBounds(expr, "id")
	if low != 11 || high != math.MaxUint32 {
		t.Errorf("GT 10: got (%d,%d), want (11,MaxUint32)", low, high)
	}
}

func TestExtractPKBounds_GT_MaxUint32(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: ">", Value: intLit("4294967295")}
	low, high := extractPKBounds(expr, "id")
	if low != math.MaxUint32 || high != math.MaxUint32 {
		t.Errorf("GT MaxUint32: got (%d,%d), want (MaxUint32,MaxUint32)", low, high)
	}
}

func TestExtractPKBounds_GTE(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: ">=", Value: intLit("10")}
	low, high := extractPKBounds(expr, "id")
	if low != 10 || high != math.MaxUint32 {
		t.Errorf("GTE 10: got (%d,%d), want (10,MaxUint32)", low, high)
	}
}

func TestExtractPKBounds_LT(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "<", Value: intLit("10")}
	low, high := extractPKBounds(expr, "id")
	if low != 0 || high != 9 {
		t.Errorf("LT 10: got (%d,%d), want (0,9)", low, high)
	}
}

func TestExtractPKBounds_LT_Zero(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "<", Value: intLit("0")}
	low, high := extractPKBounds(expr, "id")
	if low != 0 || high != 0 {
		t.Errorf("LT 0: got (%d,%d), want (0,0)", low, high)
	}
}

func TestExtractPKBounds_LTE(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "<=", Value: intLit("10")}
	low, high := extractPKBounds(expr, "id")
	if low != 0 || high != 10 {
		t.Errorf("LTE 10: got (%d,%d), want (0,10)", low, high)
	}
}

func TestExtractPKBounds_NonPKColumn_FullScan(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "name", Operator: "=", Value: strLit("alice")}
	low, high := extractPKBounds(expr, "id")
	if low != 0 || high != math.MaxUint32 {
		t.Errorf("non-PK filter: got (%d,%d), want (0,MaxUint32)", low, high)
	}
}

func TestExtractPKBounds_InvalidNumber_FullScan(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: strLit("notanumber")}
	low, high := extractPKBounds(expr, "id")
	if low != 0 || high != math.MaxUint32 {
		t.Errorf("invalid number: got (%d,%d), want (0,MaxUint32)", low, high)
	}
}

func TestExtractPKBounds_AND_NarrowsRange(t *testing.T) {
	expr := &sqllayer.LogicalExpr{
		Operator: "AND",
		Left:     &sqllayer.ComparisonExpr{Column: "id", Operator: ">=", Value: intLit("5")},
		Right:    &sqllayer.ComparisonExpr{Column: "id", Operator: "<=", Value: intLit("20")},
	}
	low, high := extractPKBounds(expr, "id")
	if low != 5 || high != 20 {
		t.Errorf("AND [5,20]: got (%d,%d), want (5,20)", low, high)
	}
}

func TestExtractPKBounds_OR_WidensRange(t *testing.T) {
	expr := &sqllayer.LogicalExpr{
		Operator: "OR",
		Left:     &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("3")},
		Right:    &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("99")},
	}
	low, high := extractPKBounds(expr, "id")
	if low != 3 || high != 99 {
		t.Errorf("OR (3,99): got (%d,%d), want (3,99)", low, high)
	}
}

func TestExtractPKBounds_UnknownOperator_FullScan(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "LIKE", Value: intLit("5")}
	low, high := extractPKBounds(expr, "id")
	if low != 0 || high != math.MaxUint32 {
		t.Errorf("LIKE operator: got (%d,%d), want (0,MaxUint32)", low, high)
	}
}

// ---------------------------------------------------------------------------
// fieldsToProto / protoFieldToBTree
// ---------------------------------------------------------------------------

func TestFieldsToProto_Empty(t *testing.T) {
	out := fieldsToProto(nil)
	if len(out) != 0 {
		t.Errorf("empty: got len=%d, want 0", len(out))
	}
}

func TestFieldsToProto_IntField(t *testing.T) {
	fields := []btree.Field{{Tag: 0, Value: btree.IntValue{V: 42}}}
	out := fieldsToProto(fields)
	if len(out) != 1 {
		t.Fatalf("len=%d, want 1", len(out))
	}
	if out[0].Tag != 0 {
		t.Errorf("tag=%d, want 0", out[0].Tag)
	}
	iv, ok := out[0].Value.Value.(*rs.FieldValue_IntVal)
	if !ok {
		t.Fatalf("value is not IntVal")
	}
	if iv.IntVal != 42 {
		t.Errorf("IntVal=%d, want 42", iv.IntVal)
	}
}

func TestFieldsToProto_StringField(t *testing.T) {
	fields := []btree.Field{{Tag: 1, Value: btree.StringValue{V: "alice"}}}
	out := fieldsToProto(fields)
	if len(out) != 1 {
		t.Fatalf("len=%d, want 1", len(out))
	}
	sv, ok := out[0].Value.Value.(*rs.FieldValue_StrVal)
	if !ok {
		t.Fatalf("value is not StrVal")
	}
	if sv.StrVal != "alice" {
		t.Errorf("StrVal=%q, want %q", sv.StrVal, "alice")
	}
}

func TestFieldsToProto_MultipleFields_TagPreserved(t *testing.T) {
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.StringValue{V: "bob"}},
	}
	out := fieldsToProto(fields)
	if len(out) != 2 {
		t.Fatalf("len=%d, want 2", len(out))
	}
	if out[0].Tag != 0 || out[1].Tag != 1 {
		t.Errorf("tags=%d,%d, want 0,1", out[0].Tag, out[1].Tag)
	}
}

func TestProtoFieldToBTree_IntValue(t *testing.T) {
	pf := &rs.Field{Tag: 3, Value: &rs.FieldValue{Value: &rs.FieldValue_IntVal{IntVal: 99}}}
	bf := protoFieldToBTree(pf)
	if bf.Tag != 3 {
		t.Errorf("tag=%d, want 3", bf.Tag)
	}
	iv, ok := bf.Value.(btree.IntValue)
	if !ok {
		t.Fatal("expected IntValue")
	}
	if iv.V != 99 {
		t.Errorf("V=%d, want 99", iv.V)
	}
}

func TestProtoFieldToBTree_StringValue(t *testing.T) {
	pf := &rs.Field{Tag: 1, Value: &rs.FieldValue{Value: &rs.FieldValue_StrVal{StrVal: "carol"}}}
	bf := protoFieldToBTree(pf)
	sv, ok := bf.Value.(btree.StringValue)
	if !ok {
		t.Fatal("expected StringValue")
	}
	if sv.V != "carol" {
		t.Errorf("V=%q, want %q", sv.V, "carol")
	}
}

func TestProtoFieldToBTree_NilValue_ReturnsZeroField(t *testing.T) {
	pf := &rs.Field{Tag: 2, Value: nil}
	bf := protoFieldToBTree(pf)
	if bf.Tag != 2 {
		t.Errorf("tag=%d, want 2", bf.Tag)
	}
	if bf.Value != nil {
		t.Errorf("expected nil value, got %v", bf.Value)
	}
}

func TestFieldsToProto_RoundTrip(t *testing.T) {
	original := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 7}},
		{Tag: 1, Value: btree.StringValue{V: "dave"}},
	}
	protos := fieldsToProto(original)
	for i, pf := range protos {
		got := protoFieldToBTree(pf)
		if got.Tag != original[i].Tag {
			t.Errorf("[%d] tag mismatch: got %d, want %d", i, got.Tag, original[i].Tag)
		}
		switch orig := original[i].Value.(type) {
		case btree.IntValue:
			if iv, ok := got.Value.(btree.IntValue); !ok || iv.V != orig.V {
				t.Errorf("[%d] IntValue mismatch: got %v, want %v", i, got.Value, orig)
			}
		case btree.StringValue:
			if sv, ok := got.Value.(btree.StringValue); !ok || sv.V != orig.V {
				t.Errorf("[%d] StringValue mismatch: got %v, want %v", i, got.Value, orig)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// exprToProto
// ---------------------------------------------------------------------------

func TestExprToProto_Nil(t *testing.T) {
	if exprToProto(nil) != nil {
		t.Error("nil expr: expected nil proto")
	}
}

func TestExprToProto_Comparison(t *testing.T) {
	expr := &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("5")}
	p := exprToProto(expr)
	if p == nil {
		t.Fatal("expected non-nil proto")
	}
	cmp, ok := p.Expr.(*rs.Expression_Comparison)
	if !ok {
		t.Fatalf("expected Comparison, got %T", p.Expr)
	}
	if cmp.Comparison.Column != "id" || cmp.Comparison.Operator != "=" || cmp.Comparison.LiteralValue != "5" {
		t.Errorf("comparison fields wrong: %+v", cmp.Comparison)
	}
}

func TestExprToProto_Logical_AND(t *testing.T) {
	expr := &sqllayer.LogicalExpr{
		Operator: "AND",
		Left:     &sqllayer.ComparisonExpr{Column: "id", Operator: ">", Value: intLit("1")},
		Right:    &sqllayer.ComparisonExpr{Column: "id", Operator: "<", Value: intLit("10")},
	}
	p := exprToProto(expr)
	if p == nil {
		t.Fatal("expected non-nil proto")
	}
	log, ok := p.Expr.(*rs.Expression_Logical)
	if !ok {
		t.Fatalf("expected Logical, got %T", p.Expr)
	}
	if log.Logical.Operator != "AND" {
		t.Errorf("operator=%q, want AND", log.Logical.Operator)
	}
	if log.Logical.Left == nil || log.Logical.Right == nil {
		t.Error("left/right must be non-nil")
	}
}

func TestExprToProto_Logical_NestedDepth(t *testing.T) {
	expr := &sqllayer.LogicalExpr{
		Operator: "OR",
		Left: &sqllayer.LogicalExpr{
			Operator: "AND",
			Left:     &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("1")},
			Right:    &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("2")},
		},
		Right: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("3")},
	}
	p := exprToProto(expr)
	if p == nil {
		t.Fatal("expected non-nil proto")
	}
	if _, ok := p.Expr.(*rs.Expression_Logical); !ok {
		t.Fatalf("top level not Logical, got %T", p.Expr)
	}
}

// ---------------------------------------------------------------------------
// colTypeByIndex
// ---------------------------------------------------------------------------

func TestColTypeByIndex_PrimaryKey(t *testing.T) {
	schema := &sqllayer.TableSchemaValue{
		PrimaryKey: sqllayer.ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []sqllayer.ColumnDef{{Name: "name", DataType: "TEXT"}},
	}
	if got := colTypeByIndex(0, schema); got != "INT" {
		t.Errorf("idx 0: got %q, want INT", got)
	}
}

func TestColTypeByIndex_FirstColumn(t *testing.T) {
	schema := &sqllayer.TableSchemaValue{
		PrimaryKey: sqllayer.ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []sqllayer.ColumnDef{{Name: "name", DataType: "TEXT"}},
	}
	if got := colTypeByIndex(1, schema); got != "TEXT" {
		t.Errorf("idx 1: got %q, want TEXT", got)
	}
}

func TestColTypeByIndex_SecondColumn(t *testing.T) {
	schema := &sqllayer.TableSchemaValue{
		PrimaryKey: sqllayer.ColumnDef{Name: "id", DataType: "INT"},
		Columns: []sqllayer.ColumnDef{
			{Name: "name", DataType: "TEXT"},
			{Name: "active", DataType: "BOOL"},
		},
	}
	if got := colTypeByIndex(2, schema); got != "BOOL" {
		t.Errorf("idx 2: got %q, want BOOL", got)
	}
}

// ---------------------------------------------------------------------------
// min/max helpers
// ---------------------------------------------------------------------------

func TestMin32(t *testing.T) {
	cases := [][3]uint32{{1, 2, 1}, {2, 1, 1}, {5, 5, 5}, {0, math.MaxUint32, 0}}
	for _, c := range cases {
		if got := min32(c[0], c[1]); got != c[2] {
			t.Errorf("min32(%d,%d)=%d, want %d", c[0], c[1], got, c[2])
		}
	}
}

func TestMax32(t *testing.T) {
	cases := [][3]uint32{{1, 2, 2}, {2, 1, 2}, {5, 5, 5}, {0, math.MaxUint32, math.MaxUint32}}
	for _, c := range cases {
		if got := max32(c[0], c[1]); got != c[2] {
			t.Errorf("max32(%d,%d)=%d, want %d", c[0], c[1], got, c[2])
		}
	}
}

func TestMin64(t *testing.T) {
	cases := [][3]uint64{{1, 2, 1}, {2, 1, 1}, {5, 5, 5}}
	for _, c := range cases {
		if got := min64(c[0], c[1]); got != c[2] {
			t.Errorf("min64(%d,%d)=%d, want %d", c[0], c[1], got, c[2])
		}
	}
}

func TestMax64(t *testing.T) {
	cases := [][3]uint64{{1, 2, 2}, {2, 1, 2}, {5, 5, 5}}
	for _, c := range cases {
		if got := max64(c[0], c[1]); got != c[2] {
			t.Errorf("max64(%d,%d)=%d, want %d", c[0], c[1], got, c[2])
		}
	}
}

// ---------------------------------------------------------------------------
// NewGateway / AddConn
// ---------------------------------------------------------------------------

func TestNewGateway_NotNil(t *testing.T) {
	coord := NewCoordinator(map[uint64]string{1: "addr"})
	router, _ := NewRouter(coord)
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	gw := NewGateway(router, sc)
	if gw == nil {
		t.Fatal("NewGateway returned nil")
	}
}

func TestAddConn_ConnectionStored(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	if len(gw.conns) != 1 {
		t.Errorf("conns len=%d, want 1", len(gw.conns))
	}
}

// ---------------------------------------------------------------------------
// sendToLeader error paths
// ---------------------------------------------------------------------------

func TestSendToLeader_NoConnection_Error(t *testing.T) {
	coord := NewCoordinator(map[uint64]string{1: "node1:8080"})
	coord.UpdateLeader(1, 1)
	router, _ := NewRouter(coord)
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	gw := NewGateway(router, sc) // no AddConn call

	desc, _ := router.RouteKey(0)
	_, err := gw.sendToLeader(desc, &rs.RangeRequest{})
	if err == nil {
		t.Fatal("expected error for missing leader connection")
	}
}

func TestSendToLeader_ServerReturnsErrorString(t *testing.T) {
	srv := &fakeRangeServer{
		respFn: func(_ *rs.RangeRequest) (*rs.RangeResponse, error) {
			return &rs.RangeResponse{Error: "disk full"}, nil
		},
	}
	gw, _ := newTestGateway(t, srv)
	desc, _ := gw.router.RouteKey(0)
	_, err := gw.sendToLeader(desc, &rs.RangeRequest{Op: rs.RangeOp_SCAN})
	if err == nil || err.Error() != "range error: disk full" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSendToLeader_RowsDeserialised(t *testing.T) {
	srv := &fakeRangeServer{
		respFn: func(_ *rs.RangeRequest) (*rs.RangeResponse, error) {
			return &rs.RangeResponse{
				Rows: []*rs.ResultRow{
					{Key: 1, Fields: []*rs.Field{
						{Tag: 0, Value: &rs.FieldValue{Value: &rs.FieldValue_IntVal{IntVal: 1}}},
					}},
				},
			}, nil
		},
	}
	gw, _ := newTestGateway(t, srv)
	desc, _ := gw.router.RouteKey(0)
	result, err := gw.sendToLeader(desc, &rs.RangeRequest{Op: rs.RangeOp_SCAN})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Key != 1 {
		t.Errorf("unexpected rows: %+v", result.Rows)
	}
}

// ---------------------------------------------------------------------------
// Execute INSERT
// ---------------------------------------------------------------------------

func TestExecute_Insert_TableNotFound(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.InsertStatement{
		Table:  "missing",
		Values: []sqllayer.Literal{intLit("1"), strLit("alice")},
	})
	if err == nil {
		t.Fatal("expected table-not-found error")
	}
}

func TestExecute_Insert_InvalidPrimaryKey(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.InsertStatement{
		Table:  "users",
		Values: []sqllayer.Literal{strLit("notanint"), strLit("alice")},
	})
	if err == nil {
		t.Fatal("expected error for invalid primary key")
	}
}

func TestExecute_Insert_SendsCorrectOp(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.InsertStatement{
		Table:  "users",
		Values: []sqllayer.Literal{intLit("1"), strLit("alice")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	if reqs[0].Op != rs.RangeOp_INSERT {
		t.Errorf("Op=%v, want INSERT", reqs[0].Op)
	}
}

func TestExecute_Insert_KeyEncodedCorrectly(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.InsertStatement{
		Table:  "users",
		Values: []sqllayer.Literal{intLit("7"), strLit("bob")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := srv.requests()[0]
	schema := gw.schema.FindTableSchema("users")
	wantKey := sqllayer.EncodeKey(schema.TableId, 7)
	if req.Key != wantKey {
		t.Errorf("Key=%d, want %d", req.Key, wantKey)
	}
}

func TestExecute_Insert_FieldsPopulated(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.InsertStatement{
		Table:  "users",
		Values: []sqllayer.Literal{intLit("3"), strLit("carol")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := srv.requests()[0]
	if len(req.Fields) != 2 {
		t.Fatalf("fields len=%d, want 2", len(req.Fields))
	}
	if sv, ok := req.Fields[1].Value.Value.(*rs.FieldValue_StrVal); !ok || sv.StrVal != "carol" {
		t.Errorf("fields[1]=%v, want StrVal=carol", req.Fields[1])
	}
}

// ---------------------------------------------------------------------------
// Execute SELECT
// ---------------------------------------------------------------------------

func TestExecute_Select_TableNotFound(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{Table: "missing", Columns: []string{"id"}})
	if err == nil {
		t.Fatal("expected table-not-found error")
	}
}

func TestExecute_Select_ColumnNotFound_Error(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"nonexistent"},
	})
	if err == nil {
		t.Fatal("expected column-not-found error")
	}
}

func TestExecute_Select_SendsScanOp(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"id"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 1 || reqs[0].Op != rs.RangeOp_SCAN {
		t.Errorf("expected 1 SCAN, got %d requests, op=%v", len(reqs), reqs[0].Op)
	}
}

func TestExecute_Select_TableIdSetInRequest(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"id"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	schema := gw.schema.FindTableSchema("users")
	req := srv.requests()[0]
	if req.TableId != schema.TableId {
		t.Errorf("TableId=%d, want %d", req.TableId, schema.TableId)
	}
}

func TestExecute_Select_WhereForwardedToServer(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	where := &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("5")}
	_, err := gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"id"},
		Where:   where,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := srv.requests()[0]
	if req.Where == nil {
		t.Error("expected WHERE expression to be forwarded, got nil")
	}
}

func TestExecute_Select_ColumnsForwardedToServer(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"id", "name"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := srv.requests()[0]
	if len(req.Columns) != 2 {
		t.Fatalf("columns len=%d, want 2", len(req.Columns))
	}
}

func TestExecute_Select_ResultRowsMerged(t *testing.T) {
	srv := &fakeRangeServer{
		respFn: func(_ *rs.RangeRequest) (*rs.RangeResponse, error) {
			return &rs.RangeResponse{
				Rows: []*rs.ResultRow{
					{Key: 1, Fields: []*rs.Field{{Tag: 0, Value: &rs.FieldValue{Value: &rs.FieldValue_IntVal{IntVal: 1}}}}},
				},
			}, nil
		},
	}
	gw, _ := newTestGateway(t, srv)
	result, err := gw.Execute(&sqllayer.SelectStatement{Table: "users", Columns: []string{"id"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("rows=%d, want 1", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Execute SELECT — scatterGather (multi-range)
// ---------------------------------------------------------------------------

func TestExecute_Select_ScatterGather_SendsRequestPerRange(t *testing.T) {
	srv := &fakeRangeServer{}
	// split at a key that falls inside the users table key space
	// EncodeKey(1, 50) puts the split midway through tableId=1
	schema := &sqllayer.TableSchemaValue{TableId: 1}
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"id"},
		Where:   nil, // full-table scan → touches both ranges
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(srv.requests()); got != 2 {
		t.Errorf("expected 2 SCAN requests (one per range), got %d", got)
	}
}

func TestExecute_Select_ScatterGather_TableIdSetOnAllRequests(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	splitKey := sqllayer.EncodeKey(tableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{Table: "users", Columns: []string{"id"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, req := range srv.requests() {
		if req.TableId != tableId {
			t.Errorf("request[%d]: TableId=%d, want %d", i, req.TableId, tableId)
		}
	}
}

func TestExecute_Select_ScatterGather_KeyBoundsScopedToRange(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	splitKey := sqllayer.EncodeKey(tableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.SelectStatement{Table: "users", Columns: []string{"id"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, req := range srv.requests() {
		if req.StartKey >= req.EndKey {
			t.Errorf("request[%d]: StartKey(%d) >= EndKey(%d)", i, req.StartKey, req.EndKey)
		}
	}
}

func TestExecute_Select_ScatterGather_RowsMergedAcrossRanges(t *testing.T) {
	var callCount int32
	srv := &fakeRangeServer{
		respFn: func(_ *rs.RangeRequest) (*rs.RangeResponse, error) {
			key := uint64(atomic.AddInt32(&callCount, 1))
			return &rs.RangeResponse{
				Rows: []*rs.ResultRow{
					{Key: key, Fields: []*rs.Field{{Tag: 0, Value: &rs.FieldValue{Value: &rs.FieldValue_IntVal{IntVal: int64(key)}}}}},
				},
			}, nil
		},
	}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	splitKey := sqllayer.EncodeKey(tableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	result, err := gw.Execute(&sqllayer.SelectStatement{Table: "users", Columns: []string{"id"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("merged rows=%d, want 2", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Execute UPDATE
// ---------------------------------------------------------------------------

func TestExecute_Update_TableNotFound(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{
		Table:  "missing",
		Column: "name",
		Value:  strLit("new"),
	})
	if err == nil {
		t.Fatal("expected table-not-found error")
	}
}

func TestExecute_Update_ColumnNotFound_Error(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "nonexistent",
		Value:  strLit("x"),
	})
	if err == nil {
		t.Fatal("expected column-not-found error (not a panic)")
	}
}

func TestExecute_Update_SendsCorrectOp(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  strLit("newname"),
		Where: &sqllayer.ComparisonExpr{
			Column: "id", Operator: "=", Value: intLit("1"),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 1 || reqs[0].Op != rs.RangeOp_UPDATE {
		t.Errorf("expected 1 UPDATE, got %d ops", len(reqs))
	}
}

func TestExecute_Update_UpdateColSetCorrectly(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  strLit("newname"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := srv.requests()[0]
	// "name" is the first non-PK column → index 1
	if req.UpdateCol != 1 {
		t.Errorf("UpdateCol=%d, want 1", req.UpdateCol)
	}
}

func TestExecute_Update_MultiRange_SendsToEachRange(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	splitKey := sqllayer.EncodeKey(tableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  strLit("x"),
		Where:  nil, // full-table → both ranges
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Multi-range mutations go through 2PC: one Prepare per range.
	if got := len(srv.prepareRequests()); got != 2 {
		t.Errorf("expected 2 Prepare calls (one per range), got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Execute DELETE
// ---------------------------------------------------------------------------

func TestExecute_Delete_TableNotFound(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.DeleteStatement{Table: "missing"})
	if err == nil {
		t.Fatal("expected table-not-found error")
	}
}

func TestExecute_Delete_SendsCorrectOp(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.DeleteStatement{
		Table: "users",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("5")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 1 || reqs[0].Op != rs.RangeOp_DELETE {
		t.Errorf("expected 1 DELETE, got %d ops", len(reqs))
	}
}

func TestExecute_Delete_WhereForwardedToServer(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	where := &sqllayer.ComparisonExpr{Column: "id", Operator: "=", Value: intLit("3")}
	_, _ = gw.Execute(&sqllayer.DeleteStatement{Table: "users", Where: where})
	req := srv.requests()[0]
	if req.Where == nil {
		t.Error("WHERE expression not forwarded to server")
	}
}

func TestExecute_Delete_MultiRange_SendsToEachRange(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	splitKey := sqllayer.EncodeKey(tableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.DeleteStatement{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Multi-range mutations go through 2PC: one Prepare per range.
	if got := len(srv.prepareRequests()); got != 2 {
		t.Errorf("expected 2 Prepare calls (one per range), got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Execute CREATE TABLE
// ---------------------------------------------------------------------------

func TestExecute_CreateTable_SendsInsertOp(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.CreateTableStatement{
		Table: "orders",
		Columns: []sqllayer.ColumnDef{
			{Name: "order_id", DataType: "INT"},
			{Name: "total", DataType: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	if reqs[0].Op != rs.RangeOp_INSERT {
		t.Errorf("Op=%v, want INSERT", reqs[0].Op)
	}
}

func TestExecute_CreateTable_TableId0_ForSchemaTable(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.CreateTableStatement{
		Table: "orders",
		Columns: []sqllayer.ColumnDef{
			{Name: "order_id", DataType: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := srv.requests()[0]
	if req.TableId != 0 {
		t.Errorf("TableId=%d, want 0 (schema table)", req.TableId)
	}
}

// ---------------------------------------------------------------------------
// Execute DROP TABLE
// ---------------------------------------------------------------------------

func TestExecute_DropTable_TableNotFound(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.DropTableStatement{Table: "missing"})
	if err == nil {
		t.Fatal("expected table-not-found error")
	}
}

func TestExecute_DropTable_SendsAtLeastTwoDeleteRequests(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.DropTableStatement{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Data deletion goes through 2PC (≥1 Prepare with DELETE op).
	preps := srv.prepareRequests()
	if len(preps) < 1 {
		t.Errorf("expected ≥1 Prepare for data deletion, got %d", len(preps))
	}
	for _, req := range preps {
		if req.Op != rs.RangeOp_DELETE {
			t.Errorf("Prepare Op=%v, want DELETE", req.Op)
		}
	}
	// Schema deletion goes through Execute (1 DELETE with TableId=0).
	execs := srv.requests()
	if len(execs) < 1 {
		t.Errorf("expected ≥1 Execute DELETE for schema deletion, got %d", len(execs))
	}
}

func TestExecute_DropTable_FirstRequestTargetsSchemaTable(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.DropTableStatement{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	first := srv.requests()[0]
	if first.TableId != 0 {
		t.Errorf("first DELETE TableId=%d, want 0 (schema table)", first.TableId)
	}
}

func TestExecute_DropTable_ReturnsEmptyResultSet(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	result, err := gw.Execute(&sqllayer.DropTableStatement{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil, want empty ResultSet")
	}
	if len(result.Rows) != 0 {
		t.Errorf("rows=%d, want 0", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// 2PC — multi-range UPDATE / DELETE via ExecuteDistributed
// ---------------------------------------------------------------------------

func TestExecute_Update_MultiRange_2PC_CommitCalledPerRange(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	splitKey := sqllayer.EncodeKey(sc.FindTableSchema("users").TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{Table: "users", Column: "name", Value: strLit("x")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(srv.commitRequests()); got != 2 {
		t.Errorf("expected 2 Commit calls (one per range), got %d", got)
	}
}

func TestExecute_Update_MultiRange_2PC_PrepareOpIsUpdate(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	splitKey := sqllayer.EncodeKey(sc.FindTableSchema("users").TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, _ = gw.Execute(&sqllayer.UpdateStatement{Table: "users", Column: "name", Value: strLit("x")})

	for i, req := range srv.prepareRequests() {
		if req.Op != rs.RangeOp_UPDATE {
			t.Errorf("prepare[%d]: Op=%v, want UPDATE", i, req.Op)
		}
	}
}

func TestExecute_Delete_MultiRange_2PC_CommitCalledPerRange(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	splitKey := sqllayer.EncodeKey(sc.FindTableSchema("users").TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.DeleteStatement{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(srv.commitRequests()); got != 2 {
		t.Errorf("expected 2 Commit calls (one per range), got %d", got)
	}
}

func TestExecute_Update_MultiRange_2PC_PrepareFailure_ReturnsError(t *testing.T) {
	var calls int32
	srv := &fakeRangeServer{
		prepareFn: func(req *rs.PrepareRequest) (*rs.PrepareResponse, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				return &rs.PrepareResponse{Success: false, Error: "lock conflict"}, nil
			}
			return &rs.PrepareResponse{Success: true}, nil
		},
	}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	splitKey := sqllayer.EncodeKey(sc.FindTableSchema("users").TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, err := gw.Execute(&sqllayer.UpdateStatement{Table: "users", Column: "name", Value: strLit("x")})
	if err == nil {
		t.Fatal("expected error when Prepare fails, got nil")
	}
}

func TestExecute_Update_MultiRange_2PC_PrepareFailure_AbortsOtherRange(t *testing.T) {
	var prepares int32
	srv := &fakeRangeServer{
		prepareFn: func(*rs.PrepareRequest) (*rs.PrepareResponse, error) {
			// First prepare succeeds, second fails.
			if atomic.AddInt32(&prepares, 1) == 2 {
				return &rs.PrepareResponse{Success: false, Error: "conflict"}, nil
			}
			return &rs.PrepareResponse{Success: true}, nil
		},
	}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	splitKey := sqllayer.EncodeKey(sc.FindTableSchema("users").TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, _ = gw.Execute(&sqllayer.UpdateStatement{Table: "users", Column: "name", Value: strLit("x")})

	// The range that succeeded its Prepare must receive an Abort.
	if got := len(srv.abortRequests()); got < 1 {
		t.Errorf("expected ≥1 Abort for the prepared range, got %d", got)
	}
	// No Commit should be sent.
	if got := len(srv.commitRequests()); got != 0 {
		t.Errorf("expected 0 Commit calls on failure, got %d", got)
	}
}

func TestExecute_Update_MultiRange_2PC_PrepareCarriesCorrectTableAndColumn(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	splitKey := sqllayer.EncodeKey(tableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, _ = gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  strLit("updated"),
		Where: &sqllayer.ComparisonExpr{
			Column: "id", Operator: "=", Value: intLit("5"),
		},
	})

	for i, req := range srv.prepareRequests() {
		if req.TableId != tableId {
			t.Errorf("prepare[%d]: TableId=%d, want %d", i, req.TableId, tableId)
		}
		if req.UpdateCol != 1 { // "name" is the first non-PK column → index 1
			t.Errorf("prepare[%d]: UpdateCol=%d, want 1", i, req.UpdateCol)
		}
		if req.Where == nil {
			t.Errorf("prepare[%d]: WHERE not forwarded", i)
		}
	}
}

func TestExecute_Delete_MultiRange_2PC_PrepareCarriesWhere(t *testing.T) {
	srv := &fakeRangeServer{}
	sc := sqllayer.NewSchemaCatalog(newTestBTreeGateway(t))
	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	splitKey := sqllayer.EncodeKey(sc.FindTableSchema("users").TableId, 50)

	gw, _ := newSplitGateway(t, splitKey, srv)
	_, _ = gw.Execute(&sqllayer.DeleteStatement{
		Table: "users",
		Where: &sqllayer.ComparisonExpr{Column: "id", Operator: ">", Value: intLit("10")},
	})

	for i, req := range srv.prepareRequests() {
		if req.Where == nil {
			t.Errorf("prepare[%d]: WHERE not forwarded in 2PC Delete", i)
		}
	}
}

func TestExecute_DropTable_2PC_DataDeleteVia2PC_SchemaDeleteViaExecute(t *testing.T) {
	srv := &fakeRangeServer{}
	gw, _ := newTestGateway(t, srv)
	_, err := gw.Execute(&sqllayer.DropTableStatement{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Data deletion uses 2PC Prepare with DELETE op.
	preps := srv.prepareRequests()
	if len(preps) == 0 {
		t.Fatal("expected at least one 2PC Prepare for data deletion")
	}
	if preps[0].Op != rs.RangeOp_DELETE {
		t.Errorf("Prepare Op=%v, want DELETE", preps[0].Op)
	}
	// Schema deletion uses a direct Execute with TableId=0.
	execs := srv.requests()
	if len(execs) == 0 {
		t.Fatal("expected at least one Execute for schema deletion")
	}
	if execs[0].TableId != 0 {
		t.Errorf("schema Execute TableId=%d, want 0", execs[0].TableId)
	}
}
