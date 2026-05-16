// Package integration — gRPC client/server integration tests.
// Boots a real gRPC server (same stack as cmd/server/main.go) on a random
// loopback port, connects a real gRPC client, and drives every test end-to-end
// across the TCP transport, proto serialisation, and the SQL engine.
package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---- gRPC server (mirrors cmd/server/main.go) ----

type sqlGRPCServer struct {
	pb.UnimplementedSQLServiceServer
	tm *lock.TransactionManager
	ex *sqllayer.Executor
}

func (s *sqlGRPCServer) Execute(_ context.Context, req *pb.SQLRequest) (*pb.SQLResponse, error) {
	tokens, err := sqllayer.Tokenize(req.Sql)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "tokenize: %v", err)
	}
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse: %v", err)
	}
	txn := s.tm.Begin()
	result, err := s.ex.Execute(stmt, txn.Id)
	if err != nil {
		s.tm.Rollback(txn.Id)
		return nil, status.Errorf(codes.Internal, "execute: %v", err)
	}
	s.tm.Commit(txn.Id)
	if result == nil {
		return &pb.SQLResponse{}, nil
	}
	return grpcResultSetToProto(result), nil
}

func grpcFieldToProto(f btree.Field, colType string) *pb.FieldValue {
	switch v := f.Value.(type) {
	case btree.IntValue:
		return &pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: v.V}}
	case btree.StringValue:
		if colType == "BOOL" {
			return &pb.FieldValue{Value: &pb.FieldValue_BoolValue{BoolValue: v.V == "TRUE"}}
		}
		return &pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: v.V}}
	default:
		return &pb.FieldValue{}
	}
}

func grpcResultSetToProto(rs *sqllayer.ResultSet) *pb.SQLResponse {
	resp := &pb.SQLResponse{Columns: rs.Columns}
	for _, row := range rs.Rows {
		pbRow := &pb.ResultRow{}
		for i, f := range row.Fields {
			pbRow.Fields = append(pbRow.Fields, grpcFieldToProto(f, rs.ColTypes[i]))
		}
		resp.Rows = append(resp.Rows, pbRow)
	}
	return resp
}

// ---- Test fixture ----

// grpcFixture is an in-process gRPC server + connected client backed by a testDB.
type grpcFixture struct {
	db     *testDB
	client pb.SQLServiceClient
}

// newGRPCFixture boots the server on a random loopback port and creates a client.
// All cleanup (server stop, connection close, db flush) is registered with t.Cleanup.
func newGRPCFixture(t *testing.T) *grpcFixture {
	t.Helper()
	db := newTestDB(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterSQLServiceServer(gs, &sqlGRPCServer{tm: db.tm, ex: db.ex})
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gs.Stop()
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		gs.GracefulStop()
		_ = conn.Close()
		db.close(t)
	})

	return &grpcFixture{
		db:     db,
		client: pb.NewSQLServiceClient(conn),
	}
}

// execute sends SQL and fails the test on any gRPC error.
func (f *grpcFixture) execute(t *testing.T, sql string) *pb.SQLResponse {
	t.Helper()
	resp, err := f.client.Execute(context.Background(), &pb.SQLRequest{Sql: sql})
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	return resp
}

// mustFail sends SQL and fails the test if NO gRPC error is returned.
func (f *grpcFixture) mustFail(t *testing.T, sql string) error {
	t.Helper()
	_, err := f.client.Execute(context.Background(), &pb.SQLRequest{Sql: sql})
	if err == nil {
		t.Fatalf("Execute(%q): expected gRPC error, got nil", sql)
	}
	return err
}

// ---- Response assertion helpers ----

func grpcIntVal(t *testing.T, fv *pb.FieldValue) int64 {
	t.Helper()
	v, ok := fv.Value.(*pb.FieldValue_IntValue)
	if !ok {
		t.Fatalf("expected FieldValue_IntValue, got %T", fv.Value)
	}
	return v.IntValue
}

func grpcStrVal(t *testing.T, fv *pb.FieldValue) string {
	t.Helper()
	v, ok := fv.Value.(*pb.FieldValue_StringValue)
	if !ok {
		t.Fatalf("expected FieldValue_StringValue, got %T", fv.Value)
	}
	return v.StringValue
}

func grpcBoolVal(t *testing.T, fv *pb.FieldValue) bool {
	t.Helper()
	v, ok := fv.Value.(*pb.FieldValue_BoolValue)
	if !ok {
		t.Fatalf("expected FieldValue_BoolValue, got %T", fv.Value)
	}
	return v.BoolValue
}

func assertGRPCRowCount(t *testing.T, resp *pb.SQLResponse, want int) {
	t.Helper()
	if got := len(resp.Rows); got != want {
		t.Errorf("row count: got %d, want %d", got, want)
	}
}

func assertGRPCColumns(t *testing.T, resp *pb.SQLResponse, want []string) {
	t.Helper()
	if len(resp.Columns) != len(want) {
		t.Fatalf("columns: got %v, want %v", resp.Columns, want)
	}
	for i, w := range want {
		if resp.Columns[i] != w {
			t.Errorf("column[%d]: got %q, want %q", i, resp.Columns[i], w)
		}
	}
}

func assertGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if got := st.Code(); got != want {
		t.Errorf("gRPC status code: got %v, want %v", got, want)
	}
}

// =========================================================================
// Basic CRUD through the gRPC transport
// =========================================================================

func TestGRPC_CreateAndInsert(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE users (id INT, name TEXT)")
	f.execute(t, "INSERT INTO users VALUES (1, 'alice')")
	f.execute(t, "INSERT INTO users VALUES (2, 'bob')")

	resp := f.execute(t, "SELECT * FROM users")
	assertGRPCRowCount(t, resp, 2)
	assertGRPCColumns(t, resp, []string{"id", "name"})
}

func TestGRPC_SelectWithWhereClause(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")
	f.execute(t, "INSERT INTO t VALUES (2, 20)")
	f.execute(t, "INSERT INTO t VALUES (3, 30)")

	resp := f.execute(t, "SELECT * FROM t WHERE v > 10")
	assertGRPCRowCount(t, resp, 2)
}

func TestGRPC_Update(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE users (id INT, age INT)")
	f.execute(t, "INSERT INTO users VALUES (1, 25)")
	f.execute(t, "UPDATE users SET age = 30 WHERE id = 1")

	resp := f.execute(t, "SELECT age FROM users WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 30 {
		t.Errorf("age: got %d, want 30", got)
	}
}

func TestGRPC_Delete(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")
	f.execute(t, "INSERT INTO t VALUES (2, 20)")
	f.execute(t, "DELETE FROM t WHERE id = 1")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 2 {
		t.Errorf("remaining row id: got %d, want 2", got)
	}
}

func TestGRPC_DropTable(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v TEXT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'hello')")
	f.execute(t, "DROP TABLE t")

	_ = f.mustFail(t, "SELECT * FROM t")
}

func TestGRPC_DropAndRecreateTable(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, val TEXT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'first')")
	f.execute(t, "DROP TABLE t")
	f.execute(t, "CREATE TABLE t (id INT, n INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 42)")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, 1)
	assertGRPCColumns(t, resp, []string{"id", "n"})
	if got := grpcIntVal(t, resp.Rows[0].Fields[1]); got != 42 {
		t.Errorf("n: got %d, want 42", got)
	}
}

func TestGRPC_UpdateAllRows(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, score INT)")
	for i := 1; i <= 10; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO t VALUES (%d, 0)", i))
	}
	f.execute(t, "UPDATE t SET score = 100")

	resp := f.execute(t, "SELECT * FROM t WHERE score = 100")
	assertGRPCRowCount(t, resp, 10)
}

func TestGRPC_DeleteAllRows(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 5; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}
	for i := 1; i <= 5; i++ {
		f.execute(t, fmt.Sprintf("DELETE FROM t WHERE id = %d", i))
	}

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, 0)
}

// =========================================================================
// Proto response format correctness
// =========================================================================

func TestGRPC_DDLResponseHasNoColumnsOrRows(t *testing.T) {
	f := newGRPCFixture(t)

	resp := f.execute(t, "CREATE TABLE t (id INT, val TEXT)")
	if len(resp.Columns) != 0 {
		t.Errorf("CREATE TABLE should return no columns, got %v", resp.Columns)
	}
	if len(resp.Rows) != 0 {
		t.Errorf("CREATE TABLE should return no rows, got %d", len(resp.Rows))
	}
}

func TestGRPC_DMLResponseHasNoColumnsOrRows(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")

	insertResp := f.execute(t, "INSERT INTO t VALUES (2, 20)")
	if len(insertResp.Columns) != 0 || len(insertResp.Rows) != 0 {
		t.Errorf("INSERT should return empty response, got columns=%v rows=%d", insertResp.Columns, len(insertResp.Rows))
	}

	updateResp := f.execute(t, "UPDATE t SET v = 99 WHERE id = 1")
	if len(updateResp.Columns) != 0 || len(updateResp.Rows) != 0 {
		t.Errorf("UPDATE should return empty response")
	}

	deleteResp := f.execute(t, "DELETE FROM t WHERE id = 2")
	if len(deleteResp.Columns) != 0 || len(deleteResp.Rows) != 0 {
		t.Errorf("DELETE should return empty response")
	}
}

func TestGRPC_IntFieldType(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, score INT)")
	f.execute(t, "INSERT INTO t VALUES (42, 9999)")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, 1)
	if _, ok := resp.Rows[0].Fields[0].Value.(*pb.FieldValue_IntValue); !ok {
		t.Errorf("id field: expected IntValue, got %T", resp.Rows[0].Fields[0].Value)
	}
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 42 {
		t.Errorf("id: got %d, want 42", got)
	}
	if got := grpcIntVal(t, resp.Rows[0].Fields[1]); got != 9999 {
		t.Errorf("score: got %d, want 9999", got)
	}
}

func TestGRPC_StringFieldType(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, label TEXT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'hello world')")

	resp := f.execute(t, "SELECT label FROM t WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if _, ok := resp.Rows[0].Fields[0].Value.(*pb.FieldValue_StringValue); !ok {
		t.Errorf("label field: expected StringValue, got %T", resp.Rows[0].Fields[0].Value)
	}
	if got := grpcStrVal(t, resp.Rows[0].Fields[0]); got != "hello world" {
		t.Errorf("label: got %q, want %q", got, "hello world")
	}
}

func TestGRPC_BoolFieldType(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE flags (id INT, active BOOL)")
	f.execute(t, "INSERT INTO flags VALUES (1, true)")
	f.execute(t, "INSERT INTO flags VALUES (2, false)")

	resp := f.execute(t, "SELECT * FROM flags WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if _, ok := resp.Rows[0].Fields[1].Value.(*pb.FieldValue_BoolValue); !ok {
		t.Errorf("active field: expected BoolValue, got %T", resp.Rows[0].Fields[1].Value)
	}
	if got := grpcBoolVal(t, resp.Rows[0].Fields[1]); !got {
		t.Error("active: expected true")
	}

	resp = f.execute(t, "SELECT * FROM flags WHERE id = 2")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcBoolVal(t, resp.Rows[0].Fields[1]); got {
		t.Error("active: expected false")
	}
}

func TestGRPC_MixedFieldTypesInSingleRow(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE items (id INT, label TEXT, count INT, active BOOL)")
	f.execute(t, "INSERT INTO items VALUES (7, 'widget', 42, true)")

	resp := f.execute(t, "SELECT * FROM items WHERE id = 7")
	assertGRPCRowCount(t, resp, 1)
	row := resp.Rows[0]

	if got := grpcIntVal(t, row.Fields[0]); got != 7 {
		t.Errorf("id: got %d, want 7", got)
	}
	if got := grpcStrVal(t, row.Fields[1]); got != "widget" {
		t.Errorf("label: got %q, want widget", got)
	}
	if got := grpcIntVal(t, row.Fields[2]); got != 42 {
		t.Errorf("count: got %d, want 42", got)
	}
	if got := grpcBoolVal(t, row.Fields[3]); !got {
		t.Error("active: expected true")
	}
}

func TestGRPC_ColumnNamesInResponse(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, first TEXT, last TEXT, age INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'John', 'Doe', 30)")

	resp := f.execute(t, "SELECT first, age FROM t")
	assertGRPCColumns(t, resp, []string{"first", "age"})
}

func TestGRPC_StarSelectReturnsAllColumns(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, x TEXT, y INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'foo', 99)")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCColumns(t, resp, []string{"id", "x", "y"})
}

func TestGRPC_EmptyTableSelectReturnsColumnsOnly(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, val TEXT)")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCColumns(t, resp, []string{"id", "val"})
	assertGRPCRowCount(t, resp, 0)
}

func TestGRPC_ColumnProjection(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, first TEXT, last TEXT, age INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'John', 'Doe', 30)")

	resp := f.execute(t, "SELECT first, age FROM t")
	assertGRPCColumns(t, resp, []string{"first", "age"})
	assertGRPCRowCount(t, resp, 1)
	if got := grpcStrVal(t, resp.Rows[0].Fields[0]); got != "John" {
		t.Errorf("first: got %q, want John", got)
	}
	if got := grpcIntVal(t, resp.Rows[0].Fields[1]); got != 30 {
		t.Errorf("age: got %d, want 30", got)
	}
}

// =========================================================================
// Error handling — correct gRPC status codes
// =========================================================================

func TestGRPC_InvalidSQLSyntaxReturnsInvalidArgument(t *testing.T) {
	f := newGRPCFixture(t)

	err := f.mustFail(t, "THIS IS NOT SQL AT ALL")
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestGRPC_SelectUnknownTableReturnsInternalError(t *testing.T) {
	f := newGRPCFixture(t)

	err := f.mustFail(t, "SELECT * FROM nonexistent")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_DuplicateKeyReturnsInternalError(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")

	err := f.mustFail(t, "INSERT INTO t VALUES (1, 20)")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_DuplicateKeyDoesNotCorruptData(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")
	_ = f.mustFail(t, "INSERT INTO t VALUES (1, 99)")

	resp := f.execute(t, "SELECT v FROM t WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 10 {
		t.Errorf("v after rejected duplicate: got %d, want 10", got)
	}
}

func TestGRPC_TypeMismatchReturnsInternalError(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, score INT)")

	err := f.mustFail(t, "INSERT INTO t VALUES (1, 'not_a_number')")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_SelectUnknownColumnReturnsError(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, val TEXT)")
	_ = f.mustFail(t, "SELECT ghost FROM t")
}

func TestGRPC_UpdateUnknownTableReturnsInternalError(t *testing.T) {
	f := newGRPCFixture(t)

	err := f.mustFail(t, "UPDATE nonexistent SET v = 1")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_UpdateUnknownColumnReturnsError(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")
	_ = f.mustFail(t, "UPDATE t SET ghost = 99")
}

func TestGRPC_DeleteUnknownTableReturnsInternalError(t *testing.T) {
	f := newGRPCFixture(t)

	err := f.mustFail(t, "DELETE FROM nonexistent")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_CreateDuplicateTableReturnsError(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT)")

	err := f.mustFail(t, "CREATE TABLE t (id INT)")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_DropNonExistentTableReturnsError(t *testing.T) {
	f := newGRPCFixture(t)

	err := f.mustFail(t, "DROP TABLE nonexistent")
	assertGRPCCode(t, err, codes.Internal)
}

func TestGRPC_InsertWrongValueCountReturnsError(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, name TEXT, age INT)")
	_ = f.mustFail(t, "INSERT INTO t VALUES (1, 'alice')")         // missing age
	_ = f.mustFail(t, "INSERT INTO t VALUES (1, 'alice', 30, 99)") // extra value
}

// =========================================================================
// Server resilience — errors must not crash or corrupt the server
// =========================================================================

func TestGRPC_ServerContinuesAfterInvalidSQL(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")

	_ = f.mustFail(t, "GARBAGE SQL !!!")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, 1)
}

func TestGRPC_ServerContinuesAfterMultipleErrors(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")

	_ = f.mustFail(t, "SELECT * FROM nonexistent")
	_ = f.mustFail(t, "INSERT INTO t VALUES (1, 99)") // duplicate
	_ = f.mustFail(t, "INVALID SYNTAX !!!")
	_ = f.mustFail(t, "DROP TABLE nonexistent")

	resp := f.execute(t, "SELECT v FROM t WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 10 {
		t.Errorf("v: got %d, want 10 (original row must be intact)", got)
	}
}

func TestGRPC_ErrorsBetweenSuccessfulOps(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v TEXT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'a')")

	_ = f.mustFail(t, "SELECT * FROM gone")

	f.execute(t, "INSERT INTO t VALUES (2, 'b')")

	_ = f.mustFail(t, "INSERT INTO t VALUES (1, 'duplicate')")

	f.execute(t, "INSERT INTO t VALUES (3, 'c')")

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, 3)
}

// =========================================================================
// WHERE clause coverage through gRPC
// =========================================================================

func TestGRPC_WhereIntEquality(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")
	f.execute(t, "INSERT INTO t VALUES (2, 20)")
	f.execute(t, "INSERT INTO t VALUES (3, 30)")

	resp := f.execute(t, "SELECT * FROM t WHERE v = 20")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 2 {
		t.Errorf("id: got %d, want 2", got)
	}
}

func TestGRPC_WhereIntRange(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 10; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	resp := f.execute(t, "SELECT * FROM t WHERE v > 50")
	assertGRPCRowCount(t, resp, 5) // 60,70,80,90,100
}

func TestGRPC_WhereStringEquality(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, name TEXT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'alice')")
	f.execute(t, "INSERT INTO t VALUES (2, 'bob')")
	f.execute(t, "INSERT INTO t VALUES (3, 'alice')")

	resp := f.execute(t, "SELECT * FROM t WHERE name = 'alice'")
	assertGRPCRowCount(t, resp, 2)
}

func TestGRPC_WhereAndClause(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, name TEXT, age INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 'alice', 30)")
	f.execute(t, "INSERT INTO t VALUES (2, 'alice', 25)")
	f.execute(t, "INSERT INTO t VALUES (3, 'bob', 30)")

	resp := f.execute(t, "SELECT * FROM t WHERE name = 'alice' AND age = 30")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 1 {
		t.Errorf("id: got %d, want 1", got)
	}
}

func TestGRPC_WhereOrClause(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 5; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	resp := f.execute(t, "SELECT * FROM t WHERE v = 10 OR v = 50")
	assertGRPCRowCount(t, resp, 2)
}

func TestGRPC_WhereNoMatchReturnsEmptyResult(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 10)")

	resp := f.execute(t, "SELECT * FROM t WHERE v = 999")
	assertGRPCRowCount(t, resp, 0)
}

func TestGRPC_BoolFilterViaWhere(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE features (id INT, enabled BOOL)")
	f.execute(t, "INSERT INTO features VALUES (1, true)")
	f.execute(t, "INSERT INTO features VALUES (2, false)")
	f.execute(t, "INSERT INTO features VALUES (3, true)")

	resp := f.execute(t, "SELECT * FROM features WHERE enabled = true")
	assertGRPCRowCount(t, resp, 2)
}

// =========================================================================
// Multi-table isolation through gRPC
// =========================================================================

func TestGRPC_MultipleTablesDoNotInterfere(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE users (id INT, name TEXT)")
	f.execute(t, "CREATE TABLE products (id INT, title TEXT)")
	f.execute(t, "INSERT INTO users VALUES (1, 'alice')")
	f.execute(t, "INSERT INTO products VALUES (1, 'widget')")

	u := f.execute(t, "SELECT name FROM users WHERE id = 1")
	if got := grpcStrVal(t, u.Rows[0].Fields[0]); got != "alice" {
		t.Errorf("users name: got %q, want alice", got)
	}
	p := f.execute(t, "SELECT title FROM products WHERE id = 1")
	if got := grpcStrVal(t, p.Rows[0].Fields[0]); got != "widget" {
		t.Errorf("products title: got %q, want widget", got)
	}
}

func TestGRPC_UpdateOneTableDoesNotAffectOther(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE a (id INT, v INT)")
	f.execute(t, "CREATE TABLE b (id INT, v INT)")
	f.execute(t, "INSERT INTO a VALUES (1, 10)")
	f.execute(t, "INSERT INTO b VALUES (1, 20)")
	f.execute(t, "UPDATE a SET v = 99 WHERE id = 1")

	resp := f.execute(t, "SELECT v FROM b WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 20 {
		t.Errorf("b.v: got %d, want 20 (must be unaffected)", got)
	}
}

func TestGRPC_DeleteFromOneTableDoesNotAffectOther(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE a (id INT, v INT)")
	f.execute(t, "CREATE TABLE b (id INT, v INT)")
	f.execute(t, "INSERT INTO a VALUES (1, 10)")
	f.execute(t, "INSERT INTO b VALUES (1, 20)")
	f.execute(t, "DELETE FROM a WHERE id = 1")

	resp := f.execute(t, "SELECT * FROM b")
	assertGRPCRowCount(t, resp, 1)
}

// =========================================================================
// Concurrent requests
// =========================================================================

func TestGRPC_ConcurrentSelectsReturnConsistentData(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 10; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := f.client.Execute(context.Background(), &pb.SQLRequest{Sql: "SELECT * FROM t"})
			if err != nil {
				errs[idx] = err
				return
			}
			if len(resp.Rows) != 10 {
				errs[idx] = fmt.Errorf("goroutine %d: expected 10 rows, got %d", idx, len(resp.Rows))
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

func TestGRPC_ConcurrentInsertsDifferentKeys(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v INT)")

	const goroutines = 5
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sql := fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", idx+1, idx*10)
			_, err := f.client.Execute(context.Background(), &pb.SQLRequest{Sql: sql})
			if err != nil {
				errs[idx] = fmt.Errorf("insert id=%d: %v", idx+1, err)
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	resp := f.execute(t, "SELECT * FROM t")
	assertGRPCRowCount(t, resp, goroutines)
}

func TestGRPC_ManySequentialRequestsOnSingleConnection(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, v TEXT)")
	for i := 1; i <= 30; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO t VALUES (%d, 'row%d')", i, i))
	}
	for i := 1; i <= 30; i++ {
		resp := f.execute(t, fmt.Sprintf("SELECT v FROM t WHERE id = %d", i))
		assertGRPCRowCount(t, resp, 1)
		want := fmt.Sprintf("row%d", i)
		if got := grpcStrVal(t, resp.Rows[0].Fields[0]); got != want {
			t.Errorf("id=%d v: got %q, want %q", i, got, want)
		}
	}
}

// =========================================================================
// Context cancellation
// =========================================================================

func TestGRPC_CancelledContextReturnsError(t *testing.T) {
	f := newGRPCFixture(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call

	_, err := f.client.Execute(ctx, &pb.SQLRequest{Sql: "CREATE TABLE t (id INT)"})
	if err == nil {
		t.Fatal("expected error with cancelled context, got nil")
	}
}

// =========================================================================
// Large result sets through gRPC
// =========================================================================

func TestGRPC_LargeResultSet(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE big (id INT, val INT)")
	const n = 100
	for i := 1; i <= n; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO big VALUES (%d, %d)", i, i*i))
	}

	resp := f.execute(t, "SELECT * FROM big")
	assertGRPCRowCount(t, resp, n)
}

func TestGRPC_LargeResultSetSpotCheck(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE nums (id INT, square INT)")
	const n = 80
	for i := 1; i <= n; i++ {
		f.execute(t, fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i, i*i))
	}

	for _, id := range []int{1, 25, 50, 80} {
		resp := f.execute(t, fmt.Sprintf("SELECT square FROM nums WHERE id = %d", id))
		assertGRPCRowCount(t, resp, 1)
		want := int64(id * id)
		if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != want {
			t.Errorf("id=%d: square got %d, want %d", id, got, want)
		}
	}
}

// =========================================================================
// Boundary and edge values
// =========================================================================

func TestGRPC_LargeIntValue(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, big INT)")
	f.execute(t, "INSERT INTO t VALUES (1, 9223372036854775807)") // MaxInt64

	resp := f.execute(t, "SELECT big FROM t WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	const maxInt64 = int64(9223372036854775807)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != maxInt64 {
		t.Errorf("big: got %d, want MaxInt64", got)
	}
}

func TestGRPC_ZeroIntValue(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, val INT)")
	f.execute(t, "INSERT INTO t VALUES (0, 0)")

	resp := f.execute(t, "SELECT val FROM t WHERE id = 0")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 0 {
		t.Errorf("val: got %d, want 0", got)
	}
}

func TestGRPC_EmptyStringValue(t *testing.T) {
	f := newGRPCFixture(t)

	f.execute(t, "CREATE TABLE t (id INT, s TEXT)")
	f.execute(t, "INSERT INTO t VALUES (1, '')")

	resp := f.execute(t, "SELECT s FROM t WHERE id = 1")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcStrVal(t, resp.Rows[0].Fields[0]); got != "" {
		t.Errorf("s: got %q, want empty string", got)
	}
}

// =========================================================================
// Full end-to-end workflow tests
// =========================================================================

func TestGRPC_FullCRUDWorkflow(t *testing.T) {
	f := newGRPCFixture(t)

	// Create
	f.execute(t, "CREATE TABLE accounts (id INT, owner TEXT, balance INT)")

	// Insert
	f.execute(t, "INSERT INTO accounts VALUES (1, 'alice', 1000)")
	f.execute(t, "INSERT INTO accounts VALUES (2, 'bob', 500)")
	f.execute(t, "INSERT INTO accounts VALUES (3, 'carol', 750)")

	// Update
	f.execute(t, "UPDATE accounts SET balance = 1200 WHERE id = 1")

	// Delete
	f.execute(t, "DELETE FROM accounts WHERE id = 3")

	// Verify count
	resp := f.execute(t, "SELECT * FROM accounts")
	assertGRPCRowCount(t, resp, 2) // alice and bob

	// Verify updated balance
	resp = f.execute(t, "SELECT balance FROM accounts WHERE owner = 'alice'")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 1200 {
		t.Errorf("alice balance: got %d, want 1200", got)
	}

	// Verify deleted row is gone
	resp = f.execute(t, "SELECT * FROM accounts WHERE owner = 'carol'")
	assertGRPCRowCount(t, resp, 0)

	// Verify bob is unchanged
	resp = f.execute(t, "SELECT balance FROM accounts WHERE owner = 'bob'")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != 500 {
		t.Errorf("bob balance: got %d, want 500", got)
	}
}

func TestGRPC_SchemaEvolutionWorkflow(t *testing.T) {
	f := newGRPCFixture(t)

	// Create v1 schema
	f.execute(t, "CREATE TABLE config (id INT, key TEXT)")
	f.execute(t, "INSERT INTO config VALUES (1, 'debug')")

	// Drop and recreate with new schema
	f.execute(t, "DROP TABLE config")
	f.execute(t, "CREATE TABLE config (id INT, key TEXT, value TEXT)")
	f.execute(t, "INSERT INTO config VALUES (1, 'debug', 'false')")
	f.execute(t, "INSERT INTO config VALUES (2, 'timeout', '30')")

	// Verify new schema
	resp := f.execute(t, "SELECT * FROM config")
	assertGRPCRowCount(t, resp, 2)
	assertGRPCColumns(t, resp, []string{"id", "key", "value"})

	resp = f.execute(t, "SELECT value FROM config WHERE key = 'timeout'")
	assertGRPCRowCount(t, resp, 1)
	if got := grpcStrVal(t, resp.Rows[0].Fields[0]); got != "30" {
		t.Errorf("value: got %q, want '30'", got)
	}
}

func TestGRPC_ManyTablesAllAccessibleViaGRPC(t *testing.T) {
	f := newGRPCFixture(t)

	const tableCount = 8
	for i := 0; i < tableCount; i++ {
		f.execute(t, fmt.Sprintf("CREATE TABLE grpc_t%d (id INT, v INT)", i))
		f.execute(t, fmt.Sprintf("INSERT INTO grpc_t%d VALUES (1, %d)", i, i*100))
	}

	for i := 0; i < tableCount; i++ {
		resp := f.execute(t, fmt.Sprintf("SELECT v FROM grpc_t%d WHERE id = 1", i))
		assertGRPCRowCount(t, resp, 1)
		if got := grpcIntVal(t, resp.Rows[0].Fields[0]); got != int64(i*100) {
			t.Errorf("grpc_t%d.v: got %d, want %d", i, got, i*100)
		}
	}
}
