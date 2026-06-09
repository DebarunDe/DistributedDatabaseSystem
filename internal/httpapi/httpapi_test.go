package httpapi_test

// Full-stack HTTP API tests: each test gets a fresh HTTPServer backed by a
// real BTree + Gateway wired via in-process gRPC (bufconn).
//
// Sections:
//   Infrastructure  — newTestStack and request helpers
//   Health          — /health endpoint, auth bypass
//   Auth            — Bearer token enforcement
//   CORS            — preflight and header propagation
//   Create Table    — POST /tables validation and success paths
//   List Tables     — GET /tables, empty and populated
//   Describe Table  — GET /tables/{name}, found and not-found
//   Drop Table      — DELETE /tables/{name}
//   Insert          — POST /tables/{name}/rows
//   Select          — GET /tables/{name}/rows, where/columns/consistency params
//   Update          — PUT /tables/{name}/rows, with and without WHERE
//   Delete          — DELETE /tables/{name}/rows, with and without WHERE
//   Alter           — PUT /tables/{name}/consistency, round-trip verify

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	ap "github.com/your-username/DistributedDatabaseSystem/internal/AP"
	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	"github.com/your-username/DistributedDatabaseSystem/internal/httpapi"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
	"github.com/your-username/DistributedDatabaseSystem/internal/partition"
	"github.com/your-username/DistributedDatabaseSystem/internal/raft"
	rs "github.com/your-username/DistributedDatabaseSystem/proto/rangeservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const testAPIKey = "test-api-key-123"

// =============================================================================
// Infrastructure
// =============================================================================

type testStack struct {
	ts     *httptest.Server
	client *http.Client
}

func newTestStack(t *testing.T) *testStack {
	t.Helper()

	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })

	bt := btree.NewBTree(pm)
	sc := sqllayer.NewSchemaCatalog(bt)
	records := partition.NewTxnRecordStore()
	timestamps := ap.NewTimestampStore()

	logPath := filepath.Join(t.TempDir(), "ap.log")
	apLog, err := ap.NewAPWriteLog(logPath)
	if err != nil {
		t.Fatalf("NewAPWriteLog: %v", err)
	}
	t.Cleanup(func() { _ = apLog.Close(); _ = os.Remove(logPath) })

	var applyFn lock.ApplyFn = func(op raft.ReplOp, key uint64, fields []btree.Field) error {
		switch op {
		case raft.ReplTxnRecord:
			records.Store(partition.DecodeTxnRecord(key, fields))
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
	rangeServer := partition.NewRangeServer(bt, tm, sc, records, timestamps, apLog)

	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	rs.RegisterRangeServiceServer(grpcSrv, rangeServer)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	coord := partition.NewCoordinator(map[uint64]string{1: "node1"})
	coord.UpdateLeader(1, 1)
	router, err := partition.NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	gw := partition.NewGateway(router, sc)
	gw.AddConn(1, conn)

	srv := httpapi.NewHTTPServer(gw, sc, map[string]bool{testAPIKey: true})
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	return &testStack{ts: ts, client: ts.Client()}
}

// req sends an authenticated request. Pass body=nil for no body.
func (s *testStack) req(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()
	return s.reqWithKey(t, method, path, body, testAPIKey)
}

func (s *testStack) reqWithKey(t *testing.T, method, path string, body any, key string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, s.ts.URL+path, r)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (s *testStack) getQ(t *testing.T, path string, params url.Values) *http.Response {
	t.Helper()
	u := s.ts.URL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	return resp
}

// mustStatus asserts the status code; on mismatch it reads and logs the body.
// On success the body is left open for the caller to read.
func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("want HTTP %d, got %d: %s", want, resp.StatusCode, b)
	}
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return v
}

func drainClose(resp *http.Response) {
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
}

// --- common operation helpers ---

func (s *testStack) createTable(t *testing.T, name string, cols ...httpapi.ColumnDefJSON) {
	t.Helper()
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{Name: name, Columns: cols})
	mustStatus(t, resp, http.StatusCreated)
	drainClose(resp)
}

func (s *testStack) insertRow(t *testing.T, table string, values map[string]any) {
	t.Helper()
	resp := s.req(t, http.MethodPost, "/tables/"+table+"/rows", httpapi.InsertRequest{Values: values})
	mustStatus(t, resp, http.StatusCreated)
	drainClose(resp)
}

func (s *testStack) selectRows(t *testing.T, table string, params url.Values) httpapi.RowsResponse {
	t.Helper()
	resp := s.getQ(t, "/tables/"+table+"/rows", params)
	mustStatus(t, resp, http.StatusOK)
	return decode[httpapi.RowsResponse](t, resp)
}

// =============================================================================
// Health
// =============================================================================

func TestHealth_Returns200(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodGet, "/health", nil)
	mustStatus(t, resp, http.StatusOK)
	body := decode[map[string]string](t, resp)
	if body["status"] != "ok" {
		t.Fatalf("want status=ok, got %q", body["status"])
	}
}

func TestHealth_NoAuthRequired(t *testing.T) {
	s := newTestStack(t)
	resp := s.reqWithKey(t, http.MethodGet, "/health", nil, "")
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)
}

// =============================================================================
// Auth middleware
// =============================================================================

func TestAuth_MissingKey_Returns401(t *testing.T) {
	s := newTestStack(t)
	resp := s.reqWithKey(t, http.MethodGet, "/tables", nil, "")
	mustStatus(t, resp, http.StatusUnauthorized)
	drainClose(resp)
}

func TestAuth_WrongKey_Returns401(t *testing.T) {
	s := newTestStack(t)
	resp := s.reqWithKey(t, http.MethodGet, "/tables", nil, "not-a-valid-key")
	mustStatus(t, resp, http.StatusUnauthorized)
	drainClose(resp)
}

func TestAuth_BearerPrefixMissing_Returns401(t *testing.T) {
	s := newTestStack(t)
	req, _ := http.NewRequest(http.MethodGet, s.ts.URL+"/tables", nil)
	req.Header.Set("Authorization", testAPIKey) // raw key, no "Bearer " prefix
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusUnauthorized)
	drainClose(resp)
}

func TestAuth_ValidKey_Succeeds(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)
}

func TestAuth_ErrorResponse_IsJSON(t *testing.T) {
	s := newTestStack(t)
	resp := s.reqWithKey(t, http.MethodGet, "/tables", nil, "bad")
	mustStatus(t, resp, http.StatusUnauthorized)
	body := decode[map[string]string](t, resp)
	if body["error"] == "" {
		t.Fatal("expected non-empty error field in 401 response")
	}
}

// =============================================================================
// CORS middleware
// =============================================================================

func TestCORS_Options_Returns204(t *testing.T) {
	s := newTestStack(t)
	req, _ := http.NewRequest(http.MethodOptions, s.ts.URL+"/tables", nil)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusNoContent)
	drainClose(resp)
}

func TestCORS_Options_HasAllowOriginHeader(t *testing.T) {
	s := newTestStack(t)
	req, _ := http.NewRequest(http.MethodOptions, s.ts.URL+"/tables", nil)
	resp, _ := s.client.Do(req)
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("want Access-Control-Allow-Origin=*, got %q", got)
	}
	drainClose(resp)
}

func TestCORS_Options_HasAllowMethodsHeader(t *testing.T) {
	s := newTestStack(t)
	req, _ := http.NewRequest(http.MethodOptions, s.ts.URL+"/tables/foo/rows", nil)
	resp, _ := s.client.Do(req)
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatal("expected Access-Control-Allow-Methods header")
	}
	drainClose(resp)
}

func TestCORS_RegularRequest_HasCORSHeaders(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodGet, "/tables", nil)
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("want CORS header on regular request, got %q", got)
	}
	drainClose(resp)
}

// =============================================================================
// Create Table
// =============================================================================

func TestCreateTable_Success_Returns201(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{
		Name:    "users",
		Columns: []httpapi.ColumnDefJSON{{Name: "id", DataType: "INT"}, {Name: "name", DataType: "TEXT"}},
	})
	mustStatus(t, resp, http.StatusCreated)
	body := decode[httpapi.SuccessResponse](t, resp)
	if !body.OK {
		t.Fatal("expected ok=true")
	}
}

func TestCreateTable_PKOnly_Success(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{
		Name:    "minimal",
		Columns: []httpapi.ColumnDefJSON{{Name: "id", DataType: "INT"}},
	})
	mustStatus(t, resp, http.StatusCreated)
	drainClose(resp)
}

func TestCreateTable_MissingName_Returns400(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{
		Columns: []httpapi.ColumnDefJSON{{Name: "id", DataType: "INT"}},
	})
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

func TestCreateTable_NoColumns_Returns400(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{Name: "empty"})
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

func TestCreateTable_EmptyPKName_Returns400(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{
		Name:    "bad",
		Columns: []httpapi.ColumnDefJSON{{Name: "", DataType: "INT"}},
	})
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

func TestCreateTable_EmptyPKType_Returns400(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{
		Name:    "bad",
		Columns: []httpapi.ColumnDefJSON{{Name: "id", DataType: ""}},
	})
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

func TestCreateTable_InvalidBody_Returns400(t *testing.T) {
	s := newTestStack(t)
	req, _ := http.NewRequest(http.MethodPost, s.ts.URL+"/tables", bytes.NewBufferString("not-json"))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

func TestCreateTable_Duplicate_ReturnsError(t *testing.T) {
	s := newTestStack(t)
	cols := []httpapi.ColumnDefJSON{{Name: "id", DataType: "INT"}}
	s.createTable(t, "dup", cols...)
	resp := s.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{Name: "dup", Columns: cols})
	if resp.StatusCode == http.StatusCreated {
		drainClose(resp)
		t.Fatal("expected non-201 on duplicate table creation")
	}
	drainClose(resp)
}

// =============================================================================
// List Tables
// =============================================================================

func TestListTables_Empty_ReturnsEmptyArray(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 0 {
		t.Fatalf("want empty list, got %d entries", len(tables))
	}
}

func TestListTables_ShowsCreatedTables(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "alpha", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.createTable(t, "beta", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 2 {
		t.Fatalf("want 2 tables, got %d", len(tables))
	}
}

func TestListTables_DefaultConsistency_IsStrong(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "t1", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 1 {
		t.Fatalf("want 1 table")
	}
	if tables[0]["consistency"] != "strong" {
		t.Fatalf("want consistency=strong, got %v", tables[0]["consistency"])
	}
}

func TestListTables_HasNameField(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "named", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if tables[0]["name"] != "named" {
		t.Fatalf("want name=named, got %v", tables[0]["name"])
	}
}

// =============================================================================
// Describe Table
// =============================================================================

func TestDescribeTable_Found_ReturnsSchema(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "users",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "email", DataType: "TEXT"},
	)

	resp := s.req(t, http.MethodGet, "/tables/users", nil)
	mustStatus(t, resp, http.StatusOK)
	body := decode[httpapi.TableInfoResponse](t, resp)

	if body.Name != "users" {
		t.Fatalf("want name=users, got %q", body.Name)
	}
	if body.PrimaryKey.Name != "id" || body.PrimaryKey.DataType != "INT" {
		t.Fatalf("unexpected PK: %+v", body.PrimaryKey)
	}
	if len(body.Columns) != 1 || body.Columns[0].Name != "email" {
		t.Fatalf("unexpected columns: %+v", body.Columns)
	}
	if body.Consistency != "strong" {
		t.Fatalf("want consistency=strong, got %q", body.Consistency)
	}
}

func TestDescribeTable_PKOnly_HasNoColumns(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "pkonly", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodGet, "/tables/pkonly", nil)
	mustStatus(t, resp, http.StatusOK)
	body := decode[httpapi.TableInfoResponse](t, resp)
	if len(body.Columns) != 0 {
		t.Fatalf("want 0 non-PK columns, got %d", len(body.Columns))
	}
}

func TestDescribeTable_NotFound_Returns404(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodGet, "/tables/nonexistent", nil)
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

// =============================================================================
// Drop Table
// =============================================================================

func TestDropTable_Success_ReturnsOK(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "to_drop", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodDelete, "/tables/to_drop", nil)
	mustStatus(t, resp, http.StatusOK)
	body := decode[httpapi.SuccessResponse](t, resp)
	if !body.OK {
		t.Fatal("expected ok=true")
	}
}

func TestDropTable_RemovedFromList(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "going_away", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	resp := s.req(t, http.MethodDelete, "/tables/going_away", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 0 {
		t.Fatalf("want 0 tables after drop, got %d", len(tables))
	}
}

func TestDropTable_NotFound_After404OnDescribe(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "temp", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	resp := s.req(t, http.MethodDelete, "/tables/temp", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodGet, "/tables/temp", nil)
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

// =============================================================================
// Insert
// =============================================================================

func TestInsert_Success_Returns201(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "items",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "label", DataType: "TEXT"},
	)

	resp := s.req(t, http.MethodPost, "/tables/items/rows", httpapi.InsertRequest{
		Values: map[string]any{"id": float64(1), "label": "hello"},
	})
	mustStatus(t, resp, http.StatusCreated)
	body := decode[httpapi.SuccessResponse](t, resp)
	if !body.OK {
		t.Fatal("expected ok=true")
	}
}

func TestInsert_TableNotFound_Returns404(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPost, "/tables/ghost/rows", httpapi.InsertRequest{
		Values: map[string]any{"id": float64(1)},
	})
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

func TestInsert_InvalidBody_Returns400(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "t", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	req, _ := http.NewRequest(http.MethodPost, s.ts.URL+"/tables/t/rows", bytes.NewBufferString("bad-json"))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

func TestInsert_MultipleRows_AllSucceed(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "batch", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 5; i++ {
		s.insertRow(t, "batch", map[string]any{"id": float64(i)})
	}
}

// =============================================================================
// Select
// =============================================================================

func TestSelect_EmptyTable_ReturnsZeroRows(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "empty", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	body := s.selectRows(t, "empty", nil)
	if body.Count != 0 {
		t.Fatalf("want 0 rows, got %d", body.Count)
	}
	if len(body.Rows) != 0 {
		t.Fatalf("want empty rows slice, got %d", len(body.Rows))
	}
}

func TestSelect_InsertedRowIsReturned(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "things",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "name", DataType: "TEXT"},
	)
	s.insertRow(t, "things", map[string]any{"id": float64(42), "name": "foo"})

	body := s.selectRows(t, "things", nil)
	if body.Count != 1 {
		t.Fatalf("want 1 row, got %d", body.Count)
	}
}

func TestSelect_MultipleRows_CountMatchesInserts(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "nums", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 5; i++ {
		s.insertRow(t, "nums", map[string]any{"id": float64(i)})
	}

	body := s.selectRows(t, "nums", nil)
	if body.Count != 5 {
		t.Fatalf("want 5 rows, got %d", body.Count)
	}
}

func TestSelect_ColumnsMatchSchema(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "schema_check",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "val", DataType: "TEXT"},
	)
	s.insertRow(t, "schema_check", map[string]any{"id": float64(1), "val": "x"})

	body := s.selectRows(t, "schema_check", nil)
	if len(body.Columns) != 2 {
		t.Fatalf("want 2 columns, got %d: %v", len(body.Columns), body.Columns)
	}
}

func TestSelect_TableNotFound_Returns404(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodGet, "/tables/no_such/rows", nil)
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

func TestSelect_Where_EqualityFilter(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "filter_test",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
	)
	for i := 1; i <= 5; i++ {
		s.insertRow(t, "filter_test", map[string]any{"id": float64(i)})
	}

	body := s.selectRows(t, "filter_test", url.Values{"where": {"id = 3"}})
	if body.Count != 1 {
		t.Fatalf("want 1 row with id=3, got %d", body.Count)
	}
}

func TestSelect_Where_RangeFilter(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "range_test",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
	)
	for i := 1; i <= 10; i++ {
		s.insertRow(t, "range_test", map[string]any{"id": float64(i)})
	}

	body := s.selectRows(t, "range_test", url.Values{"where": {"id > 7"}})
	if body.Count != 3 {
		t.Fatalf("want 3 rows with id>7, got %d", body.Count)
	}
}

func TestSelect_Where_NoMatch_ReturnsEmpty(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "nomatch",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
	)
	s.insertRow(t, "nomatch", map[string]any{"id": float64(1)})

	body := s.selectRows(t, "nomatch", url.Values{"where": {"id = 999"}})
	if body.Count != 0 {
		t.Fatalf("want 0 rows, got %d", body.Count)
	}
}

func TestSelect_ConsistencyOverride_Eventual_Succeeds(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "ev_table", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.insertRow(t, "ev_table", map[string]any{"id": float64(1)})

	body := s.selectRows(t, "ev_table", url.Values{"consistency": {"eventual"}})
	if body.Count != 1 {
		t.Fatalf("want 1 row, got %d", body.Count)
	}
}

func TestSelect_ConsistencyOverride_Strong_Succeeds(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "strong_table", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.insertRow(t, "strong_table", map[string]any{"id": float64(1)})

	body := s.selectRows(t, "strong_table", url.Values{"consistency": {"strong"}})
	if body.Count != 1 {
		t.Fatalf("want 1 row, got %d", body.Count)
	}
}

func TestSelect_RowDataValues_IntAndString(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "typed",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "label", DataType: "TEXT"},
	)
	s.insertRow(t, "typed", map[string]any{"id": float64(7), "label": "seven"})

	body := s.selectRows(t, "typed", url.Values{"where": {"id = 7"}})
	if body.Count != 1 {
		t.Fatalf("want 1 row, got %d", body.Count)
	}
	row := body.Rows[0]
	if row["label"] != "seven" {
		t.Fatalf("want label=seven, got %v", row["label"])
	}
}

// =============================================================================
// Update
// =============================================================================

func TestUpdate_Success_ReturnsOK(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "editable",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "val", DataType: "TEXT"},
	)
	s.insertRow(t, "editable", map[string]any{"id": float64(1), "val": "original"})

	resp := s.req(t, http.MethodPut, "/tables/editable/rows", httpapi.UpdateRequest{
		Set: map[string]any{"val": "changed"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := decode[httpapi.SuccessResponse](t, resp)
	if !body.OK {
		t.Fatal("expected ok=true")
	}
}

func TestUpdate_WithWhere_OnlyUpdatesMatching(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "targeted",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "v", DataType: "INT"},
	)
	s.insertRow(t, "targeted", map[string]any{"id": float64(1), "v": float64(100)})
	s.insertRow(t, "targeted", map[string]any{"id": float64(2), "v": float64(200)})

	resp := s.req(t, http.MethodPut, "/tables/targeted/rows", httpapi.UpdateRequest{
		Set:   map[string]any{"v": float64(999)},
		Where: "id = 1",
	})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	// Row id=1 should have v=999; row id=2 should still have v=200.
	row1 := s.selectRows(t, "targeted", url.Values{"where": {"id = 1"}})
	if row1.Count != 1 || row1.Rows[0]["v"] != float64(999) {
		t.Fatalf("want v=999 for id=1, got rows=%+v", row1.Rows)
	}
	row2 := s.selectRows(t, "targeted", url.Values{"where": {"id = 2"}})
	if row2.Count != 1 || row2.Rows[0]["v"] != float64(200) {
		t.Fatalf("want v=200 for id=2 unchanged, got rows=%+v", row2.Rows)
	}
}

func TestUpdate_TableNotFound_Returns404(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPut, "/tables/no_table/rows", httpapi.UpdateRequest{
		Set: map[string]any{"x": "y"},
	})
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

func TestUpdate_InvalidBody_Returns400(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "t", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	req, _ := http.NewRequest(http.MethodPut, s.ts.URL+"/tables/t/rows", bytes.NewBufferString("bad-json"))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

// =============================================================================
// Delete
// =============================================================================

func TestDelete_Success_ReturnsOK(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "targets", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.insertRow(t, "targets", map[string]any{"id": float64(1)})

	resp := s.req(t, http.MethodDelete, "/tables/targets/rows", httpapi.DeleteRequest{})
	mustStatus(t, resp, http.StatusOK)
	body := decode[httpapi.SuccessResponse](t, resp)
	if !body.OK {
		t.Fatal("expected ok=true")
	}
}

func TestDelete_NoWhere_DeletesAll(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "wipe", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 3; i++ {
		s.insertRow(t, "wipe", map[string]any{"id": float64(i)})
	}

	resp := s.req(t, http.MethodDelete, "/tables/wipe/rows", httpapi.DeleteRequest{})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	body := s.selectRows(t, "wipe", nil)
	if body.Count != 0 {
		t.Fatalf("want 0 rows after delete-all, got %d", body.Count)
	}
}

func TestDelete_WithWhere_OnlyDeletesMatching(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "pool", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 4; i++ {
		s.insertRow(t, "pool", map[string]any{"id": float64(i)})
	}

	resp := s.req(t, http.MethodDelete, "/tables/pool/rows", httpapi.DeleteRequest{Where: "id = 2"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	body := s.selectRows(t, "pool", nil)
	if body.Count != 3 {
		t.Fatalf("want 3 rows after targeted delete, got %d", body.Count)
	}
}

func TestDelete_TableNotFound_Returns404(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodDelete, "/tables/ghost/rows", httpapi.DeleteRequest{})
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

func TestDelete_InvalidBody_Returns400(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "t", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	req, _ := http.NewRequest(http.MethodDelete, s.ts.URL+"/tables/t/rows", bytes.NewBufferString("bad-json"))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}

// =============================================================================
// Alter Consistency
// =============================================================================

func TestAlterConsistency_ToEventual_ReturnsOK(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "flexi", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodPut, "/tables/flexi/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)
}

func TestAlterConsistency_ToStrong_ReturnsOK(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "bouncing", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodPut, "/tables/bouncing/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodPut, "/tables/bouncing/consistency", httpapi.AlterConsistencyRequest{Mode: "strong"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)
}

func TestAlterConsistency_ReflectedInDescribe(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "mutable", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodPut, "/tables/mutable/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodGet, "/tables/mutable", nil)
	mustStatus(t, resp, http.StatusOK)
	info := decode[httpapi.TableInfoResponse](t, resp)
	if info.Consistency != "eventual" {
		t.Fatalf("want consistency=eventual after alter, got %q", info.Consistency)
	}
}

func TestAlterConsistency_ReflectedInList(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "ap_table", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodPut, "/tables/ap_table/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 1 || tables[0]["consistency"] != "eventual" {
		t.Fatalf("want eventual in list, got %+v", tables)
	}
}

func TestAlterConsistency_UnknownTable_ReturnsError(t *testing.T) {
	s := newTestStack(t)
	resp := s.req(t, http.MethodPut, "/tables/missing/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	if resp.StatusCode == http.StatusOK {
		drainClose(resp)
		t.Fatal("expected error for unknown table")
	}
	drainClose(resp)
}

func TestAlterConsistency_InvalidBody_Returns400(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "t", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	req, _ := http.NewRequest(http.MethodPut, s.ts.URL+"/tables/t/consistency", bytes.NewBufferString("bad-json"))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	mustStatus(t, resp, http.StatusBadRequest)
	drainClose(resp)
}
