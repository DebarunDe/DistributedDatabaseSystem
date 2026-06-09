package httpapi_test

// Multi-node simulation tests for the HTTP API.
//
// Sections:
//   Infrastructure   — multi-node cluster builder (2- and 3-node)
//   Routing          — requests land on correct node, scatter-gather SELECT
//   EdgeCases        — split boundary, PK=0, empty range, multi-table
//   Stress           — concurrent inserts, mixed reads/writes, high volume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

// =============================================================================
// Infrastructure
// =============================================================================

// makeBTree creates a fresh BTree backed by a temporary page-manager DB.
func makeBTree(t *testing.T) (*btree.BTree, pagemanager.PageManager) {
	t.Helper()
	pm, err := pagemanager.NewDB(t.TempDir() + "/node.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	return btree.NewBTree(pm), pm
}

// buildRangeServer wires a BTree + shared SchemaCatalog into a fully-functional
// RangeServer. sc must already be backed by its own BTree (typically bt1 for the
// first node). Each node that receives schema-range writes calls sc.LoadSchemas()
// in its applyFn to keep the in-memory cache fresh.
func buildRangeServer(t *testing.T, bt *btree.BTree, sc *sqllayer.SchemaCatalog) *partition.RangeServer {
	t.Helper()
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
		// Schema entries have tableId=0, i.e. key>>32 == 0. When any node applies
		// a schema write it reloads the shared catalog so all goroutines see it.
		if key>>32 == 0 {
			return sc.LoadSchemas()
		}
		return nil
	}

	tm := lock.NewTransactionManager(bt, nil, applyFn)
	return partition.NewRangeServer(bt, tm, sc, records, timestamps, apLog)
}

// grpcConnect starts an in-process gRPC server for srv and returns a client conn.
func grpcConnect(t *testing.T, srv *partition.RangeServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	rs.RegisterRangeServiceServer(grpcSrv, srv)
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
	return conn
}

// clusterStack is a running multi-node HTTP server with direct BTree access for
// verification.
type clusterStack struct {
	ts  *httptest.Server
	bts []*btree.BTree // per-node BTree, bts[0] backs the SchemaCatalog
}

// newTwoNodeCluster creates a 2-node HTTP-fronted cluster split at splitKey.
//
//	Node 1 owns BTree keys [0, splitKey)       — schema + table-1 rows with pk < splitPK
//	Node 2 owns BTree keys [splitKey, maxUint64) — table-1 rows with pk >= splitPK
//
// splitKey should be computed via splitKeyForPK(tableID, pkThreshold).
func newTwoNodeCluster(t *testing.T, splitKey uint64) *clusterStack {
	t.Helper()

	// Phase 1: allocate BTrees.
	bt1, _ := makeBTree(t)
	bt2, _ := makeBTree(t)

	// Phase 2: shared SchemaCatalog backed by node 1.
	sc := sqllayer.NewSchemaCatalog(bt1)

	// Phase 3: build RangeServers and gRPC connections.
	srv1 := buildRangeServer(t, bt1, sc)
	srv2 := buildRangeServer(t, bt2, sc)
	conn1 := grpcConnect(t, srv1)
	conn2 := grpcConnect(t, srv2)

	// Phase 4: coordinator + gateway.
	coord := partition.NewCoordinator(map[uint64]string{1: "n1", 2: "n2"})
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

	router, err := partition.NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	gw := partition.NewGateway(router, sc)
	gw.AddConn(1, conn1)
	gw.AddConn(2, conn2)

	httpSrv := httpapi.NewHTTPServer(gw, sc, map[string]bool{testAPIKey: true})
	ts := httptest.NewServer(httpSrv.Routes())
	t.Cleanup(ts.Close)

	return &clusterStack{ts: ts, bts: []*btree.BTree{bt1, bt2}}
}

// newThreeNodeCluster creates a 3-node HTTP-fronted cluster with two split points.
//
//	Node 1: [0, split1)
//	Node 2: [split1, split2)
//	Node 3: [split2, ∞)
func newThreeNodeCluster(t *testing.T, split1, split2 uint64) *clusterStack {
	t.Helper()

	bt1, _ := makeBTree(t)
	bt2, _ := makeBTree(t)
	bt3, _ := makeBTree(t)
	sc := sqllayer.NewSchemaCatalog(bt1)

	srv1 := buildRangeServer(t, bt1, sc)
	srv2 := buildRangeServer(t, bt2, sc)
	srv3 := buildRangeServer(t, bt3, sc)
	conn1 := grpcConnect(t, srv1)
	conn2 := grpcConnect(t, srv2)
	conn3 := grpcConnect(t, srv3)

	coord := partition.NewCoordinator(map[uint64]string{1: "n1", 2: "n2", 3: "n3"})
	if err := coord.RequestSplit(1, split1); err != nil {
		t.Fatalf("RequestSplit 1→split1: %v", err)
	}
	// Find the range that covers [split1, ∞) and split it at split2.
	var highRangeID uint64
	for _, rd := range coord.GetAllRanges() {
		if rd.StartKey == split1 {
			highRangeID = rd.RangeID
		}
	}
	if highRangeID == 0 {
		t.Fatal("could not find high range after first split")
	}
	if err := coord.RequestSplit(highRangeID, split2); err != nil {
		t.Fatalf("RequestSplit high→split2: %v", err)
	}
	for _, rd := range coord.GetAllRanges() {
		switch rd.StartKey {
		case 0:
			coord.UpdateLeader(rd.RangeID, 1)
		case split1:
			coord.UpdateLeader(rd.RangeID, 2)
		default: // split2
			coord.UpdateLeader(rd.RangeID, 3)
		}
	}

	router, err := partition.NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	gw := partition.NewGateway(router, sc)
	gw.AddConn(1, conn1)
	gw.AddConn(2, conn2)
	gw.AddConn(3, conn3)

	httpSrv := httpapi.NewHTTPServer(gw, sc, map[string]bool{testAPIKey: true})
	ts := httptest.NewServer(httpSrv.Routes())
	t.Cleanup(ts.Close)

	return &clusterStack{ts: ts, bts: []*btree.BTree{bt1, bt2, bt3}}
}

// splitKeyForPK computes the BTree key for a given tableID and pk threshold.
// EncodeKey(tableId, pk) = (uint64(tableId) << 32) | uint64(pk)
func splitKeyForPK(tableID uint32, pkThreshold uint32) uint64 {
	return sqllayer.EncodeKey(tableID, pkThreshold)
}

// --- HTTP helpers for clusterStack ---

func (c *clusterStack) req(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.ts.URL+path, r)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := c.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (c *clusterStack) getQ(t *testing.T, path string, params url.Values) *http.Response {
	t.Helper()
	u := c.ts.URL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := c.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	return resp
}

func (c *clusterStack) createTable(t *testing.T, name string, cols ...httpapi.ColumnDefJSON) {
	t.Helper()
	resp := c.req(t, http.MethodPost, "/tables", httpapi.CreateTableRequest{Name: name, Columns: cols})
	mustStatus(t, resp, http.StatusCreated)
	drainClose(resp)
}

func (c *clusterStack) insertRow(t *testing.T, table string, values map[string]any) {
	t.Helper()
	resp := c.req(t, http.MethodPost, "/tables/"+table+"/rows", httpapi.InsertRequest{Values: values})
	mustStatus(t, resp, http.StatusCreated)
	drainClose(resp)
}

func (c *clusterStack) selectRows(t *testing.T, table string, params url.Values) httpapi.RowsResponse {
	t.Helper()
	resp := c.getQ(t, "/tables/"+table+"/rows", params)
	mustStatus(t, resp, http.StatusOK)
	return decode[httpapi.RowsResponse](t, resp)
}

// --- Low-level helpers safe to call from goroutines (no t.Fatal) ---

func rawInsert(client *http.Client, baseURL, table string, values map[string]any) error {
	b, err := json.Marshal(httpapi.InsertRequest{Values: values})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/tables/"+table+"/rows", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func rawSelect(client *http.Client, baseURL, table string) (int, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/tables/"+table+"/rows", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body httpapi.RowsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	return body.Count, nil
}

// =============================================================================
// 2-Node routing tests
// =============================================================================

func TestSimulation_TwoNode_ScatterGatherReturnsAllRows(t *testing.T) {
	// Split table 1 at PK=500: node 1 holds pk<500, node 2 holds pk>=500.
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "distributed",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
	)

	// 10 rows below the split (node 1) and 10 above (node 2).
	for i := 0; i < 10; i++ {
		c.insertRow(t, "distributed", map[string]any{"id": float64(i)})
		c.insertRow(t, "distributed", map[string]any{"id": float64(500 + i)})
	}

	body := c.selectRows(t, "distributed", nil)
	if body.Count != 20 {
		t.Fatalf("scatter-gather: want 20 rows, got %d", body.Count)
	}
}

func TestSimulation_TwoNode_WhereOnNode1Only(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "routed1", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 0; i < 5; i++ {
		c.insertRow(t, "routed1", map[string]any{"id": float64(i)})
		c.insertRow(t, "routed1", map[string]any{"id": float64(500 + i)})
	}

	body := c.selectRows(t, "routed1", url.Values{"where": {"id < 5"}})
	if body.Count != 5 {
		t.Fatalf("want 5 rows from node 1, got %d", body.Count)
	}
}

func TestSimulation_TwoNode_WhereOnNode2Only(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "routed2", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 0; i < 5; i++ {
		c.insertRow(t, "routed2", map[string]any{"id": float64(i)})
		c.insertRow(t, "routed2", map[string]any{"id": float64(500 + i)})
	}

	body := c.selectRows(t, "routed2", url.Values{"where": {"id > 499"}})
	if body.Count != 5 {
		t.Fatalf("want 5 rows from node 2, got %d", body.Count)
	}
}

func TestSimulation_TwoNode_UpdateOnSpecificNode(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "updatable",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "v", DataType: "INT"},
	)
	c.insertRow(t, "updatable", map[string]any{"id": float64(1), "v": float64(10)})   // node 1
	c.insertRow(t, "updatable", map[string]any{"id": float64(600), "v": float64(20)}) // node 2

	resp := c.req(t, http.MethodPut, "/tables/updatable/rows", httpapi.UpdateRequest{
		Set:   map[string]any{"v": float64(99)},
		Where: "id = 600",
	})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	n1row := c.selectRows(t, "updatable", url.Values{"where": {"id = 1"}})
	if n1row.Count != 1 || n1row.Rows[0]["v"] != float64(10) {
		t.Fatalf("node-1 row should be unchanged: %+v", n1row.Rows)
	}

	n2row := c.selectRows(t, "updatable", url.Values{"where": {"id = 600"}})
	if n2row.Count != 1 || n2row.Rows[0]["v"] != float64(99) {
		t.Fatalf("node-2 row should be updated: %+v", n2row.Rows)
	}
}

func TestSimulation_TwoNode_DeleteFromNode2_Node1Unaffected(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "del_test", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 0; i < 5; i++ {
		c.insertRow(t, "del_test", map[string]any{"id": float64(i)})
		c.insertRow(t, "del_test", map[string]any{"id": float64(500 + i)})
	}

	resp := c.req(t, http.MethodDelete, "/tables/del_test/rows", httpapi.DeleteRequest{
		Where: "id > 499",
	})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	body := c.selectRows(t, "del_test", nil)
	if body.Count != 5 {
		t.Fatalf("want 5 rows remaining (node 1 only), got %d", body.Count)
	}
}

func TestSimulation_TwoNode_TwoTablesOnDifferentNodes(t *testing.T) {
	// Split between table 1 and table 2: table 1 keys are all < EncodeKey(2,0).
	c := newTwoNodeCluster(t, sqllayer.EncodeKey(2, 0))

	c.createTable(t, "t1", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	c.createTable(t, "t2", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	for i := 1; i <= 5; i++ {
		c.insertRow(t, "t1", map[string]any{"id": float64(i)})
		c.insertRow(t, "t2", map[string]any{"id": float64(i)})
	}

	if got := c.selectRows(t, "t1", nil).Count; got != 5 {
		t.Fatalf("t1: want 5 rows, got %d", got)
	}
	if got := c.selectRows(t, "t2", nil).Count; got != 5 {
		t.Fatalf("t2: want 5 rows, got %d", got)
	}
}

// =============================================================================
// 3-Node routing tests
// =============================================================================

func TestSimulation_ThreeNode_RowsDistributedAcrossAllNodes(t *testing.T) {
	split1 := splitKeyForPK(1, 333)
	split2 := splitKeyForPK(1, 667)
	c := newThreeNodeCluster(t, split1, split2)

	c.createTable(t, "three_way", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// 10 rows per range zone.
	for i := 0; i < 10; i++ {
		c.insertRow(t, "three_way", map[string]any{"id": float64(i)})       // node 1: pk 0–9
		c.insertRow(t, "three_way", map[string]any{"id": float64(333 + i)}) // node 2: pk 333–342
		c.insertRow(t, "three_way", map[string]any{"id": float64(667 + i)}) // node 3: pk 667–676
	}

	body := c.selectRows(t, "three_way", nil)
	if body.Count != 30 {
		t.Fatalf("3-node scatter-gather: want 30 rows, got %d", body.Count)
	}
}

func TestSimulation_ThreeNode_WhereSpanningAllThreeNodes(t *testing.T) {
	split1 := splitKeyForPK(1, 333)
	split2 := splitKeyForPK(1, 667)
	c := newThreeNodeCluster(t, split1, split2)

	c.createTable(t, "span_all", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	for _, pk := range []int{10, 400, 700} {
		c.insertRow(t, "span_all", map[string]any{"id": float64(pk)})
	}

	body := c.selectRows(t, "span_all", url.Values{"where": {"id > 0"}})
	if body.Count != 3 {
		t.Fatalf("want 3 rows across all nodes, got %d", body.Count)
	}
}

func TestSimulation_ThreeNode_DeleteAcrossRanges(t *testing.T) {
	split1 := splitKeyForPK(1, 333)
	split2 := splitKeyForPK(1, 667)
	c := newThreeNodeCluster(t, split1, split2)

	c.createTable(t, "del_span", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	for i := 0; i < 5; i++ {
		c.insertRow(t, "del_span", map[string]any{"id": float64(i)})
		c.insertRow(t, "del_span", map[string]any{"id": float64(333 + i)})
		c.insertRow(t, "del_span", map[string]any{"id": float64(667 + i)})
	}

	// Delete the row at PK=333 (exactly the first key of range 2).
	resp := c.req(t, http.MethodDelete, "/tables/del_span/rows", httpapi.DeleteRequest{
		Where: "id = 333",
	})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	body := c.selectRows(t, "del_span", nil)
	if body.Count != 14 {
		t.Fatalf("want 14 rows after deleting 1, got %d", body.Count)
	}
}

// =============================================================================
// Edge cases
// =============================================================================

func TestSimulation_EdgeCase_PKZero_RoutesToNode1(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "edge", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	c.insertRow(t, "edge", map[string]any{"id": float64(0)})

	body := c.selectRows(t, "edge", url.Values{"where": {"id = 0"}})
	if body.Count != 1 {
		t.Fatalf("want pk=0 to be found, got %d rows", body.Count)
	}
}

func TestSimulation_EdgeCase_PKAtSplitBoundary_RoutesToNode2(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "boundary", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// pk=499 → node 1 (last before split), pk=500 → node 2 (first at split).
	c.insertRow(t, "boundary", map[string]any{"id": float64(499)})
	c.insertRow(t, "boundary", map[string]any{"id": float64(500)})

	all := c.selectRows(t, "boundary", nil)
	if all.Count != 2 {
		t.Fatalf("want 2 rows total, got %d", all.Count)
	}
}

func TestSimulation_EdgeCase_PKJustBelowAndAboveSplit(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 1000))

	c.createTable(t, "adjacent", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	c.insertRow(t, "adjacent", map[string]any{"id": float64(999)})  // node 1, last before split
	c.insertRow(t, "adjacent", map[string]any{"id": float64(1000)}) // node 2, first at split
	c.insertRow(t, "adjacent", map[string]any{"id": float64(1001)}) // node 2

	below := c.selectRows(t, "adjacent", url.Values{"where": {"id < 1000"}})
	if below.Count != 1 {
		t.Fatalf("want 1 row below split, got %d", below.Count)
	}
	above := c.selectRows(t, "adjacent", url.Values{"where": {"id > 999"}})
	if above.Count != 2 {
		t.Fatalf("want 2 rows at/above split, got %d", above.Count)
	}
}

func TestSimulation_EdgeCase_EmptyNode2_AllRowsOnNode1(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "only_n1", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// All PKs below the split → node 1 only.
	for i := 1; i <= 10; i++ {
		c.insertRow(t, "only_n1", map[string]any{"id": float64(i)})
	}

	body := c.selectRows(t, "only_n1", nil)
	if body.Count != 10 {
		t.Fatalf("want 10 rows (all on node 1), got %d", body.Count)
	}
}

func TestSimulation_EdgeCase_EmptyNode1_AllRowsOnNode2(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "only_n2", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// All PKs at or above the split → node 2 only.
	for i := 500; i < 510; i++ {
		c.insertRow(t, "only_n2", map[string]any{"id": float64(i)})
	}

	body := c.selectRows(t, "only_n2", nil)
	if body.Count != 10 {
		t.Fatalf("want 10 rows (all on node 2), got %d", body.Count)
	}
}

func TestSimulation_EdgeCase_LargePKValue(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "large_pk", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	const largePK = 1<<24 - 1 // 16777215, well within uint32 range
	c.insertRow(t, "large_pk", map[string]any{"id": float64(largePK)})

	body := c.selectRows(t, "large_pk", url.Values{"where": {fmt.Sprintf("id = %d", largePK)}})
	if body.Count != 1 {
		t.Fatalf("want 1 row for large PK, got %d", body.Count)
	}
}

func TestSimulation_EdgeCase_MultiTableCluster_IndependentRows(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "alpha", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	c.createTable(t, "beta", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	c.insertRow(t, "alpha", map[string]any{"id": float64(1)})
	c.insertRow(t, "alpha", map[string]any{"id": float64(2)})
	c.insertRow(t, "beta", map[string]any{"id": float64(1)})

	if got := c.selectRows(t, "alpha", nil).Count; got != 2 {
		t.Fatalf("alpha: want 2, got %d", got)
	}
	if got := c.selectRows(t, "beta", nil).Count; got != 1 {
		t.Fatalf("beta: want 1, got %d", got)
	}
}

func TestSimulation_EdgeCase_DropTableOnCluster(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "tmp", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	c.insertRow(t, "tmp", map[string]any{"id": float64(1)})
	c.insertRow(t, "tmp", map[string]any{"id": float64(600)})

	resp := c.req(t, http.MethodDelete, "/tables/tmp", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = c.req(t, http.MethodGet, "/tables/tmp", nil)
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

func TestSimulation_EdgeCase_WherePredicate_ANDOnNode1(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "and_test", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 0; i < 10; i++ {
		c.insertRow(t, "and_test", map[string]any{"id": float64(i)})
		c.insertRow(t, "and_test", map[string]any{"id": float64(500 + i)})
	}

	// AND predicate that selects only from node 1 (ids 4, 5, 6).
	body := c.selectRows(t, "and_test", url.Values{"where": {"id > 3 AND id < 7"}})
	if body.Count != 3 {
		t.Fatalf("want 3 rows for AND predicate on node 1, got %d", body.Count)
	}
}

func TestSimulation_EdgeCase_ConsistencyOnCluster(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))

	c.createTable(t, "cons_test", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// Default is strong.
	info := decode[httpapi.TableInfoResponse](t, c.req(t, http.MethodGet, "/tables/cons_test", nil))
	if info.Consistency != "strong" {
		t.Fatalf("want strong by default, got %q", info.Consistency)
	}

	// Insert rows in CP mode so they land in the BTree.
	c.insertRow(t, "cons_test", map[string]any{"id": float64(1)})
	c.insertRow(t, "cons_test", map[string]any{"id": float64(2)})

	// Verify rows are visible.
	if got := c.selectRows(t, "cons_test", nil).Count; got != 2 {
		t.Fatalf("want 2 rows before alter, got %d", got)
	}

	// Alter to eventual — metadata change should persist cluster-wide.
	resp := c.req(t, http.MethodPut, "/tables/cons_test/consistency",
		httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	info = decode[httpapi.TableInfoResponse](t, c.req(t, http.MethodGet, "/tables/cons_test", nil))
	if info.Consistency != "eventual" {
		t.Fatalf("want eventual after alter, got %q", info.Consistency)
	}

	// Alter back to strong.
	resp = c.req(t, http.MethodPut, "/tables/cons_test/consistency",
		httpapi.AlterConsistencyRequest{Mode: "strong"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	info = decode[httpapi.TableInfoResponse](t, c.req(t, http.MethodGet, "/tables/cons_test", nil))
	if info.Consistency != "strong" {
		t.Fatalf("want strong after alter back, got %q", info.Consistency)
	}
}

// =============================================================================
// Stress tests
// =============================================================================

func TestSimulation_Stress_ConcurrentInsertsOnSingleNode(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "stress", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	const (
		goroutines    = 20
		rowsPerThread = 50
	)
	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < rowsPerThread; i++ {
				pk := gid*rowsPerThread + i
				if err := rawInsert(s.client, s.ts.URL, "stress", map[string]any{"id": float64(pk)}); err != nil {
					failed.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Fatalf("%d inserts failed", n)
	}
	count, err := rawSelect(s.client, s.ts.URL, "stress")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if count != goroutines*rowsPerThread {
		t.Fatalf("want %d rows, got %d", goroutines*rowsPerThread, count)
	}
}

func TestSimulation_Stress_ConcurrentInsertsAcrossTwoNodes(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))
	c.createTable(t, "dist_stress", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	const goroutines = 20

	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)

	// First half writes to node 1 (pk 0–249), second half to node 2 (pk 500–749).
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			var base int
			if gid < goroutines/2 {
				base = gid * 25 // 0–249
			} else {
				base = 500 + (gid-goroutines/2)*25 // 500–749
			}
			for i := 0; i < 25; i++ {
				if err := rawInsert(c.ts.Client(), c.ts.URL, "dist_stress", map[string]any{"id": float64(base + i)}); err != nil {
					failed.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Fatalf("%d inserts failed across 2 nodes", n)
	}
	count, err := rawSelect(c.ts.Client(), c.ts.URL, "dist_stress")
	if err != nil {
		t.Fatalf("scatter-gather select: %v", err)
	}
	if count != goroutines*25 {
		t.Fatalf("want %d total rows, got %d", goroutines*25, count)
	}
}

func TestSimulation_Stress_ConcurrentReadsAndWrites(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "mixed", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// Pre-populate so reads always return data.
	for i := 0; i < 50; i++ {
		s.insertRow(t, "mixed", map[string]any{"id": float64(i)})
	}

	const (
		writers = 10
		readers = 10
		writes  = 10
		reads   = 20
	)
	var (
		wg          sync.WaitGroup
		writeFailed atomic.Int64
		readFailed  atomic.Int64
		pkCounter   atomic.Int64
	)
	pkCounter.Store(50)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				pk := pkCounter.Add(1)
				if err := rawInsert(s.client, s.ts.URL, "mixed", map[string]any{"id": float64(pk)}); err != nil {
					writeFailed.Add(1)
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < reads; i++ {
				if _, err := rawSelect(s.client, s.ts.URL, "mixed"); err != nil {
					readFailed.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	if n := writeFailed.Load(); n > 0 {
		t.Fatalf("%d writes failed under concurrent read load", n)
	}
	if n := readFailed.Load(); n > 0 {
		t.Fatalf("%d reads failed under concurrent write load", n)
	}
}

func TestSimulation_Stress_HighVolume_1000Rows_SingleNode(t *testing.T) {
	s := newTestStack(t)
	s.createTable(t, "bulk", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	const total = 1000
	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := rawInsert(s.client, s.ts.URL, "bulk", map[string]any{"id": float64(gid*100 + i)}); err != nil {
					failed.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Fatalf("%d of %d bulk inserts failed", n, total)
	}
	count, err := rawSelect(s.client, s.ts.URL, "bulk")
	if err != nil {
		t.Fatalf("bulk select: %v", err)
	}
	if count != total {
		t.Fatalf("want %d rows, got %d", total, count)
	}
}

func TestSimulation_Stress_HighVolume_1000Rows_TwoNodes(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))
	c.createTable(t, "bulk2", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	const total = 1000
	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)
	// 10 goroutines × 100 rows each, half below split (node 1), half above (node 2).
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				pk1 := gid*50 + i       // 0–499 (node 1)
				pk2 := 500 + gid*50 + i // 500–999 (node 2)
				if err := rawInsert(c.ts.Client(), c.ts.URL, "bulk2", map[string]any{"id": float64(pk1)}); err != nil {
					failed.Add(1)
				}
				if err := rawInsert(c.ts.Client(), c.ts.URL, "bulk2", map[string]any{"id": float64(pk2)}); err != nil {
					failed.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Fatalf("%d of %d inserts failed on 2-node cluster", n, total)
	}
	count, err := rawSelect(c.ts.Client(), c.ts.URL, "bulk2")
	if err != nil {
		t.Fatalf("2-node bulk select: %v", err)
	}
	if count != total {
		t.Fatalf("want %d total rows across 2 nodes, got %d", total, count)
	}
}

func TestSimulation_Stress_ManyTablesCreatedConcurrently(t *testing.T) {
	s := newTestStack(t)

	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)

	const numTables = 20
	for i := 0; i < numTables; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("tbl_%d", idx)
			col := httpapi.ColumnDefJSON{Name: "id", DataType: "INT"}
			body, _ := json.Marshal(httpapi.CreateTableRequest{Name: name, Columns: []httpapi.ColumnDefJSON{col}})
			req, _ := http.NewRequest(http.MethodPost, s.ts.URL+"/tables", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+testAPIKey)
			resp, err := s.client.Do(req)
			if err != nil || resp.StatusCode != http.StatusCreated {
				failed.Add(1)
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Fatalf("%d table creations failed", n)
	}

	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != numTables {
		t.Fatalf("want %d tables, got %d", numTables, len(tables))
	}
}

func TestSimulation_Stress_ConcurrentMixedOps_TwoNodes(t *testing.T) {
	c := newTwoNodeCluster(t, splitKeyForPK(1, 500))
	c.createTable(t, "chaos", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// Pre-seed rows on both nodes.
	for i := 0; i < 20; i++ {
		c.insertRow(t, "chaos", map[string]any{"id": float64(i)})
		c.insertRow(t, "chaos", map[string]any{"id": float64(500 + i)})
	}

	var (
		wg         sync.WaitGroup
		insertFail atomic.Int64
		selectFail atomic.Int64
		pkCounter  atomic.Int64
	)
	pkCounter.Store(1000)

	// Writers inserting to node 2.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pk := pkCounter.Add(1)
				if err := rawInsert(c.ts.Client(), c.ts.URL, "chaos", map[string]any{"id": float64(pk)}); err != nil {
					insertFail.Add(1)
				}
			}
		}()
	}

	// Readers running scatter-gather across both nodes.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := rawSelect(c.ts.Client(), c.ts.URL, "chaos"); err != nil {
					selectFail.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	if n := insertFail.Load(); n > 0 {
		t.Fatalf("%d inserts failed in mixed ops", n)
	}
	if n := selectFail.Load(); n > 0 {
		t.Fatalf("%d selects failed in mixed ops", n)
	}
}
