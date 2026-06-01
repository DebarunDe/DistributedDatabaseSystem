package partition

// Simulation tests for the partition layer.
//
// Each test spins up one or more in-process simRangeServer nodes backed by real
// BTrees, wires them through a live Coordinator / Router / Gateway, and drives
// operations via the same Gateway.Execute path used in production.  This lets us
// verify routing correctness, scatter-gather aggregation, key-boundary scoping,
// multi-range mutations, and data isolation across tables without touching the
// network.

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
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
// colMeta: column name → field-index mapping for WHERE evaluation
// ---------------------------------------------------------------------------

type colMeta struct {
	nameToIdx map[string]int // lower-cased column name → field index (0=PK, 1+=cols)
}

func buildColMeta(schema *sqllayer.TableSchemaValue) *colMeta {
	m := &colMeta{nameToIdx: make(map[string]int)}
	m.nameToIdx[strings.ToLower(schema.PrimaryKey.Name)] = 0
	for i, c := range schema.Columns {
		m.nameToIdx[strings.ToLower(c.Name)] = i + 1
	}
	return m
}

// ---------------------------------------------------------------------------
// simRangeServer: faithful in-process RangeServiceServer
// ---------------------------------------------------------------------------

type simRangeServer struct {
	rs.UnimplementedRangeServiceServer
	mu      sync.Mutex
	bt      *btree.BTree
	schemas map[uint32]*colMeta // tableId → metadata
}

func newSimRangeServer(bt *btree.BTree) *simRangeServer {
	return &simRangeServer{bt: bt, schemas: make(map[uint32]*colMeta)}
}

func (s *simRangeServer) registerSchema(tableId uint32, schema *sqllayer.TableSchemaValue) {
	s.mu.Lock()
	s.schemas[tableId] = buildColMeta(schema)
	s.mu.Unlock()
}

func (s *simRangeServer) Execute(_ context.Context, req *rs.RangeRequest) (*rs.RangeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch req.Op {
	case rs.RangeOp_INSERT:
		return s.execInsert(req)
	case rs.RangeOp_SCAN:
		return s.execScan(req)
	case rs.RangeOp_UPDATE:
		return s.execUpdate(req)
	case rs.RangeOp_DELETE:
		return s.execDelete(req)
	default:
		return &rs.RangeResponse{Error: fmt.Sprintf("unknown op %v", req.Op)}, nil
	}
}

func (s *simRangeServer) execInsert(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	fields := simProtoToFields(req.Fields)
	if err := s.bt.Insert(req.Key, fields); err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	return &rs.RangeResponse{}, nil
}

func (s *simRangeServer) execScan(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	rows, err := s.bt.RangeScan(req.StartKey, req.EndKey)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	meta := s.schemas[req.TableId]
	resp := &rs.RangeResponse{}
	for _, row := range rows {
		if req.Where != nil && !evalProtoExpr(req.Where, row.Fields, meta) {
			continue
		}
		rr := &rs.ResultRow{Key: row.Key}
		if len(req.Columns) == 0 {
			for _, f := range row.Fields {
				rr.Fields = append(rr.Fields, simFieldToProto(f))
			}
		} else {
			for _, ci := range req.Columns {
				if ci >= 0 && int(ci) < len(row.Fields) {
					rr.Fields = append(rr.Fields, simFieldToProto(row.Fields[ci]))
				}
			}
		}
		resp.Rows = append(resp.Rows, rr)
	}
	return resp, nil
}

func (s *simRangeServer) execUpdate(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	if len(req.Fields) == 0 {
		return &rs.RangeResponse{Error: "UPDATE: no field supplied"}, nil
	}
	rows, err := s.bt.RangeScan(req.StartKey, req.EndKey)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	meta := s.schemas[req.TableId]
	newField := protoFieldToBTree(req.Fields[0])
	newField.Tag = uint8(req.UpdateCol)
	colIdx := int(req.UpdateCol)

	for _, row := range rows {
		if req.Where != nil && !evalProtoExpr(req.Where, row.Fields, meta) {
			continue
		}
		updated := make([]btree.Field, len(row.Fields))
		copy(updated, row.Fields)
		if colIdx >= 0 && colIdx < len(updated) {
			updated[colIdx] = newField
		}
		if err := s.bt.Delete(row.Key); err != nil {
			return &rs.RangeResponse{Error: err.Error()}, nil
		}
		if err := s.bt.Insert(row.Key, updated); err != nil {
			return &rs.RangeResponse{Error: err.Error()}, nil
		}
	}
	return &rs.RangeResponse{}, nil
}

func (s *simRangeServer) execDelete(req *rs.RangeRequest) (*rs.RangeResponse, error) {
	rows, err := s.bt.RangeScan(req.StartKey, req.EndKey)
	if err != nil {
		return &rs.RangeResponse{Error: err.Error()}, nil
	}
	meta := s.schemas[req.TableId]
	for _, row := range rows {
		if req.Where != nil && !evalProtoExpr(req.Where, row.Fields, meta) {
			continue
		}
		if err := s.bt.Delete(row.Key); err != nil {
			return &rs.RangeResponse{Error: err.Error()}, nil
		}
	}
	return &rs.RangeResponse{}, nil
}

// ---------------------------------------------------------------------------
// Proto-expression evaluator (runs inside simRangeServer)
// ---------------------------------------------------------------------------

func evalProtoExpr(expr *rs.Expression, fields []btree.Field, meta *colMeta) bool {
	if expr == nil {
		return true
	}
	switch e := expr.Expr.(type) {
	case *rs.Expression_Comparison:
		return evalProtoCmp(e.Comparison, fields, meta)
	case *rs.Expression_Logical:
		l := evalProtoExpr(e.Logical.Left, fields, meta)
		r := evalProtoExpr(e.Logical.Right, fields, meta)
		switch strings.ToUpper(e.Logical.Operator) {
		case "AND":
			return l && r
		case "OR":
			return l || r
		}
	}
	return true
}

func evalProtoCmp(cmp *rs.ComparisonExpr, fields []btree.Field, meta *colMeta) bool {
	if meta == nil {
		return true
	}
	idx, ok := meta.nameToIdx[strings.ToLower(cmp.Column)]
	if !ok || idx >= len(fields) {
		return false
	}
	switch v := fields[idx].Value.(type) {
	case btree.IntValue:
		lit, err := strconv.ParseInt(cmp.LiteralValue, 10, 64)
		if err != nil {
			return false
		}
		return simCmpInt(v.V, lit, cmp.Operator)
	case btree.StringValue:
		return simCmpStr(v.V, cmp.LiteralValue, cmp.Operator)
	}
	return false
}

func simCmpInt(a, b int64, op string) bool {
	switch op {
	case "=":
		return a == b
	case "!=", "<>":
		return a != b
	case "<":
		return a < b
	case ">":
		return a > b
	case "<=":
		return a <= b
	case ">=":
		return a >= b
	}
	return false
}

func simCmpStr(a, b, op string) bool {
	switch op {
	case "=":
		return a == b
	case "!=", "<>":
		return a != b
	case "<":
		return a < b
	case ">":
		return a > b
	case "<=":
		return a <= b
	case ">=":
		return a >= b
	}
	return false
}

// ---------------------------------------------------------------------------
// Field conversion helpers
// ---------------------------------------------------------------------------

func simProtoToFields(pfs []*rs.Field) []btree.Field {
	out := make([]btree.Field, len(pfs))
	for i, pf := range pfs {
		out[i] = protoFieldToBTree(pf)
	}
	return out
}

func simFieldToProto(f btree.Field) *rs.Field {
	return fieldsToProto([]btree.Field{f})[0]
}

// ---------------------------------------------------------------------------
// simCluster: orchestration harness
// ---------------------------------------------------------------------------

type simCluster struct {
	t     *testing.T
	sc    *sqllayer.SchemaCatalog
	coord *Coordinator
	gw    *Gateway
	nodes map[uint64]*simRangeServer // nodeID → server
}

// newSimCluster creates a one-node cluster.  All key ranges are covered by
// node 1, which is immediately set as leader.
func newSimCluster(t *testing.T) *simCluster {
	t.Helper()
	return newMultiNodeCluster(t, 1)
}

// newMultiNodeCluster creates a cluster with nodeCount independent nodes, each
// backed by its own BTree.  All ranges start on node 1; callers use
// splitAtKey to assign ranges to specific nodes.
func newMultiNodeCluster(t *testing.T, nodeCount int) *simCluster {
	t.Helper()

	// Schema catalog gets its own BTree (metadata only).
	scPM, err := pagemanager.NewDB(t.TempDir() + "/schema.db")
	if err != nil {
		t.Fatalf("schema NewDB: %v", err)
	}
	t.Cleanup(func() { _ = scPM.Delete() })
	sc := sqllayer.NewSchemaCatalog(btree.NewBTree(scPM))

	// Build node map and per-node simRangeServers.
	allNodes := make(map[uint64]string, nodeCount)
	nodes := make(map[uint64]*simRangeServer, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		allNodes[uint64(i)] = fmt.Sprintf("node%d:8080", i)

		pm, err := pagemanager.NewDB(fmt.Sprintf("%s/node%d.db", t.TempDir(), i))
		if err != nil {
			t.Fatalf("node%d NewDB: %v", i, err)
		}
		t.Cleanup(func() { _ = pm.Delete() })
		nodes[uint64(i)] = newSimRangeServer(btree.NewBTree(pm))
	}

	coord := NewCoordinator(allNodes)
	coord.UpdateLeader(1, 1) // initial single range → leader is node 1

	router, err := NewRouter(coord)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	gw := NewGateway(router, sc)

	c := &simCluster{t: t, sc: sc, coord: coord, gw: gw, nodes: nodes}
	for id, srv := range nodes {
		conn := c.startNode(srv)
		gw.AddConn(id, conn)
	}
	return c
}

// startNode wires a simRangeServer to a bufconn and returns a client connection.
func (c *simCluster) startNode(srv *simRangeServer) *grpc.ClientConn {
	c.t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	rs.RegisterRangeServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	c.t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		c.t.Fatalf("grpc.NewClient: %v", err)
	}
	c.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// addTable creates a table in the schema catalog and registers the schema
// with every node so WHERE expressions can be evaluated.
func (c *simCluster) addTable(name, pkName string, colNames, colTypes []string) {
	c.t.Helper()
	pkType := "INT"
	if err := c.sc.CreateTable(name, pkName, pkType, colNames, colTypes); err != nil {
		c.t.Fatalf("CreateTable %q: %v", name, err)
	}
	schema := c.sc.FindTableSchema(name)
	for _, srv := range c.nodes {
		srv.registerSchema(schema.TableId, schema)
	}
}

// splitAtKey splits the range containing splitKey into two halves.
// lowerNodeID and upperNodeID specify which node leads each half.
func (c *simCluster) splitAtKey(splitKey uint64, lowerNodeID, upperNodeID uint64) {
	c.t.Helper()
	rd, err := c.gw.router.RouteKey(splitKey)
	if err != nil {
		c.t.Fatalf("RouteKey for split: %v", err)
	}
	origStart, origEnd, origID := rd.StartKey, rd.EndKey, rd.RangeID
	if err := c.coord.RequestSplit(origID, splitKey); err != nil {
		c.t.Fatalf("RequestSplit(%d, %d): %v", origID, splitKey, err)
	}
	// Identify resulting ranges by their boundaries.
	for _, r := range c.coord.GetAllRanges() {
		if r.StartKey == origStart && r.EndKey == splitKey {
			c.coord.UpdateLeader(r.RangeID, lowerNodeID)
		} else if r.StartKey == splitKey && r.EndKey == origEnd {
			c.coord.UpdateLeader(r.RangeID, upperNodeID)
		}
	}
	if err := c.gw.router.Refresh(); err != nil {
		c.t.Fatalf("router.Refresh: %v", err)
	}
}

// insert executes an INSERT via the gateway.
func (c *simCluster) insert(table string, values []sqllayer.Literal) {
	c.t.Helper()
	if _, err := c.gw.Execute(&sqllayer.InsertStatement{Table: table, Values: values}); err != nil {
		c.t.Fatalf("INSERT into %q: %v", table, err)
	}
}

// mustSelect executes a SELECT and returns keys of matching rows.
func (c *simCluster) selectRows(table string, cols []string, where sqllayer.Expression) *ResultSet {
	c.t.Helper()
	rs, err := c.gw.Execute(&sqllayer.SelectStatement{
		Table:   table,
		Columns: cols,
		Where:   where,
	})
	if err != nil {
		c.t.Fatalf("SELECT from %q: %v", table, err)
	}
	return rs
}

// rowKeys returns the sorted primary keys from a ResultSet.
func rowKeys(rs *ResultSet) []uint64 {
	keys := make([]uint64, len(rs.Rows))
	for i, r := range rs.Rows {
		keys[i] = r.Key
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// pkVal extracts the PK integer from the encoded key for a given tableId.
func pkVal(encodedKey uint64) uint32 {
	return uint32(encodedKey & 0xFFFFFFFF)
}

// intLitS returns an INT literal (same as intLit in gateway_test.go, duplicated here).
func intLitS(v string) sqllayer.Literal {
	return sqllayer.Literal{Value: v, Type: sqllayer.TOKEN_NUMBER}
}

// strLitS returns a TEXT literal.
func strLitS(v string) sqllayer.Literal {
	return sqllayer.Literal{Value: v, Type: sqllayer.TOKEN_STRING}
}

func eqWhere(col, val string) sqllayer.Expression {
	return &sqllayer.ComparisonExpr{Column: col, Operator: "=", Value: intLitS(val)}
}

func rangeWhere(col, lo, hi string) sqllayer.Expression {
	return &sqllayer.LogicalExpr{
		Operator: "AND",
		Left:     &sqllayer.ComparisonExpr{Column: col, Operator: ">=", Value: intLitS(lo)},
		Right:    &sqllayer.ComparisonExpr{Column: col, Operator: "<=", Value: intLitS(hi)},
	}
}

// ---------------------------------------------------------------------------
// Simulation tests — single range
// ---------------------------------------------------------------------------

func TestSimulation_SingleRange_InsertAndSelectAll(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS(fmt.Sprintf("user%d", i))})
	}

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_SelectEqualityWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS(fmt.Sprintf("u%d", i))})
	}

	rs := c.selectRows("users", []string{"id"}, eqWhere("id", "5"))
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if pkVal(rs.Rows[0].Key) != 5 {
		t.Errorf("wrong row: pk=%d, want 5", pkVal(rs.Rows[0].Key))
	}
}

func TestSimulation_SingleRange_SelectGTWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	rs := c.selectRows("users", []string{"id"},
		&sqllayer.ComparisonExpr{Column: "id", Operator: ">", Value: intLitS("7")})
	if len(rs.Rows) != 3 { // 8, 9, 10
		t.Errorf("GT 7: expected 3 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_SelectLTWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	rs := c.selectRows("users", []string{"id"},
		&sqllayer.ComparisonExpr{Column: "id", Operator: "<", Value: intLitS("4")})
	if len(rs.Rows) != 3 { // 1, 2, 3
		t.Errorf("LT 4: expected 3 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_SelectRangeWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	rs := c.selectRows("users", []string{"id"}, rangeWhere("id", "3", "7"))
	if len(rs.Rows) != 5 { // 3,4,5,6,7
		t.Errorf("range [3,7]: expected 5 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_SelectNonPKWhere(t *testing.T) {
	// WHERE on a non-PK column forces a full scan but must still filter correctly.
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	names := []string{"alice", "bob", "alice", "carol", "alice"}
	for i, n := range names {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i + 1)), strLitS(n)})
	}

	rs := c.selectRows("users", []string{"id"},
		&sqllayer.ComparisonExpr{Column: "name", Operator: "=",
			Value: sqllayer.Literal{Value: "alice", Type: sqllayer.TOKEN_STRING}})
	if len(rs.Rows) != 3 {
		t.Errorf("name=alice: expected 3 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_EmptyTableSelect(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("empty table: expected 0 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_SelectNoMatchingRows(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	rs := c.selectRows("users", []string{"id"}, eqWhere("id", "99"))
	if len(rs.Rows) != 0 {
		t.Errorf("no match: expected 0 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_AndWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 20; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	where := &sqllayer.LogicalExpr{
		Operator: "AND",
		Left:     &sqllayer.ComparisonExpr{Column: "id", Operator: ">=", Value: intLitS("5")},
		Right:    &sqllayer.ComparisonExpr{Column: "id", Operator: "<=", Value: intLitS("10")},
	}
	rs := c.selectRows("users", []string{"id"}, where)
	if len(rs.Rows) != 6 { // 5,6,7,8,9,10
		t.Errorf("AND [5,10]: expected 6 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_OrWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	where := &sqllayer.LogicalExpr{
		Operator: "OR",
		Left:     eqWhere("id", "2"),
		Right:    eqWhere("id", "8"),
	}
	rs := c.selectRows("users", []string{"id"}, where)
	// OR widens the key bounds to [2,8]; the range server returns rows 2..8 then
	// filters, so we get exactly 2 and 8 only if the non-PK filter applies.
	// Since both conditions are on the PK, extractPKBounds gives [2,8] and the
	// server receives all rows in that window; the WHERE filter on the server
	// narrows to exactly {2, 8}.
	keys := rowKeys(rs)
	if len(keys) != 2 || pkVal(keys[0]) != 2 || pkVal(keys[1]) != 8 {
		t.Errorf("OR(2,8): expected keys [2,8], got %v", keys)
	}
}

// ---------------------------------------------------------------------------
// Update tests — single range
// ---------------------------------------------------------------------------

func TestSimulation_SingleRange_UpdateSingleRow(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("original")})
	}

	// Update row with id=3
	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  sqllayer.Literal{Value: "updated", Type: sqllayer.TOKEN_STRING},
		Where:  eqWhere("id", "3"),
	})
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	// Verify only row 3 changed.
	schema := c.sc.FindTableSchema("users")
	node := c.nodes[1]
	node.mu.Lock()
	defer node.mu.Unlock()

	rows, _ := node.bt.RangeScan(
		sqllayer.EncodeKey(schema.TableId, 0),
		sqllayer.EncodeKey(schema.TableId, ^uint32(0)),
	)
	for _, row := range rows {
		pk := pkVal(row.Key)
		if len(row.Fields) < 2 {
			continue
		}
		sv, ok := row.Fields[1].Value.(btree.StringValue)
		if !ok {
			continue
		}
		if pk == 3 && sv.V != "updated" {
			t.Errorf("row 3: expected 'updated', got %q", sv.V)
		}
		if pk != 3 && sv.V != "original" {
			t.Errorf("row %d: expected 'original', got %q", pk, sv.V)
		}
	}
}

func TestSimulation_SingleRange_UpdateRangeOfRows(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("old")})
	}

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  sqllayer.Literal{Value: "new", Type: sqllayer.TOKEN_STRING},
		Where:  rangeWhere("id", "5", "8"),
	})
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	schema := c.sc.FindTableSchema("users")
	node := c.nodes[1]
	node.mu.Lock()
	defer node.mu.Unlock()
	rows, _ := node.bt.RangeScan(
		sqllayer.EncodeKey(schema.TableId, 0),
		sqllayer.EncodeKey(schema.TableId, ^uint32(0)),
	)
	updated, unchanged := 0, 0
	for _, row := range rows {
		if len(row.Fields) < 2 {
			continue
		}
		sv, ok := row.Fields[1].Value.(btree.StringValue)
		if !ok {
			continue
		}
		if sv.V == "new" {
			updated++
		} else {
			unchanged++
		}
	}
	if updated != 4 || unchanged != 6 {
		t.Errorf("updated=%d want 4, unchanged=%d want 6", updated, unchanged)
	}
}

// ---------------------------------------------------------------------------
// Delete tests — single range
// ---------------------------------------------------------------------------

func TestSimulation_SingleRange_DeleteSingleRow(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "users", Where: eqWhere("id", "3")})
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 4 {
		t.Errorf("after DELETE id=3: expected 4 rows, got %d", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		if pkVal(row.Key) == 3 {
			t.Error("row with pk=3 still present after DELETE")
		}
	}
}

func TestSimulation_SingleRange_DeleteRangeOfRows(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 10; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{
		Table: "users",
		Where: rangeWhere("id", "4", "7"),
	})
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 6 { // 1,2,3,8,9,10
		t.Errorf("after DELETE [4,7]: expected 6 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_DeleteAllRows(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "users"}) // no WHERE
	if err != nil {
		t.Fatalf("DELETE all: %v", err)
	}

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("after full DELETE: expected 0 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_SingleRange_ReinsertAfterDelete(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	c.insert("users", []sqllayer.Literal{intLitS("7"), strLitS("first")})

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "users", Where: eqWhere("id", "7")})
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	// Re-insert the same key with a different value.
	c.insert("users", []sqllayer.Literal{intLitS("7"), strLitS("second")})

	rs := c.selectRows("users", []string{"id"}, eqWhere("id", "7"))
	if len(rs.Rows) != 1 {
		t.Fatalf("after re-insert: expected 1 row, got %d", len(rs.Rows))
	}
}

// ---------------------------------------------------------------------------
// Two-range scatter-gather — single backing node (routing logic)
// ---------------------------------------------------------------------------

func TestSimulation_TwoRanges_ScatterGatherSelectReturnsAllRows(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1) // both halves on node 1

	// Insert rows on both sides of the split.
	for i := 1; i <= 100; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 100 {
		t.Errorf("scatter-gather: expected 100 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_TwoRanges_PointLookupHitsOnlyOneRange(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1)

	for i := 1; i <= 100; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	rs := c.selectRows("users", []string{"id"}, eqWhere("id", "25"))
	if len(rs.Rows) != 1 {
		t.Fatalf("point lookup: expected 1 row, got %d", len(rs.Rows))
	}
	if pkVal(rs.Rows[0].Key) != 25 {
		t.Errorf("wrong row: pk=%d", pkVal(rs.Rows[0].Key))
	}
}

func TestSimulation_TwoRanges_RangeWhereSpanningBoundary(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1)

	for i := 1; i <= 100; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	// WHERE spans the split boundary at pk=50.
	rs := c.selectRows("users", []string{"id"}, rangeWhere("id", "45", "55"))
	if len(rs.Rows) != 11 { // 45..55 inclusive
		t.Errorf("boundary span [45,55]: expected 11 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_TwoRanges_MultiRangeUpdate(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1)

	for i := 1; i <= 100; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("old")})
	}

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "name",
		Value:  sqllayer.Literal{Value: "new", Type: sqllayer.TOKEN_STRING},
		// No WHERE → touches all rows in both ranges.
	})
	if err != nil {
		t.Fatalf("multi-range UPDATE: %v", err)
	}

	// Verify by reading directly from node's BTree.
	node := c.nodes[1]
	node.mu.Lock()
	defer node.mu.Unlock()
	rows, _ := node.bt.RangeScan(
		sqllayer.EncodeKey(schema.TableId, 0),
		sqllayer.EncodeKey(schema.TableId, ^uint32(0)),
	)
	for _, row := range rows {
		if len(row.Fields) < 2 {
			continue
		}
		sv, ok := row.Fields[1].Value.(btree.StringValue)
		if !ok || sv.V != "new" {
			t.Errorf("row pk=%d: expected 'new', got %q", pkVal(row.Key), sv.V)
		}
	}
}

func TestSimulation_TwoRanges_MultiRangeDelete(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1)

	for i := 1; i <= 100; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "users"}) // no WHERE
	if err != nil {
		t.Fatalf("multi-range DELETE: %v", err)
	}

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 0 {
		t.Errorf("after full multi-range DELETE: expected 0 rows, got %d", len(rs.Rows))
	}
}

// ---------------------------------------------------------------------------
// Key boundary edge cases
// ---------------------------------------------------------------------------

func TestSimulation_KeyExactlyAtRangeBoundary_InUpperRange(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("items", "id", []string{"val"}, []string{"INT"})

	schema := c.sc.FindTableSchema("items")
	splitKey := sqllayer.EncodeKey(schema.TableId, 100)
	c.splitAtKey(splitKey, 1, 1)

	// pk=100 is the split key → it belongs to the upper range.
	c.insert("items", []sqllayer.Literal{intLitS("100"), intLitS("42")})

	rs := c.selectRows("items", []string{"id"}, eqWhere("id", "100"))
	if len(rs.Rows) != 1 {
		t.Errorf("key at boundary: expected 1 row, got %d", len(rs.Rows))
	}
}

func TestSimulation_KeyJustBelowBoundary_InLowerRange(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("items", "id", []string{"val"}, []string{"INT"})

	schema := c.sc.FindTableSchema("items")
	splitKey := sqllayer.EncodeKey(schema.TableId, 100)
	c.splitAtKey(splitKey, 1, 1)

	// pk=99 is just below the split key → lower range.
	c.insert("items", []sqllayer.Literal{intLitS("99"), intLitS("99")})

	rs := c.selectRows("items", []string{"id"}, eqWhere("id", "99"))
	if len(rs.Rows) != 1 {
		t.Errorf("key below boundary: expected 1 row, got %d", len(rs.Rows))
	}
}

func TestSimulation_RowsOnBothSidesOfBoundary_ScatterReturnsAll(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("items", "id", []string{"val"}, []string{"INT"})

	schema := c.sc.FindTableSchema("items")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1)

	// One row just below split, one at split, one just above.
	c.insert("items", []sqllayer.Literal{intLitS("49"), intLitS("1")})
	c.insert("items", []sqllayer.Literal{intLitS("50"), intLitS("2")})
	c.insert("items", []sqllayer.Literal{intLitS("51"), intLitS("3")})

	rs := c.selectRows("items", []string{"id"}, nil)
	if len(rs.Rows) != 3 {
		t.Errorf("boundary rows: expected 3, got %d", len(rs.Rows))
	}
}

// ---------------------------------------------------------------------------
// Three-range scatter-gather
// ---------------------------------------------------------------------------

func TestSimulation_ThreeRanges_ScatterReturnsAllRows(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("orders", "id", []string{"total"}, []string{"INT"})

	schema := c.sc.FindTableSchema("orders")
	split1 := sqllayer.EncodeKey(schema.TableId, 33)
	split2 := sqllayer.EncodeKey(schema.TableId, 66)
	c.splitAtKey(split1, 1, 1)
	c.splitAtKey(split2, 1, 1)

	for i := 1; i <= 99; i++ {
		c.insert("orders", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i * 10))})
	}

	rs := c.selectRows("orders", []string{"id"}, nil)
	if len(rs.Rows) != 99 {
		t.Errorf("3-range scatter: expected 99 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_ThreeRanges_RangeWhereSpansAllThree(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("orders", "id", []string{"total"}, []string{"INT"})

	schema := c.sc.FindTableSchema("orders")
	c.splitAtKey(sqllayer.EncodeKey(schema.TableId, 33), 1, 1)
	c.splitAtKey(sqllayer.EncodeKey(schema.TableId, 66), 1, 1)

	for i := 1; i <= 99; i++ {
		c.insert("orders", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	rs := c.selectRows("orders", []string{"id"}, rangeWhere("id", "20", "80"))
	if len(rs.Rows) != 61 { // 20..80 inclusive
		t.Errorf("3-range [20,80]: expected 61 rows, got %d", len(rs.Rows))
	}
}

// ---------------------------------------------------------------------------
// Multiple-table isolation
// ---------------------------------------------------------------------------

func TestSimulation_MultipleTables_DataIsolation(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	c.addTable("orders", "id", []string{"total"}, []string{"INT"})

	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("u")})
		c.insert("orders", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("100")})
	}

	usersRS := c.selectRows("users", []string{"id"}, nil)
	ordersRS := c.selectRows("orders", []string{"id"}, nil)

	if len(usersRS.Rows) != 5 {
		t.Errorf("users: expected 5, got %d", len(usersRS.Rows))
	}
	if len(ordersRS.Rows) != 5 {
		t.Errorf("orders: expected 5, got %d", len(ordersRS.Rows))
	}

	// Keys in users and orders encode to different ranges (different tableIds).
	for _, ur := range usersRS.Rows {
		for _, or_ := range ordersRS.Rows {
			if ur.Key == or_.Key {
				t.Error("users and orders share the same encoded key — tables are not isolated")
			}
		}
	}
}

func TestSimulation_MultipleTables_DeleteFromOneDoesNotAffectOther(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	c.addTable("orders", "id", []string{"amt"}, []string{"INT"})

	for i := 1; i <= 5; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("u")})
		c.insert("orders", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS("0")})
	}

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "users"})
	if err != nil {
		t.Fatalf("DELETE users: %v", err)
	}

	ordersRS := c.selectRows("orders", []string{"id"}, nil)
	if len(ordersRS.Rows) != 5 {
		t.Errorf("orders after deleting users: expected 5, got %d", len(ordersRS.Rows))
	}
}

// ---------------------------------------------------------------------------
// Two-node cluster: physical data distribution
// ---------------------------------------------------------------------------

func TestSimulation_TwoNodes_ScatterGatherFromDifferentBTrees(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 2) // lower → node 1, upper → node 2

	// Insert rows into each physical half.
	for i := 1; i <= 49; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("low")})
	}
	for i := 50; i <= 99; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("high")})
	}

	// Verify rows are on separate BTrees.
	node1 := c.nodes[1]
	node2 := c.nodes[2]
	node1.mu.Lock()
	rows1, _ := node1.bt.RangeScan(sqllayer.EncodeKey(schema.TableId, 0), sqllayer.EncodeKey(schema.TableId, ^uint32(0)))
	node1.mu.Unlock()
	node2.mu.Lock()
	rows2, _ := node2.bt.RangeScan(sqllayer.EncodeKey(schema.TableId, 0), sqllayer.EncodeKey(schema.TableId, ^uint32(0)))
	node2.mu.Unlock()

	if len(rows1) != 49 {
		t.Errorf("node1: expected 49 rows, got %d", len(rows1))
	}
	if len(rows2) != 50 {
		t.Errorf("node2: expected 50 rows, got %d", len(rows2))
	}

	// Scatter-gather should merge both.
	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 99 {
		t.Errorf("scatter across 2 nodes: expected 99 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_TwoNodes_PointLookupRoutesToCorrectNode(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 2)

	c.insert("users", []sqllayer.Literal{intLitS("10"), strLitS("node1row")})
	c.insert("users", []sqllayer.Literal{intLitS("75"), strLitS("node2row")})

	rs10 := c.selectRows("users", []string{"id"}, eqWhere("id", "10"))
	if len(rs10.Rows) != 1 || pkVal(rs10.Rows[0].Key) != 10 {
		t.Errorf("point lookup pk=10: %+v", rs10.Rows)
	}

	rs75 := c.selectRows("users", []string{"id"}, eqWhere("id", "75"))
	if len(rs75.Rows) != 1 || pkVal(rs75.Rows[0].Key) != 75 {
		t.Errorf("point lookup pk=75: %+v", rs75.Rows)
	}
}

func TestSimulation_TwoNodes_UpdateOnCorrectNode(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	c.addTable("users", "id", []string{"score"}, []string{"INT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 2)

	c.insert("users", []sqllayer.Literal{intLitS("20"), intLitS("0")})
	c.insert("users", []sqllayer.Literal{intLitS("80"), intLitS("0")})

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "score",
		Value:  sqllayer.Literal{Value: "99", Type: sqllayer.TOKEN_NUMBER},
		Where:  eqWhere("id", "20"),
	})
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	// pk=20 is on node1; pk=80 should be unchanged on node2.
	node1 := c.nodes[1]
	node1.mu.Lock()
	fields1, _, _ := node1.bt.Search(sqllayer.EncodeKey(schema.TableId, 20))
	node1.mu.Unlock()

	if len(fields1) < 2 {
		t.Fatal("node1: row 20 not found or missing fields")
	}
	if iv, ok := fields1[1].Value.(btree.IntValue); !ok || iv.V != 99 {
		t.Errorf("node1 row 20: expected score=99, got %v", fields1[1].Value)
	}

	node2 := c.nodes[2]
	node2.mu.Lock()
	fields2, _, _ := node2.bt.Search(sqllayer.EncodeKey(schema.TableId, 80))
	node2.mu.Unlock()

	if len(fields2) < 2 {
		t.Fatal("node2: row 80 not found or missing fields")
	}
	if iv, ok := fields2[1].Value.(btree.IntValue); !ok || iv.V != 0 {
		t.Errorf("node2 row 80: expected score=0 (unchanged), got %v", fields2[1].Value)
	}
}

func TestSimulation_TwoNodes_DeleteOnCorrectNode(t *testing.T) {
	c := newMultiNodeCluster(t, 2)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 2)

	c.insert("users", []sqllayer.Literal{intLitS("10"), strLitS("low")})
	c.insert("users", []sqllayer.Literal{intLitS("60"), strLitS("high")})

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "users", Where: eqWhere("id", "10")})
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	// Verify node1 no longer has pk=10.
	node1 := c.nodes[1]
	node1.mu.Lock()
	rows1, _ := node1.bt.RangeScan(sqllayer.EncodeKey(schema.TableId, 0), sqllayer.EncodeKey(schema.TableId, ^uint32(0)))
	node1.mu.Unlock()
	if len(rows1) != 0 {
		t.Errorf("node1: expected 0 rows after DELETE, got %d", len(rows1))
	}

	// Verify node2 still has pk=60.
	node2 := c.nodes[2]
	node2.mu.Lock()
	rows2, _ := node2.bt.RangeScan(sqllayer.EncodeKey(schema.TableId, 0), sqllayer.EncodeKey(schema.TableId, ^uint32(0)))
	node2.mu.Unlock()
	if len(rows2) != 1 {
		t.Errorf("node2: expected 1 row (pk=60), got %d", len(rows2))
	}
}

// ---------------------------------------------------------------------------
// Large dataset stress
// ---------------------------------------------------------------------------

func TestSimulation_LargeDataset_CorrectCount(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("events", "id", []string{"payload"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("events")
	c.splitAtKey(sqllayer.EncodeKey(schema.TableId, 333), 1, 1)
	c.splitAtKey(sqllayer.EncodeKey(schema.TableId, 666), 1, 1)

	const N = 999
	for i := 1; i <= N; i++ {
		c.insert("events", []sqllayer.Literal{
			intLitS(strconv.Itoa(i)),
			strLitS(fmt.Sprintf("payload-%d", i)),
		})
	}

	rs := c.selectRows("events", []string{"id"}, nil)
	if len(rs.Rows) != N {
		t.Errorf("large dataset: expected %d rows, got %d", N, len(rs.Rows))
	}
}

func TestSimulation_LargeDataset_RangeWhereCorrect(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("events", "id", []string{"payload"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("events")
	c.splitAtKey(sqllayer.EncodeKey(schema.TableId, 333), 1, 1)
	c.splitAtKey(sqllayer.EncodeKey(schema.TableId, 666), 1, 1)

	for i := 1; i <= 999; i++ {
		c.insert("events", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("p")})
	}

	rs := c.selectRows("events", []string{"id"}, rangeWhere("id", "100", "200"))
	if len(rs.Rows) != 101 { // 100..200 inclusive
		t.Errorf("range where [100,200]: expected 101 rows, got %d", len(rs.Rows))
	}
}

// ---------------------------------------------------------------------------
// Error / edge cases
// ---------------------------------------------------------------------------

func TestSimulation_MissingLeaderConn_ReturnsError(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	// Build a second gateway with NO connections registered.
	gw2 := NewGateway(c.gw.router, c.sc)

	_, err := gw2.Execute(&sqllayer.SelectStatement{Table: "users", Columns: []string{"id"}})
	if err == nil {
		t.Fatal("expected error when no leader connection registered")
	}
}

func TestSimulation_SelectFromUnknownTable_ReturnsError(t *testing.T) {
	c := newSimCluster(t)

	_, err := c.gw.Execute(&sqllayer.SelectStatement{Table: "nonexistent", Columns: []string{"id"}})
	if err == nil {
		t.Fatal("expected error for unknown table")
	}
}

func TestSimulation_UpdateUnknownColumn_ReturnsError(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "users",
		Column: "nonexistent_col",
		Value:  strLitS("x"),
	})
	if err == nil {
		t.Fatal("expected error for unknown column in UPDATE")
	}
}

func TestSimulation_SelectUnknownColumn_ReturnsError(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	_, err := c.gw.Execute(&sqllayer.SelectStatement{
		Table:   "users",
		Columns: []string{"ghost"},
	})
	if err == nil {
		t.Fatal("expected error for unknown column in SELECT")
	}
}

func TestSimulation_RouterStale_RefreshRestoresRouting(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})
	schema := c.sc.FindTableSchema("users")

	for i := 1; i <= 20; i++ {
		c.insert("users", []sqllayer.Literal{intLitS(strconv.Itoa(i)), strLitS("x")})
	}

	// Perform a split AFTER inserting (router is stale until Refresh).
	splitKey := sqllayer.EncodeKey(schema.TableId, 10)
	if err := c.coord.RequestSplit(1, splitKey); err != nil {
		t.Fatalf("RequestSplit: %v", err)
	}
	c.coord.UpdateLeader(1, 1)
	c.coord.UpdateLeader(2, 1)
	if err := c.gw.router.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// After refresh, a full-table scan should still return all rows.
	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != 20 {
		t.Errorf("after stale-router refresh: expected 20 rows, got %d", len(rs.Rows))
	}
}

func TestSimulation_ConcurrentInserts_AllRowsVisible(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("users", "id", []string{"name"}, []string{"TEXT"})

	schema := c.sc.FindTableSchema("users")
	splitKey := sqllayer.EncodeKey(schema.TableId, 50)
	c.splitAtKey(splitKey, 1, 1)

	const N = 100
	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := c.gw.Execute(&sqllayer.InsertStatement{
				Table:  "users",
				Values: []sqllayer.Literal{intLitS(strconv.Itoa(id)), strLitS("x")},
			})
			if err != nil {
				t.Errorf("concurrent INSERT id=%d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	rs := c.selectRows("users", []string{"id"}, nil)
	if len(rs.Rows) != N {
		t.Errorf("concurrent inserts: expected %d rows, got %d", N, len(rs.Rows))
	}
}
