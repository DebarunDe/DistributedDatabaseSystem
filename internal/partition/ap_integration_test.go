package partition

// AP-mode integration tests using the simCluster / simRangeServer infrastructure
// (same package as integration_test.go so we share all helpers).
//
// Coverage:
//   Schema    — consistency mode fields, BuildAlterConsistencyCommand, gateway ALTER TABLE
//   APRouting — AP INSERT/SELECT routed via any-replica on single-node cluster
//   Override  — SELECT WITH CONSISTENCY EVENTUAL|STRONG overrides
//   No2PC     — multi-range AP UPDATE/DELETE bypass Prepare; CP UPDATE fails when
//               Prepare is rigged to return Success=false
//   Mixed     — CP and AP tables coexist in the same cluster
//   Parser    — lexer + parser round-trips for new SQL syntax

import (
	"strconv"
	"testing"

	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
)

// forceAPMode sets schema.Consistency = ConsistencyAP for tableName directly
// in the shared SchemaCatalog cache.  Because FindTableSchema returns a pointer
// to the cached struct, the gateway (which holds the same *SchemaCatalog) will
// immediately see the new mode without a LoadSchemas round-trip.
func forceAPMode(t *testing.T, c *simCluster, tableName string) {
	t.Helper()
	schema := c.sc.FindTableSchema(tableName)
	if schema == nil {
		t.Fatalf("forceAPMode: table %q not found in schema catalog", tableName)
	}
	schema.Consistency = sqllayer.ConsistencyAP
}

// =============================================================================
// Schema tests
// =============================================================================

func TestSimAP_Schema_NewTable_DefaultsToCP(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("tbl", "id", []string{"v"}, []string{"INT"})
	schema := c.sc.FindTableSchema("tbl")
	if schema.Consistency != sqllayer.ConsistencyCP {
		t.Fatalf("new table: want ConsistencyCP, got %v", schema.Consistency)
	}
}

func TestSimAP_Schema_BuildAlterConsistencyCommand_ReturnsValidFields(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("tbl", "id", []string{"v"}, []string{"INT"})
	key, fields, err := c.sc.BuildAlterConsistencyCommand("tbl", sqllayer.ConsistencyAP)
	if err != nil {
		t.Fatalf("BuildAlterConsistencyCommand: %v", err)
	}
	if key == 0 {
		t.Fatal("expected non-zero schema key")
	}
	if len(fields) < 6 {
		t.Fatalf("expected at least 6 schema fields, got %d", len(fields))
	}
}

func TestSimAP_Schema_BuildAlterConsistencyCommand_UnknownTable(t *testing.T) {
	c := newSimCluster(t)
	_, _, err := c.sc.BuildAlterConsistencyCommand("nonexistent", sqllayer.ConsistencyAP)
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestSimAP_Schema_ForceAPMode_GatewaySeesNewMode(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("t", "id", []string{"v"}, []string{"INT"})

	if got := c.sc.FindTableSchema("t").Consistency; got != sqllayer.ConsistencyCP {
		t.Fatalf("before: want CP, got %v", got)
	}
	forceAPMode(t, c, "t")
	if got := c.sc.FindTableSchema("t").Consistency; got != sqllayer.ConsistencyAP {
		t.Fatalf("after forceAPMode: want AP, got %v", got)
	}
}

func TestSimAP_Schema_ExecuteAlterConsistency_NoError(t *testing.T) {
	// executeAlterConsistency routes an INSERT to the schema range via the leader.
	// In the simCluster the simRangeServer accepts any INSERT, so we just verify
	// no error is returned.
	c := newSimCluster(t)
	c.addTable("orders", "id", []string{"amount"}, []string{"INT"})

	_, err := c.gw.Execute(&sqllayer.AlterConsistencyStatement{
		Table: "orders",
		Mode:  sqllayer.ConsistencyAP,
	})
	if err != nil {
		t.Fatalf("ALTER TABLE orders SET CONSISTENCY EVENTUAL: %v", err)
	}
}

func TestSimAP_Schema_ExecuteAlterConsistency_UnknownTable_Error(t *testing.T) {
	c := newSimCluster(t)
	_, err := c.gw.Execute(&sqllayer.AlterConsistencyStatement{
		Table: "ghost",
		Mode:  sqllayer.ConsistencyAP,
	})
	if err == nil {
		t.Fatal("expected error for ALTER TABLE on unknown table")
	}
}

// =============================================================================
// AP routing — single-node cluster (any replica == leader == node 1)
// =============================================================================

func TestSimAP_Insert_SingleNode_ImmediatelyReadable(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("ap_tbl", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "ap_tbl")

	c.insert("ap_tbl", []sqllayer.Literal{intLitS("7"), intLitS("700")})

	rs := c.selectRows("ap_tbl", []string{"id"}, nil)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
}

func TestSimAP_Insert_MultipleRows_AllReadBack(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("ap_tbl", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "ap_tbl")

	for i := 1; i <= 5; i++ {
		c.insert("ap_tbl", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i * 10))})
	}

	rs := c.selectRows("ap_tbl", []string{"id"}, nil)
	if len(rs.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rs.Rows))
	}
}

func TestSimAP_Select_WithWhereFilter(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("ap_tbl", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "ap_tbl")

	for i := 1; i <= 10; i++ {
		c.insert("ap_tbl", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
	}

	rs := c.selectRows("ap_tbl", []string{"id"}, eqWhere("id", "5"))
	if len(rs.Rows) != 1 {
		t.Fatalf("SELECT id=5: expected 1 row, got %d", len(rs.Rows))
	}
	if pkVal(rs.Rows[0].Key) != 5 {
		t.Fatalf("wrong key: got %d, want 5", pkVal(rs.Rows[0].Key))
	}
}

func TestSimAP_Select_RangeWhere(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("t", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "t")

	for i := 1; i <= 8; i++ {
		c.insert("t", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
	}

	rs := c.selectRows("t", []string{"id"}, rangeWhere("id", "3", "6"))
	if len(rs.Rows) != 4 {
		t.Fatalf("range [3,6]: expected 4 rows, got %d", len(rs.Rows))
	}
}

// =============================================================================
// Consistency override on SELECT
// =============================================================================

func TestSimAP_Select_Override_Eventual_OnCPTable(t *testing.T) {
	// A SELECT WITH CONSISTENCY EVENTUAL on a CP table should succeed;
	// in a single-node cluster any-replica == leader, so results are correct.
	c := newSimCluster(t)
	c.addTable("cp", "id", []string{"v"}, []string{"INT"})
	c.insert("cp", []sqllayer.Literal{intLitS("1"), intLitS("11")})

	mode := sqllayer.ConsistencyAP
	rs, err := c.gw.Execute(&sqllayer.SelectStatement{
		Table:               "cp",
		Columns:             []string{"*"},
		ConsistencyOverride: &mode,
	})
	if err != nil {
		t.Fatalf("SELECT WITH CONSISTENCY EVENTUAL: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
}

func TestSimAP_Select_Override_Strong_OnAPTable(t *testing.T) {
	// A SELECT WITH CONSISTENCY STRONG on an AP table should force CP routing;
	// in single-node cluster this still returns correct data.
	c := newSimCluster(t)
	c.addTable("ap", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "ap")
	c.insert("ap", []sqllayer.Literal{intLitS("2"), intLitS("20")})

	mode := sqllayer.ConsistencyCP
	rs, err := c.gw.Execute(&sqllayer.SelectStatement{
		Table:               "ap",
		Columns:             []string{"*"},
		ConsistencyOverride: &mode,
	})
	if err != nil {
		t.Fatalf("SELECT WITH CONSISTENCY STRONG on AP table: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
}

func TestSimAP_Select_NoOverride_IsNoop(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("t", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "t")
	c.insert("t", []sqllayer.Literal{intLitS("3"), intLitS("30")})

	rs, err := c.gw.Execute(&sqllayer.SelectStatement{
		Table:               "t",
		Columns:             []string{"*"},
		ConsistencyOverride: nil,
	})
	if err != nil {
		t.Fatalf("SELECT without override on AP table: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
}

// =============================================================================
// No-2PC: multi-range AP mutations bypass Prepare
// =============================================================================

// setupTwoRanges creates a 2-node cluster, adds tableName (with optional AP
// mode), splits the range at PK=5, inserts 8 rows, and returns the cluster.
func setupTwoRanges(t *testing.T, tableName string, apMode bool) *simCluster {
	t.Helper()
	c := newMultiNodeCluster(t, 2)
	c.addTable(tableName, "id", []string{"v"}, []string{"INT"})
	if apMode {
		forceAPMode(t, c, tableName)
	}

	schema := c.sc.FindTableSchema(tableName)
	splitKey := sqllayer.EncodeKey(schema.TableId, 5)
	c.splitAtKey(splitKey, 1, 2)

	for i := 1; i <= 8; i++ {
		c.insert(tableName, []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i * 10))})
	}
	return c
}

func TestSimAP_MultiRange_Update_Bypasses2PC(t *testing.T) {
	// With failPrepare=true on node 2, a CP multi-range UPDATE would fail.
	// An AP UPDATE must succeed because it sends Execute directly (no Prepare).
	c := setupTwoRanges(t, "ap_tbl", true)
	c.nodes[2].failPrepare = true

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "ap_tbl",
		Column: "v",
		Value:  intLitS("999"),
	})
	if err != nil {
		t.Fatalf("AP multi-range UPDATE with failPrepare on node 2: %v", err)
	}
}

func TestSimAP_MultiRange_Delete_Bypasses2PC(t *testing.T) {
	c := setupTwoRanges(t, "del_tbl", true)
	c.nodes[1].failPrepare = true
	c.nodes[2].failPrepare = true

	_, err := c.gw.Execute(&sqllayer.DeleteStatement{Table: "del_tbl"})
	if err != nil {
		t.Fatalf("AP multi-range DELETE with failPrepare on all nodes: %v", err)
	}
}

func TestSimAP_MultiRange_CPUpdate_Uses2PC_FailsWhenPrepareFails(t *testing.T) {
	// Contrast: CP table with failPrepare must fail on multi-range UPDATE.
	c := setupTwoRanges(t, "cp_tbl", false)
	c.nodes[2].failPrepare = true

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "cp_tbl",
		Column: "v",
		Value:  intLitS("999"),
	})
	if err == nil {
		t.Fatal("expected error for CP multi-range UPDATE when Prepare fails, got nil")
	}
}

func TestSimAP_MultiRange_Update_WithRangeWhere_Bypasses2PC(t *testing.T) {
	c := setupTwoRanges(t, "ap2", true)
	c.nodes[2].failPrepare = true

	_, err := c.gw.Execute(&sqllayer.UpdateStatement{
		Table:  "ap2",
		Column: "v",
		Value:  intLitS("1"),
		Where:  rangeWhere("id", "1", "8"),
	})
	if err != nil {
		t.Fatalf("AP scoped UPDATE bypasses 2PC: %v", err)
	}
}

// =============================================================================
// Mixed CP/AP tables in the same cluster
// =============================================================================

func TestSimAP_Mixed_CPAndAPTablesCoexist(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("cp_tbl", "id", []string{"val"}, []string{"INT"})
	c.addTable("ap_tbl", "id", []string{"val"}, []string{"INT"})
	forceAPMode(t, c, "ap_tbl")

	for i := 1; i <= 3; i++ {
		c.insert("cp_tbl", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
		c.insert("ap_tbl", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
	}

	cpRS := c.selectRows("cp_tbl", []string{"id"}, nil)
	apRS := c.selectRows("ap_tbl", []string{"id"}, nil)
	if len(cpRS.Rows) != 3 {
		t.Fatalf("CP table: expected 3 rows, got %d", len(cpRS.Rows))
	}
	if len(apRS.Rows) != 3 {
		t.Fatalf("AP table: expected 3 rows, got %d", len(apRS.Rows))
	}
}

func TestSimAP_Mixed_MultiRange_APSucceeds_CPFails_SameCluster(t *testing.T) {
	// Split only cp_tbl across nodes 1 and 2.  ap_tbl stays on node 1 (single
	// range) so AP routing never calls Prepare regardless.  With failPrepare=true
	// on node 2, cp_tbl UPDATE triggers 2PC → Prepare fails → error.
	// ap_tbl UPDATE uses AP path → no Prepare → succeeds.
	c := newMultiNodeCluster(t, 2)
	c.addTable("cp_tbl", "id", []string{"v"}, []string{"INT"})
	c.addTable("ap_tbl", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "ap_tbl")

	schemaCP := c.sc.FindTableSchema("cp_tbl")

	// Only split cp_tbl — ap_tbl remains on node 1.
	c.splitAtKey(sqllayer.EncodeKey(schemaCP.TableId, 5), 1, 2)

	for i := 1; i <= 8; i++ {
		c.insert("cp_tbl", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
		c.insert("ap_tbl", []sqllayer.Literal{intLitS(strconv.Itoa(i)), intLitS(strconv.Itoa(i))})
	}

	c.nodes[2].failPrepare = true

	// AP table is on node 1 only; sendToAnyReplica never calls Prepare → succeeds.
	if _, err := c.gw.Execute(&sqllayer.UpdateStatement{Table: "ap_tbl", Column: "v", Value: intLitS("0")}); err != nil {
		t.Fatalf("AP UPDATE in mixed cluster: %v", err)
	}

	// CP table spans nodes 1 and 2; 2PC calls Prepare on node 2 → fails.
	if _, err := c.gw.Execute(&sqllayer.UpdateStatement{Table: "cp_tbl", Column: "v", Value: intLitS("0")}); err == nil {
		t.Fatal("expected CP multi-range UPDATE to fail when Prepare fails, got nil error")
	}
}

func TestSimAP_Mixed_Isolation_APWriteDoesNotAffectCPTable(t *testing.T) {
	c := newSimCluster(t)
	c.addTable("cp_tbl", "id", []string{"v"}, []string{"INT"})
	c.addTable("ap_tbl", "id", []string{"v"}, []string{"INT"})
	forceAPMode(t, c, "ap_tbl")

	c.insert("ap_tbl", []sqllayer.Literal{intLitS("1"), intLitS("100")})

	cpRS := c.selectRows("cp_tbl", []string{"id"}, nil)
	if len(cpRS.Rows) != 0 {
		t.Fatalf("CP table must be empty, got %d rows", len(cpRS.Rows))
	}
}

// =============================================================================
// Parser round-trip tests (no cluster needed)
// =============================================================================

func TestSimAP_Parser_AlterConsistency_Eventual(t *testing.T) {
	tokens, err := sqllayer.Tokenize("ALTER TABLE t SET CONSISTENCY EVENTUAL")
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	alt, ok := stmt.(*sqllayer.AlterConsistencyStatement)
	if !ok {
		t.Fatalf("expected *AlterConsistencyStatement, got %T", stmt)
	}
	if alt.Table != "t" {
		t.Fatalf("table: want t, got %q", alt.Table)
	}
	if alt.Mode != sqllayer.ConsistencyAP {
		t.Fatalf("mode: want ConsistencyAP, got %v", alt.Mode)
	}
}

func TestSimAP_Parser_AlterConsistency_Strong(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("ALTER TABLE orders SET CONSISTENCY STRONG")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	alt, ok := stmt.(*sqllayer.AlterConsistencyStatement)
	if !ok {
		t.Fatalf("expected *AlterConsistencyStatement, got %T", stmt)
	}
	if alt.Table != "orders" {
		t.Fatalf("table: want orders, got %q", alt.Table)
	}
	if alt.Mode != sqllayer.ConsistencyCP {
		t.Fatalf("mode: want ConsistencyCP, got %v", alt.Mode)
	}
}

func TestSimAP_Parser_SelectWithConsistencyEventual(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("SELECT id FROM users WITH CONSISTENCY EVENTUAL")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel, ok := stmt.(*sqllayer.SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if sel.ConsistencyOverride == nil {
		t.Fatal("expected non-nil ConsistencyOverride")
	}
	if *sel.ConsistencyOverride != sqllayer.ConsistencyAP {
		t.Fatalf("override: want ConsistencyAP, got %v", *sel.ConsistencyOverride)
	}
}

func TestSimAP_Parser_SelectWithConsistencyStrong(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("SELECT * FROM t WHERE id = 1 WITH CONSISTENCY STRONG")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel, ok := stmt.(*sqllayer.SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if sel.ConsistencyOverride == nil {
		t.Fatal("expected non-nil ConsistencyOverride")
	}
	if *sel.ConsistencyOverride != sqllayer.ConsistencyCP {
		t.Fatalf("override: want ConsistencyCP, got %v", *sel.ConsistencyOverride)
	}
}

func TestSimAP_Parser_SelectNoOverride_IsNil(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("SELECT * FROM t WHERE id > 5")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel, ok := stmt.(*sqllayer.SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if sel.ConsistencyOverride != nil {
		t.Fatalf("expected nil ConsistencyOverride, got %v", *sel.ConsistencyOverride)
	}
}

func TestSimAP_Parser_AlterConsistency_UnknownMode_Error(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("ALTER TABLE t SET CONSISTENCY GARBAGE")
	_, err := sqllayer.Parse(tokens)
	if err == nil {
		t.Fatal("expected parse error for unknown consistency mode GARBAGE")
	}
}

func TestSimAP_Parser_AlterConsistency_MissingMode_Error(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("ALTER TABLE t SET CONSISTENCY")
	_, err := sqllayer.Parse(tokens)
	if err == nil {
		t.Fatal("expected parse error when consistency mode keyword is missing")
	}
}

func TestSimAP_Parser_AlterConsistency_MissingTableName_Error(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("ALTER TABLE SET CONSISTENCY EVENTUAL")
	_, err := sqllayer.Parse(tokens)
	if err == nil {
		t.Fatal("expected parse error when table name is missing")
	}
}

func TestSimAP_Parser_SelectConsistency_WithStarAndWhere(t *testing.T) {
	tokens, _ := sqllayer.Tokenize("SELECT * FROM events WHERE id >= 10 WITH CONSISTENCY EVENTUAL")
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel, ok := stmt.(*sqllayer.SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if sel.ConsistencyOverride == nil || *sel.ConsistencyOverride != sqllayer.ConsistencyAP {
		t.Fatalf("override: want ConsistencyAP, got %v", sel.ConsistencyOverride)
	}
	if sel.Where == nil {
		t.Fatal("expected non-nil Where clause")
	}
}
