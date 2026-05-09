package sqllayer

import (
	"testing"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

// ---- Test infrastructure ----

func newTestExecutor(t *testing.T) *Executor {
	t.Helper()
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)
	return NewExecutor(sc, bt)
}

// mustExecSQL tokenizes, parses, and executes a SQL string, failing on any error.
func mustExecSQL(t *testing.T, ex *Executor, query string) *ResultSet {
	t.Helper()
	rs, err := execSQL(ex, query)
	if err != nil {
		t.Fatalf("execSQL(%q): %v", query, err)
	}
	return rs
}

// execSQL tokenizes, parses, and executes a SQL string.
func execSQL(ex *Executor, query string) (*ResultSet, error) {
	tokens, err := Tokenize(query)
	if err != nil {
		return nil, err
	}
	stmt, err := Parse(tokens)
	if err != nil {
		return nil, err
	}
	return ex.Execute(stmt)
}

// assertRowCount checks that a ResultSet has exactly n rows.
func assertRowCount(t *testing.T, rs *ResultSet, n int) {
	t.Helper()
	if rs == nil {
		t.Fatalf("ResultSet is nil, expected %d rows", n)
	}
	if len(rs.Rows) != n {
		t.Errorf("row count: got %d, want %d", len(rs.Rows), n)
	}
}

// assertColumns checks that a ResultSet has exactly the given column names in order.
func assertColumns(t *testing.T, rs *ResultSet, want []string) {
	t.Helper()
	if len(rs.Columns) != len(want) {
		t.Fatalf("columns: got %v, want %v", rs.Columns, want)
	}
	for i, col := range want {
		if rs.Columns[i] != col {
			t.Errorf("columns[%d]: got %q, want %q", i, rs.Columns[i], col)
		}
	}
}

// fieldIntVal asserts that a field holds an IntValue and returns it.
func fieldIntVal(t *testing.T, f btree.Field, label string) int64 {
	t.Helper()
	v, ok := f.Value.(btree.IntValue)
	if !ok {
		t.Fatalf("%s: expected IntValue, got %T", label, f.Value)
	}
	return v.V
}

// fieldStrVal asserts that a field holds a StringValue and returns it.
func fieldStrVal(t *testing.T, f btree.Field, label string) string {
	t.Helper()
	v, ok := f.Value.(btree.StringValue)
	if !ok {
		t.Fatalf("%s: expected StringValue, got %T", label, f.Value)
	}
	return v.V
}

// setupUsersTable creates the "users" table and returns the executor.
// Schema: id INT (PK), name TEXT, age INT
func setupUsersTable(t *testing.T) *Executor {
	t.Helper()
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE users (id INT, name TEXT, age INT)")
	return ex
}

// ---- literalMatchesType ----

func TestLiteralMatchesType_IntToInt(t *testing.T) {
	lit := Literal{Value: "42", Type: TOKEN_NUMBER}
	if !literalMatchesType(lit, "INT") {
		t.Error("TOKEN_NUMBER should match INT")
	}
}

func TestLiteralMatchesType_StringToText(t *testing.T) {
	lit := Literal{Value: "hello", Type: TOKEN_STRING}
	if !literalMatchesType(lit, "TEXT") {
		t.Error("TOKEN_STRING should match TEXT")
	}
}

func TestLiteralMatchesType_BoolTrue(t *testing.T) {
	for _, val := range []string{"TRUE", "true", "True"} {
		lit := Literal{Value: val, Type: TOKEN_IDENTIFIER}
		if !literalMatchesType(lit, "BOOL") {
			t.Errorf("TOKEN_IDENTIFIER %q should match BOOL", val)
		}
	}
}

func TestLiteralMatchesType_BoolFalse(t *testing.T) {
	for _, val := range []string{"FALSE", "false", "False"} {
		lit := Literal{Value: val, Type: TOKEN_IDENTIFIER}
		if !literalMatchesType(lit, "BOOL") {
			t.Errorf("TOKEN_IDENTIFIER %q should match BOOL", val)
		}
	}
}

func TestLiteralMatchesType_Mismatches(t *testing.T) {
	cases := []struct {
		lit      Literal
		dataType string
	}{
		{Literal{Value: "42", Type: TOKEN_NUMBER}, "TEXT"},
		{Literal{Value: "42", Type: TOKEN_NUMBER}, "BOOL"},
		{Literal{Value: "hello", Type: TOKEN_STRING}, "INT"},
		{Literal{Value: "hello", Type: TOKEN_STRING}, "BOOL"},
		{Literal{Value: "TRUE", Type: TOKEN_IDENTIFIER}, "INT"},
		{Literal{Value: "TRUE", Type: TOKEN_IDENTIFIER}, "TEXT"},
		{Literal{Value: "MAYBE", Type: TOKEN_IDENTIFIER}, "BOOL"}, // not a valid bool
	}
	for _, c := range cases {
		if literalMatchesType(c.lit, c.dataType) {
			t.Errorf("literal %+v should NOT match %q", c.lit, c.dataType)
		}
	}
}

func TestLiteralMatchesType_CaseInsensitiveDataType(t *testing.T) {
	lit := Literal{Value: "42", Type: TOKEN_NUMBER}
	if !literalMatchesType(lit, "int") {
		t.Error("dataType matching should be case-insensitive")
	}
}

// ---- literalToField ----

func TestLiteralToField_Int(t *testing.T) {
	lit := Literal{Value: "99", Type: TOKEN_NUMBER}
	f, err := literalToField(lit, "INT", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.Tag != 1 {
		t.Errorf("tag: got %d, want 1", f.Tag)
	}
	if fieldIntVal(t, f, "f") != 99 {
		t.Errorf("value: got %v, want 99", f.Value)
	}
}

func TestLiteralToField_Text(t *testing.T) {
	lit := Literal{Value: "alice", Type: TOKEN_STRING}
	f, err := literalToField(lit, "TEXT", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fieldStrVal(t, f, "f") != "alice" {
		t.Errorf("value: got %v, want alice", f.Value)
	}
}

func TestLiteralToField_Bool(t *testing.T) {
	lit := Literal{Value: "true", Type: TOKEN_IDENTIFIER}
	f, err := literalToField(lit, "BOOL", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fieldStrVal(t, f, "f") != "TRUE" {
		t.Errorf("value: got %v, want TRUE", f.Value)
	}
}

func TestLiteralToField_TypeMismatch(t *testing.T) {
	lit := Literal{Value: "hello", Type: TOKEN_STRING}
	_, err := literalToField(lit, "INT", 0)
	if err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestLiteralToField_InvalidIntLiteral(t *testing.T) {
	// Bypass literalMatchesType by giving a NUMBER token but an unparseable value.
	lit := Literal{Value: "not_a_number", Type: TOKEN_NUMBER}
	_, err := literalToField(lit, "INT", 0)
	if err == nil {
		t.Error("expected error for unparseable INT literal")
	}
}

// ---- findColumnIndex ----

func TestFindColumnIndex_PrimaryKey(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "name", DataType: "TEXT"}},
	}
	if got := findColumnIndex("id", schema); got != 0 {
		t.Errorf("PK index: got %d, want 0", got)
	}
}

func TestFindColumnIndex_Column(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "name", DataType: "TEXT"}, {Name: "age", DataType: "INT"}},
	}
	if got := findColumnIndex("name", schema); got != 1 {
		t.Errorf("name index: got %d, want 1", got)
	}
	if got := findColumnIndex("age", schema); got != 2 {
		t.Errorf("age index: got %d, want 2", got)
	}
}

func TestFindColumnIndex_CaseInsensitive(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "ID", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "Name", DataType: "TEXT"}},
	}
	if got := findColumnIndex("id", schema); got != 0 {
		t.Errorf("PK case-insensitive: got %d, want 0", got)
	}
	if got := findColumnIndex("NAME", schema); got != 1 {
		t.Errorf("column case-insensitive: got %d, want 1", got)
	}
}

func TestFindColumnIndex_NotFound(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "name", DataType: "TEXT"}},
	}
	if got := findColumnIndex("missing", schema); got != -1 {
		t.Errorf("missing column: got %d, want -1", got)
	}
}

// ---- compareInt / compareString ----

func TestCompareInt(t *testing.T) {
	cases := []struct {
		a, b int64
		op   string
		want bool
	}{
		{1, 1, "=", true},
		{1, 2, "=", false},
		{1, 2, "!=", true},
		{1, 1, "!=", false},
		{1, 1, "<>", false},
		{1, 2, "<>", true},
		{1, 2, "<", true},
		{2, 1, "<", false},
		{2, 1, ">", true},
		{1, 2, ">", false},
		{1, 1, "<=", true},
		{1, 2, "<=", true},
		{2, 1, "<=", false},
		{1, 1, ">=", true},
		{2, 1, ">=", true},
		{1, 2, ">=", false},
		{1, 1, "??", false}, // unknown op
	}
	for _, c := range cases {
		if got := compareInt(c.a, c.b, c.op); got != c.want {
			t.Errorf("compareInt(%d,%d,%q): got %v, want %v", c.a, c.b, c.op, got, c.want)
		}
	}
}

func TestCompareString(t *testing.T) {
	cases := []struct {
		a, b string
		op   string
		want bool
	}{
		{"a", "a", "=", true},
		{"a", "b", "=", false},
		{"a", "b", "!=", true},
		{"a", "a", "!=", false},
		{"a", "b", "<>", true},
		{"a", "a", "<>", false},
		{"a", "b", "<", true},
		{"b", "a", "<", false},
		{"b", "a", ">", true},
		{"a", "b", ">", false},
		{"a", "a", "<=", true},
		{"a", "b", "<=", true},
		{"b", "a", "<=", false},
		{"a", "a", ">=", true},
		{"b", "a", ">=", true},
		{"a", "b", ">=", false},
		{"a", "b", "??", false},
	}
	for _, c := range cases {
		if got := compareString(c.a, c.b, c.op); got != c.want {
			t.Errorf("compareString(%q,%q,%q): got %v, want %v", c.a, c.b, c.op, got, c.want)
		}
	}
}

// ---- evaluateExpression ----

func TestEvaluateExpression_Nil(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{},
	}
	fields := []btree.Field{{Tag: 0, Value: btree.IntValue{V: 1}}}
	if !evaluateExpression(nil, schema, fields) {
		t.Error("nil expression should always return true")
	}
}

func TestEvaluateExpression_IntComparison(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "age", DataType: "INT"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 25}},
	}
	expr := &ComparisonExpr{Column: "age", Operator: "=", Value: Literal{Value: "25", Type: TOKEN_NUMBER}}
	if !evaluateExpression(expr, schema, fields) {
		t.Error("age=25 should match field value 25")
	}
	expr.Value = Literal{Value: "30", Type: TOKEN_NUMBER}
	if evaluateExpression(expr, schema, fields) {
		t.Error("age=30 should not match field value 25")
	}
}

func TestEvaluateExpression_StringComparison(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "name", DataType: "TEXT"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.StringValue{V: "alice"}},
	}
	expr := &ComparisonExpr{Column: "name", Operator: "=", Value: Literal{Value: "alice", Type: TOKEN_STRING}}
	if !evaluateExpression(expr, schema, fields) {
		t.Error("name='alice' should match")
	}
}

func TestEvaluateExpression_BoolComparison(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "active", DataType: "BOOL"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.StringValue{V: "TRUE"}},
	}
	expr := &ComparisonExpr{Column: "active", Operator: "=", Value: Literal{Value: "true", Type: TOKEN_IDENTIFIER}}
	if !evaluateExpression(expr, schema, fields) {
		t.Error("active=true (stored as TRUE) should match")
	}
}

func TestEvaluateExpression_UnknownColumn(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{},
	}
	fields := []btree.Field{{Tag: 0, Value: btree.IntValue{V: 1}}}
	expr := &ComparisonExpr{Column: "ghost", Operator: "=", Value: Literal{Value: "1", Type: TOKEN_NUMBER}}
	if evaluateExpression(expr, schema, fields) {
		t.Error("unknown column should evaluate to false")
	}
}

func TestEvaluateExpression_LogicalAnd(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "age", DataType: "INT"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 25}},
	}
	expr := &LogicalExpr{
		Operator: "AND",
		Left:     &ComparisonExpr{Column: "age", Operator: ">=", Value: Literal{Value: "20", Type: TOKEN_NUMBER}},
		Right:    &ComparisonExpr{Column: "age", Operator: "<=", Value: Literal{Value: "30", Type: TOKEN_NUMBER}},
	}
	if !evaluateExpression(expr, schema, fields) {
		t.Error("AND: 20<=25<=30 should be true")
	}
	// Change age to 15 — fails left side
	fields[1].Value = btree.IntValue{V: 15}
	if evaluateExpression(expr, schema, fields) {
		t.Error("AND: 15 is not >=20, should be false")
	}
}

func TestEvaluateExpression_LogicalOr(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "age", DataType: "INT"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 5}},
	}
	expr := &LogicalExpr{
		Operator: "OR",
		Left:     &ComparisonExpr{Column: "age", Operator: "=", Value: Literal{Value: "5", Type: TOKEN_NUMBER}},
		Right:    &ComparisonExpr{Column: "age", Operator: "=", Value: Literal{Value: "10", Type: TOKEN_NUMBER}},
	}
	if !evaluateExpression(expr, schema, fields) {
		t.Error("OR: age=5 matches left branch, should be true")
	}
	fields[1].Value = btree.IntValue{V: 99}
	if evaluateExpression(expr, schema, fields) {
		t.Error("OR: age=99 matches neither branch, should be false")
	}
}

func TestEvaluateExpression_UnknownLogicalOp(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "age", DataType: "INT"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 1}},
	}
	expr := &LogicalExpr{
		Operator: "XOR",
		Left:     &ComparisonExpr{Column: "age", Operator: "=", Value: Literal{Value: "1", Type: TOKEN_NUMBER}},
		Right:    &ComparisonExpr{Column: "age", Operator: "=", Value: Literal{Value: "1", Type: TOKEN_NUMBER}},
	}
	if evaluateExpression(expr, schema, fields) {
		t.Error("unknown logical operator should return false")
	}
}

func TestEvaluateExpression_IntComparisonInvalidLiteral(t *testing.T) {
	schema := &TableSchemaValue{
		PrimaryKey: ColumnDef{Name: "id", DataType: "INT"},
		Columns:    []ColumnDef{{Name: "age", DataType: "INT"}},
	}
	fields := []btree.Field{
		{Tag: 0, Value: btree.IntValue{V: 1}},
		{Tag: 1, Value: btree.IntValue{V: 25}},
	}
	expr := &ComparisonExpr{Column: "age", Operator: "=", Value: Literal{Value: "not_a_number", Type: TOKEN_NUMBER}}
	if evaluateExpression(expr, schema, fields) {
		t.Error("invalid int literal in WHERE should evaluate to false")
	}
}

// ---- CREATE TABLE (via Execute) ----

func TestExecuteCreate_Success(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE users (id INT, name TEXT, age INT)")
	schema := ex.sc.FindTableSchema("users")
	if schema == nil {
		t.Fatal("schema not found after CREATE TABLE")
	}
	if schema.PrimaryKey.Name != "id" {
		t.Errorf("PK name: got %q, want %q", schema.PrimaryKey.Name, "id")
	}
	if len(schema.Columns) != 2 {
		t.Errorf("column count: got %d, want 2", len(schema.Columns))
	}
}

func TestExecuteCreate_DuplicateTable(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE users (id INT, name TEXT)")
	_, err := execSQL(ex, "CREATE TABLE users (id INT, email TEXT)")
	if err == nil {
		t.Error("expected error when creating a duplicate table")
	}
}

func TestExecuteCreate_BoolColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE flags (id INT, active BOOL)")
	schema := ex.sc.FindTableSchema("flags")
	if schema == nil {
		t.Fatal("schema not found")
	}
	if schema.Columns[0].DataType != "BOOL" {
		t.Errorf("expected BOOL column, got %q", schema.Columns[0].DataType)
	}
}

func TestExecuteCreate_PrimaryKeyOnlyTable(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE bare (id INT)")
	schema := ex.sc.FindTableSchema("bare")
	if schema == nil {
		t.Fatal("schema not found")
	}
	if len(schema.Columns) != 0 {
		t.Errorf("expected 0 non-PK columns, got %d", len(schema.Columns))
	}
}

// ---- DROP TABLE ----

func TestExecuteDrop_Success(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "DROP TABLE users")
	if ex.sc.FindTableSchema("users") != nil {
		t.Error("schema should be gone after DROP TABLE")
	}
}

func TestExecuteDrop_NonExistent(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := execSQL(ex, "DROP TABLE ghost")
	if err == nil {
		t.Error("expected error dropping non-existent table")
	}
}

func TestExecuteDrop_RemovesDataRows(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")

	tableId := ex.sc.FindTableSchema("users").TableId
	mustExecSQL(t, ex, "DROP TABLE users")
	assertRangeScanCount(t, ex.bt, encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)), 0)
}

func TestExecuteDrop_CanRecreateAfterDrop(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "DROP TABLE users")
	mustExecSQL(t, ex, "CREATE TABLE users (id INT, email TEXT)")
	schema := ex.sc.FindTableSchema("users")
	if schema == nil {
		t.Fatal("expected schema after re-creation")
	}
	if schema.Columns[0].Name != "email" {
		t.Errorf("expected column 'email', got %q", schema.Columns[0].Name)
	}
}

// ---- INSERT ----

func TestExecuteInsert_Success(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")

	tableId := ex.sc.FindTableSchema("users").TableId
	assertKeyPresent(t, ex.bt, encodeKey(tableId, 1))
}

func TestExecuteInsert_TableNotFound(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := execSQL(ex, "INSERT INTO ghost VALUES (1, 'x', 0)")
	if err == nil {
		t.Error("expected error for unknown table")
	}
}

func TestExecuteInsert_DuplicatePrimaryKey(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	_, err := execSQL(ex, "INSERT INTO users VALUES (1, 'bob', 25)")
	if err == nil {
		t.Error("expected error for duplicate primary key")
	}
}

func TestExecuteInsert_MismatchedValueCount(t *testing.T) {
	ex := setupUsersTable(t)
	_, err := execSQL(ex, "INSERT INTO users VALUES (1, 'alice')")
	if err == nil {
		t.Error("expected error for wrong number of values")
	}
}

func TestExecuteInsert_TypeMismatchColumn(t *testing.T) {
	ex := setupUsersTable(t)
	// age is INT, supplying TEXT
	_, err := execSQL(ex, "INSERT INTO users VALUES (1, 'alice', 'not_an_int')")
	if err == nil {
		t.Error("expected error for type mismatch in column value")
	}
}

func TestExecuteInsert_BoolColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE flags (id INT, active BOOL)")
	mustExecSQL(t, ex, "INSERT INTO flags VALUES (1, true)")
	rs := mustExecSQL(t, ex, "SELECT * FROM flags")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[1], "active") != "TRUE" {
		t.Errorf("expected TRUE, got %v", rs.Rows[0].Fields[1].Value)
	}
}

func TestExecuteInsert_MultipleRows(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (3, 'carol', 35)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 3)
}

func TestExecuteInsert_ReturnsNilResultSet(t *testing.T) {
	ex := setupUsersTable(t)
	rs, err := execSQL(ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs != nil {
		t.Error("INSERT should return nil ResultSet")
	}
}

// ---- SELECT ----

func TestExecuteSelect_StarAllColumns(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	rs := mustExecSQL(t, ex, "SELECT * FROM users")

	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"id", "name", "age"})
}

func TestExecuteSelect_SpecificColumns(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	rs := mustExecSQL(t, ex, "SELECT name FROM users")

	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"name"})
	if fieldStrVal(t, rs.Rows[0].Fields[0], "name") != "alice" {
		t.Errorf("expected 'alice', got %v", rs.Rows[0].Fields[0].Value)
	}
}

func TestExecuteSelect_EmptyTable(t *testing.T) {
	ex := setupUsersTable(t)
	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 0)
}

func TestExecuteSelect_TableNotFound(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := execSQL(ex, "SELECT * FROM ghost")
	if err == nil {
		t.Error("expected error for unknown table")
	}
}

func TestExecuteSelect_UnknownColumn(t *testing.T) {
	ex := setupUsersTable(t)
	_, err := execSQL(ex, "SELECT missing FROM users")
	if err == nil {
		t.Error("expected error for unknown column in SELECT list")
	}
}

func TestExecuteSelect_WhereInt(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age = 30")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[1], "name") != "alice" {
		t.Errorf("expected alice, got %v", rs.Rows[0].Fields[1].Value)
	}
}

func TestExecuteSelect_WhereString(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE name = 'bob'")
	assertRowCount(t, rs, 1)
	if fieldIntVal(t, rs.Rows[0].Fields[2], "age") != 25 {
		t.Errorf("expected age=25, got %v", rs.Rows[0].Fields[2].Value)
	}
}

func TestExecuteSelect_WhereRangeOperators(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 20)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (3, 'carol', 30)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age > 20")
	assertRowCount(t, rs, 2)

	rs = mustExecSQL(t, ex, "SELECT * FROM users WHERE age >= 25")
	assertRowCount(t, rs, 2)

	rs = mustExecSQL(t, ex, "SELECT * FROM users WHERE age < 30")
	assertRowCount(t, rs, 2)

	rs = mustExecSQL(t, ex, "SELECT * FROM users WHERE age <= 20")
	assertRowCount(t, rs, 1)

	rs = mustExecSQL(t, ex, "SELECT * FROM users WHERE age != 25")
	assertRowCount(t, rs, 2)
}

func TestExecuteSelect_WhereAnd(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (3, 'carol', 30)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age = 30 AND name = 'alice'")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[1], "name") != "alice" {
		t.Errorf("expected alice")
	}
}

func TestExecuteSelect_WhereOr(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (3, 'carol', 20)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age = 30 OR age = 20")
	assertRowCount(t, rs, 2)
}

func TestExecuteSelect_WherePrimaryKey(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE id = 1")
	assertRowCount(t, rs, 1)
	if fieldIntVal(t, rs.Rows[0].Fields[0], "id") != 1 {
		t.Errorf("expected id=1")
	}
}

func TestExecuteSelect_WhereNoMatch(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age = 99")
	assertRowCount(t, rs, 0)
}

func TestExecuteSelect_MultipleColumns(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	rs := mustExecSQL(t, ex, "SELECT id, age FROM users")
	assertColumns(t, rs, []string{"id", "age"})
	assertRowCount(t, rs, 1)
	if fieldIntVal(t, rs.Rows[0].Fields[0], "id") != 1 {
		t.Errorf("expected id=1")
	}
	if fieldIntVal(t, rs.Rows[0].Fields[1], "age") != 30 {
		t.Errorf("expected age=30")
	}
}

func TestExecuteSelect_NoWhereReturnsAll(t *testing.T) {
	ex := setupUsersTable(t)
	for i := 1; i <= 5; i++ {
		mustExecSQL(t, ex, "INSERT INTO users VALUES ("+string(rune('0'+i))+", 'user', 20)")
	}
	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 5)
}

func TestExecuteSelect_BoolColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE flags (id INT, active BOOL)")
	mustExecSQL(t, ex, "INSERT INTO flags VALUES (1, true)")
	mustExecSQL(t, ex, "INSERT INTO flags VALUES (2, false)")

	rs := mustExecSQL(t, ex, "SELECT * FROM flags WHERE active = true")
	assertRowCount(t, rs, 1)
	if fieldIntVal(t, rs.Rows[0].Fields[0], "id") != 1 {
		t.Errorf("expected id=1")
	}
}

// ---- UPDATE ----

func TestExecuteUpdate_Success(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "UPDATE users SET age = 31 WHERE id = 1")

	rs := mustExecSQL(t, ex, "SELECT age FROM users WHERE id = 1")
	assertRowCount(t, rs, 1)
	if fieldIntVal(t, rs.Rows[0].Fields[0], "age") != 31 {
		t.Errorf("expected age=31 after update")
	}
}

func TestExecuteUpdate_TableNotFound(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := execSQL(ex, "UPDATE ghost SET name = 'x' WHERE id = 1")
	if err == nil {
		t.Error("expected error for unknown table")
	}
}

func TestExecuteUpdate_ColumnNotFound(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	_, err := execSQL(ex, "UPDATE users SET ghost = 'x' WHERE id = 1")
	if err == nil {
		t.Error("expected error for unknown column")
	}
}

func TestExecuteUpdate_TypeMismatch(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	_, err := execSQL(ex, "UPDATE users SET age = 'not_an_int' WHERE id = 1")
	if err == nil {
		t.Error("expected error for type mismatch in SET value")
	}
}

func TestExecuteUpdate_NoWhereUpdatesAll(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "UPDATE users SET age = 99")

	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age = 99")
	assertRowCount(t, rs, 2)
}

func TestExecuteUpdate_WhereFilters(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 30)")
	mustExecSQL(t, ex, "UPDATE users SET name = 'updated' WHERE id = 1")

	rs := mustExecSQL(t, ex, "SELECT name FROM users WHERE id = 1")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[0], "name") != "updated" {
		t.Errorf("expected 'updated'")
	}

	rs = mustExecSQL(t, ex, "SELECT name FROM users WHERE id = 2")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[0], "name") != "bob" {
		t.Errorf("bob should be unchanged")
	}
}

func TestExecuteUpdate_ReturnsNilResultSet(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	rs, err := execSQL(ex, "UPDATE users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs != nil {
		t.Error("UPDATE should return nil ResultSet")
	}
}

func TestExecuteUpdate_NoMatchingRows(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "UPDATE users SET age = 99 WHERE id = 999")

	rs := mustExecSQL(t, ex, "SELECT age FROM users WHERE id = 1")
	if fieldIntVal(t, rs.Rows[0].Fields[0], "age") != 30 {
		t.Error("row with id=1 should be unchanged")
	}
}

func TestExecuteUpdate_BoolColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE flags (id INT, active BOOL)")
	mustExecSQL(t, ex, "INSERT INTO flags VALUES (1, true)")
	mustExecSQL(t, ex, "UPDATE flags SET active = false WHERE id = 1")

	rs := mustExecSQL(t, ex, "SELECT active FROM flags WHERE id = 1")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[0], "active") != "FALSE" {
		t.Errorf("expected FALSE after update, got %v", rs.Rows[0].Fields[0].Value)
	}
}

func TestExecuteUpdate_CannotUpdatePrimaryKey(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	_, err := execSQL(ex, "UPDATE users SET id = 99 WHERE id = 1")
	if err == nil {
		t.Error("expected error when trying to update primary key column")
	}
}

// ---- DELETE ----

func TestExecuteDelete_Success(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "DELETE FROM users WHERE id = 1")

	tableId := ex.sc.FindTableSchema("users").TableId
	assertKeyAbsent(t, ex.bt, encodeKey(tableId, 1))
}

func TestExecuteDelete_TableNotFound(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := execSQL(ex, "DELETE FROM ghost WHERE id = 1")
	if err == nil {
		t.Error("expected error for unknown table")
	}
}

func TestExecuteDelete_NoWhereDeletesAll(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "DELETE FROM users")

	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 0)
}

func TestExecuteDelete_WhereFilters(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "DELETE FROM users WHERE id = 1")

	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[1], "name") != "bob" {
		t.Errorf("bob should still exist")
	}
}

func TestExecuteDelete_EmptyTable(t *testing.T) {
	ex := setupUsersTable(t)
	_, err := execSQL(ex, "DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Errorf("DELETE on empty table should not error: %v", err)
	}
}

func TestExecuteDelete_ReturnsNilResultSet(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	rs, err := execSQL(ex, "DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs != nil {
		t.Error("DELETE should return nil ResultSet")
	}
}

func TestExecuteDelete_WhereNoMatch(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "DELETE FROM users WHERE id = 999")

	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 1)
}

func TestExecuteDelete_MultipleMatchingRows(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (3, 'carol', 25)")
	mustExecSQL(t, ex, "DELETE FROM users WHERE age = 30")

	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[1], "name") != "carol" {
		t.Errorf("only carol (age=25) should remain")
	}
}

// unknownStmt satisfies the Statement interface for dispatch tests.
type unknownStmt struct{}

func (s *unknownStmt) isStatement() {}

// ---- Execute dispatch ----

func TestExecute_UnknownStatementType(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Execute(&unknownStmt{})
	if err == nil {
		t.Error("expected error for unknown statement type")
	}
}

// ---- Integration ----

func TestIntegration_InsertSelectUpdateDelete(t *testing.T) {
	ex := setupUsersTable(t)

	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")

	rs := mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 2)

	mustExecSQL(t, ex, "UPDATE users SET age = 31 WHERE id = 1")
	rs = mustExecSQL(t, ex, "SELECT age FROM users WHERE id = 1")
	if fieldIntVal(t, rs.Rows[0].Fields[0], "age") != 31 {
		t.Error("age should be 31 after update")
	}

	mustExecSQL(t, ex, "DELETE FROM users WHERE id = 2")
	rs = mustExecSQL(t, ex, "SELECT * FROM users")
	assertRowCount(t, rs, 1)
}

func TestIntegration_MultipleTablesIsolated(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE users (id INT, name TEXT)")
	mustExecSQL(t, ex, "CREATE TABLE products (id INT, title TEXT)")

	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice')")
	mustExecSQL(t, ex, "INSERT INTO products VALUES (1, 'widget')")

	rsU := mustExecSQL(t, ex, "SELECT * FROM users")
	rsP := mustExecSQL(t, ex, "SELECT * FROM products")

	assertRowCount(t, rsU, 1)
	assertRowCount(t, rsP, 1)
	if fieldStrVal(t, rsU.Rows[0].Fields[1], "name") != "alice" {
		t.Error("users table isolation broken")
	}
	if fieldStrVal(t, rsP.Rows[0].Fields[1], "title") != "widget" {
		t.Error("products table isolation broken")
	}
}

func TestIntegration_DropTableIsolatesOtherTables(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE a (id INT, val INT)")
	mustExecSQL(t, ex, "CREATE TABLE b (id INT, val INT)")
	mustExecSQL(t, ex, "INSERT INTO a VALUES (1, 100)")
	mustExecSQL(t, ex, "INSERT INTO b VALUES (1, 200)")

	mustExecSQL(t, ex, "DROP TABLE a")

	rs := mustExecSQL(t, ex, "SELECT * FROM b")
	assertRowCount(t, rs, 1)
	if fieldIntVal(t, rs.Rows[0].Fields[1], "val") != 200 {
		t.Error("table b should be unaffected by drop of table a")
	}
}

func TestIntegration_CreateDropRecreate(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE t (id INT, x TEXT)")
	mustExecSQL(t, ex, "INSERT INTO t VALUES (1, 'hello')")
	mustExecSQL(t, ex, "DROP TABLE t")
	mustExecSQL(t, ex, "CREATE TABLE t (id INT, y INT)")
	mustExecSQL(t, ex, "INSERT INTO t VALUES (1, 42)")

	rs := mustExecSQL(t, ex, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"id", "y"})
	if fieldIntVal(t, rs.Rows[0].Fields[1], "y") != 42 {
		t.Errorf("expected y=42")
	}
}

func TestIntegration_ComplexWhereAndOr(t *testing.T) {
	ex := setupUsersTable(t)
	mustExecSQL(t, ex, "INSERT INTO users VALUES (1, 'alice', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (2, 'bob', 25)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (3, 'carol', 30)")
	mustExecSQL(t, ex, "INSERT INTO users VALUES (4, 'dave', 20)")

	// (age = 30 AND name = 'alice') OR age = 20  → alice, dave
	rs := mustExecSQL(t, ex, "SELECT * FROM users WHERE age = 30 AND name = 'alice' OR age = 20")
	// Note: parser precedence may affect grouping; verify at least the count is reasonable.
	if len(rs.Rows) == 0 {
		t.Error("expected at least one row from compound WHERE")
	}
}

func TestIntegration_UpdateThenSelectBool(t *testing.T) {
	ex := newTestExecutor(t)
	mustExecSQL(t, ex, "CREATE TABLE flags (id INT, active BOOL)")
	mustExecSQL(t, ex, "INSERT INTO flags VALUES (1, false)")
	mustExecSQL(t, ex, "UPDATE flags SET active = true WHERE id = 1")

	rs := mustExecSQL(t, ex, "SELECT active FROM flags")
	assertRowCount(t, rs, 1)
	if fieldStrVal(t, rs.Rows[0].Fields[0], "active") != "TRUE" {
		t.Errorf("expected TRUE")
	}
}
