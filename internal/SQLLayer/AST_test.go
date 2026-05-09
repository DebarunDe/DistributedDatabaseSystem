package sqllayer

import (
	"testing"
)

// parseSQL tokenizes and parses a SQL string, failing the test on any error.
func parseSQL(t *testing.T, query string) Statement {
	t.Helper()
	tokens, err := Tokenize(query)
	if err != nil {
		t.Fatalf("Tokenize(%q) error: %v", query, err)
	}
	stmt, err := Parse(tokens)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", query, err)
	}
	return stmt
}

// parseSQLError expects Parse to fail and returns the error.
func parseSQLError(t *testing.T, query string) error {
	t.Helper()
	tokens, err := Tokenize(query)
	if err != nil {
		return err
	}
	_, err = Parse(tokens)
	if err == nil {
		t.Fatalf("Parse(%q): expected an error but got none", query)
	}
	return err
}

// --- expression assertion helpers ---

func assertComparison(t *testing.T, expr Expression, col, op, val string, valType TokenType) {
	t.Helper()
	c, ok := expr.(*ComparisonExpr)
	if !ok {
		t.Fatalf("expected *ComparisonExpr, got %T", expr)
	}
	if c.Column != col {
		t.Errorf("Column: got %q, want %q", c.Column, col)
	}
	if c.Operator != op {
		t.Errorf("Operator: got %q, want %q", c.Operator, op)
	}
	if c.Value.Value != val {
		t.Errorf("Value.Value: got %q, want %q", c.Value.Value, val)
	}
	if c.Value.Type != valType {
		t.Errorf("Value.Type: got %v, want %v", c.Value.Type, valType)
	}
}

func assertLogical(t *testing.T, expr Expression, op string) (*LogicalExpr, Expression, Expression) {
	t.Helper()
	l, ok := expr.(*LogicalExpr)
	if !ok {
		t.Fatalf("expected *LogicalExpr, got %T", expr)
	}
	if l.Operator != op {
		t.Errorf("Operator: got %q, want %q", l.Operator, op)
	}
	return l, l.Left, l.Right
}

// --- SELECT ---

func TestParseSelectSimple(t *testing.T) {
	stmt := parseSQL(t, "SELECT name FROM users")
	s, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if s.Table != "users" {
		t.Errorf("Table: got %q, want %q", s.Table, "users")
	}
	if len(s.Columns) != 1 || s.Columns[0] != "name" {
		t.Errorf("Columns: got %v, want [name]", s.Columns)
	}
	if s.Where != nil {
		t.Errorf("Where: expected nil, got %v", s.Where)
	}
}

func TestParseSelectMultipleColumns(t *testing.T) {
	stmt := parseSQL(t, "SELECT id, name, age FROM employees")
	s := stmt.(*SelectStatement)
	want := []string{"id", "name", "age"}
	if len(s.Columns) != len(want) {
		t.Fatalf("Columns: got %v, want %v", s.Columns, want)
	}
	for i, col := range want {
		if s.Columns[i] != col {
			t.Errorf("Columns[%d]: got %q, want %q", i, s.Columns[i], col)
		}
	}
	if s.Table != "employees" {
		t.Errorf("Table: got %q, want %q", s.Table, "employees")
	}
}

func TestParseSelectWhereNumber(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM users WHERE age = 30")
	s := stmt.(*SelectStatement)
	assertComparison(t, s.Where, "age", "=", "30", TOKEN_NUMBER)
}

func TestParseSelectWhereString(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM users WHERE name = 'Alice'")
	s := stmt.(*SelectStatement)
	assertComparison(t, s.Where, "name", "=", "Alice", TOKEN_STRING)
}

func TestParseSelectWhereBoolean(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM users WHERE active = true")
	s := stmt.(*SelectStatement)
	assertComparison(t, s.Where, "active", "=", "true", TOKEN_IDENTIFIER)
}

func TestParseSelectWhereAllOperators(t *testing.T) {
	tests := []struct {
		query string
		op    string
	}{
		{"SELECT id FROM t WHERE x = 1", "="},
		{"SELECT id FROM t WHERE x != 1", "!="},
		{"SELECT id FROM t WHERE x < 1", "<"},
		{"SELECT id FROM t WHERE x > 1", ">"},
		{"SELECT id FROM t WHERE x <= 1", "<="},
		{"SELECT id FROM t WHERE x >= 1", ">="},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			stmt := parseSQL(t, tt.query)
			s := stmt.(*SelectStatement)
			assertComparison(t, s.Where, "x", tt.op, "1", TOKEN_NUMBER)
		})
	}
}

func TestParseSelectWhereAnd(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM users WHERE age > 18 AND age < 65")
	s := stmt.(*SelectStatement)
	_, left, right := assertLogical(t, s.Where, "AND")
	assertComparison(t, left, "age", ">", "18", TOKEN_NUMBER)
	assertComparison(t, right, "age", "<", "65", TOKEN_NUMBER)
}

func TestParseSelectWhereOr(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM users WHERE name = 'Alice' OR name = 'Bob'")
	s := stmt.(*SelectStatement)
	_, left, right := assertLogical(t, s.Where, "OR")
	assertComparison(t, left, "name", "=", "Alice", TOKEN_STRING)
	assertComparison(t, right, "name", "=", "Bob", TOKEN_STRING)
}

// AND has higher precedence than OR, so "a=1 OR b=2 AND c=3" → OR(a=1, AND(b=2,c=3))
func TestParseSelectWhereAndOrPrecedence(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM t WHERE a = 1 OR b = 2 AND c = 3")
	s := stmt.(*SelectStatement)
	_, orLeft, orRight := assertLogical(t, s.Where, "OR")
	assertComparison(t, orLeft, "a", "=", "1", TOKEN_NUMBER)
	_, andLeft, andRight := assertLogical(t, orRight, "AND")
	assertComparison(t, andLeft, "b", "=", "2", TOKEN_NUMBER)
	assertComparison(t, andRight, "c", "=", "3", TOKEN_NUMBER)
}

// Chained AND is left-associative: "a=1 AND b=2 AND c=3" → AND(AND(a=1,b=2),c=3)
func TestParseSelectWhereChainedAnd(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM t WHERE a = 1 AND b = 2 AND c = 3")
	s := stmt.(*SelectStatement)
	_, outerLeft, outerRight := assertLogical(t, s.Where, "AND")
	_, innerLeft, innerRight := assertLogical(t, outerLeft, "AND")
	assertComparison(t, innerLeft, "a", "=", "1", TOKEN_NUMBER)
	assertComparison(t, innerRight, "b", "=", "2", TOKEN_NUMBER)
	assertComparison(t, outerRight, "c", "=", "3", TOKEN_NUMBER)
}

// Chained OR is left-associative: "a=1 OR b=2 OR c=3" → OR(OR(a=1,b=2),c=3)
func TestParseSelectWhereChainedOr(t *testing.T) {
	stmt := parseSQL(t, "SELECT id FROM t WHERE a = 1 OR b = 2 OR c = 3")
	s := stmt.(*SelectStatement)
	_, outerLeft, outerRight := assertLogical(t, s.Where, "OR")
	_, innerLeft, innerRight := assertLogical(t, outerLeft, "OR")
	assertComparison(t, innerLeft, "a", "=", "1", TOKEN_NUMBER)
	assertComparison(t, innerRight, "b", "=", "2", TOKEN_NUMBER)
	assertComparison(t, outerRight, "c", "=", "3", TOKEN_NUMBER)
}

func TestParseSelectUnderscoreNaming(t *testing.T) {
	stmt := parseSQL(t, "SELECT first_name, last_name FROM user_accounts WHERE user_id = 1")
	s := stmt.(*SelectStatement)
	if s.Table != "user_accounts" {
		t.Errorf("Table: got %q, want %q", s.Table, "user_accounts")
	}
	if len(s.Columns) != 2 || s.Columns[0] != "first_name" || s.Columns[1] != "last_name" {
		t.Errorf("Columns: got %v", s.Columns)
	}
	assertComparison(t, s.Where, "user_id", "=", "1", TOKEN_NUMBER)
}

// --- INSERT ---

func TestParseInsertSingleValue(t *testing.T) {
	stmt := parseSQL(t, "INSERT INTO users VALUES (42)")
	s, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected *InsertStatement, got %T", stmt)
	}
	if s.Table != "users" {
		t.Errorf("Table: got %q, want %q", s.Table, "users")
	}
	if len(s.Values) != 1 || s.Values[0].Value != "42" || s.Values[0].Type != TOKEN_NUMBER {
		t.Errorf("Values: got %v", s.Values)
	}
}

func TestParseInsertMultipleValues(t *testing.T) {
	stmt := parseSQL(t, "INSERT INTO users VALUES (1, 'Alice', true)")
	s := stmt.(*InsertStatement)
	if len(s.Values) != 3 {
		t.Fatalf("Values: got %d, want 3", len(s.Values))
	}
	if s.Values[0].Value != "1" || s.Values[0].Type != TOKEN_NUMBER {
		t.Errorf("Values[0]: got %+v", s.Values[0])
	}
	if s.Values[1].Value != "Alice" || s.Values[1].Type != TOKEN_STRING {
		t.Errorf("Values[1]: got %+v", s.Values[1])
	}
	if s.Values[2].Value != "true" || s.Values[2].Type != TOKEN_IDENTIFIER {
		t.Errorf("Values[2]: got %+v", s.Values[2])
	}
}

func TestParseInsertBooleanFalse(t *testing.T) {
	stmt := parseSQL(t, "INSERT INTO flags VALUES (99, false)")
	s := stmt.(*InsertStatement)
	if len(s.Values) != 2 {
		t.Fatalf("Values: got %d, want 2", len(s.Values))
	}
	if s.Values[1].Value != "false" || s.Values[1].Type != TOKEN_IDENTIFIER {
		t.Errorf("Values[1]: got %+v", s.Values[1])
	}
}

func TestParseInsertStringOnly(t *testing.T) {
	stmt := parseSQL(t, "INSERT INTO tags VALUES ('golang')")
	s := stmt.(*InsertStatement)
	if len(s.Values) != 1 || s.Values[0].Value != "golang" || s.Values[0].Type != TOKEN_STRING {
		t.Errorf("Values: got %v", s.Values)
	}
}

// --- UPDATE ---

func TestParseUpdateNoWhere(t *testing.T) {
	stmt := parseSQL(t, "UPDATE users SET name = 'Charlie'")
	s, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected *UpdateStatement, got %T", stmt)
	}
	if s.Table != "users" {
		t.Errorf("Table: got %q, want %q", s.Table, "users")
	}
	if s.Column != "name" {
		t.Errorf("Column: got %q, want %q", s.Column, "name")
	}
	if s.Value.Value != "Charlie" || s.Value.Type != TOKEN_STRING {
		t.Errorf("Value: got %+v", s.Value)
	}
	if s.Where != nil {
		t.Errorf("Where: expected nil, got %v", s.Where)
	}
}

func TestParseUpdateWithWhere(t *testing.T) {
	stmt := parseSQL(t, "UPDATE users SET age = 25 WHERE id = 1")
	s := stmt.(*UpdateStatement)
	if s.Column != "age" || s.Value.Value != "25" || s.Value.Type != TOKEN_NUMBER {
		t.Errorf("SET clause: got column=%q value=%+v", s.Column, s.Value)
	}
	assertComparison(t, s.Where, "id", "=", "1", TOKEN_NUMBER)
}

func TestParseUpdateBooleanValue(t *testing.T) {
	stmt := parseSQL(t, "UPDATE users SET active = false WHERE name = 'Alice'")
	s := stmt.(*UpdateStatement)
	if s.Value.Value != "false" || s.Value.Type != TOKEN_IDENTIFIER {
		t.Errorf("Value: got %+v", s.Value)
	}
	assertComparison(t, s.Where, "name", "=", "Alice", TOKEN_STRING)
}

func TestParseUpdateNumberValue(t *testing.T) {
	stmt := parseSQL(t, "UPDATE scores SET points = 100")
	s := stmt.(*UpdateStatement)
	if s.Column != "points" || s.Value.Value != "100" || s.Value.Type != TOKEN_NUMBER {
		t.Errorf("SET clause: got column=%q value=%+v", s.Column, s.Value)
	}
}

// --- DELETE ---

func TestParseDeleteNoWhere(t *testing.T) {
	stmt := parseSQL(t, "DELETE FROM logs")
	s, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected *DeleteStatement, got %T", stmt)
	}
	if s.Table != "logs" {
		t.Errorf("Table: got %q, want %q", s.Table, "logs")
	}
	if s.Where != nil {
		t.Errorf("Where: expected nil, got %v", s.Where)
	}
}

func TestParseDeleteWithWhere(t *testing.T) {
	stmt := parseSQL(t, "DELETE FROM users WHERE id = 5")
	s := stmt.(*DeleteStatement)
	if s.Table != "users" {
		t.Errorf("Table: got %q, want %q", s.Table, "users")
	}
	assertComparison(t, s.Where, "id", "=", "5", TOKEN_NUMBER)
}

func TestParseDeleteWhereString(t *testing.T) {
	stmt := parseSQL(t, "DELETE FROM users WHERE name = 'Bob'")
	s := stmt.(*DeleteStatement)
	assertComparison(t, s.Where, "name", "=", "Bob", TOKEN_STRING)
}

func TestParseDeleteWhereBoolean(t *testing.T) {
	stmt := parseSQL(t, "DELETE FROM users WHERE active = false")
	s := stmt.(*DeleteStatement)
	assertComparison(t, s.Where, "active", "=", "false", TOKEN_IDENTIFIER)
}

// --- CREATE TABLE ---

func TestParseCreateTableSingleColumn(t *testing.T) {
	stmt := parseSQL(t, "CREATE TABLE users (id INT)")
	s, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected *CreateTableStatement, got %T", stmt)
	}
	if s.Table != "users" {
		t.Errorf("Table: got %q, want %q", s.Table, "users")
	}
	if len(s.Columns) != 1 || s.Columns[0].Name != "id" || s.Columns[0].DataType != "INT" {
		t.Errorf("Columns: got %v", s.Columns)
	}
}

func TestParseCreateTableAllDataTypes(t *testing.T) {
	stmt := parseSQL(t, "CREATE TABLE profile (id INT, name TEXT, active BOOL)")
	s := stmt.(*CreateTableStatement)
	if s.Table != "profile" {
		t.Errorf("Table: got %q, want %q", s.Table, "profile")
	}
	want := []ColumnDef{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "TEXT"},
		{Name: "active", DataType: "BOOL"},
	}
	if len(s.Columns) != len(want) {
		t.Fatalf("Columns: got %d, want %d", len(s.Columns), len(want))
	}
	for i, col := range want {
		if s.Columns[i] != col {
			t.Errorf("Columns[%d]: got %+v, want %+v", i, s.Columns[i], col)
		}
	}
}

func TestParseCreateTableUnderscoreNames(t *testing.T) {
	stmt := parseSQL(t, "CREATE TABLE user_accounts (user_id INT, first_name TEXT, is_active BOOL)")
	s := stmt.(*CreateTableStatement)
	if s.Table != "user_accounts" {
		t.Errorf("Table: got %q, want %q", s.Table, "user_accounts")
	}
	if len(s.Columns) != 3 {
		t.Fatalf("Columns: got %d, want 3", len(s.Columns))
	}
	if s.Columns[0].Name != "user_id" || s.Columns[1].Name != "first_name" || s.Columns[2].Name != "is_active" {
		t.Errorf("Column names: got %v", s.Columns)
	}
}

// --- DROP TABLE ---

func TestParseDropTable(t *testing.T) {
	stmt := parseSQL(t, "DROP TABLE users")
	s, ok := stmt.(*DropTableStatement)
	if !ok {
		t.Fatalf("expected *DropTableStatement, got %T", stmt)
	}
	if s.Table != "users" {
		t.Errorf("Table: got %q, want %q", s.Table, "users")
	}
}

func TestParseDropTableUnderscoreName(t *testing.T) {
	stmt := parseSQL(t, "DROP TABLE user_accounts")
	s := stmt.(*DropTableStatement)
	if s.Table != "user_accounts" {
		t.Errorf("Table: got %q, want %q", s.Table, "user_accounts")
	}
}

// --- Statement interface satisfaction ---

func TestStatementInterfaceSatisfied(t *testing.T) {
	stmts := []struct {
		name  string
		query string
	}{
		{"SelectStatement", "SELECT id FROM t"},
		{"InsertStatement", "INSERT INTO t VALUES (1)"},
		{"UpdateStatement", "UPDATE t SET x = 1"},
		{"DeleteStatement", "DELETE FROM t"},
		{"CreateTableStatement", "CREATE TABLE t (id INT)"},
		{"DropTableStatement", "DROP TABLE t"},
	}
	for _, tt := range stmts {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseSQL(t, tt.query)
			// isStatement() is a compile-time guarantee; just verify we got a non-nil value
			if stmt == nil {
				t.Errorf("Parse(%q) returned nil", tt.query)
			}
		})
	}
}

// --- Error cases ---

func TestParseEmptyInput(t *testing.T) {
	_, err := Parse([]Token{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestParseUnknownKeyword(t *testing.T) {
	parseSQLError(t, "FOOBAR id FROM t")
}

func TestParseSelectMissingFrom(t *testing.T) {
	parseSQLError(t, "SELECT id users")
}

func TestParseSelectMissingTable(t *testing.T) {
	parseSQLError(t, "SELECT id FROM")
}

func TestParseSelectMissingColumn(t *testing.T) {
	parseSQLError(t, "SELECT FROM users")
}

func TestParseSelectWhereMissingValue(t *testing.T) {
	parseSQLError(t, "SELECT id FROM t WHERE age =")
}

func TestParseSelectWhereMissingOperator(t *testing.T) {
	parseSQLError(t, "SELECT id FROM t WHERE age 30")
}

func TestParseInsertMissingInto(t *testing.T) {
	parseSQLError(t, "INSERT users VALUES (1)")
}

func TestParseInsertMissingValues(t *testing.T) {
	parseSQLError(t, "INSERT INTO users (1)")
}

func TestParseInsertMissingParen(t *testing.T) {
	parseSQLError(t, "INSERT INTO users VALUES 1")
}

func TestParseInsertMissingTable(t *testing.T) {
	parseSQLError(t, "INSERT INTO VALUES (1)")
}

func TestParseUpdateMissingSet(t *testing.T) {
	parseSQLError(t, "UPDATE users name = 'x'")
}

func TestParseUpdateMissingTable(t *testing.T) {
	parseSQLError(t, "UPDATE SET name = 'x'")
}

func TestParseUpdateMissingValue(t *testing.T) {
	parseSQLError(t, "UPDATE users SET name =")
}

func TestParseDeleteMissingFrom(t *testing.T) {
	parseSQLError(t, "DELETE users")
}

func TestParseDeleteMissingTable(t *testing.T) {
	parseSQLError(t, "DELETE FROM")
}

func TestParseCreateMissingTable(t *testing.T) {
	parseSQLError(t, "CREATE users (id INT)")
}

func TestParseCreateMissingColumns(t *testing.T) {
	parseSQLError(t, "CREATE TABLE users")
}

func TestParseDropMissingTable(t *testing.T) {
	parseSQLError(t, "DROP users")
}

func TestParseDropMissingName(t *testing.T) {
	parseSQLError(t, "DROP TABLE")
}
