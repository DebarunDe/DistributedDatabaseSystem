package sqllayer

import (
	"testing"
)

func TestTokenizeKeywords(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{"SELECT", []Token{{TOKEN_KEYWORD, "SELECT"}}},
		{"FROM", []Token{{TOKEN_KEYWORD, "FROM"}}},
		{"WHERE", []Token{{TOKEN_KEYWORD, "WHERE"}}},
		{"INSERT", []Token{{TOKEN_KEYWORD, "INSERT"}}},
		{"INTO", []Token{{TOKEN_KEYWORD, "INTO"}}},
		{"VALUES", []Token{{TOKEN_KEYWORD, "VALUES"}}},
		{"UPDATE", []Token{{TOKEN_KEYWORD, "UPDATE"}}},
		{"SET", []Token{{TOKEN_KEYWORD, "SET"}}},
		{"DELETE", []Token{{TOKEN_KEYWORD, "DELETE"}}},
		{"CREATE", []Token{{TOKEN_KEYWORD, "CREATE"}}},
		{"TABLE", []Token{{TOKEN_KEYWORD, "TABLE"}}},
		{"DROP", []Token{{TOKEN_KEYWORD, "DROP"}}},
		// case-insensitive
		{"select", []Token{{TOKEN_KEYWORD, "SELECT"}}},
		{"Select", []Token{{TOKEN_KEYWORD, "SELECT"}}},
		{"fRoM", []Token{{TOKEN_KEYWORD, "FROM"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeDataTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{"INT", []Token{{TOKEN_DATATYPE, "INT"}}},
		{"TEXT", []Token{{TOKEN_DATATYPE, "TEXT"}}},
		{"BOOL", []Token{{TOKEN_DATATYPE, "BOOL"}}},
		// case-insensitive
		{"int", []Token{{TOKEN_DATATYPE, "INT"}}},
		{"text", []Token{{TOKEN_DATATYPE, "TEXT"}}},
		{"Bool", []Token{{TOKEN_DATATYPE, "BOOL"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{"=", []Token{{TOKEN_OPERATOR, "="}}},
		{"!=", []Token{{TOKEN_OPERATOR, "!="}}},
		{">", []Token{{TOKEN_OPERATOR, ">"}}},
		{">=", []Token{{TOKEN_OPERATOR, ">="}}},
		{"<", []Token{{TOKEN_OPERATOR, "<"}}},
		{"<=", []Token{{TOKEN_OPERATOR, "<="}}},
		{"AND", []Token{{TOKEN_OPERATOR, "AND"}}},
		{"OR", []Token{{TOKEN_OPERATOR, "OR"}}},
		// case-insensitive
		{"and", []Token{{TOKEN_OPERATOR, "AND"}}},
		{"or", []Token{{TOKEN_OPERATOR, "OR"}}},
		{"And", []Token{{TOKEN_OPERATOR, "AND"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeIdentifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{"foo", []Token{{TOKEN_IDENTIFIER, "foo"}}},
		{"myTable", []Token{{TOKEN_IDENTIFIER, "myTable"}}},
		{"col1", []Token{{TOKEN_IDENTIFIER, "col1"}}},
		{"abc123", []Token{{TOKEN_IDENTIFIER, "abc123"}}},
		// identifiers preserve original casing
		{"MyColumn", []Token{{TOKEN_IDENTIFIER, "MyColumn"}}},
		// underscore in identifiers
		{"first_name", []Token{{TOKEN_IDENTIFIER, "first_name"}}},
		{"last_name", []Token{{TOKEN_IDENTIFIER, "last_name"}}},
		{"user_id", []Token{{TOKEN_IDENTIFIER, "user_id"}}},
		{"my_table_name", []Token{{TOKEN_IDENTIFIER, "my_table_name"}}},
		{"col_1", []Token{{TOKEN_IDENTIFIER, "col_1"}}},
		{"_private", []Token{{TOKEN_IDENTIFIER, "_private"}}},
		{"__double", []Token{{TOKEN_IDENTIFIER, "__double"}}},
		{"mixed_Case_Field", []Token{{TOKEN_IDENTIFIER, "mixed_Case_Field"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{"0", []Token{{TOKEN_NUMBER, "0"}}},
		{"42", []Token{{TOKEN_NUMBER, "42"}}},
		{"1000", []Token{{TOKEN_NUMBER, "1000"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{"'hello'", []Token{{TOKEN_STRING, "hello"}}},
		{"'hello world'", []Token{{TOKEN_STRING, "hello world"}}},
		{"''", []Token{{TOKEN_STRING, ""}}},
		{"'123'", []Token{{TOKEN_STRING, "123"}}},
		{"'foo bar baz'", []Token{{TOKEN_STRING, "foo bar baz"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizePunctuation(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{",", []Token{{TOKEN_COMMA, ","}}},
		{"(", []Token{{TOKEN_LPAREN, "("}}},
		{")", []Token{{TOKEN_RPAREN, ")"}}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{"spaces between tokens", "SELECT   FROM", []Token{{TOKEN_KEYWORD, "SELECT"}, {TOKEN_KEYWORD, "FROM"}}},
		{"tabs between tokens", "SELECT\tFROM", []Token{{TOKEN_KEYWORD, "SELECT"}, {TOKEN_KEYWORD, "FROM"}}},
		{"newlines between tokens", "SELECT\nFROM", []Token{{TOKEN_KEYWORD, "SELECT"}, {TOKEN_KEYWORD, "FROM"}}},
		{"mixed whitespace", "SELECT \t\n FROM", []Token{{TOKEN_KEYWORD, "SELECT"}, {TOKEN_KEYWORD, "FROM"}}},
		{"leading whitespace", "   SELECT", []Token{{TOKEN_KEYWORD, "SELECT"}}},
		{"trailing whitespace", "SELECT   ", []Token{{TOKEN_KEYWORD, "SELECT"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeFullQueries(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "simple SELECT",
			input: "SELECT name FROM users",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
			},
		},
		{
			name:  "SELECT with WHERE",
			input: "SELECT name FROM users WHERE age = 30",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "age"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_NUMBER, "30"},
			},
		},
		{
			name:  "SELECT with string literal",
			input: "SELECT name FROM users WHERE name = 'Alice'",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_STRING, "Alice"},
			},
		},
		{
			name:  "INSERT INTO VALUES",
			input: "INSERT INTO users VALUES (1, 'Bob')",
			expected: []Token{
				{TOKEN_KEYWORD, "INSERT"},
				{TOKEN_KEYWORD, "INTO"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "VALUES"},
				{TOKEN_LPAREN, "("},
				{TOKEN_NUMBER, "1"},
				{TOKEN_COMMA, ","},
				{TOKEN_STRING, "Bob"},
				{TOKEN_RPAREN, ")"},
			},
		},
		{
			name:  "CREATE TABLE",
			input: "CREATE TABLE users (id INT, name TEXT)",
			expected: []Token{
				{TOKEN_KEYWORD, "CREATE"},
				{TOKEN_KEYWORD, "TABLE"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_LPAREN, "("},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_DATATYPE, "INT"},
				{TOKEN_COMMA, ","},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_DATATYPE, "TEXT"},
				{TOKEN_RPAREN, ")"},
			},
		},
		{
			name:  "WHERE with AND",
			input: "SELECT id FROM orders WHERE age > 18 AND age < 65",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "orders"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "age"},
				{TOKEN_OPERATOR, ">"},
				{TOKEN_NUMBER, "18"},
				{TOKEN_OPERATOR, "AND"},
				{TOKEN_IDENTIFIER, "age"},
				{TOKEN_OPERATOR, "<"},
				{TOKEN_NUMBER, "65"},
			},
		},
		{
			name:  "WHERE with OR",
			input: "SELECT id FROM users WHERE name = 'Alice' OR name = 'Bob'",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_STRING, "Alice"},
				{TOKEN_OPERATOR, "OR"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_STRING, "Bob"},
			},
		},
		{
			name:  "WHERE with !=",
			input: "SELECT id FROM users WHERE status != 'active'",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "status"},
				{TOKEN_OPERATOR, "!="},
				{TOKEN_STRING, "active"},
			},
		},
		{
			name:  "WHERE with >= and <=",
			input: "SELECT id FROM scores WHERE val >= 10 AND val <= 100",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "scores"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "val"},
				{TOKEN_OPERATOR, ">="},
				{TOKEN_NUMBER, "10"},
				{TOKEN_OPERATOR, "AND"},
				{TOKEN_IDENTIFIER, "val"},
				{TOKEN_OPERATOR, "<="},
				{TOKEN_NUMBER, "100"},
			},
		},
		{
			name:  "DELETE with WHERE",
			input: "DELETE FROM users WHERE id = 5",
			expected: []Token{
				{TOKEN_KEYWORD, "DELETE"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_NUMBER, "5"},
			},
		},
		{
			name:  "UPDATE SET",
			input: "UPDATE users SET name = 'Charlie' WHERE id = 1",
			expected: []Token{
				{TOKEN_KEYWORD, "UPDATE"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "SET"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_STRING, "Charlie"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_NUMBER, "1"},
			},
		},
		{
			name:  "DROP TABLE",
			input: "DROP TABLE users",
			expected: []Token{
				{TOKEN_KEYWORD, "DROP"},
				{TOKEN_KEYWORD, "TABLE"},
				{TOKEN_IDENTIFIER, "users"},
			},
		},
		{
			name:  "mixed case keywords",
			input: "select name from users where id = 1",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "name"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_NUMBER, "1"},
			},
		},
		{
			name:  "underscore column names in SELECT",
			input: "SELECT first_name FROM users",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "first_name"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
			},
		},
		{
			name:  "underscore column in WHERE",
			input: "SELECT id FROM users WHERE last_name = 'Smith'",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "users"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "last_name"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_STRING, "Smith"},
			},
		},
		{
			name:  "underscore table name",
			input: "SELECT id FROM order_items",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "id"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "order_items"},
			},
		},
		{
			name:  "CREATE TABLE with underscore columns",
			input: "CREATE TABLE user_accounts (user_id INT, first_name TEXT, is_active BOOL)",
			expected: []Token{
				{TOKEN_KEYWORD, "CREATE"},
				{TOKEN_KEYWORD, "TABLE"},
				{TOKEN_IDENTIFIER, "user_accounts"},
				{TOKEN_LPAREN, "("},
				{TOKEN_IDENTIFIER, "user_id"},
				{TOKEN_DATATYPE, "INT"},
				{TOKEN_COMMA, ","},
				{TOKEN_IDENTIFIER, "first_name"},
				{TOKEN_DATATYPE, "TEXT"},
				{TOKEN_COMMA, ","},
				{TOKEN_IDENTIFIER, "is_active"},
				{TOKEN_DATATYPE, "BOOL"},
				{TOKEN_RPAREN, ")"},
			},
		},
		{
			name:  "INSERT with underscore table",
			input: "INSERT INTO order_items VALUES (1, 'widget')",
			expected: []Token{
				{TOKEN_KEYWORD, "INSERT"},
				{TOKEN_KEYWORD, "INTO"},
				{TOKEN_IDENTIFIER, "order_items"},
				{TOKEN_KEYWORD, "VALUES"},
				{TOKEN_LPAREN, "("},
				{TOKEN_NUMBER, "1"},
				{TOKEN_COMMA, ","},
				{TOKEN_STRING, "widget"},
				{TOKEN_RPAREN, ")"},
			},
		},
		{
			name:  "UPDATE with underscore columns",
			input: "UPDATE user_accounts SET first_name = 'Jane' WHERE user_id = 42",
			expected: []Token{
				{TOKEN_KEYWORD, "UPDATE"},
				{TOKEN_IDENTIFIER, "user_accounts"},
				{TOKEN_KEYWORD, "SET"},
				{TOKEN_IDENTIFIER, "first_name"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_STRING, "Jane"},
				{TOKEN_KEYWORD, "WHERE"},
				{TOKEN_IDENTIFIER, "user_id"},
				{TOKEN_OPERATOR, "="},
				{TOKEN_NUMBER, "42"},
			},
		},
		{
			name:  "multiple underscores in one identifier",
			input: "SELECT a_b_c FROM t",
			expected: []Token{
				{TOKEN_KEYWORD, "SELECT"},
				{TOKEN_IDENTIFIER, "a_b_c"},
				{TOKEN_KEYWORD, "FROM"},
				{TOKEN_IDENTIFIER, "t"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertTokens(t, got, tt.expected)
		})
	}
}

func TestTokenizeErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"bare exclamation mark", "!"},
		{"exclamation followed by non-equals", "!a"},
		{"unterminated string", "'hello"},
		{"unexpected character @", "SELECT @foo"},
		{"unexpected character #", "#comment"},
		{"unexpected character $", "$var"},
		{"unexpected character semicolon", "SELECT name;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Tokenize(tt.input)
			if err == nil {
				t.Fatalf("expected error for input %q but got none", tt.input)
			}
		})
	}
}

func TestTokenizeEmpty(t *testing.T) {
	got, err := Tokenize("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty token slice, got %v", got)
	}
}

func TestTokenizeOnlyWhitespace(t *testing.T) {
	got, err := Tokenize("   \t\n  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty token slice, got %v", got)
	}
}

// assertTokens checks that got matches expected, printing a diff on failure.
func assertTokens(t *testing.T, got, expected []Token) {
	t.Helper()
	if len(got) != len(expected) {
		t.Fatalf("token count mismatch: got %d, want %d\n  got:  %v\n  want: %v", len(got), len(expected), got, expected)
	}
	for i := range expected {
		if got[i].Type != expected[i].Type || got[i].Value != expected[i].Value {
			t.Errorf("token[%d] mismatch: got {%v %q}, want {%v %q}", i, got[i].Type, got[i].Value, expected[i].Type, expected[i].Value)
		}
	}
}
