package sqllayer

import "fmt"

// Nodes
// Literal represents a typed value: 42, 'alice', true
type Literal struct {
	Value string
	Type  TokenType
}

// Expression interface
type Expression interface {
	isExpression()
}

// comparison
type ComparisonExpr struct {
	Column   string
	Operator string
	Value    Literal
}

func (e *ComparisonExpr) isExpression() {}

type LogicalExpr struct {
	Operator string // AND,OR
	Left     Expression
	Right    Expression
}

func (e *LogicalExpr) isExpression() {}

// Column def for create table
type ColumnDef struct {
	Name     string
	DataType string
}

// SELECT columns FROM table WHERE expr
type SelectStatement struct {
	Columns []string
	Table   string
	Where   Expression // nil if no WHERE clause
}

func (s *SelectStatement) isStatement() {}

// INSERT INTO table VALUES (v1, v2, v3)
type InsertStatement struct {
	Table  string
	Values []Literal
}

func (s *InsertStatement) isStatement() {}

// UPDATE table SET column = value WHERE expr
type UpdateStatement struct {
	Table  string
	Column string
	Value  Literal
	Where  Expression // nil if no WHERE clause
}

func (s *UpdateStatement) isStatement() {}

// DELETE FROM table WHERE expr
type DeleteStatement struct {
	Table string
	Where Expression // nil if no WHERE clause
}

func (s *DeleteStatement) isStatement() {}

// CREATE TABLE name (col1 TYPE, col2 TYPE)
type CreateTableStatement struct {
	Table   string
	Columns []ColumnDef
}

func (s *CreateTableStatement) isStatement() {}

// DROP TABLE name
type DropTableStatement struct {
	Table string
}

func (s *DropTableStatement) isStatement() {}

// Statement interface for parser
type Statement interface {
	isStatement()
}

// Parse converts tokens into the appropriate statement struct.
func Parse(tokens []Token) (Statement, error) {
	p := &parser{tokens: tokens}
	first, ok := p.peek()
	if !ok {
		return nil, fmt.Errorf("empty input")
	}
	if first.Type != TOKEN_KEYWORD {
		return nil, fmt.Errorf("expected SQL keyword, got %q", first.Value)
	}
	switch first.Value {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	default:
		return nil, fmt.Errorf("unknown statement keyword %q", first.Value)
	}
}
