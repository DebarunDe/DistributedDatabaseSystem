package sqllayer

import "fmt"

type parser struct {
	tokens []Token
	pos    int
}

func (p *parser) peek() (Token, bool) {
	if p.pos >= len(p.tokens) {
		return Token{}, false
	}
	return p.tokens[p.pos], true
}

func (p *parser) consume() (Token, bool) {
	if p.pos >= len(p.tokens) {
		return Token{}, false
	}
	t := p.tokens[p.pos]
	p.pos++
	return t, true
}

// expect consumes the next token and errors if it doesn't match type and value.
func (p *parser) expect(typ TokenType, value string) (Token, error) {
	t, ok := p.consume()
	if !ok {
		return Token{}, fmt.Errorf("expected %q but got end of input", value)
	}
	if t.Type != typ || t.Value != value {
		return Token{}, fmt.Errorf("expected %q but got %q", value, t.Value)
	}
	return t, nil
}

// expectType consumes the next token and errors if its type doesn't match.
func (p *parser) expectType(typ TokenType) (Token, error) {
	t, ok := p.consume()
	if !ok {
		return Token{}, fmt.Errorf("expected token type %d but got end of input", typ)
	}
	if t.Type != typ {
		return Token{}, fmt.Errorf("unexpected token %q", t.Value)
	}
	return t, nil
}

// parseLiteral consumes a number or string token and returns a Literal.
func (p *parser) parseLiteral() (Literal, error) {
	t, ok := p.consume()
	if !ok {
		return Literal{}, fmt.Errorf("expected literal but got end of input")
	}
	switch t.Type {
	case TOKEN_NUMBER, TOKEN_STRING, TOKEN_IDENTIFIER:
		return Literal{Value: t.Value, Type: t.Type}, nil
	default:
		return Literal{}, fmt.Errorf("expected literal (number, string, or boolean), got %q", t.Value)
	}
}

// parseExpression handles OR (lowest precedence).
func (p *parser) parseExpression() (Expression, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for {
		next, ok := p.peek()
		if !ok || next.Type != TOKEN_OPERATOR || next.Value != "OR" {
			break
		}
		p.consume()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &LogicalExpr{Operator: "OR", Left: left, Right: right}
	}
	return left, nil
}

// parseAndExpr handles AND.
func (p *parser) parseAndExpr() (Expression, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for {
		next, ok := p.peek()
		if !ok || next.Type != TOKEN_OPERATOR || next.Value != "AND" {
			break
		}
		p.consume()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &LogicalExpr{Operator: "AND", Left: left, Right: right}
	}
	return left, nil
}

// parseComparison handles a single "column op literal" expression.
func (p *parser) parseComparison() (Expression, error) {
	col, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("comparison: expected column name: %w", err)
	}
	op, err := p.expectType(TOKEN_OPERATOR)
	if err != nil {
		return nil, fmt.Errorf("comparison: expected operator: %w", err)
	}
	switch op.Value {
	case "=", "!=", "<", ">", "<=", ">=":
	default:
		return nil, fmt.Errorf("comparison: unexpected operator %q", op.Value)
	}
	lit, err := p.parseLiteral()
	if err != nil {
		return nil, fmt.Errorf("comparison: %w", err)
	}
	return &ComparisonExpr{Column: col.Value, Operator: op.Value, Value: lit}, nil
}

// SELECT col1, col2, ... FROM table [WHERE expr]
func (p *parser) parseSelect() (*SelectStatement, error) {
	p.consume() // SELECT
	var columns []string
	if next, ok := p.peek(); ok && next.Type == TOKEN_STAR {
		p.consume()
		columns = []string{"*"}
	} else {
		for {
			col, err := p.expectType(TOKEN_IDENTIFIER)
			if err != nil {
				return nil, fmt.Errorf("SELECT: %w", err)
			}
			columns = append(columns, col.Value)
			next, ok := p.peek()
			if !ok || next.Type != TOKEN_COMMA {
				break
			}
			p.consume()
		}
	}
	if _, err := p.expect(TOKEN_KEYWORD, "FROM"); err != nil {
		return nil, fmt.Errorf("SELECT: %w", err)
	}
	table, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("SELECT: %w", err)
	}
	stmt := &SelectStatement{Columns: columns, Table: table.Value}
	if next, ok := p.peek(); ok && next.Type == TOKEN_KEYWORD && next.Value == "WHERE" {
		p.consume()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("SELECT WHERE: %w", err)
		}
		stmt.Where = expr
	}
	return stmt, nil
}

// INSERT INTO table VALUES (v1, v2, ...)
func (p *parser) parseInsert() (*InsertStatement, error) {
	p.consume() // INSERT
	if _, err := p.expect(TOKEN_KEYWORD, "INTO"); err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}
	table, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}
	if _, err := p.expect(TOKEN_KEYWORD, "VALUES"); err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}
	if _, err := p.expectType(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}
	var values []Literal
	for {
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, fmt.Errorf("INSERT VALUES: %w", err)
		}
		values = append(values, lit)
		next, ok := p.peek()
		if !ok || next.Type != TOKEN_COMMA {
			break
		}
		p.consume()
	}
	if _, err := p.expectType(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}
	return &InsertStatement{Table: table.Value, Values: values}, nil
}

// UPDATE table SET column = value [WHERE expr]
func (p *parser) parseUpdate() (*UpdateStatement, error) {
	p.consume() // UPDATE
	table, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("UPDATE: %w", err)
	}
	if _, err := p.expect(TOKEN_KEYWORD, "SET"); err != nil {
		return nil, fmt.Errorf("UPDATE: %w", err)
	}
	col, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("UPDATE SET: %w", err)
	}
	if _, err := p.expect(TOKEN_OPERATOR, "="); err != nil {
		return nil, fmt.Errorf("UPDATE SET: %w", err)
	}
	val, err := p.parseLiteral()
	if err != nil {
		return nil, fmt.Errorf("UPDATE SET: %w", err)
	}
	stmt := &UpdateStatement{Table: table.Value, Column: col.Value, Value: val}
	if next, ok := p.peek(); ok && next.Type == TOKEN_KEYWORD && next.Value == "WHERE" {
		p.consume()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("UPDATE WHERE: %w", err)
		}
		stmt.Where = expr
	}
	return stmt, nil
}

// DELETE FROM table [WHERE expr]
func (p *parser) parseDelete() (*DeleteStatement, error) {
	p.consume() // DELETE
	if _, err := p.expect(TOKEN_KEYWORD, "FROM"); err != nil {
		return nil, fmt.Errorf("DELETE: %w", err)
	}
	table, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("DELETE: %w", err)
	}
	stmt := &DeleteStatement{Table: table.Value}
	if next, ok := p.peek(); ok && next.Type == TOKEN_KEYWORD && next.Value == "WHERE" {
		p.consume()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("DELETE WHERE: %w", err)
		}
		stmt.Where = expr
	}
	return stmt, nil
}

// CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
func (p *parser) parseCreate() (*CreateTableStatement, error) {
	p.consume() // CREATE
	if _, err := p.expect(TOKEN_KEYWORD, "TABLE"); err != nil {
		return nil, fmt.Errorf("CREATE: %w", err)
	}
	table, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("CREATE TABLE: %w", err)
	}
	if _, err := p.expectType(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("CREATE TABLE: %w", err)
	}
	var cols []ColumnDef
	for {
		name, err := p.expectType(TOKEN_IDENTIFIER)
		if err != nil {
			return nil, fmt.Errorf("CREATE TABLE column: %w", err)
		}
		dtype, err := p.expectType(TOKEN_DATATYPE)
		if err != nil {
			return nil, fmt.Errorf("CREATE TABLE column type: %w", err)
		}
		cols = append(cols, ColumnDef{Name: name.Value, DataType: dtype.Value})
		next, ok := p.peek()
		if !ok || next.Type != TOKEN_COMMA {
			break
		}
		p.consume()
	}
	if _, err := p.expectType(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("CREATE TABLE: %w", err)
	}
	return &CreateTableStatement{Table: table.Value, Columns: cols}, nil
}

// DROP TABLE name
func (p *parser) parseDrop() (*DropTableStatement, error) {
	p.consume() // DROP
	if _, err := p.expect(TOKEN_KEYWORD, "TABLE"); err != nil {
		return nil, fmt.Errorf("DROP: %w", err)
	}
	table, err := p.expectType(TOKEN_IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("DROP TABLE: %w", err)
	}
	return &DropTableStatement{Table: table.Value}, nil
}
