package sqllayer

import (
	"fmt"
	"strconv"
	"strings"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

type ResultRow struct {
	Key    uint64
	Fields []btree.Field
}

type ResultSet struct {
	Columns []string
	Rows    []ResultRow
}

type Executor struct {
	sc *SchemaCatalog
	bt *btree.BTree
	tm *lock.TransactionManager
}

// Helpers
// literalMatchesType matches ensures dataType matches literalType
func literalMatchesType(lit Literal, dataType string) bool {
	dataType = strings.ToUpper(dataType)
	if lit.Type == TOKEN_NUMBER && dataType == "INT" {
		return true
	}
	if lit.Type == TOKEN_STRING && dataType == "TEXT" {
		return true
	}
	if lit.Type == TOKEN_IDENTIFIER && (dataType == "BOOL" && (strings.ToUpper(lit.Value) == "TRUE" || strings.ToUpper(lit.Value) == "FALSE")) {
		return true
	}

	return false
}

// literalToField converts a literal statement to a btree field
func literalToField(lit Literal, dataType string, tag uint8) (btree.Field, error) {
	if ok := literalMatchesType(lit, dataType); !ok {
		return btree.Field{}, fmt.Errorf("Literal type mismatch")
	}

	dataType = strings.ToUpper(dataType)
	if dataType == "INT" {
		n, err := strconv.ParseInt(lit.Value, 10, 64)
		if err != nil {
			return btree.Field{}, fmt.Errorf("failed to parse INT literal: %v", err)
		}

		return btree.Field{
			Tag:   tag,
			Value: btree.IntValue{V: n},
		}, nil
	}

	if dataType == "TEXT" {
		return btree.Field{
			Tag:   tag,
			Value: btree.StringValue{V: lit.Value},
		}, nil
	}

	if dataType == "BOOL" {
		return btree.Field{
			Tag:   tag,
			Value: btree.StringValue{V: strings.ToUpper(lit.Value)},
		}, nil
	}

	//non matching type
	return btree.Field{}, fmt.Errorf("incompatible field: %q", dataType)
}

// literalToPrimaryKey converts literal value into primary key
func literalToPrimaryKey(lit Literal) (uint32, error) {
	key, err := strconv.ParseUint(lit.Value, 10, 32)

	return uint32(key), err
}

// findColumnIndex returns the index into the full fields slice for colName.
// The fields slice is ordered as [PrimaryKey, Columns[0], Columns[1], ...],
// so PrimaryKey maps to index 0 and schema.Columns[i] maps to index i+1.
// Returns -1 if the column does not exist in the schema.
func findColumnIndex(colName string, schema *TableSchemaValue) int {
	if strings.EqualFold(colName, schema.PrimaryKey.Name) {
		return 0
	}
	for i, col := range schema.Columns {
		if strings.EqualFold(colName, col.Name) {
			return i + 1
		}
	}
	return -1
}

func compareInt(a, b int64, op string) bool {
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
	default:
		return false
	}
}

func compareString(a, b string, op string) bool {
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
	default:
		return false
	}
}

// evaluateExpression evaluates a WHERE expression against a row.
// fields must be ordered as [PrimaryKey, Columns[0], Columns[1], ...] to match findColumnIndex.
func evaluateExpression(expr Expression, schema *TableSchemaValue, fields []btree.Field) bool {
	if expr == nil {
		return true
	}
	switch e := expr.(type) {
	case *ComparisonExpr:
		idx := findColumnIndex(e.Column, schema)
		if idx == -1 {
			return false
		}
		switch v := fields[idx].Value.(type) {
		case btree.IntValue:
			litVal, err := strconv.ParseInt(e.Value.Value, 10, 64)
			if err != nil {
				return false
			}
			return compareInt(v.V, litVal, e.Operator)
		case btree.StringValue:
			litVal := e.Value.Value
			var colType string
			if idx == 0 {
				colType = strings.ToUpper(schema.PrimaryKey.DataType)
			} else {
				colType = strings.ToUpper(schema.Columns[idx-1].DataType)
			}
			if colType == "BOOL" {
				litVal = strings.ToUpper(litVal)
			}
			return compareString(v.V, litVal, e.Operator)
		default:
			return false
		}
	case *LogicalExpr:
		switch strings.ToUpper(e.Operator) {
		case "AND":
			return evaluateExpression(e.Left, schema, fields) && evaluateExpression(e.Right, schema, fields)
		case "OR":
			return evaluateExpression(e.Left, schema, fields) || evaluateExpression(e.Right, schema, fields)
		default:
			return false
		}
	}
	return false
}

// executeInsert executes a Insert statement
func (ex *Executor) executeInsert(s *InsertStatement, txnId uint64) (*ResultSet, error) {
	schema := ex.sc.FindTableSchema(s.Table)
	if schema == nil {
		return nil, fmt.Errorf("table %q not found", s.Table)
	}

	if strings.ToUpper(schema.PrimaryKey.DataType) != "INT" {
		return nil, fmt.Errorf("primary key %q has unsupported type %q: only INT is supported", schema.PrimaryKey.Name, schema.PrimaryKey.DataType)
	}

	if len(s.Values) != 1+len(schema.Columns) {
		return nil, fmt.Errorf("mismatched value count (%d vs %d)", len(s.Values), 1+len(schema.Columns))
	}

	if len(schema.Columns) > 255 {
		return nil, fmt.Errorf("table %q has too many columns (%d); maximum is 255", s.Table, len(schema.Columns))
	}

	pkValue, err := literalToPrimaryKey(s.Values[0])
	if err != nil {
		return nil, fmt.Errorf("unable to convert literal to primary key: %w", err)
	}

	encodedKey := encodeKey(schema.TableId, pkValue)

	if err := ex.tm.Lock(txnId, encodedKey, lock.LockExclusive); err != nil {
		return nil, fmt.Errorf("lock row %d: %w", encodedKey, err)
	}

	_, exists, err := ex.bt.Search(encodedKey)
	if err != nil {
		return nil, fmt.Errorf("duplicate check failed: %w", err)
	}
	if exists {
		return nil, fmt.Errorf("duplicate key: primary key %d already exists in table %q", pkValue, s.Table)
	}

	// Primary key goes into fields[0]
	pkField, err := literalToField(s.Values[0], schema.PrimaryKey.DataType, 0)
	if err != nil {
		return nil, fmt.Errorf("primary key: %w", err)
	}
	fields := []btree.Field{pkField}

	for i := range len(schema.Columns) {
		field, err := literalToField(s.Values[i+1], schema.Columns[i].DataType, uint8(i+1))
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", schema.Columns[i].Name, err)
		}
		fields = append(fields, field)
	}

	if err := ex.bt.Insert(encodedKey, fields); err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	ex.tm.AppendUndo(txnId, lock.UndoEntry{Op: lock.UndoInsert, Key: encodedKey, Fields: nil})

	return nil, nil
}

// executeSelect executes a select statement
func (ex *Executor) executeSelect(s *SelectStatement, txnId uint64) (*ResultSet, error) {
	schema := ex.sc.FindTableSchema(s.Table)
	if schema == nil {
		return nil, fmt.Errorf("table %q not found", s.Table)
	}

	tableId := schema.TableId

	// resolve column indices: "*" expands to PK + all columns
	var colIndices []int
	if len(s.Columns) == 1 && s.Columns[0] == "*" {
		for i := range len(schema.Columns) + 1 {
			colIndices = append(colIndices, i)
		}
	} else {
		for _, col := range s.Columns {
			idx := findColumnIndex(col, schema)
			if idx == -1 {
				return nil, fmt.Errorf("column %q not found in table %q", col, s.Table)
			}
			colIndices = append(colIndices, idx)
		}
	}

	colNames := make([]string, len(colIndices))
	for i, idx := range colIndices {
		if idx == 0 {
			colNames[i] = schema.PrimaryKey.Name
		} else {
			colNames[i] = schema.Columns[idx-1].Name
		}
	}

	results, err := ex.bt.RangeScan(encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)))
	if err != nil {
		return nil, fmt.Errorf("range scan over table %d: %w", tableId, err)
	}

	var rows []ResultRow
	for _, r := range results {
		if err := ex.tm.Lock(txnId, r.Key, lock.LockShared); err != nil {
			return nil, fmt.Errorf("lock row %d: %w", r.Key, err)
		}
		if !evaluateExpression(s.Where, schema, r.Fields) {
			continue
		}
		row := ResultRow{Key: r.Key}
		for _, idx := range colIndices {
			row.Fields = append(row.Fields, r.Fields[idx])
		}
		rows = append(rows, row)
	}

	return &ResultSet{Columns: colNames, Rows: rows}, nil
}

// executeUpdate executes a update statement
func (ex *Executor) executeUpdate(s *UpdateStatement, txnId uint64) (*ResultSet, error) {
	schema := ex.sc.FindTableSchema(s.Table)
	if schema == nil {
		return nil, fmt.Errorf("table %q not found", s.Table)
	}

	columnFound := false
	for i := range schema.Columns {
		if strings.EqualFold(schema.Columns[i].Name, s.Column) {
			columnFound = true
			if !literalMatchesType(s.Value, schema.Columns[i].DataType) {
				return nil, fmt.Errorf("datatype do not match")
			}
			break
		}
	}

	if !columnFound {
		return nil, fmt.Errorf("unable to find column %q in schema", s.Column)
	}

	tableId := schema.TableId
	results, err := ex.bt.RangeScan(encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)))
	if err != nil {
		return nil, fmt.Errorf("range scan over table %d: %w", tableId, err)
	}

	for _, r := range results {
		if evaluateExpression(s.Where, schema, r.Fields) {
			if err := ex.tm.Lock(txnId, r.Key, lock.LockExclusive); err != nil {
				return nil, fmt.Errorf("lock row %d: %w", r.Key, err)
			}

			oldFields := make([]btree.Field, len(r.Fields))
			copy(oldFields, r.Fields)
			ex.tm.AppendUndo(txnId, lock.UndoEntry{Op: lock.UndoUpdate, Key: r.Key, Fields: oldFields})

			i := findColumnIndex(s.Column, schema)

			newField, err := literalToField(s.Value, schema.Columns[i-1].DataType, uint8(i))
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", s.Column, err)
			}
			r.Fields[i] = newField

			if err := ex.bt.Insert(r.Key, r.Fields); err != nil {
				return nil, fmt.Errorf("update failed for key %d: %w", r.Key, err)
			}
		}
	}

	return nil, nil
}

// executeDelete executes a delete statement
func (ex *Executor) executeDelete(s *DeleteStatement, txnId uint64) (*ResultSet, error) {
	schema := ex.sc.FindTableSchema(s.Table)
	if schema == nil {
		return nil, fmt.Errorf("table %q not found", s.Table)
	}

	tableId := schema.TableId
	results, err := ex.bt.RangeScan(encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)))
	if err != nil {
		return nil, fmt.Errorf("range scan over table %d: %w", tableId, err)
	}

	for _, r := range results {
		if evaluateExpression(s.Where, schema, r.Fields) {
			if err := ex.tm.Lock(txnId, r.Key, lock.LockExclusive); err != nil {
				return nil, fmt.Errorf("lock row %d: %w", r.Key, err)
			}

			oldFields := make([]btree.Field, len(r.Fields))
			copy(oldFields, r.Fields)
			ex.tm.AppendUndo(txnId, lock.UndoEntry{Op: lock.UndoDelete, Key: r.Key, Fields: oldFields})

			if err := ex.bt.Delete(r.Key); err != nil {
				return nil, fmt.Errorf("delete on key %d: %w", r.Key, err)
			}
		}
	}

	return nil, nil
}

// executeCreate executes a create statement
func (ex *Executor) executeCreate(s *CreateTableStatement) (*ResultSet, error) {
	schema := ex.sc.FindTableSchema(s.Table)
	if schema != nil {
		return nil, fmt.Errorf("table %q already exists", s.Table)
	}

	pkName := s.Columns[0].Name
	pkType := s.Columns[0].DataType

	columnNames := []string{}
	columnTypes := []string{}

	for i := range len(s.Columns) - 1 {
		columnNames = append(columnNames, s.Columns[i+1].Name)
		columnTypes = append(columnTypes, s.Columns[i+1].DataType)
	}

	if err := ex.sc.CreateTable(s.Table, pkName, pkType, columnNames, columnTypes); err != nil {
		return nil, fmt.Errorf("create table %q: %w", s.Table, err)
	}

	return nil, nil
}

// executeDrop executes a drop statement
func (ex *Executor) executeDrop(s *DropTableStatement) (*ResultSet, error) {
	if err := ex.sc.DropTable(s.Table); err != nil {
		return nil, fmt.Errorf("drop table %q: %w", s.Table, err)
	}

	return nil, nil
}

func NewExecutor(sc *SchemaCatalog, bt *btree.BTree, tm *lock.TransactionManager) *Executor {
	return &Executor{sc: sc, bt: bt, tm: tm}
}

// Entrypoint
func (ex *Executor) Execute(stmt Statement, txnId uint64) (*ResultSet, error) {
	switch s := stmt.(type) {
	case *SelectStatement:
		return ex.executeSelect(s, txnId)
	case *InsertStatement:
		return ex.executeInsert(s, txnId)
	case *UpdateStatement:
		return ex.executeUpdate(s, txnId)
	case *DeleteStatement:
		return ex.executeDelete(s, txnId)
	case *CreateTableStatement:
		return ex.executeCreate(s)
	case *DropTableStatement:
		return ex.executeDrop(s)
	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}
}
