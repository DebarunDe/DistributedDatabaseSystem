package sqllayer

import (
	"fmt"
	"sync"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

type TableSchemaValue struct {
	TableId    uint32
	PrimaryKey ColumnDef
	Columns    []ColumnDef // excludes primary key, ordered by index
}

type SchemaCatalog struct {
	mu         sync.RWMutex
	bt         *btree.BTree
	cache      map[string]*TableSchemaValue
	maxTableId uint32
}

// constructor
func NewSchemaCatalog(bt *btree.BTree) *SchemaCatalog {
	return &SchemaCatalog{
		bt:    bt,
		cache: make(map[string]*TableSchemaValue),
	}
}

// encodes table keys with primary key information
func encodeKey(tableId uint32, primaryKey uint32) uint64 {
	return (uint64(tableId) << 32) | uint64(primaryKey)
}

// LoadSchemas rangescans over table 0's key range, decodes each row into TableSchemaValue, populates the cache, and tracks max table id
func (sc *SchemaCatalog) LoadSchemas() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	results, err := sc.bt.RangeScan(encodeKey(0, 0), encodeKey(0, ^uint32(0)))
	if err != nil {
		return fmt.Errorf("range scan over table 0: %w", err)
	}

	for _, r := range results {
		if len(r.Fields) < 5 {
			return fmt.Errorf("schema row for key %d has %d fields, expected 5", r.Key, len(r.Fields))
		}

		//tableId is lower 32 bits of key
		tableId := uint32(r.Key)
		sc.maxTableId = max(sc.maxTableId, tableId)

		// get table name
		sv, ok := r.Fields[0].Value.(btree.StringValue)
		if !ok {
			return fmt.Errorf("expected string value for table name")
		}
		tableName := sv.V

		//get pk name and type
		sv, ok = r.Fields[1].Value.(btree.StringValue)
		if !ok {
			return fmt.Errorf("expected string value for primarykey name")
		}
		pkName := sv.V
		sv, ok = r.Fields[2].Value.(btree.StringValue)
		if !ok {
			return fmt.Errorf("expected string value for primarykey type")
		}
		pkType := sv.V

		primaryKey := ColumnDef{
			Name:     pkName,
			DataType: pkType,
		}

		//get column names and types
		lv, ok := r.Fields[3].Value.(btree.ListValue)
		if !ok {
			return fmt.Errorf("expected list value for column names")
		}
		columnNames := lv.Elems
		lv, ok = r.Fields[4].Value.(btree.ListValue)
		if !ok {
			return fmt.Errorf("expected list value for column types")
		}
		columnTypes := lv.Elems

		if len(columnNames) != len(columnTypes) {
			return fmt.Errorf("schema row for key %d: column name/type count mismatch (%d vs %d)", r.Key, len(columnNames), len(columnTypes))
		}

		//turn columnNames and columnTypes into array of columnDefs
		columns := []ColumnDef{}
		for i := range len(columnNames) {
			name, ok := columnNames[i].(btree.StringValue)
			if !ok {
				return fmt.Errorf("expected string value for column Name")
			}
			datatype, ok := columnTypes[i].(btree.StringValue)
			if !ok {
				return fmt.Errorf("expected string value for column datatype")
			}
			columns = append(columns, ColumnDef{
				Name:     name.V,
				DataType: datatype.V,
			})
		}

		//Populate cache
		sc.cache[tableName] = &TableSchemaValue{
			TableId:    tableId,
			PrimaryKey: primaryKey,
			Columns:    columns,
		}
	}

	return nil
}

// NextTableId returns the table ID that will be assigned to the next CREATE TABLE.
func (sc *SchemaCatalog) NextTableId() uint32 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.maxTableId + 1
}

// CreateTable validates that table name to be added does not exist, assigns new table id, encodes as catalog row, and adds to cache
func (sc *SchemaCatalog) CreateTable(tableName string, primaryKeyName string, primaryKeyType string, columnNames []string, columnTypes []string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	//validate tableName not in cache
	if _, ok := sc.cache[tableName]; ok {
		return fmt.Errorf("table Name: %s already exists", tableName)
	}

	if len(columnNames) != len(columnTypes) {
		return fmt.Errorf("mismatched columnNames length and columnTypes length, received columnName length: %d, columnType length: %d", len(columnNames), len(columnTypes))
	}

	//assign new table id
	newTableId := sc.maxTableId + 1

	//encode as catalog row
	key := encodeKey(0, newTableId)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: tableName}},
		{Tag: 2, Value: btree.StringValue{V: primaryKeyName}},
		{Tag: 3, Value: btree.StringValue{V: primaryKeyType}},
	}

	nameElems := make([]btree.Value, len(columnNames))
	typeElems := make([]btree.Value, len(columnTypes))
	for i := range len(columnNames) {
		nameElems[i] = btree.StringValue{V: columnNames[i]}
		typeElems[i] = btree.StringValue{V: columnTypes[i]}
	}
	fields = append(fields, btree.Field{
		Tag:   4,
		Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: nameElems},
	})
	fields = append(fields, btree.Field{
		Tag:   5,
		Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: typeElems},
	})

	//Insert first, if failed we return without polluting cache
	err := sc.bt.Insert(key, fields)
	if err != nil {
		return fmt.Errorf("error inserting table into BTree: %w", err)
	}

	sc.maxTableId = newTableId

	//insert into cache
	PrimaryKey := ColumnDef{
		Name:     primaryKeyName,
		DataType: primaryKeyType,
	}

	//turn columnNames and columnTypes into array of columnDefs
	columns := []ColumnDef{}
	for i := range len(columnNames) {
		name := columnNames[i]
		datatype := columnTypes[i]
		columns = append(columns, ColumnDef{
			Name:     name,
			DataType: datatype,
		})
	}

	sc.cache[tableName] = &TableSchemaValue{
		TableId:    newTableId,
		PrimaryKey: PrimaryKey,
		Columns:    columns,
	}

	return nil
}

// DropTable checks the cache, deletes the key if present, and removes from cache
func (sc *SchemaCatalog) DropTable(tableName string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	//validate tableName in cache
	if _, ok := sc.cache[tableName]; !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	//get rid of data rows
	tableId := sc.cache[tableName].TableId
	rows, err := sc.bt.RangeScan(encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)))
	if err != nil {
		return fmt.Errorf("scanning table rows: %w", err)
	}

	for _, r := range rows {
		if err := sc.bt.Delete(r.Key); err != nil {
			return fmt.Errorf("deleting row %d: %w", r.Key, err)
		}
	}

	//get rid of schema row
	if err := sc.bt.Delete(encodeKey(0, tableId)); err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	//get rid of cache row
	delete(sc.cache, tableName)

	return nil
}

// FindTableSchema searches the cache for tableName
func (sc *SchemaCatalog) FindTableSchema(tableName string) *TableSchemaValue {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.cache[tableName]
}
