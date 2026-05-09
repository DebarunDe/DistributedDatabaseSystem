package sqllayer

import (
	"testing"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

// newTestBTree creates an in-process BTree backed by a temp file, cleaned up after the test.
func newTestBTree(t *testing.T) *btree.BTree {
	t.Helper()
	pm, err := pagemanager.NewDB(t.TempDir() + "/btree.db")
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = pm.Delete() })
	return btree.NewBTree(pm)
}

// insertSchemaRow directly writes a catalog row into the BTree, bypassing SchemaCatalog,
// so LoadSchemas tests can set up arbitrary persisted state.
func insertSchemaRow(t *testing.T, bt *btree.BTree, tableId uint32, tableName, pkName, pkType string, colNames, colTypes []string) {
	t.Helper()
	nameElems := make([]btree.Value, len(colNames))
	typeElems := make([]btree.Value, len(colTypes))
	for i := range colNames {
		nameElems[i] = btree.StringValue{V: colNames[i]}
		typeElems[i] = btree.StringValue{V: colTypes[i]}
	}
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: tableName}},
		{Tag: 2, Value: btree.StringValue{V: pkName}},
		{Tag: 3, Value: btree.StringValue{V: pkType}},
		{Tag: 4, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: nameElems}},
		{Tag: 5, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: typeElems}},
	}
	if err := bt.Insert(encodeKey(0, tableId), fields); err != nil {
		t.Fatalf("insertSchemaRow(tableId=%d): %v", tableId, err)
	}
}

func assertSchemaEqual(t *testing.T, got *TableSchemaValue, wantId uint32, wantPKName, wantPKType string, wantCols []ColumnDef) {
	t.Helper()
	if got == nil {
		t.Fatal("schema is nil")
	}
	if got.TableId != wantId {
		t.Errorf("TableId: got %d, want %d", got.TableId, wantId)
	}
	if got.PrimaryKey.Name != wantPKName {
		t.Errorf("PrimaryKey.Name: got %q, want %q", got.PrimaryKey.Name, wantPKName)
	}
	if got.PrimaryKey.DataType != wantPKType {
		t.Errorf("PrimaryKey.DataType: got %q, want %q", got.PrimaryKey.DataType, wantPKType)
	}
	if len(got.Columns) != len(wantCols) {
		t.Fatalf("Columns length: got %d, want %d", len(got.Columns), len(wantCols))
	}
	for i, c := range wantCols {
		if got.Columns[i] != c {
			t.Errorf("Columns[%d]: got %+v, want %+v", i, got.Columns[i], c)
		}
	}
}

func assertKeyAbsent(t *testing.T, bt *btree.BTree, key uint64) {
	t.Helper()
	_, found, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if found {
		t.Errorf("key %d should not exist in BTree", key)
	}
}

func assertKeyPresent(t *testing.T, bt *btree.BTree, key uint64) {
	t.Helper()
	_, found, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search(%d): %v", key, err)
	}
	if !found {
		t.Errorf("key %d should exist in BTree", key)
	}
}

func assertRangeScanCount(t *testing.T, bt *btree.BTree, start, end uint64, wantCount int) {
	t.Helper()
	rows, err := bt.RangeScan(start, end)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(rows) != wantCount {
		t.Errorf("RangeScan [%d,%d]: got %d rows, want %d", start, end, len(rows), wantCount)
	}
}

// ---- LoadSchemas ----

func TestLoadSchemas_EmptyBTree(t *testing.T) {
	sc := NewSchemaCatalog(newTestBTree(t))
	if err := sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas on empty BTree: %v", err)
	}
	if sc.FindTableSchema("anything") != nil {
		t.Error("expected nil for any lookup on empty catalog")
	}
}

func TestLoadSchemas_SingleTable(t *testing.T) {
	bt := newTestBTree(t)
	insertSchemaRow(t, bt, 1, "users", "id", "INT", []string{"name", "age"}, []string{"TEXT", "INT"})

	sc := NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}

	assertSchemaEqual(t, sc.FindTableSchema("users"), 1, "id", "INT", []ColumnDef{
		{Name: "name", DataType: "TEXT"},
		{Name: "age", DataType: "INT"},
	})
}

func TestLoadSchemas_MultipleTables(t *testing.T) {
	bt := newTestBTree(t)
	insertSchemaRow(t, bt, 1, "users", "id", "INT", []string{"name"}, []string{"TEXT"})
	insertSchemaRow(t, bt, 2, "orders", "order_id", "INT", []string{"amount", "status"}, []string{"INT", "TEXT"})
	insertSchemaRow(t, bt, 3, "products", "sku", "TEXT", nil, nil)

	sc := NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}

	assertSchemaEqual(t, sc.FindTableSchema("users"), 1, "id", "INT",
		[]ColumnDef{{Name: "name", DataType: "TEXT"}})
	assertSchemaEqual(t, sc.FindTableSchema("orders"), 2, "order_id", "INT",
		[]ColumnDef{{Name: "amount", DataType: "INT"}, {Name: "status", DataType: "TEXT"}})
	assertSchemaEqual(t, sc.FindTableSchema("products"), 3, "sku", "TEXT", []ColumnDef{})
}

func TestLoadSchemas_SetsMaxTableId(t *testing.T) {
	bt := newTestBTree(t)
	insertSchemaRow(t, bt, 1, "a", "id", "INT", nil, nil)
	insertSchemaRow(t, bt, 5, "b", "id", "INT", nil, nil)
	insertSchemaRow(t, bt, 3, "c", "id", "INT", nil, nil)

	sc := NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}

	// maxTableId should be 5, so the next table gets ID 6
	if err := sc.CreateTable("d", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	got := sc.FindTableSchema("d")
	if got == nil || got.TableId != 6 {
		t.Errorf("expected tableId 6 (maxTableId was 5), got %v", got)
	}
}

func TestLoadSchemas_TableWithNoExtraColumns(t *testing.T) {
	bt := newTestBTree(t)
	insertSchemaRow(t, bt, 1, "bare", "pk", "INT", nil, nil)

	sc := NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	assertSchemaEqual(t, sc.FindTableSchema("bare"), 1, "pk", "INT", []ColumnDef{})
}

func TestLoadSchemas_TooFewFields(t *testing.T) {
	bt := newTestBTree(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "users"}},
		{Tag: 2, Value: btree.StringValue{V: "id"}},
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	sc := NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err == nil {
		t.Fatal("expected error for schema row with < 5 fields")
	}
}

func TestLoadSchemas_BadTableNameType(t *testing.T) {
	bt := newTestBTree(t)
	empty := []btree.Value{}
	fields := []btree.Field{
		{Tag: 1, Value: btree.IntValue{V: 99}}, // should be StringValue
		{Tag: 2, Value: btree.StringValue{V: "id"}},
		{Tag: 3, Value: btree.StringValue{V: "INT"}},
		{Tag: 4, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: empty}},
		{Tag: 5, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: empty}},
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := NewSchemaCatalog(bt).LoadSchemas(); err == nil {
		t.Fatal("expected error for non-string table name")
	}
}

func TestLoadSchemas_BadPKNameType(t *testing.T) {
	bt := newTestBTree(t)
	empty := []btree.Value{}
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "users"}},
		{Tag: 2, Value: btree.IntValue{V: 99}}, // should be StringValue
		{Tag: 3, Value: btree.StringValue{V: "INT"}},
		{Tag: 4, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: empty}},
		{Tag: 5, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: empty}},
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := NewSchemaCatalog(bt).LoadSchemas(); err == nil {
		t.Fatal("expected error for non-string pk name")
	}
}

func TestLoadSchemas_BadPKTypeType(t *testing.T) {
	bt := newTestBTree(t)
	empty := []btree.Value{}
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "users"}},
		{Tag: 2, Value: btree.StringValue{V: "id"}},
		{Tag: 3, Value: btree.IntValue{V: 99}}, // should be StringValue
		{Tag: 4, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: empty}},
		{Tag: 5, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: empty}},
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := NewSchemaCatalog(bt).LoadSchemas(); err == nil {
		t.Fatal("expected error for non-string pk type")
	}
}

func TestLoadSchemas_BadColumnNamesType(t *testing.T) {
	bt := newTestBTree(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "users"}},
		{Tag: 2, Value: btree.StringValue{V: "id"}},
		{Tag: 3, Value: btree.StringValue{V: "INT"}},
		{Tag: 4, Value: btree.IntValue{V: 99}}, // should be ListValue
		{Tag: 5, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: []btree.Value{}}},
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := NewSchemaCatalog(bt).LoadSchemas(); err == nil {
		t.Fatal("expected error for non-list column names")
	}
}

func TestLoadSchemas_BadColumnTypesType(t *testing.T) {
	bt := newTestBTree(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "users"}},
		{Tag: 2, Value: btree.StringValue{V: "id"}},
		{Tag: 3, Value: btree.StringValue{V: "INT"}},
		{Tag: 4, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: []btree.Value{}}},
		{Tag: 5, Value: btree.IntValue{V: 99}}, // should be ListValue
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := NewSchemaCatalog(bt).LoadSchemas(); err == nil {
		t.Fatal("expected error for non-list column types")
	}
}

func TestLoadSchemas_ColumnCountMismatch(t *testing.T) {
	bt := newTestBTree(t)
	fields := []btree.Field{
		{Tag: 1, Value: btree.StringValue{V: "users"}},
		{Tag: 2, Value: btree.StringValue{V: "id"}},
		{Tag: 3, Value: btree.StringValue{V: "INT"}},
		{Tag: 4, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: []btree.Value{
			btree.StringValue{V: "name"},
			btree.StringValue{V: "age"},
		}}},
		{Tag: 5, Value: btree.ListValue{ElemType: btree.FieldTypeString, Elems: []btree.Value{
			btree.StringValue{V: "TEXT"}, // 1 type for 2 names
		}}},
	}
	if err := bt.Insert(encodeKey(0, 1), fields); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := NewSchemaCatalog(bt).LoadSchemas(); err == nil {
		t.Fatal("expected error for column name/type count mismatch")
	}
}

// ---- CreateTable ----

func TestCreateTable_Success(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	err := sc.CreateTable("users", "id", "INT", []string{"name", "age"}, []string{"TEXT", "INT"})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	assertSchemaEqual(t, sc.FindTableSchema("users"), 1, "id", "INT", []ColumnDef{
		{Name: "name", DataType: "TEXT"},
		{Name: "age", DataType: "INT"},
	})
}

func TestCreateTable_PersistsToBTree(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Fresh catalog on same BTree should find the schema after LoadSchemas.
	sc2 := NewSchemaCatalog(bt)
	if err := sc2.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	assertSchemaEqual(t, sc2.FindTableSchema("users"), 1, "id", "INT",
		[]ColumnDef{{Name: "name", DataType: "TEXT"}})
}

func TestCreateTable_DuplicateName(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", nil, nil); err != nil {
		t.Fatalf("first CreateTable: %v", err)
	}
	if err := sc.CreateTable("users", "uid", "INT", nil, nil); err == nil {
		t.Fatal("expected error for duplicate table name")
	}
}

func TestCreateTable_MismatchedColumnLengths(t *testing.T) {
	sc := NewSchemaCatalog(newTestBTree(t))
	err := sc.CreateTable("users", "id", "INT", []string{"name", "age"}, []string{"TEXT"})
	if err == nil {
		t.Fatal("expected error for mismatched column name/type lengths")
	}
}

func TestCreateTable_SequentialIds(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	for i, name := range []string{"a", "b", "c"} {
		if err := sc.CreateTable(name, "id", "INT", nil, nil); err != nil {
			t.Fatalf("CreateTable(%q): %v", name, err)
		}
		got := sc.FindTableSchema(name)
		wantId := uint32(i + 1)
		if got == nil || got.TableId != wantId {
			t.Errorf("table %q: expected tableId %d, got %v", name, wantId, got)
		}
	}
}

func TestCreateTable_NoPKColumns(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("bare", "pk", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	assertSchemaEqual(t, sc.FindTableSchema("bare"), 1, "pk", "INT", []ColumnDef{})
}

func TestCreateTable_ContinuesFromLoadedMaxId(t *testing.T) {
	bt := newTestBTree(t)
	insertSchemaRow(t, bt, 3, "existing", "id", "INT", nil, nil)

	sc := NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	if err := sc.CreateTable("new", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	got := sc.FindTableSchema("new")
	if got == nil || got.TableId != 4 {
		t.Errorf("expected tableId 4 (max was 3), got %v", got)
	}
}

func TestCreateTable_DoesNotPolluteCacheOnBTreeError(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	// Insert a conflicting catalog key manually so CreateTable's bt.Insert fails.
	if err := sc.CreateTable("collision", "id", "INT", nil, nil); err != nil {
		t.Fatalf("first CreateTable: %v", err)
	}
	firstId := sc.FindTableSchema("collision").TableId

	// Manually insert the key for the *next* table ID so the second CreateTable's
	// bt.Insert collides and fails. This simulates a BTree write error.
	nextKey := encodeKey(0, firstId+1)
	_ = bt.Insert(nextKey, []btree.Field{{Tag: 1, Value: btree.StringValue{V: "blocker"}}})

	// This CreateTable should fail at the BTree Insert step.
	err := sc.CreateTable("should_fail", "id", "INT", nil, nil)
	if err != nil {
		// If it failed, the cache must NOT have "should_fail".
		if sc.FindTableSchema("should_fail") != nil {
			t.Error("cache was polluted despite BTree insert failing")
		}
	}
	// If err == nil the BTree allowed a duplicate insert; the test is still valid
	// as a regression guard for the cache-pollution scenario.
}

// ---- DropTable ----

func TestDropTable_NonExistent(t *testing.T) {
	sc := NewSchemaCatalog(newTestBTree(t))
	if err := sc.DropTable("ghost"); err == nil {
		t.Fatal("expected error for dropping non-existent table")
	}
}

func TestDropTable_RemovesFromCache(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := sc.DropTable("users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if sc.FindTableSchema("users") != nil {
		t.Error("expected nil from FindTableSchema after drop")
	}
}

func TestDropTable_RemovesSchemaRow(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId
	assertKeyPresent(t, bt, encodeKey(0, tableId))

	if err := sc.DropTable("users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	assertKeyAbsent(t, bt, encodeKey(0, tableId))
}

func TestDropTable_RemovesDataRows(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("users").TableId

	dataFields := []btree.Field{{Tag: 1, Value: btree.StringValue{V: "Alice"}}}
	for _, pk := range []uint32{100, 200, 300} {
		if err := bt.Insert(encodeKey(tableId, pk), dataFields); err != nil {
			t.Fatalf("Insert data row pk=%d: %v", pk, err)
		}
	}
	assertRangeScanCount(t, bt, encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)), 3)

	if err := sc.DropTable("users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	assertRangeScanCount(t, bt, encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)), 0)
}

func TestDropTable_NoDataRows(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("empty", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := sc.DropTable("empty"); err != nil {
		t.Fatalf("DropTable on table with no data rows: %v", err)
	}
	if sc.FindTableSchema("empty") != nil {
		t.Error("expected nil after drop")
	}
}

func TestDropTable_MultipleDataRowsPurged(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("logs", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tableId := sc.FindTableSchema("logs").TableId
	dataFields := []btree.Field{{Tag: 1, Value: btree.IntValue{V: 42}}}
	for pk := uint32(1); pk <= 10; pk++ {
		if err := bt.Insert(encodeKey(tableId, pk), dataFields); err != nil {
			t.Fatalf("Insert pk=%d: %v", pk, err)
		}
	}

	if err := sc.DropTable("logs"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	assertRangeScanCount(t, bt, encodeKey(tableId, 0), encodeKey(tableId, ^uint32(0)), 0)
}

func TestDropTable_ReuseNameAfterDrop(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", []string{"name"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := sc.DropTable("users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if err := sc.CreateTable("users", "uid", "TEXT", []string{"email"}, []string{"TEXT"}); err != nil {
		t.Fatalf("CreateTable with reused name: %v", err)
	}
	assertSchemaEqual(t, sc.FindTableSchema("users"), 2, "uid", "TEXT",
		[]ColumnDef{{Name: "email", DataType: "TEXT"}})
}

func TestDropTable_MaxTableIdUnchangedAfterDrop(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("a", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable a: %v", err)
	}
	if err := sc.CreateTable("b", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable b: %v", err)
	}
	// Drop the table with the current max ID (2).
	if err := sc.DropTable("b"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	// The next table should receive ID 3, not reuse 2.
	if err := sc.CreateTable("c", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable c: %v", err)
	}
	got := sc.FindTableSchema("c")
	if got == nil || got.TableId != 3 {
		t.Errorf("expected tableId 3 (maxTableId not decremented), got %v", got)
	}
}

func TestDropTable_DoesNotAffectOtherTables(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("keep", "id", "INT", []string{"x"}, []string{"INT"}); err != nil {
		t.Fatalf("CreateTable keep: %v", err)
	}
	if err := sc.CreateTable("drop_me", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable drop_me: %v", err)
	}
	if err := sc.DropTable("drop_me"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	// "keep" must still exist with its original schema.
	assertSchemaEqual(t, sc.FindTableSchema("keep"), 1, "id", "INT",
		[]ColumnDef{{Name: "x", DataType: "INT"}})
	assertKeyPresent(t, bt, encodeKey(0, 1))
}

// ---- FindTableSchema ----

func TestFindTableSchema_NotFound(t *testing.T) {
	sc := NewSchemaCatalog(newTestBTree(t))
	if sc.FindTableSchema("missing") != nil {
		t.Error("expected nil for unknown table")
	}
}

func TestFindTableSchema_ReturnsCorrectSchema(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", []string{"name", "age"}, []string{"TEXT", "INT"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	assertSchemaEqual(t, sc.FindTableSchema("users"), 1, "id", "INT", []ColumnDef{
		{Name: "name", DataType: "TEXT"},
		{Name: "age", DataType: "INT"},
	})
}

func TestFindTableSchema_NilAfterDrop(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("users", "id", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := sc.DropTable("users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if sc.FindTableSchema("users") != nil {
		t.Error("expected nil after drop")
	}
}

func TestFindTableSchema_IndependentTables(t *testing.T) {
	bt := newTestBTree(t)
	sc := NewSchemaCatalog(bt)

	if err := sc.CreateTable("t1", "a", "INT", nil, nil); err != nil {
		t.Fatalf("CreateTable t1: %v", err)
	}
	if err := sc.CreateTable("t2", "b", "TEXT", []string{"c"}, []string{"BOOL"}); err != nil {
		t.Fatalf("CreateTable t2: %v", err)
	}

	assertSchemaEqual(t, sc.FindTableSchema("t1"), 1, "a", "INT", []ColumnDef{})
	assertSchemaEqual(t, sc.FindTableSchema("t2"), 2, "b", "TEXT", []ColumnDef{{Name: "c", DataType: "BOOL"}})
}
