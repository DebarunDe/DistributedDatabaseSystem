// Package integration tests the full database stack:
// BufferPool → WAL → PageManager → BTree → SchemaCatalog → Executor.
// Every test that performs a close/reopen exercises real disk persistence,
// WAL truncation, and recovery — the layers skipped by the executor unit tests.
package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

// ---- Infrastructure ----

const defaultCacheSize = 256

// testDB is a fully wired database session identical to the REPL's stack.
type testDB struct {
	ex   *sqllayer.Executor
	sc   *sqllayer.SchemaCatalog
	tm   *lock.TransactionManager
	pm   pagemanager.PageManager // outermost layer (BufferPool)
	path string
}

// newTestDB creates a fresh database in a temp directory and registers cleanup.
func newTestDB(t *testing.T) *testDB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db := createAt(t, path)
	t.Cleanup(func() { _ = os.Remove(path); _ = os.Remove(path + "_WAL") })
	return db
}

// createAt creates a brand-new database file at path.
func createAt(t *testing.T, path string) *testDB {
	t.Helper()
	disk, err := pagemanager.NewDB(path)
	if err != nil {
		t.Fatalf("NewDB(%q): %v", path, err)
	}
	return wrap(t, disk, path, false)
}

// reopenAt opens an existing database, running WAL recovery automatically.
func reopenAt(t *testing.T, path string) *testDB {
	t.Helper()
	disk, err := pagemanager.OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB(%q): %v", path, err)
	}
	return wrap(t, disk, path, true)
}

// wrap layers WAL + BufferPool + BTree + SchemaCatalog + Executor on top of disk.
// loadSchemas=true is used on reopen so the schema cache is populated from disk.
func wrap(t *testing.T, disk pagemanager.PageManager, path string, loadSchemas bool) *testDB {
	t.Helper()
	wal, err := pagemanager.NewWAL(disk, path)
	if err != nil {
		_ = disk.Close()
		t.Fatalf("NewWAL: %v", err)
	}
	bp := pagemanager.NewBufferPool(wal, defaultCacheSize)
	bt := btree.NewBTree(bp)
	sc := sqllayer.NewSchemaCatalog(bt)
	if loadSchemas {
		if err := sc.LoadSchemas(); err != nil {
			_ = bp.Close()
			t.Fatalf("LoadSchemas: %v", err)
		}
	}
	tm := lock.NewTransactionManager(bt)
	return &testDB{
		ex:   sqllayer.NewExecutor(sc, bt, tm),
		sc:   sc,
		tm:   tm,
		pm:   bp,
		path: path,
	}
}

// close flushes all dirty buffer pool pages → WAL → disk, then truncates the WAL.
func (db *testDB) close(t *testing.T) {
	t.Helper()
	if err := db.pm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// exec runs a SQL string end-to-end, failing the test on any error.
func (db *testDB) exec(t *testing.T, query string) *sqllayer.ResultSet {
	t.Helper()
	rs, err := db.run(query)
	if err != nil {
		t.Fatalf("exec(%q): %v", query, err)
	}
	return rs
}

// mustFail runs a SQL string and fails the test if it does NOT return an error.
func (db *testDB) mustFail(t *testing.T, query string) {
	t.Helper()
	_, err := db.run(query)
	if err == nil {
		t.Fatalf("mustFail(%q): expected an error but got none", query)
	}
}

// run tokenises, parses, and executes a SQL string. Returns the raw error.
func (db *testDB) run(query string) (*sqllayer.ResultSet, error) {
	tokens, err := sqllayer.Tokenize(query)
	if err != nil {
		return nil, err
	}
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		return nil, err
	}
	txn := db.tm.Begin()
	rs, err := db.ex.Execute(stmt, txn.Id)
	if err != nil {
		db.tm.Rollback(txn.Id)
		return nil, err
	}
	db.tm.Commit(txn.Id)
	return rs, nil
}

// ---- Assertion helpers ----

func assertRowCount(t *testing.T, rs *sqllayer.ResultSet, want int) {
	t.Helper()
	if rs == nil {
		t.Fatalf("ResultSet is nil, want %d rows", want)
	}
	if len(rs.Rows) != want {
		t.Errorf("row count: got %d, want %d", len(rs.Rows), want)
	}
}

func assertColumns(t *testing.T, rs *sqllayer.ResultSet, want []string) {
	t.Helper()
	if len(rs.Columns) != len(want) {
		t.Fatalf("columns: got %v, want %v", rs.Columns, want)
	}
	for i, col := range want {
		if rs.Columns[i] != col {
			t.Errorf("column[%d]: got %q, want %q", i, rs.Columns[i], col)
		}
	}
}

func intVal(t *testing.T, f btree.Field) int64 {
	t.Helper()
	v, ok := f.Value.(btree.IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T", f.Value)
	}
	return v.V
}

func strVal(t *testing.T, f btree.Field) string {
	t.Helper()
	v, ok := f.Value.(btree.StringValue)
	if !ok {
		t.Fatalf("expected StringValue, got %T", f.Value)
	}
	return v.V
}

// ---- WAL file helpers ----

func walPath(dbPath string) string { return dbPath + "_WAL" }

func walSize(t *testing.T, dbPath string) int64 {
	t.Helper()
	info, err := os.Stat(walPath(dbPath))
	if os.IsNotExist(err) {
		return -1
	}
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	return info.Size()
}

// ---- CRUD through full stack (single session, no reopen) ----

func TestFullStack_InsertSelect(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice', 30)")
	db.exec(t, "INSERT INTO users VALUES (2, 'bob', 25)")

	rs := db.exec(t, "SELECT * FROM users")
	assertRowCount(t, rs, 2)
	assertColumns(t, rs, []string{"id", "name", "age"})
}

func TestFullStack_InsertUpdateSelect(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice', 30)")
	db.exec(t, "UPDATE users SET age = 31 WHERE id = 1")

	rs := db.exec(t, "SELECT age FROM users WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 31 {
		t.Errorf("expected age=31 after update")
	}
}

func TestFullStack_InsertDeleteSelect(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice', 30)")
	db.exec(t, "INSERT INTO users VALUES (2, 'bob', 25)")
	db.exec(t, "DELETE FROM users WHERE id = 1")

	rs := db.exec(t, "SELECT * FROM users")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 2 {
		t.Errorf("expected only id=2 to remain")
	}
}

func TestFullStack_BoolColumn(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE flags (id INT, active BOOL)")
	db.exec(t, "INSERT INTO flags VALUES (1, true)")
	db.exec(t, "INSERT INTO flags VALUES (2, false)")

	rs := db.exec(t, "SELECT * FROM flags WHERE active = true")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 1 {
		t.Errorf("expected id=1")
	}
}

func TestFullStack_CreateDropCreate(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, val TEXT)")
	db.exec(t, "INSERT INTO t VALUES (1, 'hello')")
	db.exec(t, "DROP TABLE t")
	db.exec(t, "CREATE TABLE t (id INT, num INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 42)")

	rs := db.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"id", "num"})
	if intVal(t, rs.Rows[0].Fields[1]) != 42 {
		t.Errorf("expected num=42")
	}
}

func TestFullStack_MultipleTablesIsolated(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE users (id INT, name TEXT)")
	db.exec(t, "CREATE TABLE products (id INT, title TEXT)")

	// Insert id=1 into both tables
	db.exec(t, "INSERT INTO users VALUES (1, 'alice')")
	db.exec(t, "INSERT INTO products VALUES (1, 'widget')")

	rsU := db.exec(t, "SELECT * FROM users")
	rsP := db.exec(t, "SELECT * FROM products")

	assertRowCount(t, rsU, 1)
	assertRowCount(t, rsP, 1)
	if strVal(t, rsU.Rows[0].Fields[1]) != "alice" {
		t.Error("users table corrupted")
	}
	if strVal(t, rsP.Rows[0].Fields[1]) != "widget" {
		t.Error("products table corrupted")
	}
}

// ---- Persistence: close then reopen ----

func TestPersistence_SchemaAloneReloads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	schema := db2.sc.FindTableSchema("users")
	if schema == nil {
		t.Fatal("schema not found after reopen")
	}
	if schema.PrimaryKey.Name != "id" {
		t.Errorf("PK name: got %q, want %q", schema.PrimaryKey.Name, "id")
	}
	if len(schema.Columns) != 2 {
		t.Errorf("column count: got %d, want 2", len(schema.Columns))
	}
}

func TestPersistence_InsertedRowsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice', 30)")
	db.exec(t, "INSERT INTO users VALUES (2, 'bob', 25)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM users")
	assertRowCount(t, rs, 2)
}

func TestPersistence_UpdateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice', 30)")
	db.exec(t, "UPDATE users SET age = 99 WHERE id = 1")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT age FROM users WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 99 {
		t.Errorf("expected age=99 after reopen, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}
}

func TestPersistence_DeletedRowsAbsentAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE users (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice', 30)")
	db.exec(t, "INSERT INTO users VALUES (2, 'bob', 25)")
	db.exec(t, "DELETE FROM users WHERE id = 1")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM users")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 2 {
		t.Errorf("expected only id=2 after reopen")
	}
}

func TestPersistence_DroppedTableAbsentAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE users (id INT, name TEXT)")
	db.exec(t, "INSERT INTO users VALUES (1, 'alice')")
	db.exec(t, "DROP TABLE users")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	if db2.sc.FindTableSchema("users") != nil {
		t.Error("dropped table schema should not reload after reopen")
	}
	db2.mustFail(t, "SELECT * FROM users")
}

func TestPersistence_MultipleTablesAllSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE a (id INT, x INT)")
	db.exec(t, "CREATE TABLE b (id INT, y TEXT)")
	db.exec(t, "INSERT INTO a VALUES (1, 100)")
	db.exec(t, "INSERT INTO a VALUES (2, 200)")
	db.exec(t, "INSERT INTO b VALUES (1, 'hello')")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rsA := db2.exec(t, "SELECT * FROM a")
	rsB := db2.exec(t, "SELECT * FROM b")
	assertRowCount(t, rsA, 2)
	assertRowCount(t, rsB, 1)
	if strVal(t, rsB.Rows[0].Fields[1]) != "hello" {
		t.Errorf("expected 'hello' in table b after reopen")
	}
}

func TestPersistence_DataValuesExactAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE items (id INT, label TEXT, count INT)")
	db.exec(t, "INSERT INTO items VALUES (42, 'widget', 7)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM items")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 42 {
		t.Errorf("id: expected 42, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}
	if strVal(t, rs.Rows[0].Fields[1]) != "widget" {
		t.Errorf("label: expected 'widget', got %q", strVal(t, rs.Rows[0].Fields[1]))
	}
	if intVal(t, rs.Rows[0].Fields[2]) != 7 {
		t.Errorf("count: expected 7, got %d", intVal(t, rs.Rows[0].Fields[2]))
	}
}

func TestPersistence_BoolColumnSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE flags (id INT, active BOOL)")
	db.exec(t, "INSERT INTO flags VALUES (1, true)")
	db.exec(t, "INSERT INTO flags VALUES (2, false)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM flags WHERE active = true")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 1 {
		t.Errorf("expected id=1 after reopen")
	}
}

func TestPersistence_MultipleReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE counters (id INT, val INT)")
	db.exec(t, "INSERT INTO counters VALUES (1, 10)")
	db.close(t)

	// Reopen, update, close, reopen again
	db2 := reopenAt(t, path)
	db2.exec(t, "UPDATE counters SET val = 20 WHERE id = 1")
	db2.close(t)

	db3 := reopenAt(t, path)
	defer db3.close(t)
	rs := db3.exec(t, "SELECT val FROM counters WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 20 {
		t.Errorf("expected val=20 after two reopen cycles")
	}
}

func TestPersistence_CreateDropRecreateWithDataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	// Session 1: create, insert, drop
	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, v TEXT)")
	db.exec(t, "INSERT INTO t VALUES (1, 'first')")
	db.exec(t, "DROP TABLE t")
	db.close(t)

	// Session 2: recreate with different schema, insert new data
	db2 := reopenAt(t, path)
	db2.exec(t, "CREATE TABLE t (id INT, n INT)")
	db2.exec(t, "INSERT INTO t VALUES (1, 999)")
	db2.close(t)

	// Session 3: verify only new schema + data is present
	db3 := reopenAt(t, path)
	defer db3.close(t)
	rs := db3.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"id", "n"})
	if intVal(t, rs.Rows[0].Fields[1]) != 999 {
		t.Errorf("expected n=999 after drop-recreate cycle")
	}
}

// ---- Large dataset: forces real BTree page splits and multi-page reads ----

func TestLargeDataset_ManyRowsSurviveSingleSession(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE big (id INT, val INT)")
	const n = 200
	for i := 1; i <= n; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO big VALUES (%d, %d)", i, i*10))
	}

	rs := db.exec(t, "SELECT * FROM big")
	assertRowCount(t, rs, n)
}

func TestLargeDataset_ManyRowsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE big (id INT, val INT)")
	const n = 200
	for i := 1; i <= n; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO big VALUES (%d, %d)", i, i*10))
	}
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM big")
	assertRowCount(t, rs, n)
}

func TestLargeDataset_ValuesIntactAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE nums (id INT, square INT)")
	const n = 150
	for i := 1; i <= n; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i, i*i))
	}
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	// Spot-check a few rows at the beginning, middle, and end
	for _, id := range []int{1, 50, 100, 150} {
		rs := db2.exec(t, fmt.Sprintf("SELECT square FROM nums WHERE id = %d", id))
		assertRowCount(t, rs, 1)
		want := int64(id * id)
		if intVal(t, rs.Rows[0].Fields[0]) != want {
			t.Errorf("id=%d: expected square=%d, got %d", id, want, intVal(t, rs.Rows[0].Fields[0]))
		}
	}
}

func TestLargeDataset_DeleteHalfThenReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE rows (id INT, v INT)")
	const n = 100
	for i := 1; i <= n; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO rows VALUES (%d, %d)", i, i))
	}
	// Delete even-numbered rows
	for i := 2; i <= n; i += 2 {
		db.exec(t, fmt.Sprintf("DELETE FROM rows WHERE id = %d", i))
	}
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM rows")
	assertRowCount(t, rs, n/2)
}

func TestLargeDataset_UpdateAllThenReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE items (id INT, score INT)")
	const n = 80
	for i := 1; i <= n; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO items VALUES (%d, 0)", i))
	}
	db.exec(t, "UPDATE items SET score = 100")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM items WHERE score = 100")
	assertRowCount(t, rs, n)
}

func TestLargeDataset_MultipleTablesLargeData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE a (id INT, av INT)")
	db.exec(t, "CREATE TABLE b (id INT, bv INT)")
	const n = 100
	for i := 1; i <= n; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO a VALUES (%d, %d)", i, i))
		db.exec(t, fmt.Sprintf("INSERT INTO b VALUES (%d, %d)", i, i*2))
	}
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rsA := db2.exec(t, "SELECT * FROM a")
	rsB := db2.exec(t, "SELECT * FROM b")
	assertRowCount(t, rsA, n)
	assertRowCount(t, rsB, n)

	// Spot-check that tables didn't bleed into each other
	rs := db2.exec(t, "SELECT bv FROM b WHERE id = 10")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 20 {
		t.Errorf("b.bv for id=10: expected 20, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}
}

// ---- Multi-table isolation ----

func TestMultiTable_OverlappingPKsDontInterfere(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE users (id INT, name TEXT)")
	db.exec(t, "CREATE TABLE orders (id INT, amount INT)")

	db.exec(t, "INSERT INTO users VALUES (1, 'alice')")
	db.exec(t, "INSERT INTO users VALUES (2, 'bob')")
	db.exec(t, "INSERT INTO orders VALUES (1, 500)")
	db.exec(t, "INSERT INTO orders VALUES (2, 750)")

	rsU := db.exec(t, "SELECT * FROM users")
	rsO := db.exec(t, "SELECT * FROM orders")

	assertRowCount(t, rsU, 2)
	assertRowCount(t, rsO, 2)

	// id=1 must refer to different records in each table
	rs := db.exec(t, "SELECT name FROM users WHERE id = 1")
	if strVal(t, rs.Rows[0].Fields[0]) != "alice" {
		t.Error("users id=1 should be alice")
	}
	rs = db.exec(t, "SELECT amount FROM orders WHERE id = 1")
	if intVal(t, rs.Rows[0].Fields[0]) != 500 {
		t.Error("orders id=1 should be amount=500")
	}
}

func TestMultiTable_DropOneTableDoesNotAffectOther(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE keep (id INT, val INT)")
	db.exec(t, "CREATE TABLE gone (id INT, val INT)")
	db.exec(t, "INSERT INTO keep VALUES (1, 42)")
	db.exec(t, "INSERT INTO gone VALUES (1, 99)")
	db.exec(t, "DROP TABLE gone")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	if db2.sc.FindTableSchema("gone") != nil {
		t.Error("dropped table should not exist after reopen")
	}
	rs := db2.exec(t, "SELECT val FROM keep WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 42 {
		t.Errorf("keep table should be unaffected by drop of gone")
	}
}

func TestMultiTable_UpdateOneTableDoesNotAffectOther(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE a (id INT, v INT)")
	db.exec(t, "CREATE TABLE b (id INT, v INT)")
	db.exec(t, "INSERT INTO a VALUES (1, 10)")
	db.exec(t, "INSERT INTO b VALUES (1, 20)")
	db.exec(t, "UPDATE a SET v = 99 WHERE id = 1")

	rs := db.exec(t, "SELECT v FROM b WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 20 {
		t.Error("table b should be unaffected by update to table a")
	}
}

func TestMultiTable_DeleteFromOneTableDoesNotAffectOther(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE a (id INT, v INT)")
	db.exec(t, "CREATE TABLE b (id INT, v INT)")
	db.exec(t, "INSERT INTO a VALUES (1, 10)")
	db.exec(t, "INSERT INTO b VALUES (1, 20)")
	db.exec(t, "DELETE FROM a WHERE id = 1")

	rs := db.exec(t, "SELECT * FROM b")
	assertRowCount(t, rs, 1)
}

func TestMultiTable_ManyTablesAllPersist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	tableCount := 10
	db := createAt(t, path)
	for i := 0; i < tableCount; i++ {
		db.exec(t, fmt.Sprintf("CREATE TABLE t%d (id INT, v INT)", i))
		db.exec(t, fmt.Sprintf("INSERT INTO t%d VALUES (1, %d)", i, i*100))
	}
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	for i := 0; i < tableCount; i++ {
		rs := db2.exec(t, fmt.Sprintf("SELECT v FROM t%d WHERE id = 1", i))
		assertRowCount(t, rs, 1)
		if intVal(t, rs.Rows[0].Fields[0]) != int64(i*100) {
			t.Errorf("t%d.v: expected %d, got %d", i, i*100, intVal(t, rs.Rows[0].Fields[0]))
		}
	}
}

// ---- WHERE clause correctness through full stack ----

func TestWhere_IntEquality(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")
	db.exec(t, "INSERT INTO t VALUES (2, 20)")
	db.exec(t, "INSERT INTO t VALUES (3, 30)")

	rs := db.exec(t, "SELECT * FROM t WHERE v = 20")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 2 {
		t.Error("expected id=2")
	}
}

func TestWhere_IntRange(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 10; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	rs := db.exec(t, "SELECT * FROM t WHERE v > 50")
	assertRowCount(t, rs, 5) // v=60,70,80,90,100
}

func TestWhere_StringEquality(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, name TEXT)")
	db.exec(t, "INSERT INTO t VALUES (1, 'alice')")
	db.exec(t, "INSERT INTO t VALUES (2, 'bob')")
	db.exec(t, "INSERT INTO t VALUES (3, 'alice')")

	rs := db.exec(t, "SELECT * FROM t WHERE name = 'alice'")
	assertRowCount(t, rs, 2)
}

func TestWhere_AndComposite(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, name TEXT, age INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 'alice', 30)")
	db.exec(t, "INSERT INTO t VALUES (2, 'alice', 25)")
	db.exec(t, "INSERT INTO t VALUES (3, 'bob', 30)")

	rs := db.exec(t, "SELECT * FROM t WHERE name = 'alice' AND age = 30")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 1 {
		t.Error("expected id=1 (alice, age=30)")
	}
}

func TestWhere_OrComposite(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 5; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*10))
	}

	rs := db.exec(t, "SELECT * FROM t WHERE v = 10 OR v = 50")
	assertRowCount(t, rs, 2)
}

func TestWhere_NoMatchReturnsEmpty(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")

	rs := db.exec(t, "SELECT * FROM t WHERE v = 999")
	assertRowCount(t, rs, 0)
}

func TestWhere_NoWhereClauseReturnsAll(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 20; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	rs := db.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 20)
}

// ---- Error propagation through the full stack ----

func TestError_InsertDuplicateKey(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")
	db.mustFail(t, "INSERT INTO t VALUES (1, 20)")

	// Original row should be unchanged
	rs := db.exec(t, "SELECT v FROM t WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 10 {
		t.Error("original row should be unchanged after duplicate key rejection")
	}
}

func TestError_InsertWrongValueCount(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, name TEXT, age INT)")
	db.mustFail(t, "INSERT INTO t VALUES (1, 'alice')")         // missing age
	db.mustFail(t, "INSERT INTO t VALUES (1, 'alice', 30, 99)") // extra value
}

func TestError_InsertTypeMismatch(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, age INT)")
	db.mustFail(t, "INSERT INTO t VALUES (1, 'not_a_number')")
}

func TestError_SelectUnknownTable(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)
	db.mustFail(t, "SELECT * FROM ghost")
}

func TestError_SelectUnknownColumn(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, name TEXT)")
	db.mustFail(t, "SELECT missing FROM t")
}

func TestError_UpdateUnknownTable(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)
	db.mustFail(t, "UPDATE ghost SET v = 1")
}

func TestError_UpdateUnknownColumn(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.mustFail(t, "UPDATE t SET ghost = 1")
}

func TestError_UpdateTypeMismatch(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, age INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 25)")
	db.mustFail(t, "UPDATE t SET age = 'hello'")
}

func TestError_DeleteUnknownTable(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)
	db.mustFail(t, "DELETE FROM ghost")
}

func TestError_CreateDuplicateTable(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT)")
	db.mustFail(t, "CREATE TABLE t (id INT)")
}

func TestError_DropNonExistentTable(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)
	db.mustFail(t, "DROP TABLE ghost")
}

func TestError_DuplicateKeyDoesNotCorruptTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")
	db.exec(t, "INSERT INTO t VALUES (2, 20)")
	// Attempt duplicate — should fail, leaving table intact
	_, _ = db.run("INSERT INTO t VALUES (1, 99)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 2)
	// id=1 must still have v=10, not v=99
	rs = db2.exec(t, "SELECT v FROM t WHERE id = 1")
	if intVal(t, rs.Rows[0].Fields[0]) != 10 {
		t.Error("failed duplicate insert must not have modified existing row")
	}
}

// ---- Schema catalog specifics ----

func TestSchema_MaxTableIdContinuesAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE a (id INT)")
	db.exec(t, "CREATE TABLE b (id INT)")
	idA := db.sc.FindTableSchema("a").TableId
	idB := db.sc.FindTableSchema("b").TableId
	db.close(t)

	db2 := reopenAt(t, path)
	db2.exec(t, "CREATE TABLE c (id INT)")
	idC := db2.sc.FindTableSchema("c").TableId
	db2.close(t)

	if idB <= idA {
		t.Errorf("b.TableId (%d) should be > a.TableId (%d)", idB, idA)
	}
	if idC <= idB {
		t.Errorf("c.TableId (%d) should be > b.TableId (%d) after reopen", idC, idB)
	}
}

func TestSchema_DropAndReuseNameGetsNewId(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, name TEXT)")
	firstId := db.sc.FindTableSchema("t").TableId
	db.exec(t, "DROP TABLE t")
	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	secondId := db.sc.FindTableSchema("t").TableId
	db.close(t)

	if secondId <= firstId {
		t.Errorf("recreated table should get a higher ID: first=%d, second=%d", firstId, secondId)
	}

	db2 := reopenAt(t, path)
	defer db2.close(t)
	schema := db2.sc.FindTableSchema("t")
	if schema == nil {
		t.Fatal("recreated table not found after reopen")
	}
	if schema.TableId != secondId {
		t.Errorf("TableId mismatch after reopen: got %d, want %d", schema.TableId, secondId)
	}
}

func TestSchema_ColumnsPreservedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, name TEXT, score INT, active BOOL)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	schema := db2.sc.FindTableSchema("t")
	if schema == nil {
		t.Fatal("schema not found after reopen")
	}
	if len(schema.Columns) != 3 {
		t.Fatalf("expected 3 non-PK columns, got %d", len(schema.Columns))
	}
	wantCols := []struct{ Name, DataType string }{
		{"name", "TEXT"},
		{"score", "INT"},
		{"active", "BOOL"},
	}
	for i, w := range wantCols {
		if schema.Columns[i].Name != w.Name {
			t.Errorf("column[%d].Name: got %q, want %q", i, schema.Columns[i].Name, w.Name)
		}
		if schema.Columns[i].DataType != w.DataType {
			t.Errorf("column[%d].DataType: got %q, want %q", i, schema.Columns[i].DataType, w.DataType)
		}
	}
}

// ---- WAL file lifecycle ----

func TestWAL_FileCreatedOnNewDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT)")
	db.exec(t, "INSERT INTO t VALUES (1)")

	// WAL file should exist while the session is open
	if walSize(t, path) < 0 {
		t.Error("WAL file should exist during an open session")
	}
	db.close(t)
}

func TestWAL_FileTruncatedOnCleanClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	for i := 1; i <= 50; i++ {
		db.exec(t, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}
	db.close(t)

	size := walSize(t, path)
	if size != 0 {
		t.Errorf("WAL file should be 0 bytes after clean close, got %d bytes", size)
	}
}

func TestWAL_DataIntactAfterReopenWithEmptyWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 42)")
	db.close(t)

	// Confirm WAL is empty before reopening
	if walSize(t, path) != 0 {
		t.Fatalf("WAL should be truncated after clean close")
	}

	// Reopen — RecoverFromWAL runs over an empty WAL (no-op) then loads from disk
	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT v FROM t WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 42 {
		t.Errorf("expected v=42 after reopen with empty WAL")
	}
}

func TestWAL_NewSessionStartsWithFreshWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	// Session 1: create + close (WAL truncated)
	db1 := createAt(t, path)
	db1.exec(t, "CREATE TABLE t (id INT)")
	db1.close(t)

	// Session 2: more writes
	db2 := reopenAt(t, path)
	db2.exec(t, "INSERT INTO t VALUES (1)")
	db2.close(t)

	// Both sessions should end with a truncated WAL
	if walSize(t, path) != 0 {
		t.Errorf("WAL should be 0 bytes after second session close")
	}
}

// ---- Column projection ----

func TestProjection_SpecificColumnsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, first TEXT, last TEXT, age INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 'John', 'Doe', 30)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT first, age FROM t")
	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"first", "age"})
	if strVal(t, rs.Rows[0].Fields[0]) != "John" {
		t.Error("expected first='John'")
	}
	if intVal(t, rs.Rows[0].Fields[1]) != 30 {
		t.Error("expected age=30")
	}
}

func TestProjection_StarAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, a TEXT, b INT)")
	db.exec(t, "INSERT INTO t VALUES (7, 'hello', 42)")
	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	assertColumns(t, rs, []string{"id", "a", "b"})
	if intVal(t, rs.Rows[0].Fields[0]) != 7 {
		t.Error("expected id=7")
	}
}

// ---- End-to-end lifecycle ----

func TestLifecycle_FullCRUDWithReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	// Session 1: create schema and seed data
	db1 := createAt(t, path)
	db1.exec(t, "CREATE TABLE accounts (id INT, owner TEXT, balance INT)")
	db1.exec(t, "INSERT INTO accounts VALUES (1, 'alice', 1000)")
	db1.exec(t, "INSERT INTO accounts VALUES (2, 'bob', 500)")
	db1.exec(t, "INSERT INTO accounts VALUES (3, 'carol', 750)")
	db1.close(t)

	// Session 2: update and delete
	db2 := reopenAt(t, path)
	db2.exec(t, "UPDATE accounts SET balance = 1200 WHERE id = 1")
	db2.exec(t, "DELETE FROM accounts WHERE id = 3")
	db2.exec(t, "INSERT INTO accounts VALUES (4, 'dave', 300)")
	db2.close(t)

	// Session 3: verify final state
	db3 := reopenAt(t, path)
	defer db3.close(t)

	rs := db3.exec(t, "SELECT * FROM accounts")
	assertRowCount(t, rs, 3) // alice, bob, dave (carol deleted)

	rs = db3.exec(t, "SELECT balance FROM accounts WHERE owner = 'alice'")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 1200 {
		t.Errorf("alice balance: expected 1200, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}

	rs = db3.exec(t, "SELECT * FROM accounts WHERE owner = 'carol'")
	assertRowCount(t, rs, 0) // deleted

	rs = db3.exec(t, "SELECT balance FROM accounts WHERE owner = 'dave'")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 300 {
		t.Errorf("dave balance: expected 300, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}
}

func TestLifecycle_SchemaEvolutionAcrossSessions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	// Session 1: create v1 schema
	db1 := createAt(t, path)
	db1.exec(t, "CREATE TABLE config (id INT, key TEXT)")
	db1.exec(t, "INSERT INTO config VALUES (1, 'debug')")
	db1.close(t)

	// Session 2: drop and recreate with different schema
	db2 := reopenAt(t, path)
	db2.exec(t, "DROP TABLE config")
	db2.exec(t, "CREATE TABLE config (id INT, key TEXT, value TEXT)")
	db2.exec(t, "INSERT INTO config VALUES (1, 'debug', 'false')")
	db2.exec(t, "INSERT INTO config VALUES (2, 'timeout', '30')")
	db2.close(t)

	// Session 3: verify new schema and data
	db3 := reopenAt(t, path)
	defer db3.close(t)

	rs := db3.exec(t, "SELECT * FROM config")
	assertRowCount(t, rs, 2)
	assertColumns(t, rs, []string{"id", "key", "value"})

	rs = db3.exec(t, "SELECT value FROM config WHERE key = 'timeout'")
	assertRowCount(t, rs, 1)
	if strVal(t, rs.Rows[0].Fields[0]) != "30" {
		t.Errorf("expected value='30'")
	}
}

// ---- Rollback scenarios through the full stack ----

// beginRun parses and executes query in a new transaction, returning the result and
// the open transaction. The caller must call db.tm.Commit or db.tm.Rollback.
func (db *testDB) beginRun(t *testing.T, query string) (*sqllayer.ResultSet, *lock.Transaction) {
	t.Helper()
	tokens, err := sqllayer.Tokenize(query)
	if err != nil {
		t.Fatalf("beginRun tokenize(%q): %v", query, err)
	}
	stmt, err := sqllayer.Parse(tokens)
	if err != nil {
		t.Fatalf("beginRun parse(%q): %v", query, err)
	}
	txn := db.tm.Begin()
	rs, err := db.ex.Execute(stmt, txn.Id)
	if err != nil {
		db.tm.Rollback(txn.Id)
		t.Fatalf("beginRun execute(%q): %v", query, err)
	}
	return rs, txn
}

func TestRollback_InsertRevertedInFullStack(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")

	_, txn := db.beginRun(t, "INSERT INTO t VALUES (1, 10)")
	db.tm.Rollback(txn.Id)

	rs := db.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 0)
}

func TestRollback_DeleteRevertedInFullStack(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")

	_, txn := db.beginRun(t, "DELETE FROM t WHERE id = 1")
	db.tm.Rollback(txn.Id)

	rs := db.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[1]) != 10 {
		t.Errorf("v: expected 10 after DELETE rollback, got %d", intVal(t, rs.Rows[0].Fields[1]))
	}
}

func TestRollback_UpdateRevertedInFullStack(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")

	_, txn := db.beginRun(t, "UPDATE t SET v = 99 WHERE id = 1")
	db.tm.Rollback(txn.Id)

	rs := db.exec(t, "SELECT v FROM t WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 10 {
		t.Errorf("v: expected 10 after UPDATE rollback, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}
}

func TestRollback_MixedOpsRevertedInFullStack(t *testing.T) {
	// Commit rows 1 and 2. In one txn: delete row 1, update row 2, insert row 3. Rollback.
	// Expected: rows 1 and 2 back to originals, row 3 absent.
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")
	db.exec(t, "INSERT INTO t VALUES (2, 20)")

	txn := db.tm.Begin()
	for _, q := range []string{
		"DELETE FROM t WHERE id = 1",
		"UPDATE t SET v = 99 WHERE id = 2",
		"INSERT INTO t VALUES (3, 30)",
	} {
		tokens, _ := sqllayer.Tokenize(q)
		stmt, _ := sqllayer.Parse(tokens)
		if _, err := db.ex.Execute(stmt, txn.Id); err != nil {
			db.tm.Rollback(txn.Id)
			t.Fatalf("execute(%q): %v", q, err)
		}
	}
	db.tm.Rollback(txn.Id)

	rs := db.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 2)

	rs = db.exec(t, "SELECT v FROM t WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 10 {
		t.Errorf("row 1 v: expected 10 after rollback, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}

	rs = db.exec(t, "SELECT v FROM t WHERE id = 2")
	if intVal(t, rs.Rows[0].Fields[0]) != 20 {
		t.Errorf("row 2 v: expected 20 after rollback, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}

	rs = db.exec(t, "SELECT * FROM t WHERE id = 3")
	assertRowCount(t, rs, 0)
}

func TestRollback_CommittedRowsSurvive(t *testing.T) {
	db := newTestDB(t)
	defer db.close(t)

	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)") // committed

	_, txn := db.beginRun(t, "INSERT INTO t VALUES (2, 20)")
	db.tm.Rollback(txn.Id)

	rs := db.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 1 {
		t.Error("only the committed row (id=1) should be visible")
	}
}

func TestRollback_DeleteRevertedSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 42)")

	_, txn := db.beginRun(t, "DELETE FROM t WHERE id = 1")
	db.tm.Rollback(txn.Id) // row re-inserted in memory

	db.close(t) // flush to disk

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT * FROM t")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[1]) != 42 {
		t.Errorf("v: expected 42 after reopen, got %d", intVal(t, rs.Rows[0].Fields[1]))
	}
}

func TestRollback_UpdateRevertedSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	db := createAt(t, path)
	db.exec(t, "CREATE TABLE t (id INT, v INT)")
	db.exec(t, "INSERT INTO t VALUES (1, 10)")

	_, txn := db.beginRun(t, "UPDATE t SET v = 99 WHERE id = 1")
	db.tm.Rollback(txn.Id)

	db.close(t)

	db2 := reopenAt(t, path)
	defer db2.close(t)

	rs := db2.exec(t, "SELECT v FROM t WHERE id = 1")
	assertRowCount(t, rs, 1)
	if intVal(t, rs.Rows[0].Fields[0]) != 10 {
		t.Errorf("v: expected 10 after reopen, got %d", intVal(t, rs.Rows[0].Fields[0]))
	}
}
