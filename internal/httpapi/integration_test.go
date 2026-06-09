package httpapi_test

// Integration tests: multi-step scenarios that verify end-to-end behaviour
// across the full stack (HTTP → Gateway → gRPC → BTree). Each test exercises
// a complete user workflow rather than a single endpoint in isolation.
//
// These tests share the newTestStack helper from httpapi_test.go.
//
// Sections:
//   TableLifecycle  — create / list / describe / drop round-trips
//   RowRoundTrips   — insert → select → update → delete sequences
//   Consistency     — alter consistency, visible in describe and list
//   MultiTable      — operations across multiple tables

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/your-username/DistributedDatabaseSystem/internal/httpapi"
)

// =============================================================================
// Table lifecycle
// =============================================================================

func TestIntegration_CreateTable_AppearsInList(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "users",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "name", DataType: "TEXT"},
	)

	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 1 {
		t.Fatalf("want 1 table, got %d", len(tables))
	}
	if tables[0]["name"] != "users" {
		t.Fatalf("want name=users, got %v", tables[0]["name"])
	}
}

func TestIntegration_DescribeTable_MatchesCreationSchema(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "products",
		httpapi.ColumnDefJSON{Name: "sku", DataType: "TEXT"},
		httpapi.ColumnDefJSON{Name: "price", DataType: "INT"},
	)

	resp := s.req(t, http.MethodGet, "/tables/products", nil)
	mustStatus(t, resp, http.StatusOK)
	info := decode[httpapi.TableInfoResponse](t, resp)

	if info.Name != "products" {
		t.Fatalf("want name=products, got %q", info.Name)
	}
	if info.PrimaryKey.Name != "sku" || info.PrimaryKey.DataType != "TEXT" {
		t.Fatalf("unexpected PK: %+v", info.PrimaryKey)
	}
	if len(info.Columns) != 1 || info.Columns[0].Name != "price" {
		t.Fatalf("unexpected columns: %+v", info.Columns)
	}
	if info.Consistency != "strong" {
		t.Fatalf("want default consistency=strong, got %q", info.Consistency)
	}
}

func TestIntegration_DropTable_DisappearsFromListAndDescribe(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "temp", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodDelete, "/tables/temp", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	// List should be empty
	resp = s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 0 {
		t.Fatalf("want 0 tables after drop, got %d", len(tables))
	}

	// Describe should 404
	resp = s.req(t, http.MethodGet, "/tables/temp", nil)
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

// =============================================================================
// Row round-trips
// =============================================================================

func TestIntegration_InsertSelect_RoundTrip(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "items",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "label", DataType: "TEXT"},
	)
	s.insertRow(t, "items", map[string]any{"id": float64(1), "label": "alpha"})
	s.insertRow(t, "items", map[string]any{"id": float64(2), "label": "beta"})

	body := s.selectRows(t, "items", nil)
	if body.Count != 2 {
		t.Fatalf("want 2 rows, got %d", body.Count)
	}
	if len(body.Columns) != 2 {
		t.Fatalf("want 2 columns, got %d: %v", len(body.Columns), body.Columns)
	}
}

func TestIntegration_Select_WhereFilter_ReturnsMatchingRow(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "scores", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 5; i++ {
		s.insertRow(t, "scores", map[string]any{"id": float64(i)})
	}

	body := s.selectRows(t, "scores", url.Values{"where": {"id = 3"}})
	if body.Count != 1 {
		t.Fatalf("want 1 row matching id=3, got %d", body.Count)
	}
}

func TestIntegration_Update_ChangeVisibleOnSelect(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "records",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "v", DataType: "INT"},
	)
	s.insertRow(t, "records", map[string]any{"id": float64(1), "v": float64(10)})
	s.insertRow(t, "records", map[string]any{"id": float64(2), "v": float64(20)})

	resp := s.req(t, http.MethodPut, "/tables/records/rows", httpapi.UpdateRequest{
		Set:   map[string]any{"v": float64(99)},
		Where: "id = 1",
	})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	// id=1 updated
	row1 := s.selectRows(t, "records", url.Values{"where": {"id = 1"}})
	if row1.Count != 1 || row1.Rows[0]["v"] != float64(99) {
		t.Fatalf("want v=99 for id=1, got %+v", row1.Rows)
	}

	// id=2 unchanged
	row2 := s.selectRows(t, "records", url.Values{"where": {"id = 2"}})
	if row2.Count != 1 || row2.Rows[0]["v"] != float64(20) {
		t.Fatalf("want v=20 for id=2 unchanged, got %+v", row2.Rows)
	}
}

func TestIntegration_Delete_WithWhere_ReducesCount(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "pool", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 5; i++ {
		s.insertRow(t, "pool", map[string]any{"id": float64(i)})
	}

	resp := s.req(t, http.MethodDelete, "/tables/pool/rows", httpapi.DeleteRequest{Where: "id > 3"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	body := s.selectRows(t, "pool", nil)
	if body.Count != 3 {
		t.Fatalf("want 3 rows after deleting id>3, got %d", body.Count)
	}
}

func TestIntegration_Delete_All_LeavesEmptyTable(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "wipeable", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	for i := 1; i <= 3; i++ {
		s.insertRow(t, "wipeable", map[string]any{"id": float64(i)})
	}

	resp := s.req(t, http.MethodDelete, "/tables/wipeable/rows", httpapi.DeleteRequest{})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	body := s.selectRows(t, "wipeable", nil)
	if body.Count != 0 {
		t.Fatalf("want 0 rows after delete-all, got %d", body.Count)
	}
}

func TestIntegration_FullCRUD_Sequence(t *testing.T) {
	s := newTestStack(t)

	// Create
	s.createTable(t, "things",
		httpapi.ColumnDefJSON{Name: "id", DataType: "INT"},
		httpapi.ColumnDefJSON{Name: "name", DataType: "TEXT"},
	)

	// Insert three rows
	s.insertRow(t, "things", map[string]any{"id": float64(1), "name": "a"})
	s.insertRow(t, "things", map[string]any{"id": float64(2), "name": "b"})
	s.insertRow(t, "things", map[string]any{"id": float64(3), "name": "c"})

	if got := s.selectRows(t, "things", nil).Count; got != 3 {
		t.Fatalf("after insert: want 3 rows, got %d", got)
	}

	// Update row 2
	resp := s.req(t, http.MethodPut, "/tables/things/rows", httpapi.UpdateRequest{
		Set: map[string]any{"name": "updated"}, Where: "id = 2",
	})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	// Delete row 3
	resp = s.req(t, http.MethodDelete, "/tables/things/rows", httpapi.DeleteRequest{Where: "id = 3"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	if got := s.selectRows(t, "things", nil).Count; got != 2 {
		t.Fatalf("after update+delete: want 2 rows, got %d", got)
	}

	// Drop table
	resp = s.req(t, http.MethodDelete, "/tables/things", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodGet, "/tables/things", nil)
	mustStatus(t, resp, http.StatusNotFound)
	drainClose(resp)
}

// =============================================================================
// Consistency
// =============================================================================

func TestIntegration_AlterConsistency_ReflectedInDescribeAndList(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "mutable", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	// Default is strong
	info := decode[httpapi.TableInfoResponse](t, s.req(t, http.MethodGet, "/tables/mutable", nil))
	if info.Consistency != "strong" {
		t.Fatalf("want strong by default, got %q", info.Consistency)
	}

	// Alter to eventual
	resp := s.req(t, http.MethodPut, "/tables/mutable/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	// Describe reflects change
	info = decode[httpapi.TableInfoResponse](t, s.req(t, http.MethodGet, "/tables/mutable", nil))
	if info.Consistency != "eventual" {
		t.Fatalf("want eventual after alter, got %q", info.Consistency)
	}

	// List also reflects change
	resp = s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if tables[0]["consistency"] != "eventual" {
		t.Fatalf("want eventual in list, got %v", tables[0]["consistency"])
	}
}

func TestIntegration_APTable_InsertAndSelectWithEventualOverride(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "ap_data", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodPut, "/tables/ap_data/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	s.insertRow(t, "ap_data", map[string]any{"id": float64(42)})

	body := s.selectRows(t, "ap_data", url.Values{"consistency": {"eventual"}})
	if body.Count != 1 {
		t.Fatalf("want 1 row via eventual read, got %d", body.Count)
	}
}

func TestIntegration_AlterBackToStrong_InsertsWork(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "bouncy", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodPut, "/tables/bouncy/consistency", httpapi.AlterConsistencyRequest{Mode: "eventual"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	resp = s.req(t, http.MethodPut, "/tables/bouncy/consistency", httpapi.AlterConsistencyRequest{Mode: "strong"})
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	s.insertRow(t, "bouncy", map[string]any{"id": float64(1)})

	body := s.selectRows(t, "bouncy", nil)
	if body.Count != 1 {
		t.Fatalf("want 1 row after strong insert, got %d", body.Count)
	}
}

// =============================================================================
// Multi-table
// =============================================================================

func TestIntegration_MultiTable_IndependentRowSets(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "t1", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.createTable(t, "t2", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	s.insertRow(t, "t1", map[string]any{"id": float64(1)})
	s.insertRow(t, "t1", map[string]any{"id": float64(2)})
	s.insertRow(t, "t2", map[string]any{"id": float64(10)})

	if got := s.selectRows(t, "t1", nil).Count; got != 2 {
		t.Fatalf("t1: want 2 rows, got %d", got)
	}
	if got := s.selectRows(t, "t2", nil).Count; got != 1 {
		t.Fatalf("t2: want 1 row, got %d", got)
	}
}

func TestIntegration_MultiTable_DropOneDoesNotAffectOther(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "keep", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.createTable(t, "drop_me", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.insertRow(t, "keep", map[string]any{"id": float64(1)})

	resp := s.req(t, http.MethodDelete, "/tables/drop_me", nil)
	mustStatus(t, resp, http.StatusOK)
	drainClose(resp)

	// "keep" table and its rows are unaffected
	body := s.selectRows(t, "keep", nil)
	if body.Count != 1 {
		t.Fatalf("want 1 row in keep after dropping drop_me, got %d", body.Count)
	}
}

func TestIntegration_MultiTable_ListShowsAll(t *testing.T) {
	s := newTestStack(t)

	s.createTable(t, "a", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.createTable(t, "b", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})
	s.createTable(t, "c", httpapi.ColumnDefJSON{Name: "id", DataType: "INT"})

	resp := s.req(t, http.MethodGet, "/tables", nil)
	mustStatus(t, resp, http.StatusOK)
	tables := decode[[]map[string]any](t, resp)
	if len(tables) != 3 {
		t.Fatalf("want 3 tables, got %d", len(tables))
	}
}
