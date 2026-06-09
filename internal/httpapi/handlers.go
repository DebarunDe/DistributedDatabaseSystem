package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
)

func (h *HTTPServer) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Name == "" {
		respondError(w, http.StatusBadRequest, "table name is required")
		return
	}
	if len(req.Columns) == 0 {
		respondError(w, http.StatusBadRequest, "at least one column is required")
		return
	}
	if req.Columns[0].Name == "" || req.Columns[0].DataType == "" {
		respondError(w, http.StatusBadRequest, "first column (primary key) must have a name and type")
		return
	}

	cols := make([]sqllayer.ColumnDef, len(req.Columns))
	for i, c := range req.Columns {
		cols[i] = sqllayer.ColumnDef{Name: c.Name, DataType: c.DataType}
	}

	_, err := h.gw.Execute(&sqllayer.CreateTableStatement{
		Table:   req.Name,
		Columns: cols,
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			respondError(w, http.StatusBadRequest, err.Error())
		} else {
			respondError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	respondJSON(w, http.StatusCreated, SuccessResponse{OK: true})
}

func (h *HTTPServer) handleListTables(w http.ResponseWriter, r *http.Request) {
	names := h.sc.AllTableNames()
	type tableEntry struct {
		Name        string `json:"name"`
		Consistency string `json:"consistency"`
	}
	result := make([]tableEntry, 0, len(names))
	for _, name := range names {
		schema := h.sc.FindTableSchema(name)
		consistency := "strong"
		if schema.Consistency == sqllayer.ConsistencyAP {
			consistency = "eventual"
		}
		result = append(result, tableEntry{Name: name, Consistency: consistency})
	}
	respondJSON(w, http.StatusOK, result)
}

func (h *HTTPServer) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	schema := h.sc.FindTableSchema(name)
	if schema == nil {
		respondError(w, http.StatusNotFound, "table not found")
		return
	}

	consistency := "strong"
	if schema.Consistency == sqllayer.ConsistencyAP {
		consistency = "eventual"
	}

	cols := make([]ColumnDefJSON, len(schema.Columns))
	for i, c := range schema.Columns {
		cols[i] = ColumnDefJSON{Name: c.Name, DataType: c.DataType}
	}

	respondJSON(w, http.StatusOK, TableInfoResponse{
		Name:        name,
		PrimaryKey:  ColumnDefJSON{Name: schema.PrimaryKey.Name, DataType: schema.PrimaryKey.DataType},
		Columns:     cols,
		Consistency: consistency,
	})
}

func (h *HTTPServer) handleDropTable(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	_, err := h.gw.Execute(&sqllayer.DropTableStatement{Table: name})
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
			respondError(w, http.StatusBadRequest, err.Error())
		} else {
			respondError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	respondJSON(w, http.StatusOK, SuccessResponse{OK: true})
}

func (h *HTTPServer) handleInsert(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req InsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	schema := h.sc.FindTableSchema(name)
	if schema == nil {
		respondError(w, http.StatusNotFound, "table not found")
		return
	}

	values := make([]sqllayer.Literal, 0, 1+len(schema.Columns))
	values = append(values, toLiteral(req.Values[schema.PrimaryKey.Name], schema.PrimaryKey.DataType))
	for _, col := range schema.Columns {
		values = append(values, toLiteral(req.Values[col.Name], col.DataType))
	}

	_, err := h.gw.Execute(&sqllayer.InsertStatement{Table: name, Values: values})
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "already exists") {
			respondError(w, http.StatusBadRequest, err.Error())
		} else {
			respondError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	respondJSON(w, http.StatusCreated, SuccessResponse{OK: true})
}

func (h *HTTPServer) handleSelect(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	schema := h.sc.FindTableSchema(name)
	if schema == nil {
		respondError(w, http.StatusNotFound, "table not found")
		return
	}

	q := r.URL.Query()
	colList := []string{"*"}
	if cols := q.Get("columns"); cols != "" {
		colList = strings.Split(cols, ",")
	}

	stmt := &sqllayer.SelectStatement{Table: name, Columns: colList}

	if where := q.Get("where"); where != "" {
		tokens, err := sqllayer.Tokenize(where)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid where clause")
			return
		}
		stmt.Where = parseExpressionFromTokens(tokens)
	}

	if consistency := q.Get("consistency"); consistency != "" {
		switch consistency {
		case "eventual":
			mode := sqllayer.ConsistencyAP
			stmt.ConsistencyOverride = &mode
		case "strong":
			mode := sqllayer.ConsistencyCP
			stmt.ConsistencyOverride = &mode
		}
	}

	result, err := h.gw.Execute(stmt)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	jsonRows := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		m := make(map[string]any, len(result.Columns))
		for i, col := range result.Columns {
			m[col] = fieldToJSON(row.Fields[i])
		}
		jsonRows = append(jsonRows, m)
	}

	respondJSON(w, http.StatusOK, RowsResponse{
		Columns: result.Columns,
		Rows:    jsonRows,
		Count:   len(jsonRows),
	})
}

func (h *HTTPServer) handleUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	schema := h.sc.FindTableSchema(name)
	if schema == nil {
		respondError(w, http.StatusNotFound, "table not found")
		return
	}

	for colName, newVal := range req.Set {
		dataType := colTypeFor(colName, schema)
		stmt := &sqllayer.UpdateStatement{
			Table:  name,
			Column: colName,
			Value:  toLiteral(newVal, dataType),
		}
		if req.Where != "" {
			tokens, err := sqllayer.Tokenize(req.Where)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid where clause")
				return
			}
			stmt.Where = parseExpressionFromTokens(tokens)
		}
		if _, err := h.gw.Execute(stmt); err != nil {
			respondError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	respondJSON(w, http.StatusOK, SuccessResponse{OK: true})
}

func (h *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	schema := h.sc.FindTableSchema(name)
	if schema == nil {
		respondError(w, http.StatusNotFound, "table not found")
		return
	}

	stmt := &sqllayer.DeleteStatement{Table: name}
	if req.Where != "" {
		tokens, err := sqllayer.Tokenize(req.Where)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid where clause")
			return
		}
		stmt.Where = parseExpressionFromTokens(tokens)
	}

	if _, err := h.gw.Execute(stmt); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, SuccessResponse{OK: true})
}

func (h *HTTPServer) handleAlterConsistency(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req AlterConsistencyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	mode := sqllayer.ConsistencyCP
	if req.Mode == "eventual" {
		mode = sqllayer.ConsistencyAP
	}

	_, err := h.gw.Execute(&sqllayer.AlterConsistencyStatement{Table: name, Mode: mode})
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
			respondError(w, http.StatusBadRequest, err.Error())
		} else {
			respondError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	respondJSON(w, http.StatusOK, SuccessResponse{OK: true})
}

func (h *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// colTypeFor returns the data type for a column name, checking PK then regular columns.
func colTypeFor(colName string, schema *sqllayer.TableSchemaValue) string {
	if schema.PrimaryKey.Name == colName {
		return schema.PrimaryKey.DataType
	}
	for _, c := range schema.Columns {
		if c.Name == colName {
			return c.DataType
		}
	}
	return "STRING"
}
