package httpapi

// Requests

type CreateTableRequest struct {
	Name    string         `json:"name"`
	Columns []ColumnDefJSON `json:"columns"`
}

type ColumnDefJSON struct {
	Name     string `json:"name"`
	DataType string `json:"type"`
}

type InsertRequest struct {
	Values map[string]any `json:"values"`
}

type UpdateRequest struct {
	Set   map[string]any `json:"set"`
	Where string         `json:"where"`
}

type DeleteRequest struct {
	Where string `json:"where"`
}

type AlterConsistencyRequest struct {
	Mode string `json:"mode"`
}

// Responses

type SuccessResponse struct {
	OK bool `json:"ok"`
}

type RowsResponse struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Count   int              `json:"count"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type TableInfoResponse struct {
	Name        string          `json:"name"`
	PrimaryKey  ColumnDefJSON   `json:"primary_key"`
	Columns     []ColumnDefJSON `json:"columns"`
	Consistency string          `json:"consistency"`
}
