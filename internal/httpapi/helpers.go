package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
)

// toLiteral converts a JSON value to a sqllayer.Literal based on the column type
func toLiteral(val any, dataType string) sqllayer.Literal {
	switch dataType {
	case "INT":
		return sqllayer.Literal{Type: sqllayer.TOKEN_NUMBER, Value: fmt.Sprintf("%d", int64(val.(float64)))}
	case "STRING", "TEXT", "VARCHAR":
		return sqllayer.Literal{Type: sqllayer.TOKEN_STRING, Value: val.(string)}
	case "BOOL":
		if val.(bool) {
			return sqllayer.Literal{Type: sqllayer.TOKEN_IDENTIFIER, Value: "true"}
		} else {
			return sqllayer.Literal{Type: sqllayer.TOKEN_IDENTIFIER, Value: "false"}
		}
	default:
		return sqllayer.Literal{Type: sqllayer.TOKEN_IDENTIFIER, Value: "null"}
	}
}

// fieldToJSON converts a btree.Field to a JSON-friendly value
func fieldToJSON(f btree.Field) any {
	switch v := f.Value.(type) {
	case btree.IntValue:
		return v.V
	case btree.StringValue:
		return v.V
	case btree.NullValue:
		return nil
	default:
		return nil
	}
}

// parseExpressionFromTokens parses a token slice as a WHERE expression
func parseExpressionFromTokens(tokens []sqllayer.Token) sqllayer.Expression {
	expr, err := sqllayer.ParseExpression(tokens)
	if err != nil {
		return nil
	}
	return expr
}

// respondJSON writes a JSON response with the given status code
func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// respondError writes an error response
func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, map[string]string{"error": msg})
}
