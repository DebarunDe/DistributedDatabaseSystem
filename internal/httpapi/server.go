package httpapi

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	"github.com/your-username/DistributedDatabaseSystem/internal/partition"
)

type HTTPServer struct {
	gw      *partition.Gateway
	sc      *sqllayer.SchemaCatalog
	apiKeys map[string]bool
	router  chi.Router
}

func NewHTTPServer(
	gw *partition.Gateway,
	sc *sqllayer.SchemaCatalog,
	apiKeys map[string]bool,
) *HTTPServer {
	return &HTTPServer{gw: gw, sc: sc, apiKeys: apiKeys}
}

func (h *HTTPServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		token := r.Header.Get("Authorization")
		if !strings.HasPrefix(token, "Bearer ") {
			respondError(w, http.StatusUnauthorized, "Missing or invalid Authorization header")
			return
		}

		apiKey := strings.TrimPrefix(token, "Bearer ")
		if !h.apiKeys[apiKey] {
			respondError(w, http.StatusUnauthorized, "Invalid API key")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (h *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *HTTPServer) Routes() chi.Router {
	r := chi.NewRouter()

	// Middleware
	r.Use(h.corsMiddleware)
	r.Use(h.authMiddleware)
	r.Use(middleware.Logger) // chi built-in request logging

	// Table management
	r.Post("/tables", h.handleCreateTable)
	r.Get("/tables", h.handleListTables)
	r.Get("/tables/{name}", h.handleDescribeTable)
	r.Delete("/tables/{name}", h.handleDropTable)
	r.Put("/tables/{name}/consistency", h.handleAlterConsistency)

	// Row operations
	r.Post("/tables/{name}/rows", h.handleInsert)
	r.Get("/tables/{name}/rows", h.handleSelect)
	r.Put("/tables/{name}/rows", h.handleUpdate)
	r.Delete("/tables/{name}/rows", h.handleDelete)

	// Health check (no auth)
	r.Get("/health", h.handleHealth)

	return r
}
