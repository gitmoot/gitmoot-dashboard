package dashboard

import (
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

// Serve returns an http.Handler serving the read-only dashboard: the embedded
// static UI (with SPA fallback to index.html) plus the JSON API and the
// /api/events change-ping stream (api.go).
func Serve(ds DataSource) http.Handler {
	s := newServer(ds)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/jobs", s.handleJobs)
	mux.HandleFunc("GET /api/agents", s.handleAgents)
	mux.HandleFunc("GET /api/agent/{name}", s.handleAgent)
	mux.HandleFunc("GET /api/charts", s.handleCharts)
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /api/overview", s.handleOverview)
	mux.HandleFunc("GET /api/tasks", s.handleTasks)
	mux.HandleFunc("GET /api/org", s.handleOrg)
	mux.HandleFunc("GET /api/org/role/{name}", s.handleOrgRole)
	mux.HandleFunc("GET /api/workflows", s.handleWorkflows)
	mux.HandleFunc("GET /api/workflow/{label}", s.handleWorkflow)
	mux.HandleFunc("GET /api/attention", s.handleAttention)
	mux.HandleFunc("GET /api/job/{id}/checks", s.handleJobChecks)
	mux.HandleFunc("GET /api/job/{id}", s.handleJob)
	mux.HandleFunc("GET /api/graph", s.handleGraph)
	mux.HandleFunc("GET /api/events", s.handleChangeEvents)

	// Everything else is served from the embedded static assets, with an SPA
	// fallback to index.html for unknown paths.
	mux.Handle("/", s.staticHandler())

	return mux
}

type server struct {
	ds              DataSource
	changes         *changeWatcher
	changeHeartbeat time.Duration
}

func newServer(ds DataSource) *server {
	s := &server{ds: ds, changeHeartbeat: changeHeartbeatInterval}
	if source, ok := ds.(ChangeCursorDataSource); ok {
		s.changes = newChangeWatcher(source, changePollInterval, changeClientCap)
	}
	return s
}

// staticHandler serves the embedded web/dist assets. Requests that do not map
// to an existing file fall back to index.html so the client-side router can
// take over — except under /api/ and at /events (the retired run-state stream),
// where an unknown or removed endpoint must answer 404 rather than a 200 HTML
// shell a stale client would try to parse as JSON or an event stream.
func (s *server) staticHandler() http.Handler {
	dist := webDistFS()
	fileServer := http.FileServer(http.FS(dist))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The embedded assets carry no validators (embed.FS files have zero
		// mod times, so there is no Last-Modified/ETag), which lets browsers
		// and proxies hold a stale app shell across deploys — clicking a
		// newly shipped feature then does nothing until a hard refresh.
		// no-cache forces revalidation-or-refetch on every load; the shell is
		// a single small file and all live data comes from the /api endpoints.
		w.Header().Set("Cache-Control", "no-cache")
		upath := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if upath == "" {
			upath = "index.html"
		}
		if upath == "api" || strings.HasPrefix(upath, "api/") || upath == "events" {
			http.NotFound(w, r)
			return
		}
		if f, err := dist.Open(upath); err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}
		serveIndex(w, r, dist)
	})
}

// serveIndex writes web/dist/index.html for SPA fallback.
func serveIndex(w http.ResponseWriter, r *http.Request, dist fs.FS) {
	data, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
