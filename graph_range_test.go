package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// rangeRecordingDataSource records the range /api/graph hands to Graph.
type rangeRecordingDataSource struct {
	DataSource
	repo string
	rng  GraphRange
}

func (r *rangeRecordingDataSource) Graph(ctx context.Context, repo string, rng GraphRange) (Graph, error) {
	r.repo, r.rng = repo, rng
	return r.DataSource.Graph(ctx, repo, rng)
}

// /api/graph passes a canonical range to the DataSource: the three known values
// through, and anything else (missing, unknown, wrong case) as the 30-day
// default, so a client that never sends ?range= gets today's view.
func TestGraphRangeReachesDataSourceCanonical(t *testing.T) {
	ds := &rangeRecordingDataSource{DataSource: NewFakeDataSource()}
	handler := Serve(ds)
	for target, want := range map[string]GraphRange{
		"/api/graph":                     GraphRange30d,
		"/api/graph?range=30d":           GraphRange30d,
		"/api/graph?range=90d":           GraphRange90d,
		"/api/graph?range=all":           GraphRangeAll,
		"/api/graph?range=ALL":           GraphRange30d,
		"/api/graph?range=365d":          GraphRange30d,
		"/api/graph?range=":              GraphRange30d,
		"/api/graph?repo=a/b&range=all":  GraphRangeAll,
		"/api/graph?range=90d&range=all": GraphRange90d,
	} {
		ds.rng = ""
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", target, rec.Code, rec.Body.String())
		}
		if ds.rng != want {
			t.Fatalf("GET %s passed range %q, want %q", target, ds.rng, want)
		}
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/graph?repo=a/b&range=all", nil))
	if ds.repo != "a/b" {
		t.Fatalf("repo = %q, want a/b alongside the range", ds.repo)
	}
}
