package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeIndex(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(strings.ToLower(string(body[:n])), "<!doctype html") {
		t.Fatalf("index body is not an HTML document: %q", string(body[:n]))
	}
	// Embedded assets have no validators, so the shell must force
	// revalidation — otherwise browsers hold a stale app across deploys.
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("GET / Cache-Control = %q, want %q", got, "no-cache")
	}
}

func TestServeSPAFallback(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/some/client/route")
	if err != nil {
		t.Fatalf("GET /some/client/route: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SPA fallback status = %d, want 200", resp.StatusCode)
	}
}

// TestRemovedRoutesNotFound pins that endpoints removed from the contract (the
// wake ledger, the SkillOpt verdicts stub, the single-run graph feed and its
// /events stream, and the Learning page, gitmoot/gitmoot#2593) and any other
// unknown /api/ path answer 404 instead of the 200 SPA shell.
func TestRemovedRoutesNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	for _, path := range []string{
		"/api/wakes",
		"/api/wakes/summary",
		"/api/wakes/receipts",
		"/api/run/eval-reviewer-0007/verdicts",
		"/api/runs",
		"/api/state",
		"/api/state?run=run-noted-001",
		"/api/learning/skills",
		"/events",
		"/events?run=run-noted-001",
		"/api/no-such-endpoint",
		"/api",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want 404", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/html") {
			t.Fatalf("GET %s served the HTML shell (Content-Type %q)", path, ct)
		}
	}
}

// TestOrgRoutesRegistered asserts both optional Org API routes are wired into
// the mux and served as JSON rather than falling through to the SPA shell.
func TestOrgRoutesRegistered(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	for _, path := range []string{"/api/org", "/api/org/role/g4"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("GET %s Content-Type = %q, want application/json", path, ct)
		}
	}
}
