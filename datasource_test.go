package dashboard

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestWorkflowAdditiveJSONContracts(t *testing.T) {
	t.Run("legacy graph node omits workflow fields", func(t *testing.T) {
		node := GraphNode{ID: "job-1", Type: "job", Label: "legacy", State: "running"}
		got, err := json.Marshal(node)
		if err != nil {
			t.Fatalf("marshal legacy GraphNode: %v", err)
		}
		const wantNode = `{"id":"job-1","type":"job","label":"legacy","state":"running"}`
		if string(got) != wantNode {
			t.Fatalf("legacy GraphNode JSON = %s, want %s", got, wantNode)
		}
	})

	t.Run("labeled graph node round trips", func(t *testing.T) {
		const payload = `{"id":"workflow::panel","type":"workflow","label":"panel","jobCount":7,"noteCount":3,"tokensIn":22400,"tokensOut":9700}`
		var node GraphNode
		if err := json.Unmarshal([]byte(payload), &node); err != nil {
			t.Fatalf("unmarshal workflow GraphNode: %v", err)
		}
		if node.Type != "workflow" || node.JobCount != 7 || node.NoteCount != 3 || node.TokensIn != 22400 || node.TokensOut != 9700 {
			t.Fatalf("workflow GraphNode fields = %+v", node)
		}
		got, err := json.Marshal(node)
		if err != nil {
			t.Fatalf("marshal workflow GraphNode: %v", err)
		}
		if !bytes.Equal(got, []byte(payload)) {
			t.Fatalf("workflow GraphNode JSON = %s, want %s", got, payload)
		}
	})
}

func TestOrgBadgesActiveJobsAdditiveJSONContract(t *testing.T) {
	t.Run("legacy payload omits active_jobs", func(t *testing.T) {
		const payload = `{"blocked_since":"2026-07-23T01:31:00Z","overdue":"23m","missed_wakes":2}`
		var badges OrgBadges
		if err := json.Unmarshal([]byte(payload), &badges); err != nil {
			t.Fatalf("unmarshal legacy OrgBadges: %v", err)
		}
		if badges.ActiveJobs != 0 {
			t.Fatalf("legacy OrgBadges.ActiveJobs = %d, want 0", badges.ActiveJobs)
		}
		got, err := json.Marshal(badges)
		if err != nil {
			t.Fatalf("marshal legacy OrgBadges: %v", err)
		}
		if string(got) != payload {
			t.Fatalf("legacy OrgBadges JSON = %s, want %s", got, payload)
		}
	})

	t.Run("active jobs round trip", func(t *testing.T) {
		badges := OrgBadges{ActiveJobs: 3}
		got, err := json.Marshal(badges)
		if err != nil {
			t.Fatalf("marshal OrgBadges with ActiveJobs: %v", err)
		}
		const want = `{"missed_wakes":0,"active_jobs":3}`
		if string(got) != want {
			t.Fatalf("OrgBadges JSON = %s, want %s", got, want)
		}
		var roundTrip OrgBadges
		if err := json.Unmarshal(got, &roundTrip); err != nil {
			t.Fatalf("unmarshal OrgBadges: %v", err)
		}
		if roundTrip.ActiveJobs != 3 {
			t.Fatalf("round-trip OrgBadges.ActiveJobs = %d, want 3", roundTrip.ActiveJobs)
		}
	})
}
