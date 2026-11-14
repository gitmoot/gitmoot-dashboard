package dashboard

import (
	"context"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sentinel errors returned by FakeDataSource (and mapped to HTTP status codes
// by the API handlers).
var (
	// ErrJobNotFound indicates the requested job/node does not exist.
	ErrJobNotFound = errors.New("job not found")
	// ErrAgentNotFound indicates the requested agent does not exist.
	ErrAgentNotFound = errors.New("agent not found")
	// ErrOrgRoleNotFound indicates the requested org role does not exist.
	ErrOrgRoleNotFound = errors.New("org role not found")
	// ErrWorkflowNotFound indicates the requested workflow label does not exist.
	ErrWorkflowNotFound = errors.New("workflow not found")
)

const (
	fakeRunID            = "run-noted-001"
	fakeWorkflow         = "fable/dashboard-redesign"
	fakeStalledWorkflow  = "fable/arxiv-retry-hardening"
	fakeUnregisteredName = "retired-impl-7f3a"
	fakeUnregisteredJob  = "job-unregistered-demo"
	// fakeRepo is the repository the fake run operates on (shared by Jobs/Graph).
	fakeRepo = "acme/webapp"
	// fakeTickInterval is how often the background goroutine advances the run.
	fakeTickInterval = 1200 * time.Millisecond
)

// FakeDataSource is an in-memory DataSource used for local development and
// tests. It models a realistic `noted` run: a coordinator job with three
// parallel implement jobs, an integrate/review/open-PR job that depends on all
// three, and a final summary job. A background goroutine advances job states
// (queued -> running -> succeeded) on a timer and moves the change cursor.
//
// FakeDataSource is safe for concurrent use.
type FakeDataSource struct {
	interval         time.Duration
	workflowsEnabled bool

	mu      sync.Mutex
	nodes   []Node
	step    int
	started bool
}

// NewFakeDataSource returns a FakeDataSource seeded with the noted run and
// starts the background goroutine that advances it.
func NewFakeDataSource() *FakeDataSource {
	f := &FakeDataSource{
		interval:         fakeTickInterval,
		workflowsEnabled: os.Getenv("FAKEFEED_WORKFLOWS") != "0",
		nodes:            initialFakeNodes(),
	}
	f.start()
	return f
}

// initialFakeNodes builds the seeded run with the coordinator running and every
// downstream job queued.
func initialFakeNodes() []Node {
	now := time.Now().UnixMilli()
	queued := func(id, title, agent string, deps ...string) Node {
		return Node{
			ID: id, ParentID: "job-1", Deps: deps, Title: title, Agent: agent,
			Runtime: "codex", Model: "gpt-5.5", State: "queued", Depth: 1,
			Events: []Event{{T: now, Label: "queued"}},
		}
	}
	return []Node{
		{
			ID:        "job-1",
			Title:     "coordinate: note search, delete, export",
			Agent:     "project-lead",
			Runtime:   "codex",
			Model:     "gpt-5.5",
			State:     "running",
			StartedAt: now,
			WorkerID:  "codex-coordinator",
			Events:    []Event{{T: now, Label: "queued"}, {T: now, Label: "started"}},
		},
		queued("job-2", "implement: note search", "implementer"),
		queued("job-3", "implement: note delete", "implementer"),
		queued("job-4", "implement: note export", "implementer"),
		queued("job-5", "integrate + review + open PR", "integrator", "job-2", "job-3", "job-4"),
		queued("job-6", "summarize outcome", "project-lead", "job-5"),
	}
}

// fakeStep mutates the run for a single tick. Each step is applied with f.mu held.
type fakeStep func(f *FakeDataSource)

// fakeSteps is the ordered timeline the background goroutine walks. The initial
// snapshot already has the coordinator running and the other jobs queued.
var fakeSteps = []fakeStep{
	func(f *FakeDataSource) { f.transition("job-2", "running", "worker started: note search") },
	func(f *FakeDataSource) { f.transition("job-3", "running", "worker started: note delete") },
	func(f *FakeDataSource) { f.transition("job-4", "running", "worker started: note export") },
	func(f *FakeDataSource) { f.transition("job-2", "succeeded", "PR opened, review clean") },
	func(f *FakeDataSource) { f.transition("job-3", "succeeded", "PR opened, review clean") },
	func(f *FakeDataSource) { f.transition("job-4", "succeeded", "PR opened, review clean") },
	func(f *FakeDataSource) { f.transition("job-5", "running", "all deps satisfied; integrating") },
	func(f *FakeDataSource) {
		f.setPRURL("job-5", "https://github.com/jerryfane/noted/pull/42")
		f.transition("job-5", "succeeded", "integration PR opened, review clean")
	},
	func(f *FakeDataSource) { f.transition("job-6", "running", "summarizing outcome") },
	func(f *FakeDataSource) { f.transition("job-6", "succeeded", "summary posted") },
	func(f *FakeDataSource) { f.transition("job-1", "succeeded", "run complete") },
}

func (f *FakeDataSource) start() {
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return
	}
	f.started = true
	f.mu.Unlock()
	go f.run()
}

// run advances the timeline on a ticker, stopping once the run reaches its
// terminal state. Each step moves the change cursor.
func (f *FakeDataSource) run() {
	t := time.NewTicker(f.interval)
	defer t.Stop()
	for range t.C {
		f.mu.Lock()
		if f.step >= len(fakeSteps) {
			f.mu.Unlock()
			return
		}
		fakeSteps[f.step](f)
		f.step++
		f.mu.Unlock()
	}
}

// transition sets a node's state and appends a timeline event. It must be
// called with f.mu held.
func (f *FakeDataSource) transition(id string, state NodeState, label string) {
	now := time.Now().UnixMilli()
	for i := range f.nodes {
		n := &f.nodes[i]
		if n.ID != id {
			continue
		}
		n.State = state
		if state == "running" && n.StartedAt == 0 {
			n.StartedAt = now
			if n.WorkerID == "" {
				n.WorkerID = "codex-worker-" + id
			}
		}
		if isTerminal(state) && n.EndedAt == 0 {
			n.EndedAt = now
		}
		n.Events = append(n.Events, Event{T: now, Label: label})
		return
	}
}

func (f *FakeDataSource) setPRURL(id, url string) {
	for i := range f.nodes {
		if f.nodes[i].ID == id {
			f.nodes[i].PRURL = url
			return
		}
	}
}

func isTerminal(s NodeState) bool {
	switch s {
	case "succeeded", "failed", "cancelled":
		return true
	}
	return false
}

// cloneNodesLocked returns a deep copy of the current run's nodes so callers
// can read/encode them without holding f.mu. Must be called with f.mu held.
func (f *FakeDataSource) cloneNodesLocked() []Node {
	out := make([]Node, len(f.nodes))
	for i, n := range f.nodes {
		cp := n
		if n.Deps != nil {
			cp.Deps = append([]string(nil), n.Deps...)
		}
		cp.Events = append([]Event(nil), n.Events...)
		out[i] = cp
	}
	return out
}

// anchor returns the run start, the time every fixture is anchored on.
func (f *FakeDataSource) anchor() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nodes[0].StartedAt
}

// Job implements DataSource.
func (f *FakeDataSource) Job(ctx context.Context, jobID string) (Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.nodes {
		if n.ID == jobID {
			cp := n
			cp.Deps = append([]string(nil), n.Deps...)
			cp.Events = append([]Event(nil), n.Events...)
			return cp, nil
		}
	}
	if f.workflowsEnabled && jobID == "demo-single-job" {
		return fakeWorkflowSingleNode(f.nodes[0].StartedAt), nil
	}
	if jobID == fakeUnregisteredJob {
		return fakeUnregisteredGalaxyNode(f.nodes[0].StartedAt), nil
	}
	return Node{}, ErrJobNotFound
}

// ChangeCursor implements the optional dashboard invalidation stream. Each
// completed fake timeline mutation increments step, so the opaque cursor moves
// in lockstep with the jobs served by the dev harness.
func (f *FakeDataSource) ChangeCursor(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strconv.Itoa(f.step), nil
}

// Graph implements DataSource. The fake feed has a single run, so the galaxy is
// that run's jobs plus a repo hub and per-agent hubs — enough to exercise the
// Galaxy view standalone.
func (f *FakeDataSource) Graph(ctx context.Context, repo string) (Graph, error) {
	f.mu.Lock()
	nodes := f.cloneNodesLocked()
	workflowsEnabled := f.workflowsEnabled
	f.mu.Unlock()
	g := Graph{Nodes: []GraphNode{}, Links: []GraphLink{}, Repos: []string{fakeRepo}}
	if repo != "" && repo != fakeRepo {
		return g, nil
	}
	ids := map[string]bool{}
	for _, n := range nodes {
		ids[n.ID] = true
	}
	agents := map[string]bool{}
	for _, n := range nodes {
		g.Nodes = append(g.Nodes, GraphNode{ID: n.ID, Type: "job", Label: n.Title, State: n.State, Agent: n.Agent, Repo: fakeRepo, Run: fakeRunID})
		if n.ParentID != "" && ids[n.ParentID] {
			g.Links = append(g.Links, GraphLink{Source: n.ParentID, Target: n.ID, Kind: "parent"})
		}
		for _, d := range n.Deps {
			if ids[d] {
				g.Links = append(g.Links, GraphLink{Source: d, Target: n.ID, Kind: "dep"})
			}
		}
		g.Links = append(g.Links, GraphLink{Source: n.ID, Target: "repo::" + fakeRepo, Kind: "repo"})
		if n.Agent != "" {
			agents[n.Agent] = true
			g.Links = append(g.Links, GraphLink{Source: n.ID, Target: "agent::" + n.Agent, Kind: "agent"})
		}
	}
	unregistered := fakeUnregisteredGalaxyNode(nodes[0].StartedAt)
	g.Nodes = append(g.Nodes, GraphNode{ID: unregistered.ID, Type: "job", Label: unregistered.Title, State: unregistered.State, Agent: unregistered.Agent, Repo: fakeRepo, Run: fakeRunID})
	g.Links = append(g.Links,
		GraphLink{Source: unregistered.ID, Target: "repo::" + fakeRepo, Kind: "repo"},
		GraphLink{Source: unregistered.ID, Target: "agent::" + unregistered.Agent, Kind: "agent"},
	)
	agents[unregistered.Agent] = true
	g.Nodes = append(g.Nodes, GraphNode{ID: "repo::" + fakeRepo, Type: "repo", Label: fakeRepo, Repo: fakeRepo})
	for a := range agents {
		g.Nodes = append(g.Nodes, GraphNode{ID: "agent::" + a, Type: "agent", Label: a, Agent: a})
	}
	if workflowsEnabled {
		extra := fakeWorkflowSingleNode(nodes[0].StartedAt)
		g.Nodes = append(g.Nodes, GraphNode{ID: extra.ID, Type: "job", Label: extra.Title, State: extra.State, Agent: extra.Agent, Repo: fakeRepo, Run: "run-demo-single"})
		g.Links = append(g.Links,
			GraphLink{Source: extra.ID, Target: "repo::" + fakeRepo, Kind: "repo"},
			GraphLink{Source: extra.ID, Target: "agent::" + extra.Agent, Kind: "agent"},
		)
		if !agents[extra.Agent] {
			g.Nodes = append(g.Nodes, GraphNode{ID: "agent::" + extra.Agent, Type: "agent", Label: extra.Agent, Agent: extra.Agent})
		}
		for _, n := range append(nodes, extra) {
			g.Links = append(g.Links, GraphLink{Source: n.ID, Target: "workflow::" + fakeWorkflow, Kind: "workflow"})
		}
		g.Nodes = append(g.Nodes, GraphNode{
			ID: "workflow::" + fakeWorkflow, Type: "workflow", Label: fakeWorkflow,
			JobCount: len(nodes) + 1, NoteCount: len(fakeWorkflowNotes(nodes[0].StartedAt)), TokensIn: 1_230_000, TokensOut: 15_300,
		})
	}
	return g, nil
}

// fakeUnregisteredGalaxyNode is intentionally present in Graph and Jobs but
// absent from Agents/Agent, matching history from an agent that is no longer
// registered.
func fakeUnregisteredGalaxyNode(anchor int64) Node {
	started := anchor - int64(4*time.Minute/time.Millisecond)
	ended := started + int64(2*time.Minute/time.Millisecond)
	return Node{
		ID: fakeUnregisteredJob, Title: "implement: patch from a retired agent",
		Agent: fakeUnregisteredName, Runtime: "codex", Model: "gpt-5.5",
		State: "succeeded", StartedAt: started, EndedAt: ended,
		Events: []Event{{T: started, Label: "started"}, {T: ended, Label: "completed"}},
	}
}

func fakeWorkflowSingleNode(anchor int64) Node {
	started := anchor - int64(2*time.Hour/time.Millisecond)
	ended := started + int64(7*time.Minute/time.Millisecond)
	return Node{
		ID: "demo-single-job", Title: "review: workflow panel accessibility",
		Agent: "reviewer-kimi", Runtime: "kimi", Model: "kimi-k2.5",
		State: "succeeded", StartedAt: started, EndedAt: ended,
		Events: []Event{{T: started, Label: "started"}, {T: ended, Label: "review complete"}},
	}
}

func compactWorkflowNode(n Node) WorkflowNode {
	return WorkflowNode{
		ID: n.ID, ParentID: n.ParentID, Deps: append([]string(nil), n.Deps...),
		Title: n.Title, Agent: n.Agent, Runtime: n.Runtime, Model: n.Model,
		State: n.State, StartedAt: n.StartedAt, EndedAt: n.EndedAt,
	}
}

func fakeWorkflowNotes(anchor int64) []WorkflowNoteView {
	notes := []WorkflowNoteView{
		{ID: 109, Author: "claude-coordinator", Body: "Designs agree across all three researchers — dispatching wave-impl to implement the exact-head review guard. Holding review until the validation pass returns.", Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64(2*time.Minute/time.Millisecond)},
		{ID: 108, Author: "panel-synth", Body: "render <script>window.__workflowScriptExecuted=true</script> literally & safely", Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64(10*time.Minute/time.Millisecond)},
		{ID: 107, Author: "claude-coordinator", Body: "Panel fired (3 models): core decision = keep the merge gate closed until a review exists at the exact head; evidence caps set to 16KiB / 64KiB.", Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64(20*time.Minute/time.Millisecond)},
		{ID: 106, Author: "research-kimi", Body: "Kimi review favors the exact-head guard and a separate validation child before synthesis.", Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64(27*time.Minute/time.Millisecond)},
		{ID: 105, Author: "research-sol", Body: "The baseline confirms stale-head approvals slip through after force-pushes; branch-name matching is insufficient.", Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64(40*time.Minute/time.Millisecond)},
		{ID: 104, Author: "claude-coordinator", Body: "Baseline audit complete. Launching independent design research with Claude, Codex, and Kimi.", Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64(55*time.Minute/time.Millisecond)},
		{ID: 103, Author: "project-lead", Body: "Workflow opened from issue #78 with the dashboard half isolated from server lifecycle derivation.", Repo: "jerryfane/gitmoot-dashboard", CreatedAt: anchor - int64(75*time.Minute/time.Millisecond)},
		{ID: 102, Author: "project-lead", Body: "Coordinator pane reserved and journal handoff verified.", Repo: "jerryfane/gitmoot-dashboard", CreatedAt: anchor - int64(100*time.Minute/time.Millisecond)},
		{ID: 101, Author: "project-lead", Body: "Campaign label created for the redesign mission log.", Repo: "jerryfane/gitmoot-dashboard", CreatedAt: anchor - int64(3*time.Hour/time.Millisecond)},
	}
	authors := []string{"claude-coordinator", "researcher", "wave-impl", "project-lead"}
	bodies := []string{
		"Historical checkpoint: validation evidence attached and the next worker dispatched.",
		"Review note: outcomes reconciled against the issue acceptance criteria.",
		"Coordinator handoff recorded after the implementation batch settled.",
		"Repository state refreshed; no operator action was required for this batch.",
	}
	for i := 0; i < 24; i++ {
		notes = append(notes, WorkflowNoteView{
			ID: 100 - int64(i), Author: authors[i%len(authors)],
			Body: bodies[i%len(bodies)] + " Batch " + strconv.Itoa(i+1) + ".",
			Repo: "jerryfane/gitmoot", CreatedAt: anchor - int64((240+i*37)*int(time.Minute/time.Millisecond)),
		})
	}
	return notes
}

func fakeWorkflowNode(id, parentID, title, agent, runtime string, state NodeState, startedAt, endedAt int64) WorkflowNode {
	return WorkflowNode{
		ID: id, ParentID: parentID, Title: title, Agent: agent, Runtime: runtime,
		State: state, StartedAt: startedAt, EndedAt: endedAt, Deps: []string{},
	}
}

func fakeWorkflowRuns(anchor int64) []WorkflowRun {
	minute := int64(time.Minute / time.Millisecond)
	currentRoot := fakeWorkflowNode("wf-current-root", "", "Merge gate exact-head guard — validation pass", "claude-coordinator", "claude", "running", anchor-8*minute, 0)
	currentImpl := fakeWorkflowNode("wf-current-impl", currentRoot.ID, "implement: exact-head guard", "wave-impl", "codex", "running", anchor-6*minute, 0)
	currentReview := fakeWorkflowNode("wf-current-review", currentRoot.ID, "review: validation results", "researcher", "claude", "queued", 0, 0)
	failedRoot := fakeWorkflowNode("wf-failed-root", "", "Independent design research — exact-head guard", "research-fable", "claude", "failed", anchor-18*minute, anchor-18*minute+15_000)
	failedChild := fakeWorkflowNode("wf-failed-child", failedRoot.ID, "ask: evaluate stale-head approvals", "research-fable", "claude", "failed", anchor-18*minute, anchor-18*minute+15_000)
	solRoot := fakeWorkflowNode("wf-sol-root", "", "Independent design research — exact-head guard", "research-sol", "codex", "succeeded", anchor-38*minute, anchor-33*minute)
	solChild := fakeWorkflowNode("wf-sol-child", solRoot.ID, "review: compare guard strategies", "research-sol", "codex", "succeeded", anchor-37*minute, anchor-33*minute)
	baseRoot := fakeWorkflowNode("wf-base-root", "", "Baseline: stale-head approval audit", "researcher", "kimi", "succeeded", anchor-70*minute, anchor-64*minute)
	runs := []WorkflowRun{
		{
			RunID: fakeRunID, Title: currentRoot.Title, Agent: currentRoot.Agent, Runtime: currentRoot.Runtime,
			Repo: "jerryfane/gitmoot", State: currentRoot.State, StartedAt: currentRoot.StartedAt, ElapsedS: 8 * 60,
			Children: []WorkflowChild{
				{ID: currentImpl.ID, Action: "implement", Agent: currentImpl.Agent, Runtime: currentImpl.Runtime, State: currentImpl.State, ElapsedS: 6 * 60},
				{ID: currentReview.ID, Action: "review", Agent: currentReview.Agent, Runtime: currentReview.Runtime, State: currentReview.State},
			},
			Nodes: []WorkflowNode{currentRoot, currentImpl, currentReview},
		},
		{
			RunID: "run-research-fable", Title: failedRoot.Title, Agent: failedRoot.Agent, Runtime: failedRoot.Runtime,
			Repo: "jerryfane/gitmoot", State: failedRoot.State, StartedAt: failedRoot.StartedAt, EndedAt: failedRoot.EndedAt, ElapsedS: 15,
			Children: []WorkflowChild{{ID: failedChild.ID, Action: "ask", Agent: failedChild.Agent, Runtime: failedChild.Runtime, State: failedChild.State, ElapsedS: 15}},
			Nodes:    []WorkflowNode{failedRoot, failedChild},
		},
		{
			RunID: "run-research-sol", Title: solRoot.Title, Agent: solRoot.Agent, Runtime: solRoot.Runtime,
			Repo: "jerryfane/gitmoot", State: solRoot.State, StartedAt: solRoot.StartedAt, EndedAt: solRoot.EndedAt, ElapsedS: 5 * 60,
			Children: []WorkflowChild{{ID: solChild.ID, Action: "review", Agent: solChild.Agent, Runtime: solChild.Runtime, State: solChild.State, ElapsedS: 4 * 60}},
			Nodes:    []WorkflowNode{solRoot, solChild},
		},
		{
			RunID: "run-demo-single", Title: baseRoot.Title, Agent: baseRoot.Agent, Runtime: baseRoot.Runtime,
			Repo: "jerryfane/gitmoot", State: baseRoot.State, StartedAt: baseRoot.StartedAt, EndedAt: baseRoot.EndedAt, ElapsedS: 6 * 60,
			Children: []WorkflowChild{}, Nodes: []WorkflowNode{baseRoot},
		},
	}
	titles := []string{"Contract validation sweep", "Mission-log interaction audit", "Pagination cursor verification", "Coordinator synthesis checkpoint"}
	agents := []string{"wave-impl", "researcher", "reviewer-kimi", "claude-coordinator"}
	runtimes := []string{"codex", "claude", "kimi", "claude"}
	for i := 0; i < 12; i++ {
		started := anchor - int64((270+i*50)*int(time.Minute/time.Millisecond))
		ended := started + int64((3+i%5)*int(time.Minute/time.Millisecond))
		state := NodeState("succeeded")
		if i == 5 {
			state = "failed"
		}
		rootID := "wf-history-" + strconv.Itoa(i+1) + "-root"
		childID := "wf-history-" + strconv.Itoa(i+1) + "-child"
		root := fakeWorkflowNode(rootID, "", titles[i%len(titles)]+" · batch "+strconv.Itoa(i+1), agents[i%len(agents)], runtimes[i%len(runtimes)], state, started, ended)
		child := fakeWorkflowNode(childID, rootID, "review: historical batch evidence", agents[(i+1)%len(agents)], runtimes[(i+1)%len(runtimes)], state, started+30_000, ended)
		runs = append(runs, WorkflowRun{
			RunID: "run-history-" + strconv.Itoa(i+1), Title: root.Title, Agent: root.Agent, Runtime: root.Runtime,
			Repo: "jerryfane/gitmoot", State: state, StartedAt: started, EndedAt: ended, ElapsedS: (ended - started) / 1000,
			Children: []WorkflowChild{{ID: child.ID, Action: "review", Agent: child.Agent, Runtime: child.Runtime, State: state, ElapsedS: (ended - child.StartedAt) / 1000}},
			Nodes:    []WorkflowNode{root, child},
		})
	}
	return runs
}

func summarizeFakeWorkflow(runs []WorkflowRun, notes []WorkflowNoteView) WorkflowSummary {
	summary := WorkflowSummary{Label: fakeWorkflow, Notes: len(notes), TokensIn: 1_230_000, TokensOut: 15_300}
	for _, run := range runs {
		for _, n := range run.Nodes {
			summary.Jobs++
			switch n.State {
			case "queued":
				summary.Queued++
			case "running":
				summary.Running++
			case "succeeded":
				summary.Succeeded++
			case "failed":
				summary.Failed++
			case "blocked":
				summary.Blocked++
			case "cancelled":
				summary.Cancelled++
			}
			if summary.FirstAt == 0 || (n.StartedAt > 0 && n.StartedAt < summary.FirstAt) {
				summary.FirstAt = n.StartedAt
			}
			if n.StartedAt > summary.LastAt {
				summary.LastAt = n.StartedAt
			}
			if n.EndedAt > summary.LastAt {
				summary.LastAt = n.EndedAt
			}
		}
	}
	for _, note := range notes {
		if note.CreatedAt > summary.LastAt {
			summary.LastAt = note.CreatedAt
		}
	}
	return summary
}

func fakeWorkflowIndex(anchor int64) []WorkflowIndexEntry {
	minute := int64(time.Minute / time.Millisecond)
	hour := int64(time.Hour / time.Millisecond)
	day := int64(24 * time.Hour / time.Millisecond)
	runs, notes := fakeWorkflowRuns(anchor), fakeWorkflowNotes(anchor)
	summary := summarizeFakeWorkflow(runs, notes)
	primary := WorkflowIndexEntry{
		Label: fakeWorkflow, Summary: "Building the dashboard workflow mission log and operator navigation.", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "fable", SessionID: "7b2e04"}, State: "active",
		Counts:   WorkflowCounts{Jobs: summary.Jobs, Running: summary.Running, Queued: summary.Queued, Succeeded: summary.Succeeded, Failed: summary.Failed, Blocked: summary.Blocked, Notes: summary.Notes},
		TokensIn: summary.TokensIn, TokensOut: summary.TokensOut, FirstAt: summary.FirstAt, LastAt: summary.LastAt, LastNote: notes[0].Body, Repos: []string{"jerryfane/gitmoot", "jerryfane/gitmoot-dashboard"},
	}
	return []WorkflowIndexEntry{
		{Label: fakeStalledWorkflow, Summary: "Hardening scheduled arXiv retries after repeated rate-limit failures.", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "fable", SessionID: "a3f19c"}, State: "stalled", StalledForS: 40 * 60, Counts: WorkflowCounts{Jobs: 4, Succeeded: 2, Failed: 2, Notes: 5}, TokensIn: 64_000, TokensOut: 16_000, FirstAt: anchor - 3*hour, LastAt: anchor - 40*minute, LastNote: "Panel synthesized temp JSON + env var; no resume note since.", Repos: []string{"jerryfane/arxiv-post-agent"}},
		primary,
		{Label: "sol/review-sweep", Summary: "Sweeping open PRs for reviews that predate their current head.", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "sol", SessionID: "c40d8a"}, State: "active", Counts: WorkflowCounts{Jobs: 11, Running: 2, Succeeded: 9, Notes: 12}, TokensIn: 680_000, TokensOut: 210_000, FirstAt: anchor - 2*hour, LastAt: anchor - 4*minute, LastNote: "Validation workers are still moving.", Repos: []string{"jerryfane/gitmoot"}},
		{Label: "officeqa-point-formula", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "kimi", SessionID: "d7c410"}, State: "settled", Counts: WorkflowCounts{Jobs: 16, Succeeded: 12, Failed: 3, Blocked: 1, Notes: 9}, TokensIn: 2_100_000, TokensOut: 800_000, FirstAt: anchor - day, LastAt: anchor - 8*hour, LastNote: "Closing note published.", Repos: []string{"jerryfane/officeqa"}},
		{Label: "sol/token-accounting", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "sol", SessionID: "1f9b22"}, State: "settled", Counts: WorkflowCounts{Jobs: 9, Succeeded: 7, Failed: 1, Blocked: 1, Notes: 6}, TokensIn: 470_000, TokensOut: 170_000, FirstAt: anchor - 9*hour, LastAt: anchor - 6*hour, LastNote: "Token totals reconciled.", Repos: []string{"jerryfane/gitmoot"}},
		{Label: "ops/design-notes", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "ops", SessionID: "0f621a"}, State: "settled", Counts: WorkflowCounts{Jobs: 5, Succeeded: 5}, TokensIn: 18_000, TokensOut: 4_000, FirstAt: anchor - 5*hour, LastAt: anchor - 4*hour, Repos: []string{"jerryfane/gitmoot"}},
		{Label: "adhoc/researcher", Coordinator: WorkflowCoordinator{Author: "claude-coordinator", Pane: "adhoc", SessionID: "8aa912"}, State: "settled", Counts: WorkflowCounts{Jobs: 26, Succeeded: 22, Failed: 4}, TokensIn: 3_600_000, TokensOut: 1_300_000, FirstAt: anchor - 3*day, LastAt: anchor - day, Repos: []string{"jerryfane/gitmoot", "jerryfane/sophon"}},
	}
}

func cursorStartRun(runs []WorkflowRun, cursor string) int {
	if cursor == "" {
		return 0
	}
	for i := range runs {
		if runs[i].RunID == cursor {
			return i + 1
		}
	}
	return len(runs)
}

func workflowNoteCursor(n WorkflowNoteView) string {
	return strconv.FormatInt(n.CreatedAt, 10) + ":" + strconv.FormatInt(n.ID, 10)
}

func cursorStartNote(notes []WorkflowNoteView, cursor string) int {
	if cursor == "" {
		return 0
	}
	for i := range notes {
		if workflowNoteCursor(notes[i]) == cursor {
			return i + 1
		}
	}
	return len(notes)
}

// Workflows implements the deterministic index side of WorkflowDataSource.
func (f *FakeDataSource) Workflows(ctx context.Context) ([]WorkflowIndexEntry, error) {
	if !f.workflowsEnabled {
		return []WorkflowIndexEntry{}, nil
	}
	return fakeWorkflowIndex(f.anchor()), nil
}

// Overview implements the optional operator-first landing-page contract. Every
// value is anchored to the fake run start so repeated reads are byte-stable.
func (f *FakeDataSource) Overview(ctx context.Context) (Overview, error) {
	return Overview{
		NeedsYou: []OverviewNeedsYou{
			{Kind: "pr_awaiting_merge", Repo: "jerryfane/vetrina", Ref: "#77", Title: "Charlotte selfimprove pass 3 — layout tokens", AgeS: 11 * 3600, CI: "pending", Link: "https://github.com/jerryfane/vetrina/pull/77"},
			{Kind: "stalled_workflow", Repo: "jerryfane/arxiv-post-agent", Ref: "40m", Title: "1 run failed · coordinator silent", AgeS: 40 * 60, Link: "/workflows/fable%2Farxiv-retry-hardening", Label: fakeStalledWorkflow, Pane: "fable", SessionID: "a3f19c", LastNote: "synthesized temp JSON + env var; 16KiB caps"},
			{Kind: "pr_awaiting_merge", Repo: "jerryfane/gitmoot", Ref: "#892", Title: "Require an exact-head review before the merge gate opens", AgeS: 2 * 3600, CI: "green", Link: "https://github.com/jerryfane/gitmoot/pull/892"},
			{Kind: "blocked_job", Repo: "jerryfane/gitmoot", Ref: "planner", Title: "Job blocked · needs writable /root/.gitmoot", AgeS: 5 * 86400, Link: "/attention"},
			{Kind: "pr_awaiting_merge", Repo: "jerryfane/arxiv-post-agent", Ref: "#418", Title: "Fix arxiv summary retry backoff on 429", AgeS: 6 * 3600, CI: "red", Link: "https://github.com/jerryfane/arxiv-post-agent/pull/418"},
		},
		Activity: OverviewActivity{
			Workflows: []OverviewWorkflowActivity{
				{Label: "sol/dashboard-redesign", Running: 2, Agents: []string{"sol-impl-a", "ui-designer-researcher"}, StartedAgoS: 4 * 60},
				{Label: "fable/review-sweep", Running: 3, Agents: []string{"researcher", "research-sol", "wave-impl"}, StartedAgoS: 25 * 60},
			},
			UnattendedNote: "ops/… + adhoc/… · 4 more jobs running",
			Queued:         8,
		},
		Today: OverviewToday{
			Completed: 142, Failed: 14, Cancelled: 6, TokensIn: 38_100_000, TokensOut: 9_100_000,
			PerHour: [24]int{8, 7, 5, 5, 4, 4, 3, 5, 10, 18, 30, 42, 51, 56, 49, 44, 38, 45, 52, 43, 36, 25, 19, 17},
			Notable: []OverviewNotable{
				{Agent: "researcher", Title: "Design review for gitmoot #863 — merge gate evidence", Outcome: "succeeded", ElapsedS: 516, AgeS: 12 * 60},
				{Agent: "research-fable", Title: "Independent design research for gitmoot #864: workflow", Outcome: "failed", ElapsedS: 15, AgeS: 34 * 60},
				{Agent: "wave-impl", Title: "FEATURE: gitmoot server half of dashboard #75", Outcome: "succeeded", ElapsedS: 608, AgeS: 3600},
				{Agent: "btc-coordinator", Title: "Price model backtest sweep r8", Outcome: "succeeded", ElapsedS: 276, AgeS: 5400},
				{Agent: "planner", Title: "Plan MOOT for jerryfane/gitmoot #779", Outcome: "cancelled", ElapsedS: 0, AgeS: 2 * 3600},
			},
		},
		Fleet: []OverviewFleet{
			{Agent: "researcher", Runtime: "claude", Running: true, JobsToday: 22},
			{Agent: "btc-coordinator", Runtime: "codex", Running: true, JobsToday: 41},
			{Agent: "lead", Runtime: "claude", JobsToday: 28},
			{Agent: "planner", Runtime: "codex", JobsToday: 12},
			{Agent: "researcher-kimi", Runtime: "kimi", JobsToday: 9},
		},
	}, nil
}

func fakeTaskTitles() []string {
	return []string{
		"Require an exact-head review before the merge gate opens",
		"Fix arxiv summary retry backoff on 429 responses",
		"Record weekly design notes from merged review findings",
		"Baseline repo reconciliation for GitHub fleet sync",
		"Messaging, approvals and agent delivery for waybread",
		"Charlotte selfimprove pass 3 — vetrina layout tokens",
		"Wire gitmoot activepieces support end to end",
		"Wake the requesting seat when a review verdict lands",
		"Design review for dashboard #78 information architecture",
		"Backtest sweep r8 for the bitcoin price model",
		"Org chart role scopes for the review fleet",
		"Health page resource-lock amber thresholds",
	}
}

func fakeTaskRepo(i int) string {
	repos := []string{"jerryfane/gitmoot", "jerryfane/arxiv-post-agent", "jerryfane/bitcoin-price-pred", "jerryfane/vetrina", "jerryfane/waybread", "jerryfane/sentient-arena", "jerryfane/joltra"}
	return repos[i%len(repos)]
}

func fakeTaskAgent(i int) string {
	agents := []string{"researcher", "project-lead", "planner", "btc-coordinator", "wave-impl", "reviewer-kimi", "implementer"}
	return agents[i%len(agents)]
}

// Tasks implements the optional task-board contract with the mockup's target
// volumes: 10 PR-open, 5 blocked, and 4 merged.
func (f *FakeDataSource) Tasks(ctx context.Context) ([]TaskSummary, error) {
	anchor := f.anchor()
	titles := fakeTaskTitles()
	counts := []struct {
		state string
		count int
	}{{"pr_open", 10}, {"blocked", 5}, {"merged", 4}}
	tasks := make([]TaskSummary, 0, 19)
	serial := 0
	for _, group := range counts {
		for i := 0; i < group.count; i++ {
			serial++
			age := int64((serial*7 + 3) * 60)
			if group.state == "blocked" {
				age = int64(i+1) * 86400
			} else if group.state == "merged" {
				age = int64(i+1) * 86400
			}
			title := titles[(serial-1)%len(titles)]
			if serial > len(titles) {
				title += " · pass " + strconv.Itoa((serial-1)/len(titles)+1)
			}
			task := TaskSummary{
				ID: "task-" + strconv.Itoa(serial), Title: title, Repo: fakeTaskRepo(serial - 1), State: group.state,
				Agent: fakeTaskAgent(serial - 1), UpdatedAt: anchor - age*1000, AgeS: age,
			}
			switch group.state {
			case "pr_open":
				task.PRNumber = 620 + i*7
				ci := []string{"green", "green", "red", "pending"}
				task.CI = ci[i%len(ci)]
			case "blocked":
				reasons := []string{"CI red on main — 3 failing e2e specs", "needs writable /root/.gitmoot", "awaiting owner decision on API shape", "merge conflict after upstream refactor", "local toolchain unavailable on worker"}
				task.BlockedReason = reasons[i%len(reasons)]
				if i == 0 {
					task.Agent = "implementer"
				}
			case "merged":
				task.PRNumber = 700 + i
				task.CI = "green"
			}
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

const fakeOrgDataAsOf = "2026-07-23T01:45:00Z"

func fakeOrgView() OrgView {
	escalations := []OrgEscalation{
		{
			From:     "g4",
			To:       "lead",
			Wf:       "g2/1097-org-dashboard",
			Question: "Should the blocked role keep the current store projection or wait for the RFC field rename?",
			At:       "2026-07-23T01:34:00Z",
		},
		{
			From:     "vetrina",
			To:       "owner",
			Wf:       "vetrina/dashboard-visual-audit",
			Question: "The recycle deadline passed with the comparison pass still open; should the role hand off now?",
			At:       "2026-07-23T01:08:00Z",
		},
	}
	return OrgView{
		DataAsOf:         fakeOrgDataAsOf,
		DetectionEnabled: true,
		DetectionHint:    "",
		Health: OrgHealth{
			Roles:           10,
			Working:         3,
			Blocked:         1,
			Overdue:         1,
			OpenEscalations: len(escalations),
			StalledWakes:    3,
		},
		Roles: []OrgNode{
			{Name: "owner", DisplayName: "Jerry", Depth: 0, Scope: []string{"fleet"}, MergeRule: "owner-approval", Pane: "owner", PresenceState: "idle", PresenceDetail: "fleet oversight", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T01:42:00Z"},
			{Name: "lead", Parent: "owner", Depth: 1, Scope: []string{"coordination", "delivery"}, MergeRule: "review", Pane: "lead", PresenceState: "working", PresenceDetail: "coordinating RFC #1042 phase 4", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T01:44:00Z"},
			{Name: "g2", Parent: "lead", Depth: 2, Scope: []string{"gitmoot", "dashboard"}, MergeRule: "squash", Pane: "g2", PresenceState: "working", PresenceDetail: "implementing the Org data API", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T01:43:00Z"},
			{Name: "g3", Parent: "lead", Depth: 2, Scope: []string{"research", "review"}, MergeRule: "squash", Pane: "g3", PresenceState: "idle", PresenceDetail: "available", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T01:19:00Z"},
			{Name: "g4", Parent: "lead", Depth: 2, Scope: []string{"gitmoot", "integration"}, MergeRule: "squash", Pane: "g4", PresenceState: "blocked", PresenceDetail: "waiting on an API contract decision", Badges: OrgBadges{BlockedSince: "2026-07-23T01:31:00Z", MissedWakes: 2}, LastSeenAt: "2026-07-23T01:30:00Z"},
			{Name: "herdres", Parent: "lead", Depth: 2, Scope: []string{"messaging", "telegram"}, MergeRule: "squash", Pane: "herdres", PresenceState: "idle", PresenceDetail: "delivery queue clear", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T00:48:00Z"},
			{Name: "jarvis", Parent: "lead", Depth: 2, Scope: []string{"operations"}, MergeRule: "squash", Pane: "jarvis", PresenceState: "never-seen", Badges: OrgBadges{}},
			{Name: "joltra", Parent: "lead", Depth: 2, Scope: []string{"media", "production"}, MergeRule: "squash", Pane: "joltra", PresenceState: "working", PresenceDetail: "rendering the weekly clip batch", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T01:41:00Z"},
			{Name: "trend-scout", Parent: "lead", Depth: 2, Scope: []string{"research", "signals"}, MergeRule: "squash", Pane: "trend-scout", PresenceState: "idle", PresenceDetail: "next scan scheduled", Badges: OrgBadges{}, LastSeenAt: "2026-07-23T00:37:00Z"},
			{Name: "vetrina", Parent: "lead", Depth: 2, Scope: []string{"design", "visual-review"}, MergeRule: "squash", Pane: "vetrina", PresenceState: "idle", PresenceDetail: "comparison pass awaiting handoff", Badges: OrgBadges{Overdue: "23m", MissedWakes: 1}, LastSeenAt: "2026-07-23T00:52:00Z"},
		},
		Escalations: escalations,
		Feed: []OrgFeedRow{
			{Kind: "blocked_since", Role: "g4", At: "2026-07-23T01:31:00Z", Since: "14m", Detail: "waiting on an API contract decision"},
			{Kind: "recycle", Role: "joltra", At: "2026-07-23T00:58:00Z", Detail: "handoff recorded after the weekly clip batch"},
		},
	}
}

// Org implements the deterministic read-only organization snapshot.
func (f *FakeDataSource) Org(ctx context.Context) (OrgView, error) {
	return fakeOrgView(), nil
}

// OrgRole implements the organization role drill-down.
func (f *FakeDataSource) OrgRole(ctx context.Context, name string) (OrgRoleView, error) {
	org := fakeOrgView()
	var node *OrgNode
	for i := range org.Roles {
		if org.Roles[i].Name == name {
			node = &org.Roles[i]
			break
		}
	}
	if node == nil {
		return OrgRoleView{}, ErrOrgRoleNotFound
	}

	path := []string{"owner"}
	if node.Name != "owner" {
		path = append(path, "lead")
	}
	if node.Depth > 1 {
		path = append(path, node.Name)
	}
	escalations := make([]OrgEscalation, 0, 1)
	for _, escalation := range org.Escalations {
		if escalation.From == name || escalation.To == name {
			escalations = append(escalations, escalation)
		}
	}

	view := OrgRoleView{
		Identity: OrgRoleIdentity{
			Name: node.Name, DisplayName: node.DisplayName, Parent: node.Parent, MergeRule: node.MergeRule,
			Pane: node.Pane, Scope: append([]string(nil), node.Scope...), Depth: node.Depth, Path: path,
		},
		Presence: OrgRolePresence{
			State: node.PresenceState, BlockedSince: node.Badges.BlockedSince,
			LastSeenAt: node.LastSeenAt, MissedWakes: node.Badges.MissedWakes,
		},
		Recycle: OrgRoleRecycle{
			LastHandoffAt: "2026-07-23T00:58:00Z", LastHandoffText: "Completed the previous assignment and left a concise handoff.",
			RecycleAfter: "2h", Remaining: "1h13m",
		},
		Activity:    OrgRoleActivity{JobsToday: map[string]int{"succeeded": 4, "failed": 0, "running": 0}, Notes: 3},
		Escalations: escalations,
	}
	switch name {
	case "g2":
		view.Activity = OrgRoleActivity{JobsToday: map[string]int{"succeeded": 5, "failed": 0, "running": 1}, Notes: 4}
	case "g4":
		view.Recycle = OrgRoleRecycle{
			LastHandoffAt: "2026-07-23T00:55:00Z", LastHandoffText: "Integration tests passed; contract naming still needs a decision.",
			RecycleAfter: "2h", Remaining: "1h10m",
		}
		view.Activity = OrgRoleActivity{JobsToday: map[string]int{"succeeded": 3, "failed": 1, "blocked": 1}, Notes: 6}
	case "jarvis":
		view.Recycle = OrgRoleRecycle{}
		view.Activity = OrgRoleActivity{JobsToday: map[string]int{}, Notes: 0}
	case "vetrina":
		view.Recycle = OrgRoleRecycle{
			LastHandoffAt: "2026-07-22T23:22:00Z", LastHandoffText: "Desktop comparison is complete; mobile review remains.",
			RecycleAfter: "2h", Overdue: "23m",
		}
		view.Activity = OrgRoleActivity{JobsToday: map[string]int{"succeeded": 2, "failed": 0}, Notes: 5}
	}
	return view, nil
}

// Workflow implements WorkflowDataSource with complete run trees and an
// independently paginated, newest-first note journal.
func (f *FakeDataSource) Workflow(ctx context.Context, label string, q WorkflowQuery) (WorkflowView, error) {
	if !f.workflowsEnabled || (label != fakeWorkflow && label != fakeStalledWorkflow) {
		return WorkflowView{}, ErrWorkflowNotFound
	}
	anchor := f.anchor()

	if q.MaxRuns <= 0 || q.MaxRuns > workflowMaxRuns {
		q.MaxRuns = workflowMaxRuns
	}
	if q.MaxNotes <= 0 || q.MaxNotes > workflowMaxNotes {
		q.MaxNotes = workflowMaxNotes
	}

	runs := fakeWorkflowRuns(anchor)
	notes := fakeWorkflowNotes(anchor)
	state := "active"
	coordinator := WorkflowCoordinator{Author: "claude-coordinator", Pane: "fable", SessionID: "7b2e04"}
	workDir := "/root/gitmoot"
	stalledFor := int64(0)
	if label == fakeStalledWorkflow {
		runs = []WorkflowRun{runs[1], runs[2]}
		shift := int64(22 * time.Minute / time.Millisecond)
		for i := range runs {
			runs[i].StartedAt -= shift
			runs[i].EndedAt -= shift
			for j := range runs[i].Nodes {
				if runs[i].Nodes[j].StartedAt > 0 {
					runs[i].Nodes[j].StartedAt -= shift
				}
				if runs[i].Nodes[j].EndedAt > 0 {
					runs[i].Nodes[j].EndedAt -= shift
				}
			}
		}
		notes = append([]WorkflowNoteView(nil), notes[4:9]...)
		for i := range notes {
			notes[i].CreatedAt -= int64(40 * time.Minute / time.Millisecond)
			notes[i].Repo = "jerryfane/arxiv-post-agent"
		}
		state = "stalled"
		stalledFor = 40 * 60
		coordinator.SessionID = "a3f19c"
		workDir = "/root/arxiv-post-agent"
	}
	summary := summarizeFakeWorkflow(runs, notes)
	summary.Label = label
	if label == fakeStalledWorkflow {
		summary.Summary = "Hardening scheduled arXiv retries after repeated rate-limit failures."
		summary.TokensIn = 64_000
		summary.TokensOut = 16_000
	} else {
		summary.Summary = "Building the dashboard workflow mission log and operator navigation."
	}

	runStart := cursorStartRun(runs, q.RunCursor)
	runEnd := runStart + q.MaxRuns
	if runEnd > len(runs) {
		runEnd = len(runs)
	}
	noteStart := cursorStartNote(notes, q.NoteCursor)
	noteEnd := noteStart + q.MaxNotes
	if noteEnd > len(notes) {
		noteEnd = len(notes)
	}
	view := WorkflowView{
		Summary: summary, State: state, StalledForS: stalledFor,
		Coordinator: coordinator, WorkDir: workDir, Runs: runs[runStart:runEnd], Notes: notes[noteStart:noteEnd],
	}
	if runEnd < len(runs) && runEnd > runStart {
		view.NextRunCursor = runs[runEnd-1].RunID
	}
	if noteEnd < len(notes) && noteEnd > noteStart {
		view.NextNoteCursor = workflowNoteCursor(notes[noteEnd-1])
	}
	view.Truncated = view.NextRunCursor != "" || view.NextNoteCursor != ""
	return view, nil
}

// nodeUpdated returns a node's most-recent activity time (epoch ms): its
// last timeline event, falling back to EndedAt/StartedAt.
func nodeUpdated(n Node) int64 {
	updated := n.StartedAt
	if n.EndedAt > updated {
		updated = n.EndedAt
	}
	for _, e := range n.Events {
		if e.T > updated {
			updated = e.T
		}
	}
	return updated
}

// fakeKind maps a fake node to a plausible job kind (ask|review|implement) from
// its title so the Jobs page has something to group on.
func fakeKind(n Node) string {
	switch {
	case strings.HasPrefix(n.Title, "implement"):
		return "implement"
	case strings.Contains(n.Title, "review"):
		return "review"
	default:
		return "ask"
	}
}

// prNumberFromURL extracts the trailing integer of a .../pull/<n> URL, or 0.
func prNumberFromURL(url string) int {
	if url == "" {
		return 0
	}
	i := strings.LastIndex(url, "/")
	if i < 0 || i+1 >= len(url) {
		return 0
	}
	n, err := strconv.Atoi(url[i+1:])
	if err != nil {
		return 0
	}
	return n
}

// jobSummaryFor flattens a Node into a JobSummary for the Jobs page. TokensIn/
// TokensOut are synthesized (deterministically from the id) so standalone dev
// shows plausible per-job token usage.
func jobSummaryFor(n Node, runID string) JobSummary {
	updated := nodeUpdated(n)
	var duration int64
	if n.StartedAt > 0 && updated > n.StartedAt {
		duration = updated - n.StartedAt
	}
	tokensIn, tokensOut := 0, 0
	if n.StartedAt > 0 { // only started jobs have accrued tokens
		seed := len(n.ID) + len(n.Title)
		tokensIn = 1500 + seed*130
		tokensOut = 600 + seed*70
	}
	return JobSummary{
		ID:        n.ID,
		Title:     n.Title,
		Agent:     n.Agent,
		Runtime:   n.Runtime,
		Repo:      fakeRepo,
		Kind:      fakeKind(n),
		State:     n.State,
		Depth:     n.Depth,
		Run:       runID,
		PR:        prNumberFromURL(n.PRURL),
		Started:   n.StartedAt,
		Updated:   updated,
		Duration:  duration,
		TokensIn:  tokensIn,
		TokensOut: tokensOut,
	}
}

// Jobs implements DataSource. The fake feed has a single run, so this flattens
// that run's nodes into per-job rows, sorted Updated desc.
func (f *FakeDataSource) Jobs(ctx context.Context) ([]JobSummary, error) {
	f.mu.Lock()
	nodes := f.cloneNodesLocked()
	f.mu.Unlock()

	jobs := make([]JobSummary, 0, len(nodes)+1)
	for _, n := range nodes {
		jobs = append(jobs, jobSummaryFor(n, fakeRunID))
	}
	jobs = append(jobs, jobSummaryFor(fakeUnregisteredGalaxyNode(nodes[0].StartedAt), fakeRunID))
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Updated != jobs[j].Updated {
			return jobs[i].Updated > jobs[j].Updated // Updated desc
		}
		return jobs[i].ID < jobs[j].ID // stable tiebreak
	})
	return jobs, nil
}

// fakeAgent is the static registration for a fake agent; its live counts are
// filled in from the run's jobs by Agents().
type fakeAgent struct {
	name           string
	role           string
	runtime        string
	repoScope      []string
	model          string
	capabilities   []string
	autonomyPolicy string
	health         string
	// config is the agent's [agents.<name>] section for the detail panel, or nil
	// when the agent has no config entry.
	config *AgentConfigInfo
}

// fakeAgents is a handful of registered agents with varied runtimes/health so
// the Agents page has realistic rows standalone. project-lead/implementer/
// integrator match the names used by the seeded run (so their counts are live);
// the rest are idle registrations.
// The fake agents span every config UI branch so the Agents page renders them
// all standalone: agents that carry an [agents.<name>] config section
// (project-lead/implementer/researcher/reviewer-kimi, the last one degraded on
// the kimi runtime) and plain agents with none (integrator/ci-runner). All
// config values are constant (no time.Now()) so a clicked-through detail is
// byte-stable.
var fakeAgents = []fakeAgent{
	{name: "project-lead", role: "coordinator", runtime: "codex", model: "gpt-5.5", capabilities: []string{"ask", "review"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo},
		config: &AgentConfigInfo{JobTimeout: "1h", Model: "gpt-5.5", Capabilities: []string{"ask", "review"}}},
	{name: "implementer", role: "implementer", runtime: "codex", model: "gpt-5.5", capabilities: []string{"implement"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo},
		config: &AgentConfigInfo{JobTimeout: "45m", Model: "gpt-5.5", Capabilities: []string{"implement"}}},
	{name: "integrator", role: "integrator", runtime: "codex", capabilities: []string{"review", "integrate"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
	{name: "researcher", role: "researcher", runtime: "claude", model: "claude-opus-4-8", capabilities: []string{"research"}, autonomyPolicy: "read-only", health: "healthy",
		config: &AgentConfigInfo{JobTimeout: "30m", Model: "claude-opus-4-8", Capabilities: []string{"research"}}},
	{name: "reviewer-kimi", role: "reviewer", runtime: "kimi", model: "kimi-code", capabilities: []string{"review"}, autonomyPolicy: "read-only", health: "degraded",
		config: &AgentConfigInfo{JobTimeout: "30m", Model: "kimi-code", Capabilities: []string{"review"}}},
	{name: "ci-runner", role: "ci", runtime: "shell", capabilities: []string{"ci", "lint"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
}

// Agents implements DataSource. It returns the registered agents with job
// counts aggregated live from the seeded run.
func (f *FakeDataSource) Agents(ctx context.Context) ([]AgentSummary, error) {
	f.mu.Lock()
	nodes := f.cloneNodesLocked()
	f.mu.Unlock()

	// Aggregate per-agent counts from the run's jobs.
	type agg struct {
		jobs, running, succeeded, failed int
		lastActive                       int64
	}
	byAgent := map[string]*agg{}
	for _, n := range nodes {
		a := byAgent[n.Agent]
		if a == nil {
			a = &agg{}
			byAgent[n.Agent] = a
		}
		a.jobs++
		switch n.State {
		case "running":
			a.running++
		case "succeeded":
			a.succeeded++
		case "failed":
			a.failed++
		}
		if u := nodeUpdated(n); u > a.lastActive {
			a.lastActive = u
		}
	}

	out := make([]AgentSummary, 0, len(fakeAgents))
	for _, fa := range fakeAgents {
		s := AgentSummary{
			Name:           fa.name,
			Role:           fa.role,
			Runtime:        fa.runtime,
			RepoScope:      fa.repoScope,
			Model:          fa.model,
			Capabilities:   fa.capabilities,
			AutonomyPolicy: fa.autonomyPolicy,
			Health:         fa.health,
		}
		if a := byAgent[fa.name]; a != nil {
			s.JobCount = a.jobs
			s.RunningCount = a.running
			s.SucceededCount = a.succeeded
			s.FailedCount = a.failed
			s.LastActive = a.lastActive
		}
		out = append(out, s)
	}

	return out, nil
}

// Agent implements DataSource. It returns the click-through detail for a single
// agent: the same AgentSummary row Agents() lists (so counts line up with the
// Agents page) plus its config section. Unknown names return ErrAgentNotFound.
func (f *FakeDataSource) Agent(ctx context.Context, name string) (AgentDetail, error) {
	agents, err := f.Agents(ctx)
	if err != nil {
		return AgentDetail{}, err
	}
	for _, a := range agents {
		if a.Name != name {
			continue
		}
		detail := AgentDetail{AgentSummary: a}
		// Config section comes from the registered fakeAgent (nil config => "no
		// config entry" in the panel). Constant, so the detail stays byte-stable
		// across calls.
		for i := range fakeAgents {
			fa := &fakeAgents[i]
			if fa.name != name {
				continue
			}
			if fa.config != nil {
				cfg := *fa.config
				detail.Config = &cfg
			}
			break
		}
		return detail, nil
	}
	return AgentDetail{}, ErrAgentNotFound
}

// fakeChartsNow is the fixed reference instant the Charts/Health fake views
// treat as "now". Unlike the live-advancing run (Jobs/Agents,
// which embed time.Now() and evolve as the background goroutine ticks), the
// Charts and Health views must be byte-stable across polls so their handlers'
// repeat-call equality tests hold. Every relative value they derive — day
// buckets, "queued older than 10 min", recent-failure ordering, lock ages — is
// anchored on this constant rather than time.Now(). It is an arbitrary fixed
// UTC instant and is the max timestamp in the seeded charts/health fixture.
var fakeChartsNow = time.Date(2026, 6, 27, 14, 30, 0, 0, time.UTC)

// fakeJobRecord is one synthetic job in the fixed Charts/Health fixture. All
// fields are constant (derived from fakeChartsNow, never time.Now()) so the two
// views are deterministic and identical across calls.
type fakeJobRecord struct {
	ID        string
	Title     string
	Agent     string
	Runtime   string
	Repo      string
	State     NodeState
	Started   int64 // epoch ms
	TokensIn  int
	TokensOut int
}

// fakeChartRepos are the repositories the fixture spreads jobs across (more than
// one so the Charts top-repos breakdown has something to rank).
var fakeChartRepos = []string{fakeRepo, "jerryfane/noted", "acme/api"}

// fakeChartAgents pairs the fixture's agents with their runtimes (a subset of
// fakeAgents so agent names line up with the Agents page).
var fakeChartAgents = []struct{ name, runtime string }{
	{"project-lead", "codex"},
	{"implementer", "codex"},
	{"integrator", "codex"},
	{"researcher", "claude"},
	{"reviewer-kimi", "kimi"},
	{"ci-runner", "shell"},
}

// fakeUTCDayStart returns UTC midnight of the day that is daysAgo days before
// fakeChartsNow.
func fakeUTCDayStart(daysAgo int) time.Time {
	d := fakeChartsNow.UTC()
	day := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	return day.AddDate(0, 0, -daysAgo)
}

// fakeHistory builds the fixed set of jobs behind Charts and Health. Days 9..1
// hold terminal history (bucketed by their Started day); day 0 (today) holds a
// live mix including running/queued/blocked work so Health has current jobs, a
// stuck job and a recent failure. The result is identical on every call.
func fakeHistory() []fakeJobRecord {
	var recs []fakeJobRecord

	// Days 9..1: terminal history only, a deterministic handful per day.
	termStates := []NodeState{"succeeded", "succeeded", "succeeded", "failed", "succeeded", "cancelled"}
	idx := 0
	for d := 9; d >= 1; d-- {
		n := 2 + (d*3+1)%4 // 2..5 jobs that day, deterministic
		for k := 0; k < n; k++ {
			a := fakeChartAgents[idx%len(fakeChartAgents)]
			repo := fakeChartRepos[idx%len(fakeChartRepos)]
			st := termStates[idx%len(termStates)]
			started := fakeUTCDayStart(d).Add(time.Duration((idx*53)%1400) * time.Minute).UnixMilli()
			recs = append(recs, fakeJobRecord{
				ID:        "hist-" + strconv.Itoa(d) + "-" + strconv.Itoa(k),
				Title:     fakeHistTitle(st, repo),
				Agent:     a.name,
				Runtime:   a.runtime,
				Repo:      repo,
				State:     st,
				Started:   started,
				TokensIn:  1200 + (idx%9)*450,
				TokensOut: 500 + (idx%7)*220,
			})
			idx++
		}
	}

	// Day 0 (today): a live mix. minsAgo stays well under fakeChartsNow's
	// time-of-day so every job lands in today's UTC bucket.
	today := []struct {
		id, title, agent, runtime, repo string
		state                           NodeState
		minsAgo                         int
		tin, tout                       int
	}{
		{"job-live-1", "review: nightly maintenance PR", "project-lead", "codex", fakeRepo, "running", 6, 4200, 1800},
		{"job-live-2", "implement: export to CSV", "implementer", "codex", fakeRepo, "running", 3, 3100, 900},
		{"job-live-3", "implement: bulk delete", "implementer", "codex", "jerryfane/noted", "queued", 22, 0, 0},
		{"job-live-4", "review: auth refactor", "reviewer-kimi", "kimi", "acme/api", "blocked", 47, 800, 120},
		{"job-live-5", "ci: lint + test", "ci-runner", "shell", fakeRepo, "succeeded", 90, 600, 200},
		{"job-live-6", "implement: search index", "implementer", "codex", "jerryfane/noted", "failed", 35, 2600, 700},
		{"job-live-7", "research: rate-limit design", "researcher", "claude", "acme/api", "succeeded", 120, 5200, 2600},
	}
	for _, t := range today {
		recs = append(recs, fakeJobRecord{
			ID:        t.id,
			Title:     t.title,
			Agent:     t.agent,
			Runtime:   t.runtime,
			Repo:      t.repo,
			State:     t.state,
			Started:   fakeChartsNow.Add(-time.Duration(t.minsAgo) * time.Minute).UnixMilli(),
			TokensIn:  t.tin,
			TokensOut: t.tout,
		})
	}
	return recs
}

// fakeHistTitle gives a terminal history job a plausible title from its state.
func fakeHistTitle(st NodeState, repo string) string {
	switch st {
	case "failed":
		return "implement: " + repo + " (failed)"
	case "cancelled":
		return "review: " + repo + " (cancelled)"
	default:
		return "implement: " + repo
	}
}

// fakeUTCDayKey returns the UTC YYYY-MM-DD bucket key for an epoch-ms instant.
func fakeUTCDayKey(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02")
}

// Charts implements DataSource. It aggregates the fixed fakeHistory fixture into
// a zero-filled per-day series plus top-agent/top-repo/totals breakdowns over
// the requested window (days 7|30|90; 0 = all history). Output is deterministic
// and byte-stable across calls.
func (f *FakeDataSource) Charts(ctx context.Context, days int) (Charts, error) {
	recs := fakeHistory()

	now := fakeChartsNow.UTC()
	anchorDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Window start (inclusive). days > 0 => the last `days` days ending today;
	// days <= 0 => all history (earliest seeded job day .. today).
	start := anchorDay
	if days > 0 {
		start = anchorDay.AddDate(0, 0, -(days - 1))
	} else {
		for _, r := range recs {
			d := time.UnixMilli(r.Started).UTC()
			dd := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
			if dd.Before(start) {
				start = dd
			}
		}
	}

	// Continuous zero-filled buckets, oldest -> newest.
	idxByDate := map[string]int{}
	daysOut := []ChartDay{}
	for day := start; !day.After(anchorDay); day = day.AddDate(0, 0, 1) {
		key := day.Format("2006-01-02")
		idxByDate[key] = len(daysOut)
		daysOut = append(daysOut, ChartDay{Date: key})
	}

	type agAcc struct {
		jobs, tokensOut int
		runtime         string
	}
	agents := map[string]*agAcc{}
	repos := map[string]int{}
	activeAgents := map[string]bool{}
	totals := ChartTotals{}

	for _, r := range recs {
		bi, in := idxByDate[fakeUTCDayKey(r.Started)]
		if !in {
			continue // outside the window
		}
		d := &daysOut[bi]
		switch r.State {
		case "succeeded":
			d.Succeeded++
			totals.Succeeded++
		case "failed":
			d.Failed++
			totals.Failed++
		case "cancelled":
			d.Cancelled++
		case "blocked":
			d.Blocked++
		case "queued":
			d.Queued++
		case "running":
			d.Running++
		}
		d.TokensIn += r.TokensIn
		d.TokensOut += r.TokensOut

		totals.Jobs++
		totals.TokensIn += r.TokensIn
		totals.TokensOut += r.TokensOut

		a := agents[r.Agent]
		if a == nil {
			a = &agAcc{runtime: r.Runtime}
			agents[r.Agent] = a
		}
		a.jobs++
		a.tokensOut += r.TokensOut
		repos[r.Repo]++
		activeAgents[r.Agent] = true
	}
	totals.ActiveAgents = len(activeAgents)

	// Top 12 agents by Jobs desc, name tie-break.
	agOut := make([]ChartAgent, 0, len(agents))
	for name, a := range agents {
		agOut = append(agOut, ChartAgent{Name: name, Runtime: a.runtime, Jobs: a.jobs, TokensOut: a.tokensOut})
	}
	sort.Slice(agOut, func(i, j int) bool {
		if agOut[i].Jobs != agOut[j].Jobs {
			return agOut[i].Jobs > agOut[j].Jobs
		}
		return agOut[i].Name < agOut[j].Name
	})
	if len(agOut) > 12 {
		agOut = agOut[:12]
	}

	// Top 12 repos by Jobs desc, repo tie-break.
	rpOut := make([]ChartRepo, 0, len(repos))
	for repo, n := range repos {
		rpOut = append(rpOut, ChartRepo{Repo: repo, Jobs: n})
	}
	sort.Slice(rpOut, func(i, j int) bool {
		if rpOut[i].Jobs != rpOut[j].Jobs {
			return rpOut[i].Jobs > rpOut[j].Jobs
		}
		return rpOut[i].Repo < rpOut[j].Repo
	})
	if len(rpOut) > 12 {
		rpOut = rpOut[:12]
	}

	return Charts{Days: daysOut, Agents: agOut, Repos: rpOut, Totals: totals}, nil
}

// fakeDaemonVersion is the version the fake running daemon reports, and
// fakeDaemonLatest is a newer release so the update badge is exercised in dev.
// Both are constant so Health stays byte-stable across calls.
const (
	fakeDaemonVersion = "v0.8.1"
	fakeDaemonLatest  = "v0.8.3"
)

// Health implements DataSource. It derives the daemon liveness, fleet totals,
// held resource locks, wedged jobs and recent failures from the fixed
// fakeHistory fixture (and fixed lock fixtures), anchored on fakeChartsNow. Output is deterministic
// and byte-stable across calls.
func (f *FakeDataSource) Health(ctx context.Context) (Health, error) {
	recs := fakeHistory()
	now := fakeChartsNow.UnixMilli()
	const minute = int64(60 * 1000)
	const stuckCutoff = 10 * minute

	// Fleet totals by state (lifetime terminal counts + today's live jobs).
	totals := HealthTotals{}
	for _, r := range recs {
		switch r.State {
		case "queued":
			totals.Queued++
		case "running":
			totals.Running++
		case "blocked":
			totals.Blocked++
		case "succeeded":
			totals.Succeeded++
		case "failed":
			totals.Failed++
		case "cancelled":
			totals.Cancelled++
		}
	}

	// Resource locks (runtime sessions, etc.), oldest first.
	resLocks := []HealthResourceLock{
		{Key: "runtime:claude:sess-7f3a", Owner: "researcher", AcquiredAt: now - 12*minute, ExpiresAt: now + 18*minute},
		{Key: "runtime:codex:sess-91c2", Owner: "implementer", AcquiredAt: now - 2*minute, ExpiresAt: now + 28*minute},
	}
	sort.SliceStable(resLocks, func(i, j int) bool { return resLocks[i].AcquiredAt < resLocks[j].AcquiredAt })

	// Stuck: blocked jobs + queued older than 10 min, oldest first.
	stuck := []HealthStuckJob{}
	for _, r := range recs {
		var reason string
		switch {
		case r.State == "blocked":
			reason = "blocked awaiting human"
		case r.State == "queued" && now-r.Started > stuckCutoff:
			reason = "queued > 10m (no free worker slot)"
		default:
			continue
		}
		stuck = append(stuck, HealthStuckJob{
			ID:     r.ID,
			Title:  r.Title,
			Agent:  r.Agent,
			Repo:   r.Repo,
			State:  string(r.State),
			Reason: reason,
			Since:  r.Started,
		})
	}
	sort.SliceStable(stuck, func(i, j int) bool {
		if stuck[i].Since != stuck[j].Since {
			return stuck[i].Since < stuck[j].Since // oldest first
		}
		return stuck[i].ID < stuck[j].ID
	})

	// Recent failures, newest first, capped at 10.
	failures := []HealthFailure{}
	for _, r := range recs {
		if r.State != "failed" {
			continue
		}
		failures = append(failures, HealthFailure{
			ID:    r.ID,
			Title: r.Title,
			Agent: r.Agent,
			Repo:  r.Repo,
			At:    r.Started,
		})
	}
	sort.SliceStable(failures, func(i, j int) bool {
		if failures[i].At != failures[j].At {
			return failures[i].At > failures[j].At // newest first
		}
		return failures[i].ID < failures[j].ID
	})
	if len(failures) > 10 {
		failures = failures[:10]
	}

	return Health{
		Daemon: HealthDaemon{Running: true, PID: 4242, StartedAt: now - 6*60*minute, Version: fakeDaemonVersion},
		Update: &HealthUpdate{
			Current:         fakeDaemonVersion,
			Latest:          fakeDaemonLatest,
			ReleaseURL:      "https://github.com/jerryfane/gitmoot/releases/tag/" + fakeDaemonLatest,
			UpdateAvailable: true,
			CheckedAt:       now - 30*minute,
		},
		Totals:         totals,
		ResourceLocks:  resLocks,
		Stuck:          stuck,
		RecentFailures: failures,
	}, nil
}

// Config implements DataSource with a realistic, strictly sanitized effective
// configuration. The fixed timestamp and explicit ordering keep the fixture
// byte-stable across requests.
func (f *FakeDataSource) Config(ctx context.Context) (ConfigSnapshot, error) {
	return ConfigSnapshot{
		ContractVersion: 1,
		Path:            "/home/operator/.config/gitmoot/config.toml",
		ModifiedAt:      fakeChartsNow.Add(-37 * time.Minute).UnixMilli(),
		Exists:          true,
		Sections: []ConfigSection{
			{Name: "github", Knobs: []ConfigKnob{
				{Key: "rate_limit_per_hour", Value: 4200, Default: 4500, IsDefault: false, Kind: "int", Doc: "Reserve-aware GitHub request budget per hour."},
			}},
			{Name: "orchestrate", Knobs: []ConfigKnob{
				{Key: "blocked_ttl", Value: "30m", Default: "30m", IsDefault: true, Kind: "duration", Doc: "Maximum time a blocked job remains awaiting a human; empty disables expiry."},
			}},
		},
		Agents: []ConfigAgent{
			{Name: "galaxy-impl", Runtime: "codex", Model: "gpt-5.6-codex", Capabilities: []string{"ask", "implement"}, AutonomyPolicy: "workspace-write"},
			{Name: "lead", Runtime: "claude", Model: "opus-4.6", Capabilities: []string{"ask", "implement", "review"}, AutonomyPolicy: "workspace-write"},
			{Name: "researcher", Runtime: "kimi", Model: "kimi-k2.5", Capabilities: []string{"ask", "review"}, AutonomyPolicy: "read-only"},
			{Name: "reviewer", Runtime: "codex", Model: "gpt-5.6-codex", Capabilities: []string{"review"}, AutonomyPolicy: "read-only"},
		},
		UnknownKeys: []string{"experimental.scheduler_bias", "plugins.private_token"},
		Keychain:    fakeKeychain(),
	}, nil
}

// fakeKeychain is the healthy, names-only registry fixture shown by the Config
// page. Ordering is explicit and stable; no credential values or value-derived
// data exist in this projection.
func fakeKeychain() KeychainView {
	return KeychainView{
		File: KeychainFileStatus{Path: "/home/operator/.config/gitmoot/keychain.env", Status: "ok"},
		Keys: []KeychainKeyView{
			{
				Name:      "GH_TOKEN",
				Mode:      "injected",
				CreatedAt: "2026-07-08T09:12:00Z",
			},
			{
				Name:          "OPENAI_API_KEY",
				Mode:          "proxied",
				ProxyUpstream: "https://api.openai.com/v1",
				ProxyAuth:     "bearer",
				CreatedAt:     "2026-07-09T14:35:00Z",
			},
			{
				Name:      "TELEGRAM_BOT_TOKEN",
				Mode:      "injected",
				CreatedAt: "2026-07-10T07:48:00Z",
			},
			{
				Name:      "TELEGRAM_CHAT_ID",
				Mode:      "injected",
				CreatedAt: "2026-07-10T07:49:00Z",
			},
		},
	}
}

// fakeMissingKeychain is the second Config fixture: the configured keychain
// path is known, but the file is absent and no registry rows are projected.
func fakeMissingKeychain() KeychainView {
	return KeychainView{
		File: KeychainFileStatus{Path: "/home/operator/.config/gitmoot/keychain.env", Status: "missing"},
		Keys: []KeychainKeyView{},
	}
}

// fakeJobChecks builds the fixed job-detail failed-check fixture (gitmoot #711),
// keyed by job id. job-5 (the integrate/review/open-PR job) failed two
// deterministic result checks under a block policy; every other job passed
// (empty Failed) under the default warn policy. Explanations carry angle
// brackets/ampersands so the client's escaping of untrusted text is exercised.
func fakeJobChecks() map[string]JobChecks {
	return map[string]JobChecks{
		"job-5": {
			JobID: "job-5", Mode: "block",
			Failed: []ResultCheck{
				{CheckID: "pr-opened", Question: "Did the job open a pull request and record its URL?",
					Explanation: "No PR URL was recorded on the result — the branch was pushed but `gh pr create` returned <auth invalid> (sandbox network blocked)."},
				{CheckID: "tests-run", Question: "Does the result show tests were actually run (a command + outcome)?",
					Explanation: "The summary claims \"all green\" but includes no command output; `go test ./...` was never invoked & no results block is present."},
			},
		},
		"job-6": {JobID: "job-6", Mode: "warn", Failed: []ResultCheck{}},
	}
}

// JobChecks implements DataSource. It returns the fixed fakeJobChecks fixture for
// a job id; an unknown job is not an error — it returns the default warn Mode
// with an empty Failed list. Output is deterministic and byte-stable.
func (f *FakeDataSource) JobChecks(ctx context.Context, jobID string) (JobChecks, error) {
	if jc, ok := fakeJobChecks()[jobID]; ok {
		jc.Failed = append([]ResultCheck(nil), jc.Failed...)
		return jc, nil
	}
	return JobChecks{JobID: jobID, Mode: "warn", Failed: []ResultCheck{}}, nil
}

// fakeAttention builds the fixed "Needs a human" fixture (gitmoot #528): three
// blocked job gates. Every timestamp is anchored on fakeChartsNow (never
// time.Now()) so the section is byte-stable across polls.
func fakeAttention() Attention {
	const h = time.Hour
	at := func(x time.Duration) int64 { return fakeChartsNow.Add(-x).UnixMilli() }
	att := Attention{
		Gates: []AttentionGate{
			{JobID: "job-5", Need: "human:confirm-pr-target", Title: "integrate + review + open PR",
				Agent: "integrator", Repo: "jerryfane/noted", State: "blocked", PR: 42, CreatedAt: at(2 * h)},
			{JobID: "job-nightly-score-19", Need: "secret:r2-token", Title: "nightly-deploy · score stage",
				Agent: "claude-a", Repo: "jerryfane/noted", State: "blocked", CreatedAt: at(90 * time.Minute)},
			// Long suggested-action regression fixture (issue #88): the chip must
			// ellipsize instead of squeezing the title into a per-character wrap.
			{JobID: "job-long-action-7", Need: "Commit the intact working-tree changes with: git add internal/cli/merge_gate.go internal/cli/merge_gate_test.go internal/db/store.go internal/db/review_head_store.go && git commit -m 'feat(merge): exact-head review guard' then re-run the gate",
				Title: "Implement the exact-head review guard for the merge gate with bounded evidence and recursive head lookup",
				Agent: "wave-impl", Repo: "jerryfane/gitmoot", State: "blocked", CreatedAt: at(6 * 24 * h)},
		},
	}
	att.Total = len(att.Gates)
	return att
}

// Attention implements DataSource. It returns the fixed fakeAttention fixture;
// output is deterministic and byte-stable across calls.
func (f *FakeDataSource) Attention(ctx context.Context) (Attention, error) {
	return fakeAttention(), nil
}
