# gitmoot-dashboard

Read-only web backend for the [gitmoot](https://github.com/jerryfane/gitmoot)
dashboard. It serves the embedded dashboard UI and a JSON API over gitmoot's
store.

This is a small Go module (stdlib only, no third-party dependencies). The UI
lives in `web/dist` and is embedded into the binary.

## Contract

The package exposes a `DataSource` interface (plus optional extensions such as
`WorkflowDataSource`, `OverviewDataSource`, `TasksDataSource`,
`OrgDataSource` and `ChangeCursorDataSource`) and an HTTP `Serve` entry point;
see `datasource.go`.

```go
func Serve(ds DataSource) http.Handler
```

HTTP surface:

| Route | Returns |
| --- | --- |
| `GET /api/overview` | `Overview` |
| `GET /api/tasks` | `[]TaskSummary` |
| `GET /api/workflows`, `GET /api/workflow/{label}` | `[]WorkflowIndexEntry`, `WorkflowView` |
| `GET /api/jobs`, `GET /api/job/{id}` | `[]JobSummary`, `Node` |
| `GET /api/job/{id}/checks` | `JobChecks` |
| `GET /api/agents`, `GET /api/agent/{name}` | `[]AgentSummary`, `AgentDetail` |
| `GET /api/org`, `GET /api/org/role/{name}` | `OrgView`, `OrgRoleView` |
| `GET /api/graph?repo=` | `Graph` (Galaxy) |
| `GET /api/charts?days=` | `Charts` |
| `GET /api/health` | `Health` |
| `GET /api/config` | `ConfigSnapshot` |
| `GET /api/attention` | `Attention` |
| `GET /api/events` | SSE change pings (`event: changed`) |
| other `/api/...` paths and `/events` | 404 |
| everything else | embedded static UI (SPA fallback) |

The server is **read-only** and must never run in the gitmoot daemon path.

## Development

Serve the embedded UI against an in-memory fake feed:

```sh
go run ./cmd/gitmoot-dashboard-dev   # http://localhost:8099
```

Gate:

```sh
go build ./... && go vet ./... && go test ./...
```

## License

Apache-2.0. See [LICENSE](LICENSE).
