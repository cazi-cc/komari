# Upstream sync requires manual resolution

- Upstream: `komari-monitor/komari:main`
- Base: `cazi-cc/komari:main`
- Run: https://github.com/cazi-cc/komari/actions/runs/33379347411

## Conflicting files

```text
database/metricstore/metricstore.go
database/tasks/ping.go
internal/config/settings.go
internal/metricstore/report_test.go
internal/server/runtime.go
protocol/v2/jsonrpc.go
utils/messageSender/sender.go
utils/pingSchedule.go
web/agent/v2_events.go
web/api/client/report.go
web/api/client/report_v2.go
web/rpc/jsonrpc/admin.misc.go
web/rpc/jsonrpc/client.go
web/rpc/jsonrpc/public.metric.go
web/rpc/jsonrpc/public_metric_test.go
```

## Fork-owned files to preserve

```text
.github/workflows/notify-komari-web-release.yml
.github/workflows/sync-upstream.yml
.github/workflows/test-release-automation.yml
protocol/v2/jsonrpc.go
database/models/pingTask.go
database/tasks/unlock_quality.go
database/tasks/unlock_quality_snapshot.go
database/tasks/tcp_quality_snapshot.go
database/tasks/probe_schedule.go
utils/unlockQualitySchedule.go
docs/AGENT_DISTRIBUTION.md
```

Do not merge this report-only commit. Resolve the upstream merge on this branch, delete this file, run the repository tests, and then update the pull request.
