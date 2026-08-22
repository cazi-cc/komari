# Upstream sync requires manual resolution

- Upstream: `komari-monitor/komari:main`
- Base: `cazi-cc/komari:main`
- Run: https://github.com/cazi-cc/komari/actions/runs/32095668287

## Conflicting files

```text
database/metricstore/metricstore.go
database/tasks/ping.go
internal/config/settings.go
internal/metricstore/report_test.go
internal/server/runtime.go
utils/messageSender/sender.go
web/rpc/jsonrpc/admin.misc.go
web/rpc/jsonrpc/public.metric.go
web/rpc/jsonrpc/public_metric_test.go
```

## Fork-owned files to preserve

```text
.github/workflows/notify-komari-web-release.yml
.github/workflows/sync-upstream.yml
.github/workflows/test-release-automation.yml
```

Do not merge this report-only commit. Resolve the upstream merge on this branch, delete this file, run the repository tests, and then update the pull request.
