# Upstream sync requires manual resolution

- Upstream: `komari-monitor/komari:main`
- Base: `cazi-cc/komari:main`
- Run: https://github.com/cazi-cc/komari/actions/runs/30790698720

## Conflicting files

```text
database/metricstore/metricstore.go
internal/config/settings.go
internal/metricstore/report_test.go
web/rpc/jsonrpc/public.metric.go
web/rpc/jsonrpc/public_metric_test.go
```

Do not merge this report-only commit. Resolve the upstream merge on this branch, delete this file, run the repository tests, and then update the pull request.
