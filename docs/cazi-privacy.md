# Cazi Privacy Fork

This fork is the backend used by `ping.cazi.cc`.

## Privacy guarantees

- Anonymous `common:getNodes` responses never contain node IPv4 or IPv6
  addresses, even if the legacy `send_ip_addr_to_guest` setting is enabled.
- Visitor page views are accepted only when `visitor_audit_enabled` is enabled.
- Visitor IP and User-Agent values come from the server request context, not
  from frontend-supplied fields.
- Anonymous writes are bounded and rate limited by the upstream visitor audit
  implementation.
- `admin:getVisitorLogs` is administrator-only and returns structured visitor
  entries for the theme settings page.
- Public TCP and unlock-quality snapshots contain labels and aggregate quality
  metrics only. Probe domains, resolved addresses, DNS servers, fixed entries,
  ports, and address fingerprints remain private.
- Public analysis calls read server-generated snapshots and cannot dispatch an
  Agent probe.
- Scheduled ping, TCP-quality, and unlock-quality tasks use stable phases that
  spread tasks sharing an interval instead of dispatching them in one burst.
- Audit logs are removed by Komari's existing 30-day cleanup task.

The frontend must send only a route path without query parameters, tokens,
search terms, cookies, clipboard data, or other sensitive values.
