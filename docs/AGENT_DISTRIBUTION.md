# Agent distribution

The Cazi Komari backend owns the agent distribution manifest returned by
`admin:getAgentDistribution` and
`GET /api/admin/settings/agent-distribution`.

Production defaults:

- repository: `cazi-cc/komari-agent`
- installer ref: `main`
- container image: `ghcr.io/cazi-cc/komari-agent:snapshot`
- required arguments: metrics only, 5-second reports, 15-minute basic info

The runtime may override the distribution without rebuilding the frontend:

```text
KOMARI_AGENT_REPOSITORY=cazi-cc/komari-agent
KOMARI_AGENT_SCRIPT_REF=main
KOMARI_AGENT_DOCKER_IMAGE=ghcr.io/cazi-cc/komari-agent:snapshot
```

The default admin frontend is built from `cazi-cc/komari-web`. It must consume
this backend manifest and must not hard-code the upstream agent repository.

## Existing node upgrades

The command shown by the deployed admin frontend is the source of truth because
it contains that node's current endpoint and token. For an existing Linux node:

1. Connect to the node over SSH.
2. Inspect the current unit with `systemctl cat komari-agent` and confirm the
   architecture with `uname -m`.
3. Copy the Linux one-click command from that node's admin action and run it as
   root. The installer replaces the binary and recreates the service with the
   complete command arguments.
4. Verify `systemctl is-active komari-agent` and inspect recent logs with
   `journalctl -u komari-agent -n 50 --no-pager`.

The ChatGPT unlock-quality task requires an Agent build that advertises the
`unlock_quality` capability. Re-running the fork-owned one-click command is the
supported upgrade path; changing only the backend cannot add this capability
to an older Agent.

ChatGPT relay-quality monitoring additionally requires a Cazi Agent build from
2026-08-21 or later. The relay proxy URL is private task configuration: it is
sent only to assigned Agents and must never be copied into public snapshots,
visitor APIs, logs, notifications, or release metadata.

Agents already on a `Snapshot-*` build check this fork at startup and every six
hours. An upstream stable Agent does not automatically migrate to the fork, so
it must be reinstalled once through the fork-owned one-click command. Never
publish a node token in documentation or chat logs.

Before merging an upstream synchronization pull request, verify:

1. `go test ./...` still passes.
2. The distribution endpoint returns only the configured fork.
3. The generated Linux, Windows, macOS and Docker commands contain no
   `komari-monitor/komari-agent` distribution URL.
4. `--disable-web-ssh` remains in the default command.
