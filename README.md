# monitor

Single-file prod monitor managed by pm2. Five responsibilities in one process:

1. **Discord alarm** — webhook on crash, health failure, or recovery
2. **Healthcheck polling** — Docker socket `/containers/{name}/json` every 60s
3. **Docker event capture** — `die`/`kill`/`oom` events via Docker event stream
4. **Log server** — read-only HTTP API backed by `journalctl` (systemd-journald)
5. **Docker stats polling** — memory + CPU sampled from Docker socket every 20s; `MEMORY_PRESSURE` early-warning alarm before OOM

## Setup

```bash
cp .env.example .env
# edit .env — set DISCORD_ALARM_WEBHOOK, LOG_SERVER_TOKEN, MONITOR_TARGETS

pm2 start ecosystem.config.js
pm2 save
```

No install step needed — `monitor.js` is plain Node.js with no dependencies.

## Log source: systemd-journald

Container logs are read from the host's systemd journal. Monitored containers must ship stdout to journald via the `journald` docker log driver:

```yaml
services:
  hoie-inbox:
    logging:
      driver: journald
      options:
        tag: "hoie-inbox"
```

The log server queries `journalctl CONTAINER_NAME=<name> -n <tail> --output=cat --no-pager`. Retention is host-side, configured in `/etc/systemd/journald.conf`:

```
Storage=persistent
SystemMaxUse=2G
MaxRetentionSec=30day
```

`systemctl restart systemd-journald` to apply. If `/var/log/journal/` doesn't exist the journal is in-memory only — `mkdir -p /var/log/journal && systemctl restart systemd-journald`.

The user running monitor (via pm2) needs read access to the journal — either run as `root` or add the user to the `systemd-journal` group: `sudo usermod -aG systemd-journal <user>` then relogin.

## HTTP API

| Endpoint | Auth | Description |
|---|---|---|
| `GET /health` | none | Server health + monitored container states |
| `GET /containers` | Bearer token | List running containers |
| `GET /logs/:container` | Bearer token | Last 100 log lines |
| `GET /logs/:container?tail=500` | Bearer token | Configurable tail |
| `GET /stats/:container` | Bearer token | Current + up-to-hour ring buffer of Docker stats (memory usage, working set, anon/file/inactive-file breakdown, memory limit, usage ratio, CPU %) |

Auth: `Authorization: Bearer $LOG_SERVER_TOKEN`

Stats are polled from the Docker socket (`/containers/{name}/stats?stream=false`), so they reflect cgroup-derived memory + CPU exactly the way `docker stats` does. Memory pressure is measured on the working set — `memory_stats.usage` minus inactive file cache (`inactive_file` on cgroup v2, `total_inactive_file` on v1), the figure `docker stats` shows as MEM USAGE — because the kernel reclaims that cache before it OOM-kills. A `MEMORY_PRESSURE` alarm fires to Discord when a container's working set / `memory_stats.limit` stays above `MEMORY_PRESSURE_RATIO` for `MEMORY_PRESSURE_SUSTAINED` consecutive samples — early warning before the kernel OOM-kills the container. Only fires when the container has an explicit `--memory` / `mem_limit` set (no explicit limit → the "limit" Docker reports is host RAM, making the ratio uninformative).

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DISCORD_ALARM_WEBHOOK` | yes | — | Discord webhook URL |
| `LOG_SERVER_TOKEN` | yes | — | Bearer token for log API |
| `MONITOR_TARGETS` | yes | — | Comma-separated container names |
| `LOG_PORT` | no | `9000` | Log server port |
| `HEALTH_POLL_INTERVAL_MS` | no | `60000` | Health poll interval |
| `HEALTH_FAIL_THRESHOLD` | no | `1` | Failures before alarm |
| `ALARM_COOLDOWN_MS` | no | `60000` | Cooldown between same alarms |
| `DOCKER_SOCKET` | no | `/var/run/docker.sock` | Docker socket path (healthcheck + event stream + stats poll) |
| `JOURNALCTL_BIN` | no | `journalctl` | Path to `journalctl`; override if not on `$PATH` |
| `STATS_POLL_INTERVAL_MS` | no | `20000` | Docker stats poll interval per container |
| `JOURNALCTL_TIMEOUT_MS` | no | `15000` | Hard-kill `journalctl` if it hangs |
| `STATS_HISTORY_SIZE` | no | `60` | Samples kept in the ring buffer (default = 1 hour at 60s interval) |
| `MEMORY_PRESSURE_RATIO` | no | `0.85` | Alarm when `working set / limit` ≥ this ratio |
| `MEMORY_PRESSURE_SUSTAINED` | no | `3` | Consecutive polls required before alarming |

## Design Doc

https://doc.hoie.kim/s/moltboie/p/prod-monitoring-HRzq5Krhlg
