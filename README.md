# monitor

Single-file prod monitor managed by pm2. Six responsibilities in one process:

1. **Discord alarm** — webhook on crash, health failure, or recovery
2. **Healthcheck polling** — Docker socket `/containers/{name}/json` every 60s
3. **Docker event capture** — `die`/`kill`/`oom` events via Docker event stream
4. **Log server** — read-only HTTP API backed by `journalctl` (systemd-journald)
5. **Docker stats polling** — memory + CPU sampled from Docker socket every 20s; `MEMORY_PRESSURE` early-warning alarm before OOM
6. **Frontend** — static dashboard (`frontend.html`) served at `/`, with cookie login (see below)

## Setup

```bash
cp .env.example .env
# edit .env — set DISCORD_ALARM_WEBHOOK, LOG_SERVER_TOKEN, MONITOR_PROJECT

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
| `GET /` | none | Static frontend dashboard (`frontend.html`); the page itself carries no data |
| `GET /frontend.html` | none | Same dashboard file, direct path |
| `POST /login` | token in body | Exchange `LOG_SERVER_TOKEN` for a session cookie (see Frontend) |
| `POST /logout` | session cookie | Drop the session and clear the cookie |
| `GET /containers` | Bearer token or session cookie | List running containers |
| `GET /logs/:container` | Bearer token or session cookie | Last 100 log lines |
| `GET /logs/:container?tail=500` | Bearer token or session cookie | Configurable tail |
| `GET /stats/:container` | Bearer token or session cookie | Current + up-to-hour ring buffer of Docker stats (memory usage, working set, anon/file/inactive-file breakdown, memory limit, usage ratio (working set / limit), CPU %) |

Auth: `Authorization: Bearer $LOG_SERVER_TOKEN`, or the session cookie set by `POST /login`.

## Frontend

Open `/` in a browser. Log in with the existing `LOG_SERVER_TOKEN` — the same token the API accepts as a Bearer token. On success the server sets an `HttpOnly` session cookie (`monitor_session`) valid for 1 day, refreshed to a fresh 1 day on every authenticated request (sliding expiry: the session only lapses after a full day with no requests). The dashboard then shows running containers with health states, per-container memory/CPU stats, and log tails (configurable tail + optional `since` window, same parameters as the API), auto-refreshing every 30s. Log out with the button in the header.

The token itself is never stored in the browser: the page sends it once in the login POST body, and afterwards the browser only presents the opaque session id. API clients are unaffected — Bearer auth keeps working exactly as before.

## Building

Write code in `src/monitor.ts`, then compile it to `monitor.js` and commit both. `monitor.js` is the only thing pm2 runs — there is no build step on the server; deploy is `git pull` + `pm2 restart monitor`. `frontend.html` ships in the repo next to `monitor.js` and is read from disk per request, so a pull updates the dashboard with no restart.

```bash
# TypeScript and the Node type declarations are build-time tools only
# (never committed, never installed on the server). Install once:
npm install -g typescript @types/node

# Then, from the repo root:
tsc src/monitor.ts --outDir /tmp/monitor-out --module commonjs --target es2022 --esModuleInterop --skipLibCheck --typeRoots "$(npm root -g)/@types"
cp /tmp/monitor-out/monitor.js ./monitor.js
```

Never edit `monitor.js` by hand — it is generated output.

Stats are polled from the Docker socket (`/containers/{name}/stats?stream=false`), so they reflect cgroup-derived memory + CPU exactly the way `docker stats` does. Memory pressure is measured on the working set — `memory_stats.usage` minus inactive file cache (`inactive_file` on cgroup v2, `total_inactive_file` on v1), the figure `docker stats` shows as MEM USAGE — because the kernel reclaims that cache before it OOM-kills. A `MEMORY_PRESSURE` alarm fires to Discord when a container's working set / `memory_stats.limit` stays above `MEMORY_PRESSURE_RATIO` for `MEMORY_PRESSURE_SUSTAINED` consecutive samples — early warning before the kernel OOM-kills the container. Only fires when the container has an explicit `--memory` / `mem_limit` set (no explicit limit → the "limit" Docker reports is host RAM, making the ratio uninformative).

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DISCORD_ALARM_WEBHOOK` | yes | — | Discord webhook URL |
| `LOG_SERVER_TOKEN` | yes | — | Bearer token for log API |
| `MONITOR_PROJECT` | one of these two | — | Compose project name; every service container of the project is monitored, rediscovered before each poll (one-off `compose run` containers excluded) |
| `MONITOR_TARGETS` | one of these two | — | Comma-separated container names; ignored when `MONITOR_PROJECT` is set |
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
