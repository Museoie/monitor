"use strict";
/**
 * monitor.ts — Single-file prod monitor + log server
 *
 * Responsibilities:
 *   1. Discord alarm trigger — webhook on any error/crash/health failure
 *   2. Healthcheck polling — Docker socket, GET /containers/{name}/json every 60s
 *   3. Docker event capture — die/oom events via Docker socket event stream (kill events ignored)
 *   4. Log server — read-only HTTP API backed by systemd-journald
 *
 * All four run in one process, managed by pm2 on the host.
 *
 * Log-source note: monitored containers are expected to log via the
 * `journald` docker log driver so their history persists across
 * container recreate. journalctl reads by `CONTAINER_NAME=<name>`.
 * See hoiekim/inbox PR #641 for the compose-side setup.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const http_1 = __importDefault(require("http"));
const https_1 = __importDefault(require("https"));
const child_process_1 = require("child_process");
const fs_1 = require("fs");
// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const DISCORD_WEBHOOK = process.env.DISCORD_ALARM_WEBHOOK ?? "";
const LOG_SERVER_TOKEN = process.env.LOG_SERVER_TOKEN ?? "";
const MONITOR_TARGETS = (process.env.MONITOR_TARGETS ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
const HEALTH_FAIL_THRESHOLD = parseInt(process.env.HEALTH_FAIL_THRESHOLD ?? "1", 10);
const HEALTH_POLL_INTERVAL_MS = parseInt(process.env.HEALTH_POLL_INTERVAL_MS ?? "60000", 10);
const ALARM_COOLDOWN_MS = parseInt(process.env.ALARM_COOLDOWN_MS ?? "60000", 10);
const LOG_PORT = parseInt(process.env.LOG_PORT ?? "9000", 10);
const DOCKER_SOCKET = process.env.DOCKER_SOCKET ?? "/var/run/docker.sock";
const JOURNALCTL_BIN = process.env.JOURNALCTL_BIN ?? "journalctl";
// Tightened default to 20s (was 60s). Typical OOM lead time from
// 85%→kill is 10-60s under sustained load; a 3× minute poll cadence
// often missed the pre-death signal the alarm exists to catch.
const STATS_POLL_INTERVAL_MS = parseIntEnv("STATS_POLL_INTERVAL_MS", 20_000);
const STATS_HISTORY_SIZE = parseIntEnv("STATS_HISTORY_SIZE", 60);
const MEMORY_PRESSURE_RATIO = parseFloatEnv("MEMORY_PRESSURE_RATIO", 0.85);
const MEMORY_PRESSURE_SUSTAINED = parseIntEnv("MEMORY_PRESSURE_SUSTAINED", 3);
const JOURNALCTL_TIMEOUT_MS = parseIntEnv("JOURNALCTL_TIMEOUT_MS", 15_000);
// Host RAM ceiling. Docker reports `memory_stats.limit` as host total
// when a container has no `--memory` cap; treating that as a real
// limit yields misleading `usage_ratio` values (a 4 GB app on 128 GB
// host reads as 0.03 "healthy"). Read /proc/meminfo once at startup.
const HOST_MEM_TOTAL_BYTES = readHostMemTotalBytes();
function parseIntEnv(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === "")
        return fallback;
    const parsed = parseInt(raw, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        console.warn(`[config] ${name}=${JSON.stringify(raw)} is not a positive int — using default ${fallback}`);
        return fallback;
    }
    return parsed;
}
function parseFloatEnv(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === "")
        return fallback;
    const parsed = parseFloat(raw);
    if (!Number.isFinite(parsed)) {
        console.warn(`[config] ${name}=${JSON.stringify(raw)} is not a valid float — using default ${fallback}`);
        return fallback;
    }
    return parsed;
}
function readHostMemTotalBytes() {
    try {
        const meminfo = (0, fs_1.readFileSync)("/proc/meminfo", "utf-8");
        const match = /MemTotal:\s+(\d+)\s+kB/.exec(meminfo);
        if (match)
            return parseInt(match[1], 10) * 1024;
    }
    catch {
        /* not Linux, or /proc unreadable — fall back to a very large value
           so the "cap unset" guard below never accidentally treats a real
           limit as unlimited. */
    }
    return Number.MAX_SAFE_INTEGER;
}
// A `memory_stats.limit` at or above 90% of host RAM is almost
// certainly Docker reporting "no cap" — a legitimate container-level
// cap is orders of magnitude lower on our hosts. Used by both
// `pollStats` and `/stats` to skip the misleading ratio.
const IS_UNLIMITED = (limit) => limit == null || limit <= 0 || limit >= HOST_MEM_TOTAL_BYTES * 0.9;
function dockerRequest(method, path) {
    return new Promise((resolve, reject) => {
        const req = http_1.default.request({ socketPath: DOCKER_SOCKET, method, path }, (res) => {
            const chunks = [];
            res.on("data", (chunk) => chunks.push(chunk));
            res.on("end", () => {
                const raw = Buffer.concat(chunks).toString();
                try {
                    resolve({ status: res.statusCode ?? 0, body: JSON.parse(raw) });
                }
                catch {
                    resolve({ status: res.statusCode ?? 0, body: raw });
                }
            });
        });
        req.on("error", reject);
        req.end();
    });
}
/** Stream raw bytes from Docker socket into an IncomingMessage-like stream. */
function dockerStream(path) {
    return new Promise((resolve, reject) => {
        const req = http_1.default.request({ socketPath: DOCKER_SOCKET, method: "GET", path }, resolve);
        req.on("error", reject);
        req.end();
    });
}
// ---------------------------------------------------------------------------
// Docker log framing (shared utility)
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Discord alarm
// ---------------------------------------------------------------------------
const lastAlarmAt = {};
async function sendAlarm(type, service, detail) {
    if (!DISCORD_WEBHOOK)
        return;
    const key = `${type}:${service}`;
    const now = Date.now();
    if (lastAlarmAt[key] !== undefined && now - lastAlarmAt[key] < ALARM_COOLDOWN_MS) {
        return; // cooldown active
    }
    lastAlarmAt[key] = now;
    const emoji = type === "RECOVERY" ? "✅" : type === "HEALTHCHECK" ? "⚠️" : "🚨";
    const content = [
        `${emoji} **[${type}]** \`${service}\``,
        `⏰ ${new Date().toISOString()}`,
        detail,
        `📄 Logs: <http://localhost:${LOG_PORT}/logs/${service}>`,
    ].join("\n");
    const payload = Buffer.from(JSON.stringify({ content }));
    const url = new URL(DISCORD_WEBHOOK);
    await new Promise((resolve) => {
        const req = https_1.default.request({
            hostname: url.hostname,
            path: url.pathname + url.search,
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Content-Length": payload.length,
            },
        }, (res) => {
            res.resume();
            res.on("end", resolve);
        });
        req.on("error", (e) => {
            console.error("[alarm] Discord webhook failed:", e.message);
            resolve();
        });
        req.write(payload);
        req.end();
    });
}
const healthFailCount = {};
const healthState = {};
// Crash time (ms) of each container that sent CRASH and has not been reported
// back up. The healthcheck path can't report these: the restart passes through
// `starting`, which resets healthFailCount.
const crashedAt = {};
function markCrashed(name, eventTimeNano) {
    crashedAt[name] = eventTimeNano ? eventTimeNano / 1e6 : Date.now();
    delete lastAlarmAt[`RECOVERY:${name}`];
}
// A container is back once an instance started after the crash is out of
// Docker's restart backoff (which reports Running) and has proven itself:
// healthy, or with no healthcheck, still up one poll interval later.
function isBackAfterCrash(name, state, health) {
    const crashTime = crashedAt[name];
    const startedAt = Date.parse(state?.StartedAt ?? "");
    if (crashTime === undefined || state?.Running !== true || state.Restarting === true)
        return false;
    if (!(startedAt > crashTime))
        return false;
    if (health === "healthy")
        return true;
    return health === "none" && Date.now() - startedAt >= HEALTH_POLL_INTERVAL_MS;
}
async function pollHealth() {
    for (const name of MONITOR_TARGETS) {
        try {
            const { status, body } = await dockerRequest("GET", `/containers/${name}/json`);
            if (status !== 200) {
                console.warn(`[health] ${name}: container not found (${status})`);
                continue;
            }
            const current = body.State?.Health?.Status ?? "none";
            const prev = healthState[name];
            healthState[name] = current;
            if (current === "unhealthy") {
                healthFailCount[name] = (healthFailCount[name] ?? 0) + 1;
                if (healthFailCount[name] >= HEALTH_FAIL_THRESHOLD) {
                    console.error(`[health] ${name}: UNHEALTHY (count=${healthFailCount[name]})`);
                    await sendAlarm("HEALTHCHECK", name, `Health status: \`${current}\``);
                }
            }
            else if (current === "starting") {
                // Container is coming up. NO alarm — every path that reaches
                // `starting` is either voluntary or already surfaced by another
                // alarm type:
                //
                // - Involuntary crash / OOM: the docker-events handler below
                //   fires CRASH on `oom` or `die` with non-graceful exit BEFORE
                //   the container recreates and lands here. CRASH is event-
                //   driven (no poll race) and specific ("Container killed by
                //   OOM killer" / "exit code: 137"), so it's strictly more
                //   useful than a RESTARTING follow-on.
                // - Container hangs without exiting: HEALTHCHECK fires when the
                //   healthcheck flips to `unhealthy`. That path doesn't reach
                //   `starting` at all.
                // - Deploy / `compose restart`: voluntary, no signal needed.
                // - Initial boot: voluntary, no signal needed.
                //
                // Docker's restart policies fire on process EXIT (which
                // triggers CRASH), not on healthcheck failure — so on plain
                // docker-compose there's no involuntary path that produces
                // `unhealthy → starting` without a prior CRASH. And the 60s
                // health poll interval means a transient unhealthy transition
                // between polls could hide from a `prev === "unhealthy"` gate
                // anyway, giving false negatives. Dropping the alarm entirely
                // avoids that whole class of misclassification.
                console.log(`[health] ${name}: restarting (prev=${prev})`);
                // Always reset the HEALTHCHECK cooldown: a new container
                // instance is starting and its first unhealthy poll should fire
                // immediately, not sit under the old instance's cooldown.
                delete lastAlarmAt[`HEALTHCHECK:${name}`];
                healthFailCount[name] = 0;
            }
            else {
                // healthy (or unknown state)
                const healthRestored = (healthFailCount[name] ?? 0) >= HEALTH_FAIL_THRESHOLD && prev === "unhealthy" && current === "healthy";
                const backAfterCrash = isBackAfterCrash(name, body.State, current);
                if (healthRestored || backAfterCrash) {
                    delete crashedAt[name];
                    console.log(`[health] ${name}: recovered → ${current}`);
                    const detail = healthRestored ? "Health restored" : "Back up after crash";
                    await sendAlarm("RECOVERY", name, `${detail}: \`${current}\``);
                }
                healthFailCount[name] = 0;
            }
        }
        catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            console.error(`[health] poll failed for ${name}:`, msg);
        }
    }
}
const statsHistory = {};
const pressureCount = {};
/**
 * Compute the `docker stats`-style CPU percentage. Returns null when we
 * don't have both a current and prior CPU sample (first poll after startup)
 * or when totals are unset — treating those as null is what `docker stats`
 * itself does on the first frame.
 */
function calcCpuPercent(s) {
    const cpu = s.cpu_stats?.cpu_usage?.total_usage;
    const system = s.cpu_stats?.system_cpu_usage;
    const preCpu = s.precpu_stats?.cpu_usage?.total_usage;
    const preSystem = s.precpu_stats?.system_cpu_usage;
    const onlineCpus = s.cpu_stats?.online_cpus ?? 1;
    if (cpu === undefined ||
        system === undefined ||
        preCpu === undefined ||
        preSystem === undefined)
        return null;
    const cpuDelta = cpu - preCpu;
    const systemDelta = system - preSystem;
    if (systemDelta <= 0 || cpuDelta < 0)
        return null;
    return Math.round((cpuDelta / systemDelta) * onlineCpus * 100 * 10) / 10;
}
/**
 * Inactive file cache in bytes, from cgroup v1's `total_inactive_file` or
 * cgroup v2's `inactive_file`. Null when Docker reports neither.
 */
function inactiveFileBytes(mem) {
    return mem?.stats?.total_inactive_file ?? mem?.stats?.inactive_file ?? null;
}
/**
 * Memory the container can't give back without OOM risk: usage minus inactive
 * file cache, the same figure `docker stats` reports as MEM USAGE. Mirrors the
 * Docker CLI: `total_inactive_file` if below usage, else `inactive_file` if
 * below usage, else raw usage.
 */
function calcWorkingSetBytes(mem) {
    const usage = mem?.usage ?? 0;
    const totalInactiveFile = mem?.stats?.total_inactive_file;
    if (totalInactiveFile !== undefined && totalInactiveFile < usage) {
        return usage - totalInactiveFile;
    }
    const inactiveFile = mem?.stats?.inactive_file;
    if (inactiveFile !== undefined && inactiveFile < usage)
        return usage - inactiveFile;
    return usage;
}
async function pollStats() {
    for (const name of MONITOR_TARGETS) {
        try {
            const { status, body } = await dockerRequest("GET", `/containers/${name}/stats?stream=false`);
            if (status !== 200)
                continue;
            const usage = body.memory_stats?.usage ?? 0;
            const workingSet = calcWorkingSetBytes(body.memory_stats);
            const memStats = body.memory_stats?.stats;
            const limit = body.memory_stats?.limit ?? null;
            // Docker reports the host's total RAM as "limit" when the
            // container has no explicit `--memory` cap. Ratios computed
            // against host RAM are misleading — the numbers look fine
            // (0.03) even for a container about to OOM against a real cap.
            // Use `IS_UNLIMITED` (defined at top with the host-mem read) so
            // `/stats` reports `null` and the alarm block skips this
            // container consistently.
            const usageRatio = IS_UNLIMITED(limit) ? null : workingSet / limit;
            const cpuPercent = calcCpuPercent(body);
            const sample = {
                ts: body.read ?? new Date().toISOString(),
                usage_bytes: usage,
                working_set_bytes: workingSet,
                anon_bytes: memStats?.anon ?? memStats?.total_rss ?? null,
                file_bytes: memStats?.file ?? memStats?.total_cache ?? null,
                inactive_file_bytes: inactiveFileBytes(body.memory_stats),
                limit_bytes: limit,
                usage_ratio: usageRatio,
                cpu_percent: cpuPercent,
            };
            const ring = statsHistory[name] ?? [];
            ring.push(sample);
            if (ring.length > STATS_HISTORY_SIZE)
                ring.shift();
            statsHistory[name] = ring;
            // Sustained memory-pressure alarm: N consecutive samples above
            // the ratio threshold. Only fires when the container has an
            // explicit memory limit (usage_ratio non-null). Uses `>=` (not
            // `===`) so continued pressure re-attempts the alarm on every
            // subsequent poll — `ALARM_COOLDOWN_MS` gates the actual
            // Discord spam. With `===` a single ping fired at poll N and
            // then stayed silent for the entire pressure episode.
            if (usageRatio !== null && usageRatio >= MEMORY_PRESSURE_RATIO) {
                pressureCount[name] = (pressureCount[name] ?? 0) + 1;
                if (pressureCount[name] >= MEMORY_PRESSURE_SUSTAINED) {
                    const pct = Math.round(usageRatio * 100);
                    const workingSetMB = Math.round(workingSet / 1024 / 1024);
                    const usageMB = Math.round(usage / 1024 / 1024);
                    const limitMB = limit ? Math.round(limit / 1024 / 1024) : "?";
                    await sendAlarm("MEMORY_PRESSURE", name, `Memory at ${pct}% of container limit (${workingSetMB} MB / ${limitMB} MB working set, ${usageMB} MB with page cache) sustained across ${MEMORY_PRESSURE_SUSTAINED}+ polls — OOM likely if trend continues.`);
                }
            }
            else if (usageRatio !== null) {
                pressureCount[name] = 0;
            }
        }
        catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            console.error(`[stats] poll failed for ${name}:`, msg);
        }
    }
}
// ---------------------------------------------------------------------------
// Journald log reader
// ---------------------------------------------------------------------------
/**
 * Read the last `tail` log lines a container has written to systemd-journald.
 * The `journald` docker log driver labels each log entry with
 * `CONTAINER_NAME=<name>`; we filter by that and use `--output=cat` to
 * return just the message bodies (no timestamp/host prefix — the caller
 * decides how to render). Streams stdout back through `onChunk` so a
 * `follow=1` mode can be added later without refactor.
 *
 * Returns a disposer that kills the child when the caller is done
 * (usually the HTTP client disconnecting mid-tail).
 */
const STDERR_BUFFER_MAX = 64 * 1024;
function readContainerLogsFromJournal(params) {
    const { container, tail, since, onChunk, onEnd, onError } = params;
    const args = [
        `CONTAINER_NAME=${container}`,
        "-n",
        String(tail),
        "--output=cat",
        "--no-pager",
    ];
    if (since) {
        args.push("--since", since);
    }
    const child = (0, child_process_1.spawn)(JOURNALCTL_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    // `error` (ENOENT / spawn failure) and `close` (with nonzero code)
    // both fire on failed spawn — without a settled guard, `onError`
    // runs twice and the caller writes to a response that's already
    // ended (ERR_STREAM_WRITE_AFTER_END). Also protects against the
    // disposer-then-close race when the HTTP client disconnects.
    let settled = false;
    const settle = (fn) => {
        if (settled)
            return;
        settled = true;
        fn();
    };
    // Bound `stderr` growth. journalctl normally exits on first fatal
    // error, but a corrupt journal under stress could stream KB/s of
    // errors indefinitely.
    child.stdout?.on("data", onChunk);
    child.stderr?.on("data", (chunk) => {
        if (stderr.length < STDERR_BUFFER_MAX)
            stderr += chunk.toString();
    });
    child.on("error", (e) => settle(() => onError(e)));
    child.on("close", (code) => {
        settle(() => {
            if (code === 0)
                onEnd();
            else
                onError(new Error(`journalctl exited ${code}${stderr ? ": " + stderr.trim() : ""}`));
        });
    });
    // Hard timeout — a hung journalctl (rare, but possible on
    // systemd-journald under load) shouldn't leak the child if the HTTP
    // client isn't enforcing its own deadline.
    const timeoutHandle = setTimeout(() => {
        settle(() => {
            try {
                child.kill("SIGKILL");
            }
            catch { /* already gone */ }
            onError(new Error(`journalctl timeout after ${JOURNALCTL_TIMEOUT_MS}ms`));
        });
    }, JOURNALCTL_TIMEOUT_MS);
    child.on("close", () => clearTimeout(timeoutHandle));
    return () => {
        // Mark settled BEFORE killing so the ensuing `close` event's
        // handler no-ops (otherwise it would `onError` on an
        // already-ended socket).
        settled = true;
        clearTimeout(timeoutHandle);
        try {
            child.kill();
        }
        catch { /* already gone */ }
    };
}
// ---------------------------------------------------------------------------
// Docker event stream (#3)
// ---------------------------------------------------------------------------
// Exit codes that indicate a clean/intentional stop, not a crash:
//   0   — process exited cleanly (handled SIGTERM gracefully)
//   137 — killed by SIGKILL (128+9), e.g. docker compose up -d replacing a container
//   143 — killed by SIGTERM (128+15), e.g. docker stop / compose graceful shutdown
// Any other non-zero exit code is treated as a crash and triggers an alarm.
const GRACEFUL_EXIT_CODES = new Set([0, 137, 143]);
async function watchEvents() {
    try {
        const stream = await dockerStream("/events");
        // Docker event stream sends one JSON object per line
        let buf = "";
        stream.on("data", (chunk) => {
            buf += chunk.toString();
            const lines = buf.split("\n");
            buf = lines.pop() ?? "";
            for (const line of lines) {
                if (!line.trim())
                    continue;
                try {
                    const event = JSON.parse(line);
                    const action = event.Action;
                    const name = event.Actor?.Attributes?.name ?? "";
                    // Only process "die" and "oom" events for monitored containers.
                    // "die" carries the real exit code; "kill" fires before the process exits
                    // and always lacks an exit code — using it caused false alarms on deployments.
                    // "oom" has no exit code but always signals a real crash (kernel OOM kill).
                    if (!MONITOR_TARGETS.includes(name))
                        continue;
                    if (action === "oom") {
                        console.error(`[events] ${name}: OOM kill`);
                        markCrashed(name, event.timeNano);
                        sendAlarm("CRASH", name, "Container killed by OOM killer").catch((e) => {
                            console.error("[events] sendAlarm failed:", e instanceof Error ? e.message : String(e));
                        });
                        continue;
                    }
                    if (action === "die") {
                        const exitCodeRaw = event.Actor?.Attributes?.exitCode;
                        const exitCode = exitCodeRaw !== undefined ? parseInt(exitCodeRaw, 10) : NaN;
                        const isGraceful = !isNaN(exitCode) && GRACEFUL_EXIT_CODES.has(exitCode);
                        const exitDisplay = isNaN(exitCode) ? "?" : String(exitCode);
                        // Container logs live in systemd-journald (see file header) and
                        // survive container recreate independently — no pre-deploy dump
                        // needed. `journalctl CONTAINER_NAME=<name> --since <die-ts>`
                        // recovers the tail post-hoc when triage requires it.
                        if (isGraceful) {
                            console.log(`[events] ${name}: die (exit=${exitCode}) — graceful stop, no alarm`);
                            continue;
                        }
                        console.error(`[events] ${name}: die (exit=${exitDisplay}) — crash`);
                        markCrashed(name, event.timeNano);
                        sendAlarm("CRASH", name, `Container exited unexpectedly (exit code: \`${exitDisplay}\`)`).catch((e) => {
                            console.error("[events] sendAlarm failed:", e instanceof Error ? e.message : String(e));
                        });
                    }
                }
                catch {
                    // partial JSON or non-event line
                }
            }
        });
        stream.on("end", () => {
            console.error("[events] stream ended, reconnecting in 5s...");
            setTimeout(() => void watchEvents(), 5_000);
        });
        stream.on("error", (e) => {
            console.error("[events] stream error:", e.message, "— reconnecting in 5s");
            setTimeout(() => void watchEvents(), 5_000);
        });
    }
    catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error("[events] connect failed:", msg, "— retrying in 5s");
        setTimeout(() => void watchEvents(), 5_000);
    }
}
// ---------------------------------------------------------------------------
// Log server (#4)
// ---------------------------------------------------------------------------
function isAuthenticated(req) {
    if (!LOG_SERVER_TOKEN) {
        console.error("[security] LOG_SERVER_TOKEN is not set — all authenticated endpoints are blocked");
        return false; // fail closed
    }
    return req.headers.authorization === `Bearer ${LOG_SERVER_TOKEN}`;
}
function sendJson(res, status, body) {
    const payload = JSON.stringify(body);
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(payload);
}
const server = http_1.default.createServer((req, res) => {
    handleRequest(req, res).catch((e) => {
        const msg = e instanceof Error ? e.message : String(e);
        console.error("[server] unhandled error:", msg);
        try {
            if (!res.headersSent)
                sendJson(res, 500, { error: "Internal server error" });
            else
                res.end();
        }
        catch { /* socket already gone */ }
    });
});
async function handleRequest(req, res) {
    const rawUrl = req.url ?? "/";
    let url;
    try {
        url = new URL(rawUrl, "http://localhost");
    }
    catch {
        sendJson(res, 400, { error: "Bad request" });
        return;
    }
    const pathname = url.pathname;
    // `/health` is intentionally unauthenticated — its body is
    // low-sensitivity (process uptime + monitored container names +
    // last-known health/restart states) and orchestrator health probes
    // need a public endpoint. README documents this row as "no auth"; a
    // Bearer gate here would silent-401 the probes.
    if (pathname === "/health") {
        sendJson(res, 200, {
            status: "ok",
            targets: MONITOR_TARGETS,
            health: healthState,
            uptime: process.uptime(),
        });
        return;
    }
    if (!isAuthenticated(req)) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
    }
    // GET /containers — list running containers
    if (pathname === "/containers" && req.method === "GET") {
        try {
            const { body } = await dockerRequest("GET", "/containers/json");
            const list = (Array.isArray(body) ? body : []).map((c) => ({
                id: c.Id.slice(0, 12),
                name: (c.Names[0] ?? "").replace(/^\//, ""),
                image: c.Image,
                status: c.Status,
                state: c.State,
            }));
            sendJson(res, 200, list);
        }
        catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            sendJson(res, 500, { error: msg });
        }
        return;
    }
    // GET /stats/:container — restricted to MONITOR_TARGETS
    const statsMatch = /^\/stats\/([^/]+)$/.exec(pathname);
    if (statsMatch && req.method === "GET") {
        const container = statsMatch[1] ?? "";
        if (!MONITOR_TARGETS.includes(container)) {
            sendJson(res, 403, { error: "Container not in MONITOR_TARGETS" });
            return;
        }
        const ring = statsHistory[container] ?? [];
        const latest = ring.length > 0 ? ring[ring.length - 1] : null;
        sendJson(res, 200, {
            container,
            poll_interval_ms: STATS_POLL_INTERVAL_MS,
            pressure_threshold_ratio: MEMORY_PRESSURE_RATIO,
            latest,
            history: ring,
        });
        return;
    }
    // GET /logs/:container — restricted to MONITOR_TARGETS
    const logsMatch = /^\/logs\/([^/]+)$/.exec(pathname);
    if (logsMatch && req.method === "GET") {
        const container = logsMatch[1] ?? "";
        if (!MONITOR_TARGETS.includes(container)) {
            sendJson(res, 403, { error: "Container not in MONITOR_TARGETS" });
            return;
        }
        // Validate `tail`: unbounded / non-numeric / negative values used to
        // reach journalctl and either error out or return an unbounded
        // history. Clamp to [1, 10000] and default to 100.
        const tailRaw = parseInt(url.searchParams.get("tail") ?? "100", 10);
        const tail = Number.isFinite(tailRaw) && tailRaw > 0 && tailRaw <= 10_000 ? tailRaw : 100;
        // Optional time-based window. journalctl's `--since` accepts absolute
        // (`2026-07-28 17:00:00`) and relative (`10m ago`, `2 hours ago`,
        // `yesterday`) forms per systemd.time(7). Combined with `-n tail`,
        // this returns "at most `tail` entries from within the requested
        // window", which is what triage needs during a log-storm — the tail
        // cap alone can't reach older entries when the storm generates more
        // lines than the cap.
        //
        // Length-capped defensively — a very long value could make
        // journalctl parse-error at the process-arg boundary. 200 octets
        // covers every legitimate expression comfortably (longest realistic
        // form is ~44 chars: `2026-07-28+17:00:00+America/Los_Angeles`).
        // Byte-cap via `Buffer.byteLength` because a multi-byte-UTF-8 value
        // could otherwise smuggle past a naive char cap.
        const sinceRaw = url.searchParams.get("since");
        let since;
        if (sinceRaw !== null && sinceRaw.length > 0) {
            if (Buffer.byteLength(sinceRaw, "utf8") > 200) {
                // Explicit 400 rather than silently dropping the filter. A silent
                // drop would serve the default `-n 100` tail with no signal that
                // the caller's window was ignored — the exact "wrong answer, no
                // error" shape #13's premature-200 fix worked to eliminate.
                sendJson(res, 400, {
                    error: "since must be 1-200 UTF-8 octets",
                });
                return;
            }
            since = sinceRaw;
        }
        // Delay `writeHead` until first stdout chunk arrives. journalctl
        // spawn failures (missing binary, permission denied) previously
        // returned 200 with an error message in the body, so any consumer
        // gating on status treated it as success.
        let headersSent = false;
        const startStreaming = () => {
            if (headersSent)
                return;
            headersSent = true;
            res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
        };
        const disposer = readContainerLogsFromJournal({
            container,
            tail,
            since,
            onChunk: (chunk) => {
                startStreaming();
                res.write(chunk);
            },
            onEnd: () => {
                startStreaming();
                res.end();
            },
            onError: (e) => {
                if (!headersSent) {
                    sendJson(res, 500, { error: `journalctl failed: ${e.message}` });
                    return;
                }
                res.write(`\n[error reading logs: ${e.message}]\n`);
                res.end();
            },
        });
        // Kill journalctl if the HTTP client disconnects mid-read.
        req.on("close", disposer);
        return;
    }
    sendJson(res, 404, { error: "Not found" });
}
// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
server.listen(LOG_PORT, () => {
    console.log(`[monitor] Log server listening on port ${LOG_PORT}`);
    console.log(`[monitor] Targets: ${MONITOR_TARGETS.join(", ") || "(none)"}`);
    console.log(`[monitor] Health poll interval: ${HEALTH_POLL_INTERVAL_MS}ms`);
    console.log(`[monitor] Stats poll interval: ${STATS_POLL_INTERVAL_MS}ms · history=${STATS_HISTORY_SIZE} · pressure=${MEMORY_PRESSURE_RATIO}×${MEMORY_PRESSURE_SUSTAINED}`);
    if (!DISCORD_WEBHOOK)
        console.warn("[monitor] DISCORD_ALARM_WEBHOOK not set — alarms disabled");
    if (!LOG_SERVER_TOKEN)
        console.warn("[monitor] LOG_SERVER_TOKEN not set — log API is open");
});
setInterval(() => void pollHealth(), HEALTH_POLL_INTERVAL_MS);
void pollHealth(); // immediate first check
setInterval(() => void pollStats(), STATS_POLL_INTERVAL_MS);
void pollStats(); // immediate first sample
void watchEvents();
