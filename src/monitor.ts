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

import http from "http";
import https from "https";
import { spawn } from "child_process";
import { readFileSync } from "fs";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const DISCORD_WEBHOOK = process.env.DISCORD_ALARM_WEBHOOK ?? "";
const LOG_SERVER_TOKEN = process.env.LOG_SERVER_TOKEN ?? "";
const MONITOR_PROJECT = (process.env.MONITOR_PROJECT ?? "").trim();
const MONITOR_TARGETS = (process.env.MONITOR_TARGETS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const HEALTH_FAIL_THRESHOLD = parseInt(
  process.env.HEALTH_FAIL_THRESHOLD ?? "1",
  10
);
const HEALTH_POLL_INTERVAL_MS = parseInt(
  process.env.HEALTH_POLL_INTERVAL_MS ?? "60000",
  10
);
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

function parseIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    console.warn(`[config] ${name}=${JSON.stringify(raw)} is not a positive int — using default ${fallback}`);
    return fallback;
  }
  return parsed;
}

function parseFloatEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = parseFloat(raw);
  if (!Number.isFinite(parsed)) {
    console.warn(`[config] ${name}=${JSON.stringify(raw)} is not a valid float — using default ${fallback}`);
    return fallback;
  }
  return parsed;
}

function readHostMemTotalBytes(): number {
  try {
    const meminfo = readFileSync("/proc/meminfo", "utf-8");
    const match = /MemTotal:\s+(\d+)\s+kB/.exec(meminfo);
    if (match) return parseInt(match[1], 10) * 1024;
  } catch {
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
const IS_UNLIMITED = (limit: number | null | undefined): boolean =>
  limit == null || limit <= 0 || limit >= HOST_MEM_TOTAL_BYTES * 0.9;

// ---------------------------------------------------------------------------
// Docker socket helpers
// ---------------------------------------------------------------------------

interface DockerResponse<T = unknown> {
  status: number;
  body: T;
}

function dockerRequest<T = unknown>(
  method: string,
  path: string
): Promise<DockerResponse<T>> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { socketPath: DOCKER_SOCKET, method, path },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString();
          try {
            resolve({ status: res.statusCode ?? 0, body: JSON.parse(raw) as T });
          } catch {
            resolve({ status: res.statusCode ?? 0, body: raw as unknown as T });
          }
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

/** Stream raw bytes from Docker socket into an IncomingMessage-like stream. */
function dockerStream(path: string): Promise<http.IncomingMessage> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { socketPath: DOCKER_SOCKET, method: "GET", path },
      resolve
    );
    req.on("error", reject);
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

const PROJECT_LABEL = "com.docker.compose.project";
const ONEOFF_LABEL = "com.docker.compose.oneoff";

/**
 * Containers being monitored. With `MONITOR_PROJECT` set, every service
 * container of that compose project, rediscovered before each poll so added
 * and removed services are picked up without a restart; otherwise the fixed
 * `MONITOR_TARGETS` list.
 */
let targets: string[] = MONITOR_PROJECT ? [] : MONITOR_TARGETS;

function isProjectService(labels: Record<string, string> | undefined): boolean {
  return labels?.[PROJECT_LABEL] === MONITOR_PROJECT && labels?.[ONEOFF_LABEL] !== "True";
}

let discovered = false;
let refreshing: Promise<void> | null = null;

/** Health and stats polls share one in-flight lookup so an older answer can't overwrite a newer one. */
function refreshTargets(): Promise<void> {
  if (!MONITOR_PROJECT) return Promise.resolve();
  refreshing ??= discoverTargets().finally(() => {
    refreshing = null;
  });
  return refreshing;
}

async function discoverTargets(): Promise<void> {
  // `all` keeps stopped and crashed containers in the set until they are removed.
  const filters = encodeURIComponent(JSON.stringify({ label: [`${PROJECT_LABEL}=${MONITOR_PROJECT}`] }));
  try {
    const { status, body } = await dockerRequest<{ Names?: string[]; Labels?: Record<string, string> }[]>(
      "GET",
      `/containers/json?all=true&filters=${filters}`
    );
    if (status !== 200 || !Array.isArray(body)) {
      console.warn(`[targets] container list failed (${status}); keeping ${targets.length} known targets`);
      return;
    }
    const next = body
      .filter((c) => isProjectService(c.Labels))
      .map((c) => (c.Names?.[0] ?? "").replace(/^\//, ""))
      .filter(Boolean)
      .sort();
    if (discovered && next.join(",") === targets.join(",")) return;
    discovered = true;
    if (next.length) console.log(`[targets] ${next.join(", ")}`);
    else console.warn(`[targets] no containers in compose project "${MONITOR_PROJECT}"`);
    for (const name of targets) {
      if (next.includes(name)) continue;
      for (const state of [healthState, healthFailCount, crashedAt, statsHistory, pressureCount]) delete state[name];
    }
    targets = next;
  } catch (e: unknown) {
    console.warn("[targets] container list failed:", e instanceof Error ? e.message : String(e));
  }
}

// ---------------------------------------------------------------------------
// Docker log framing (shared utility)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Discord alarm
// ---------------------------------------------------------------------------

const lastAlarmAt: Record<string, number> = {};

async function sendAlarm(
  type: string,
  service: string,
  detail: string
): Promise<void> {
  if (!DISCORD_WEBHOOK) return;

  const key = `${type}:${service}`;
  const now = Date.now();
  if (lastAlarmAt[key] !== undefined && now - lastAlarmAt[key] < ALARM_COOLDOWN_MS) {
    return; // cooldown active
  }
  lastAlarmAt[key] = now;

  const emoji =
    type === "RECOVERY" ? "✅" : type === "HEALTHCHECK" ? "⚠️" : "🚨";
  const content = [
    `${emoji} **[${type}]** \`${service}\``,
    `⏰ ${new Date().toISOString()}`,
    detail,
    `📄 Logs: <http://localhost:${LOG_PORT}/logs/${service}>`,
  ].join("\n");

  const payload = Buffer.from(JSON.stringify({ content }));
  const url = new URL(DISCORD_WEBHOOK);

  await new Promise<void>((resolve) => {
    const req = https.request(
      {
        hostname: url.hostname,
        path: url.pathname + url.search,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": payload.length,
        },
      },
      (res) => {
        res.resume();
        res.on("end", resolve);
      }
    );
    req.on("error", (e) => {
      console.error("[alarm] Discord webhook failed:", e.message);
      resolve();
    });
    req.write(payload);
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Healthcheck polling (#2)
// ---------------------------------------------------------------------------

interface ContainerJson {
  State?: {
    Health?: { Status?: string };
    Running?: boolean;
    Restarting?: boolean;
    StartedAt?: string;
  };
}

const healthFailCount: Record<string, number> = {};
const healthState: Record<string, string> = {};
// Crash time (ms) of each container that sent CRASH and has not been reported
// back up. The healthcheck path can't report these: the restart passes through
// `starting`, which resets healthFailCount.
const crashedAt: Record<string, number> = {};

function markCrashed(name: string, eventTimeNano: number | undefined): void {
  crashedAt[name] = eventTimeNano ? eventTimeNano / 1e6 : Date.now();
  delete lastAlarmAt[`RECOVERY:${name}`];
}

// A container is back once an instance started after the crash is out of
// Docker's restart backoff (which reports Running) and has proven itself:
// healthy, or with no healthcheck, still up one poll interval later.
function isBackAfterCrash(name: string, state: ContainerJson["State"], health: string): boolean {
  const crashTime = crashedAt[name];
  const startedAt = Date.parse(state?.StartedAt ?? "");
  if (crashTime === undefined || state?.Running !== true || state.Restarting === true) return false;
  if (!(startedAt > crashTime)) return false;
  if (health === "healthy") return true;
  return health === "none" && Date.now() - startedAt >= HEALTH_POLL_INTERVAL_MS;
}

async function pollHealth(): Promise<void> {
  await refreshTargets();
  for (const name of targets) {
    try {
      const { status, body } = await dockerRequest<ContainerJson>(
        "GET",
        `/containers/${name}/json`
      );

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
      } else if (current === "starting") {
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
      } else {
        // healthy (or unknown state)
        const healthRestored =
          (healthFailCount[name] ?? 0) >= HEALTH_FAIL_THRESHOLD && prev === "unhealthy" && current === "healthy";
        const backAfterCrash = isBackAfterCrash(name, body.State, current);
        if (healthRestored || backAfterCrash) {
          delete crashedAt[name];
          console.log(`[health] ${name}: recovered → ${current}`);
          const detail = healthRestored ? "Health restored" : "Back up after crash";
          await sendAlarm("RECOVERY", name, `${detail}: \`${current}\``);
        }
        healthFailCount[name] = 0;
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error(`[health] poll failed for ${name}:`, msg);
    }
  }
}

// ---------------------------------------------------------------------------
// Container memory / CPU stats polling
// ---------------------------------------------------------------------------

/**
 * Snapshot of one docker stats sample, distilled to the fields we actually
 * use for triage. `usage_bytes` and `limit_bytes` come from `memory_stats`
 * exactly as Docker reports them. `usage_bytes` includes page cache the kernel
 * reclaims before it OOM-kills, so `working_set_bytes` subtracts inactive file
 * cache the way `docker stats` does, and `usage_ratio` is computed from it.
 * `anon_bytes` / `file_bytes` / `inactive_file_bytes` are the cgroup's own
 * breakdown (null when Docker doesn't report them). `cpu_percent` is a nominal
 * single-core % (Docker's `docker stats` calculation) so we can spot a runaway
 * loop.
 */
interface StatsSample {
  ts: string;
  usage_bytes: number;
  working_set_bytes: number;
  anon_bytes: number | null;
  file_bytes: number | null;
  inactive_file_bytes: number | null;
  limit_bytes: number | null;
  usage_ratio: number | null;
  cpu_percent: number | null;
}

interface DockerStatsResponse {
  read?: string;
  memory_stats?: {
    usage?: number;
    limit?: number;
    stats?: Record<string, number>;
  };
  cpu_stats?: {
    cpu_usage?: { total_usage?: number };
    system_cpu_usage?: number;
    online_cpus?: number;
  };
  precpu_stats?: {
    cpu_usage?: { total_usage?: number };
    system_cpu_usage?: number;
  };
}

const statsHistory: Record<string, StatsSample[]> = {};
const pressureCount: Record<string, number> = {};

/**
 * Compute the `docker stats`-style CPU percentage. Returns null when we
 * don't have both a current and prior CPU sample (first poll after startup)
 * or when totals are unset — treating those as null is what `docker stats`
 * itself does on the first frame.
 */
function calcCpuPercent(s: DockerStatsResponse): number | null {
  const cpu = s.cpu_stats?.cpu_usage?.total_usage;
  const system = s.cpu_stats?.system_cpu_usage;
  const preCpu = s.precpu_stats?.cpu_usage?.total_usage;
  const preSystem = s.precpu_stats?.system_cpu_usage;
  const onlineCpus = s.cpu_stats?.online_cpus ?? 1;
  if (
    cpu === undefined ||
    system === undefined ||
    preCpu === undefined ||
    preSystem === undefined
  )
    return null;
  const cpuDelta = cpu - preCpu;
  const systemDelta = system - preSystem;
  if (systemDelta <= 0 || cpuDelta < 0) return null;
  return Math.round((cpuDelta / systemDelta) * onlineCpus * 100 * 10) / 10;
}

/**
 * Inactive file cache in bytes, from cgroup v1's `total_inactive_file` or
 * cgroup v2's `inactive_file`. Null when Docker reports neither.
 */
function inactiveFileBytes(mem: DockerStatsResponse["memory_stats"]): number | null {
  return mem?.stats?.total_inactive_file ?? mem?.stats?.inactive_file ?? null;
}

/**
 * Memory the container can't give back without OOM risk: usage minus inactive
 * file cache, the same figure `docker stats` reports as MEM USAGE. Mirrors the
 * Docker CLI: `total_inactive_file` if below usage, else `inactive_file` if
 * below usage, else raw usage.
 */
function calcWorkingSetBytes(mem: DockerStatsResponse["memory_stats"]): number {
  const usage = mem?.usage ?? 0;
  const totalInactiveFile = mem?.stats?.total_inactive_file;
  if (totalInactiveFile !== undefined && totalInactiveFile < usage) {
    return usage - totalInactiveFile;
  }
  const inactiveFile = mem?.stats?.inactive_file;
  if (inactiveFile !== undefined && inactiveFile < usage) return usage - inactiveFile;
  return usage;
}

async function pollStats(): Promise<void> {
  await refreshTargets();
  for (const name of targets) {
    try {
      const { status, body } = await dockerRequest<DockerStatsResponse>(
        "GET",
        `/containers/${name}/stats?stream=false`
      );
      if (status !== 200) continue;

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
      const usageRatio = IS_UNLIMITED(limit) ? null : workingSet / (limit as number);
      const cpuPercent = calcCpuPercent(body);

      const sample: StatsSample = {
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
      if (ring.length > STATS_HISTORY_SIZE) ring.shift();
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
          await sendAlarm(
            "MEMORY_PRESSURE",
            name,
            `Memory at ${pct}% of container limit (${workingSetMB} MB / ${limitMB} MB working set, ${usageMB} MB with page cache) sustained across ${MEMORY_PRESSURE_SUSTAINED}+ polls — OOM likely if trend continues.`
          );
        }
      } else if (usageRatio !== null) {
        pressureCount[name] = 0;
      }
    } catch (e: unknown) {
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

function readContainerLogsFromJournal(params: {
  container: string;
  tail: number;
  /**
   * Optional `journalctl --since` expression. When set, journalctl returns
   * every entry from that instant onward — the `-n tail` cap still applies
   * as a safety upper bound but the WINDOW is now time-based rather than
   * line-count-based. Callers use this to reach logs older than `tail`
   * lines' worth of history during a log-storm (e.g. an inbox retry storm
   * generating ~1700 lines/min drowns the default 10k-line cap in 6 min;
   * `since=30m ago` recovers the older, quieter pre-storm entries).
   *
   * journalctl accepts absolute (`2026-07-28 17:00:00`) or relative
   * (`10m ago`, `2 hours ago`, `yesterday`) expressions per systemd.time(7).
   * The caller is trusted to pass a well-formed value; a malformed one is
   * surfaced as a journalctl-exit-nonzero error via the standard error
   * pipeline below.
   */
  since?: string;
  onChunk: (buf: Buffer) => void;
  onEnd: () => void;
  onError: (err: Error) => void;
}): () => void {
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
  const child = spawn(JOURNALCTL_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  // `error` (ENOENT / spawn failure) and `close` (with nonzero code)
  // both fire on failed spawn — without a settled guard, `onError`
  // runs twice and the caller writes to a response that's already
  // ended (ERR_STREAM_WRITE_AFTER_END). Also protects against the
  // disposer-then-close race when the HTTP client disconnects.
  let settled = false;
  const settle = (fn: () => void) => {
    if (settled) return;
    settled = true;
    fn();
  };

  // Bound `stderr` growth. journalctl normally exits on first fatal
  // error, but a corrupt journal under stress could stream KB/s of
  // errors indefinitely.
  child.stdout?.on("data", onChunk);
  child.stderr?.on("data", (chunk: Buffer) => {
    if (stderr.length < STDERR_BUFFER_MAX) stderr += chunk.toString();
  });
  child.on("error", (e) => settle(() => onError(e)));
  child.on("close", (code) => {
    settle(() => {
      if (code === 0) onEnd();
      else onError(new Error(`journalctl exited ${code}${stderr ? ": " + stderr.trim() : ""}`));
    });
  });

  // Hard timeout — a hung journalctl (rare, but possible on
  // systemd-journald under load) shouldn't leak the child if the HTTP
  // client isn't enforcing its own deadline.
  const timeoutHandle = setTimeout(() => {
    settle(() => {
      try { child.kill("SIGKILL"); } catch { /* already gone */ }
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
    try { child.kill(); } catch { /* already gone */ }
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

async function watchEvents(): Promise<void> {
  try {
    const stream = await dockerStream("/events");

    // Docker event stream sends one JSON object per line
    let buf = "";
    stream.on("data", (chunk: Buffer) => {
      buf += chunk.toString();
      const lines = buf.split("\n");
      buf = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const event = JSON.parse(line) as {
            Action: string;
            Actor: { Attributes: { name: string; exitCode?: string } & Record<string, string> };
            timeNano?: number;
          };
          const action = event.Action;
          const name = event.Actor?.Attributes?.name ?? "";

          // Only process "die" and "oom" events for monitored containers.
          // "die" carries the real exit code; "kill" fires before the process exits
          // and always lacks an exit code — using it caused false alarms on deployments.
          // "oom" has no exit code but always signals a real crash (kernel OOM kill).
          const monitored = MONITOR_PROJECT
            ? isProjectService(event.Actor?.Attributes)
            : MONITOR_TARGETS.includes(name);
          if (!monitored) continue;

          if (action === "oom") {
            console.error(`[events] ${name}: OOM kill`);
            markCrashed(name, event.timeNano);
            sendAlarm("CRASH", name, "Container killed by OOM killer").catch(
              (e: unknown) => {
                console.error("[events] sendAlarm failed:", e instanceof Error ? e.message : String(e));
              }
            );
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
            sendAlarm(
              "CRASH",
              name,
              `Container exited unexpectedly (exit code: \`${exitDisplay}\`)`
            ).catch((e: unknown) => {
              console.error("[events] sendAlarm failed:", e instanceof Error ? e.message : String(e));
            });
          }
        } catch {
          // partial JSON or non-event line
        }
      }
    });

    stream.on("end", () => {
      console.error("[events] stream ended, reconnecting in 5s...");
      setTimeout(() => void watchEvents(), 5_000);
    });

    stream.on("error", (e: Error) => {
      console.error("[events] stream error:", e.message, "— reconnecting in 5s");
      setTimeout(() => void watchEvents(), 5_000);
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("[events] connect failed:", msg, "— retrying in 5s");
    setTimeout(() => void watchEvents(), 5_000);
  }
}

// ---------------------------------------------------------------------------
// Log server (#4)
// ---------------------------------------------------------------------------

function isAuthenticated(req: http.IncomingMessage): boolean {
  if (!LOG_SERVER_TOKEN) {
    console.error("[security] LOG_SERVER_TOKEN is not set — all authenticated endpoints are blocked");
    return false; // fail closed
  }
  return req.headers.authorization === `Bearer ${LOG_SERVER_TOKEN}`;
}

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(payload);
}

interface ContainerListItem {
  Id: string;
  Names: string[];
  Image: string;
  Status: string;
  State: string;
}

const server = http.createServer(
  (req: http.IncomingMessage, res: http.ServerResponse) => {
    handleRequest(req, res).catch((e: unknown) => {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("[server] unhandled error:", msg);
      try {
        if (!res.headersSent) sendJson(res, 500, { error: "Internal server error" });
        else res.end();
      } catch { /* socket already gone */ }
    });
  }
);

async function handleRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse
): Promise<void> {
  const rawUrl = req.url ?? "/";
  let url: URL;
  try {
    url = new URL(rawUrl, "http://localhost");
  } catch {
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
      targets,
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
      const { body } = await dockerRequest<ContainerListItem[]>(
        "GET",
        "/containers/json"
      );
      const list = (Array.isArray(body) ? body : []).map((c) => ({
        id: c.Id.slice(0, 12),
        name: (c.Names[0] ?? "").replace(/^\//, ""),
        image: c.Image,
        status: c.Status,
        state: c.State,
      }));
      sendJson(res, 200, list);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      sendJson(res, 500, { error: msg });
    }
    return;
  }

  // GET /stats/:container — restricted to monitored containers
  const statsMatch = /^\/stats\/([^/]+)$/.exec(pathname);
  if (statsMatch && req.method === "GET") {
    const container = statsMatch[1] ?? "";
    if (!targets.includes(container)) {
      sendJson(res, 403, { error: "Container not monitored" });
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

  // GET /logs/:container — restricted to monitored containers
  const logsMatch = /^\/logs\/([^/]+)$/.exec(pathname);
  if (logsMatch && req.method === "GET") {
    const container = logsMatch[1] ?? "";
    if (!targets.includes(container)) {
      sendJson(res, 403, { error: "Container not monitored" });
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
    let since: string | undefined;
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
      if (headersSent) return;
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
  console.log(
    MONITOR_PROJECT
      ? `[monitor] Targets: services of compose project "${MONITOR_PROJECT}"`
      : `[monitor] Targets: ${MONITOR_TARGETS.join(", ") || "(none)"}`
  );
  console.log(`[monitor] Health poll interval: ${HEALTH_POLL_INTERVAL_MS}ms`);
  console.log(`[monitor] Stats poll interval: ${STATS_POLL_INTERVAL_MS}ms · history=${STATS_HISTORY_SIZE} · pressure=${MEMORY_PRESSURE_RATIO}×${MEMORY_PRESSURE_SUSTAINED}`);
  if (!DISCORD_WEBHOOK) console.warn("[monitor] DISCORD_ALARM_WEBHOOK not set — alarms disabled");
  if (!LOG_SERVER_TOKEN) console.warn("[monitor] LOG_SERVER_TOKEN not set — log API is open");
});

setInterval(() => void pollHealth(), HEALTH_POLL_INTERVAL_MS);
void pollHealth(); // immediate first check
setInterval(() => void pollStats(), STATS_POLL_INTERVAL_MS);
void pollStats(); // immediate first sample
void watchEvents();
