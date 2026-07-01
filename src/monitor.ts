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

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const DISCORD_WEBHOOK = process.env.DISCORD_ALARM_WEBHOOK ?? "";
const LOG_SERVER_TOKEN = process.env.LOG_SERVER_TOKEN ?? "";
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
const STATS_POLL_INTERVAL_MS = parseInt(
  process.env.STATS_POLL_INTERVAL_MS ?? "60000",
  10
);
const STATS_HISTORY_SIZE = parseInt(process.env.STATS_HISTORY_SIZE ?? "60", 10);
const MEMORY_PRESSURE_RATIO = parseFloat(
  process.env.MEMORY_PRESSURE_RATIO ?? "0.85"
);
const MEMORY_PRESSURE_SUSTAINED = parseInt(
  process.env.MEMORY_PRESSURE_SUSTAINED ?? "3",
  10
);

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
// Docker log framing (shared utility)
// ---------------------------------------------------------------------------

/**
 * Docker log stream uses a multiplexed framing format:
 *   [stream_type(1)] [0(3)] [size(4 BE)] [payload(size)]
 * Strip headers so callers receive plain text.
 */
function stripDockerFraming(buf: Buffer): Buffer {
  const parts: Buffer[] = [];
  let offset = 0;
  while (offset + 8 <= buf.length) {
    const size = buf.readUInt32BE(offset + 4);
    if (size === 0) { offset += 8; continue; }
    if (offset + 8 + size > buf.length) break;
    parts.push(buf.subarray(offset + 8, offset + 8 + size));
    offset += 8 + size;
  }
  return parts.length > 0 ? Buffer.concat(parts) : buf;
}

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
    type === "RECOVERY" ? "✅" : type === "RESTARTING" ? "🔄" : type === "HEALTHCHECK" ? "⚠️" : "🚨";
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
  };
}

const healthFailCount: Record<string, number> = {};
const healthState: Record<string, string> = {};

async function pollHealth(): Promise<void> {
  for (const name of MONITOR_TARGETS) {
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
        // Container restarted — fire a RESTARTING alarm and reset the HEALTHCHECK
        // cooldown so the next unhealthy poll fires immediately.
        console.log(`[health] ${name}: restarting (prev=${prev})`);
        await sendAlarm("RESTARTING", name, `Container restarting after \`${prev}\``);
        delete lastAlarmAt[`HEALTHCHECK:${name}`];
        healthFailCount[name] = 0;
      } else {
        // healthy (or unknown state)
        if ((healthFailCount[name] ?? 0) >= HEALTH_FAIL_THRESHOLD && prev === "unhealthy" && current === "healthy") {
          console.log(`[health] ${name}: recovered → ${current}`);
          await sendAlarm("RECOVERY", name, `Health restored: \`${current}\``);
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
 * exactly as Docker reports them; `usage_ratio` is precomputed so callers
 * don't repeat the division. `cpu_percent` is a nominal single-core %
 * (Docker's `docker stats` calculation) so we can spot a runaway loop.
 */
interface StatsSample {
  ts: string;
  usage_bytes: number;
  limit_bytes: number | null;
  usage_ratio: number | null;
  cpu_percent: number | null;
}

interface DockerStatsResponse {
  read?: string;
  memory_stats?: {
    usage?: number;
    limit?: number;
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

async function pollStats(): Promise<void> {
  for (const name of MONITOR_TARGETS) {
    try {
      const { status, body } = await dockerRequest<DockerStatsResponse>(
        "GET",
        `/containers/${name}/stats?stream=false`
      );
      if (status !== 200) continue;

      const usage = body.memory_stats?.usage ?? 0;
      const limit = body.memory_stats?.limit ?? null;
      // Docker reports the host's total RAM as the "limit" when the container
      // has no explicit --memory constraint. That makes the ratio effectively
      // useless (nothing will ever push it to 85% of host RAM). Treat any
      // limit > 90% of host RAM as "unlimited" — the container has no cap.
      const usageRatio = limit && limit > 0 ? usage / limit : null;
      const cpuPercent = calcCpuPercent(body);

      const sample: StatsSample = {
        ts: body.read ?? new Date().toISOString(),
        usage_bytes: usage,
        limit_bytes: limit,
        usage_ratio: usageRatio,
        cpu_percent: cpuPercent,
      };

      const ring = statsHistory[name] ?? [];
      ring.push(sample);
      if (ring.length > STATS_HISTORY_SIZE) ring.shift();
      statsHistory[name] = ring;

      // Sustained memory-pressure alarm: N consecutive samples above the
      // ratio threshold. Only fires when the container has an explicit
      // memory limit (usage_ratio non-null).
      if (usageRatio !== null && usageRatio >= MEMORY_PRESSURE_RATIO) {
        pressureCount[name] = (pressureCount[name] ?? 0) + 1;
        if (pressureCount[name] === MEMORY_PRESSURE_SUSTAINED) {
          const pct = Math.round(usageRatio * 100);
          const usageMB = Math.round(usage / 1024 / 1024);
          const limitMB = limit ? Math.round(limit / 1024 / 1024) : "?";
          await sendAlarm(
            "MEMORY_PRESSURE",
            name,
            `Memory at ${pct}% of container limit (${usageMB} MB / ${limitMB} MB) sustained across ${MEMORY_PRESSURE_SUSTAINED} polls — OOM likely if trend continues.`
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
function readContainerLogsFromJournal(params: {
  container: string;
  tail: number;
  onChunk: (buf: Buffer) => void;
  onEnd: () => void;
  onError: (err: Error) => void;
}): () => void {
  const { container, tail, onChunk, onEnd, onError } = params;
  const args = [
    `CONTAINER_NAME=${container}`,
    "-n",
    String(tail),
    "--output=cat",
    "--no-pager",
  ];
  const child = spawn(JOURNALCTL_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";

  child.stdout?.on("data", onChunk);
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  child.on("error", onError);
  child.on("close", (code) => {
    if (code === 0) {
      onEnd();
    } else {
      onError(new Error(`journalctl exited ${code}${stderr ? ": " + stderr.trim() : ""}`));
    }
  });

  return () => {
    try {
      child.kill();
    } catch {
      /* already gone */
    }
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
            Actor: { Attributes: { name: string; exitCode?: string } };
          };
          const action = event.Action;
          const name = event.Actor?.Attributes?.name ?? "";

          // Only process "die" and "oom" events for monitored containers.
          // "die" carries the real exit code; "kill" fires before the process exits
          // and always lacks an exit code — using it caused false alarms on deployments.
          // "oom" has no exit code but always signals a real crash (kernel OOM kill).
          if (!MONITOR_TARGETS.includes(name)) continue;

          if (action === "oom") {
            console.error(`[events] ${name}: OOM kill`);
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

  if (!isAuthenticated(req)) {
    sendJson(res, 401, { error: "Unauthorized" });
    return;
  }

  if (pathname === "/health") {
    sendJson(res, 200, {
      status: "ok",
      targets: MONITOR_TARGETS,
      health: healthState,
      uptime: process.uptime(),
    });
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
    const tail = parseInt(url.searchParams.get("tail") ?? "100", 10);

    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });

    const disposer = readContainerLogsFromJournal({
      container,
      tail,
      onChunk: (chunk) => {
        res.write(chunk);
      },
      onEnd: () => res.end(),
      onError: (e) => {
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
  if (!DISCORD_WEBHOOK) console.warn("[monitor] DISCORD_ALARM_WEBHOOK not set — alarms disabled");
  if (!LOG_SERVER_TOKEN) console.warn("[monitor] LOG_SERVER_TOKEN not set — log API is open");
});

setInterval(() => void pollHealth(), HEALTH_POLL_INTERVAL_MS);
void pollHealth(); // immediate first check
setInterval(() => void pollStats(), STATS_POLL_INTERVAL_MS);
void pollStats(); // immediate first sample
void watchEvents();
