// Loads `.env` (next to this file) into `process.env` before the `env`
// block below is evaluated, so `pm2 start` / `pm2 restart
// ecosystem.config.js` picks up `.env` values with no `source .env` step
// and no dependency. pm2 itself never reads `.env`.
//
// Precedence: the shell wins — a variable already set in the environment
// that invokes pm2 is never overridden by `.env` (the dotenv rule). A
// missing `.env` is fine: values then come from the shell alone, and the
// `|| "default"` fallbacks below apply as before.
//
// This only takes effect when pm2 (re)reads this file, i.e. a restart
// that names the file. `pm2 restart monitor` (by name) reuses the
// previously loaded configuration and does NOT re-read `.env`; neither
// does a reboot resurrect from `pm2 save` until the next config-file
// restart. After editing `.env`, run `pm2 restart ecosystem.config.js`
// (then `pm2 save`). That re-reads every variable in the `env` block
// below. Two variables the app reads directly rather than through this
// block — `STATS_DATA_DIR` and `JOURNALCTL_TIMEOUT_MS` — reach the
// process from the daemon environment on a fresh start; to refresh them
// after an edit, restart the daemon (`pm2 kill`, then `pm2 start
// ecosystem.config.js`) or export them in the shell and restart with
// `--update-env`.
//
// Parser: one `KEY=value` per line, optional `export ` prefix, `#`
// full-line comments, double/single quotes, and ` #` inline comments on
// unquoted values. Malformed lines are warned about and skipped, never
// fatal — a bad `.env` must not stop pm2 from starting.
const { readFileSync } = require("fs");
const { join } = require("path");

function loadDotenv() {
  let text;
  try {
    text = readFileSync(join(__dirname, ".env"), "utf-8");
  } catch {
    return; // no .env file — shell environment only
  }
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1); // UTF-8 BOM
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const body = line.replace(/^export[ \t]+/, "");
    const eq = body.indexOf("=");
    if (eq === -1) {
      console.warn(`[ecosystem] ignoring malformed .env line: ${JSON.stringify(rawLine)}`);
      continue;
    }
    const key = body.slice(0, eq).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      console.warn(`[ecosystem] ignoring malformed .env line: ${JSON.stringify(rawLine)}`);
      continue;
    }
    // Already set (shell, or an earlier line in this file) wins.
    if (Object.prototype.hasOwnProperty.call(process.env, key)) continue;
    let value = body.slice(eq + 1).trim();
    const quote = value[0];
    if (quote === '"' || quote === "'") {
      const end = value.indexOf(quote, 1);
      if (end === -1) {
        console.warn(`[ecosystem] ignoring unterminated quoted value in .env line: ${JSON.stringify(rawLine)}`);
        continue;
      }
      const inner = value.slice(1, end);
      // Single-quoted values are literal; double-quoted values honour
      // backslash escapes (single pass, so `\\n` stays backslash + n).
      value =
        quote === "'"
          ? inner
          : inner.replace(/\\(.)/g, (_match, ch) => {
              switch (ch) {
                case "n":
                  return "\n";
                case "r":
                  return "\r";
                case "t":
                  return "\t";
                default:
                  return ch;
              }
            });
    } else {
      // Unquoted: a ` #` starts an inline comment; a `#` glued to the
      // value (e.g. in a URL fragment) is part of the value.
      const hash = value.indexOf(" #");
      if (hash !== -1) value = value.slice(0, hash).trimEnd();
    }
    process.env[key] = value;
  }
}

loadDotenv();

/** @type {import('pm2').StartOptions} */
module.exports = {
  apps: [
    {
      name: "monitor",
      script: "monitor.js",
      interpreter: "node",
      autorestart: true,
      watch: false,
      max_memory_restart: "100M",
      env: {
        NODE_ENV: "production",
        // Required, plus one of MONITOR_PROJECT or MONITOR_TARGETS
        DISCORD_ALARM_WEBHOOK: process.env.DISCORD_ALARM_WEBHOOK,
        LOG_SERVER_TOKEN: process.env.LOG_SERVER_TOKEN,
        MONITOR_PROJECT: process.env.MONITOR_PROJECT,
        MONITOR_TARGETS: process.env.MONITOR_TARGETS,
        // Optional (defaults shown)
        LOG_PORT: process.env.LOG_PORT || "9000",
        HEALTH_POLL_INTERVAL_MS: process.env.HEALTH_POLL_INTERVAL_MS || "60000",
        HEALTH_FAIL_THRESHOLD: process.env.HEALTH_FAIL_THRESHOLD || "1",
        ALARM_COOLDOWN_MS: process.env.ALARM_COOLDOWN_MS || "60000",
        DOCKER_SOCKET: process.env.DOCKER_SOCKET || "/var/run/docker.sock",
        JOURNALCTL_BIN: process.env.JOURNALCTL_BIN || "journalctl",
        STATS_POLL_INTERVAL_MS: process.env.STATS_POLL_INTERVAL_MS || "1000",
        STATS_RAW_RETENTION_DAYS: process.env.STATS_RAW_RETENTION_DAYS || "30",
        MEMORY_PRESSURE_RATIO: process.env.MEMORY_PRESSURE_RATIO || "0.85",
        MEMORY_PRESSURE_SUSTAINED_MS: process.env.MEMORY_PRESSURE_SUSTAINED_MS || "180000",
        // STATS_DATA_DIR is intentionally not set here: the code
        // default resolves next to the running script, and a relative
        // path in this file would depend on pm2's working directory.
      },
    },
  ],
};
