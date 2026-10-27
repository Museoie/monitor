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
