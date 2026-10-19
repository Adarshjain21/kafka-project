require("dotenv").config();
const { Pool } = require("pg");

if (!process.env.DATABASE_URL) {
  console.error(
    "DATABASE_URL is not set. Copy .env.example to .env and put your Neon connection string there."
  );
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS rider_updates (
      id          BIGSERIAL PRIMARY KEY,
      rider_name  TEXT        NOT NULL,
      location    TEXT        NOT NULL,
      partition   INTEGER     NOT NULL,
      topic       TEXT        NOT NULL,
      received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS rider_updates_received_at_idx
       ON rider_updates (received_at DESC);`
  );
}

const NOTIFY_CHANNEL = "rider_updates_channel";

module.exports = { pool, initSchema, NOTIFY_CHANNEL };
