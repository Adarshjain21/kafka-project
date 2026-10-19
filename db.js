require("dotenv").config();
const { PrismaClient } = require("@prisma/client");
const { PrismaPg } = require("@prisma/adapter-pg");
const { Client } = require("pg");

// BigInt JSON support (ids & kafka_offset). The Prisma model uses BigInt for
// BIGSERIAL columns and JSON.stringify would otherwise throw.
if (typeof BigInt.prototype.toJSON !== "function") {
  BigInt.prototype.toJSON = function () {
    return this.toString();
  };
}

const POOLED_URL = process.env.DATABASE_URL;
const DIRECT_URL = process.env.DIRECT_URL || POOLED_URL;

if (!POOLED_URL) {
  console.error(
    "DATABASE_URL is not set. Copy .env.example to .env and set DATABASE_URL " +
      "to the Neon POOLED URL (with '-pooler') and DIRECT_URL to the direct URL."
  );
  process.exit(1);
}
if (!process.env.DIRECT_URL) {
  console.warn(
    "DIRECT_URL not set — falling back to DATABASE_URL for migrations and " +
      "LISTEN. If DATABASE_URL is the pooled endpoint, LISTEN will silently " +
      "receive nothing."
  );
}

// Prisma Client (Prisma 7): URL comes from the driver adapter, not schema.prisma.
const adapter = new PrismaPg({ connectionString: POOLED_URL });
const prisma = new PrismaClient({ adapter, log: ["warn", "error"] });

const NOTIFY_CHANNEL = "rider_updates_channel";

/**
 * Reshape a Prisma RiderUpdate row to the snake_case shape the UI and the
 * NOTIFY payload use — single source of truth for both.
 */
function serializeRow(r) {
  return {
    id: r.id.toString(),
    rider_name: r.riderName,
    location: r.location,
    partition: r.partition,
    topic: r.topic,
    received_at: r.receivedAt,
  };
}

/**
 * Dedicated DIRECT Postgres connection for LISTEN. Prisma Client cannot
 * surface pg notifications, so this is one raw pg.Client — reconnects with
 * exponential backoff on any failure.
 */
function startListener({ channel, onPayload, onReady }) {
  let client = null;
  let backoff = 500;
  let stopped = false;

  const connect = async () => {
    if (stopped) return;
    client = new Client({
      connectionString: DIRECT_URL,
      ssl: { rejectUnauthorized: true },
    });

    client.on("notification", (msg) => {
      if (msg.channel === channel) onPayload(msg.payload);
    });
    client.on("error", (err) => {
      console.error(`LISTEN error on "${channel}":`, err.message);
      reconnect();
    });
    client.on("end", () => {
      console.warn(`LISTEN connection ended on "${channel}"`);
      reconnect();
    });

    try {
      await client.connect();
      await client.query(`LISTEN ${channel}`);
      backoff = 500;
      console.log(`Listening on Postgres channel "${channel}"`);
      if (onReady) onReady();
    } catch (err) {
      console.error("LISTEN connect failed:", err.message);
      reconnect();
    }
  };

  const reconnect = () => {
    if (stopped) return;
    try {
      client && client.end().catch(() => {});
    } catch {}
    const wait = backoff;
    backoff = Math.min(backoff * 2, 15_000);
    setTimeout(connect, wait);
  };

  connect();

  return {
    stop: async () => {
      stopped = true;
      try {
        await client?.end();
      } catch {}
    },
  };
}

module.exports = { prisma, NOTIFY_CHANNEL, startListener, serializeRow };
