require("dotenv").config();
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");
const { Partitioners } = require("kafkajs");
const { kafka } = require("./client");
const { prisma, startListener, NOTIFY_CHANNEL, serializeRow } = require("./db");
const redis = require("./redis");

const PORT = process.env.PORT || 3000;
const RL_LIMIT = parseInt(process.env.RATE_LIMIT || "20", 10);
const RL_WINDOW = parseInt(process.env.RATE_WINDOW_SEC || "10", 10);

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Behind a proxy (nginx / cloud LB), req.ip resolves via X-Forwarded-For once
// `trust proxy` is set. Safe default: trust the first hop.
app.set("trust proxy", 1);

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const httpServer = http.createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

// --- WebSocket fanout ----------------------------------------------------
// Both the Redis pub/sub channel AND the Postgres NOTIFY channel deliver the
// same row. Redis is the primary (lowest latency, multi-instance scalable);
// PG NOTIFY is the backstop that keeps the UI alive if Redis is down. We
// dedup by row id so clients see each row exactly once.
const recentBroadcastIds = new Set();
const RECENT_CAP = 2000;

function broadcast(row) {
  if (!row || !row.id) return;
  const key = String(row.id);
  if (recentBroadcastIds.has(key)) return;
  recentBroadcastIds.add(key);
  if (recentBroadcastIds.size > RECENT_CAP) {
    // Drop the oldest half — cheap and bounded.
    const toDelete = recentBroadcastIds.size - RECENT_CAP / 2;
    let i = 0;
    for (const k of recentBroadcastIds) {
      if (i++ >= toDelete) break;
      recentBroadcastIds.delete(k);
    }
  }
  const payload = JSON.stringify({ type: "message", row });
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(payload);
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });
  ws.send(JSON.stringify({ type: "hello", ok: true }));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30_000);
wss.on("close", () => clearInterval(heartbeat));

// --- Rate limit middleware ----------------------------------------------
// Fixed window per IP, implemented atomically in Redis via Lua. Fail-open:
// a Redis outage must not take the write path down.
async function rateLimitSend(req, res, next) {
  const bucket = `send:${req.ip}`;
  const { allowed, remaining, resetMs } = await redis.checkRateLimit(bucket, {
    limit: RL_LIMIT,
    windowSec: RL_WINDOW,
  });
  res.set("X-RateLimit-Limit", String(RL_LIMIT));
  res.set("X-RateLimit-Remaining", String(Math.max(0, remaining)));
  if (!allowed) {
    res.set("Retry-After", String(Math.ceil(resetMs / 1000)));
    return res.status(429).json({ error: "rate limit exceeded", retry_ms: resetMs });
  }
  next();
}

// --- Health & readiness --------------------------------------------------
app.get("/healthz", (_req, res) => res.json({ ok: true }));

app.get("/readyz", async (_req, res) => {
  const [dbOk, redisOk] = await Promise.all([
    prisma.$queryRaw`SELECT 1`.then(() => true).catch(() => false),
    redis.ping(),
  ]);
  const ok = dbOk && redisOk;
  res.status(ok ? 200 : 503).json({ ok, db: dbOk, redis: redisOk });
});

// --- API: produce --------------------------------------------------------
app.post("/api/send", rateLimitSend, async (req, res) => {
  const { name, location } = req.body || {};
  if (!name || !location) {
    return res.status(400).json({ error: "name and location are required" });
  }

  const partition = String(location).toLowerCase() === "north" ? 0 : 1;

  try {
    await producer.send({
      topic: "rider-updates",
      messages: [
        {
          partition,
          key: "location-update",
          value: JSON.stringify({ name, location }),
        },
      ],
    });
    res.json({ ok: true, partition });
  } catch (err) {
    console.error("Produce failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- API: latest-N feed (Redis-first) -----------------------------------
// Redis sorted set serves ~sub-ms. On miss (Redis down or cold cache the
// consumer hasn't warmed yet), fall back to Postgres. The DB result shape
// matches the Redis shape via serializeRow, so the client never notices.
app.get("/api/messages", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 500);
  console.log("limit----->", limit)
  const source = req.query.source; // ?source=db forces the DB path (demo)

  if (source !== "db") {
    const cached = await redis.getFeed(limit);
    if (cached && cached.length > 0) {
      return res.json({ messages: cached, source: "redis" });
    }
  }

  try {
    const rows = await prisma.riderUpdate.findMany({
      orderBy: { receivedAt: "desc" },
      take: limit,
    });
    res.json({ messages: rows.map(serializeRow), source: "postgres" });
  } catch (err) {
    console.error("Query failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- API: current rider states ------------------------------------------
// "Where is each rider right now?" — a view that would need a GROUP BY +
// window function on Postgres but is just an HGETALL per key here.
app.get("/api/riders", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
  const riders = await redis.getRiders(limit);
  if (riders === null) {
    return res.status(503).json({ error: "redis unavailable" });
  }
  res.json({ riders });
});

app.get("/api/riders/:name", async (req, res) => {
  const rider = await redis.getRider(req.params.name);
  if (!rider) return res.status(404).json({ error: "not found" });
  res.json({ rider });
});

// --- API: metrics --------------------------------------------------------
app.get("/api/stats", async (_req, res) => {
  const stats = await redis.getMetrics();
  if (stats === null) return res.status(503).json({ error: "redis unavailable" });
  res.json(stats);
});

async function start() {
  await prisma.$connect();
  await redis.connect().catch((err) =>
    console.warn("[server] Redis not reachable at start:", err.message)
  );
  await producer.connect();
  console.log("Kafka producer connected");

  // Primary WS fanout path: Redis pub/sub (horizontally scalable).
  redis.subscribe((row) => broadcast(row));
  console.log(`Subscribed to Redis channel "${redis.KEYS.channel}"`);

  // Backup WS fanout path: Postgres NOTIFY. Still useful when Redis is down.
  startListener({
    channel: NOTIFY_CHANNEL,
    onPayload: (payload) => {
      try {
        broadcast(JSON.parse(payload));
      } catch (err) {
        console.error("Bad NOTIFY payload:", err.message);
      }
    },
  });

  httpServer.listen(PORT, () => {
    console.log(`UI running at http://localhost:${PORT}`);
    console.log(`WebSocket at ws://localhost:${PORT}/ws`);
  });
}

async function shutdown(signal) {
  console.log(`\n${signal} received, shutting down...`);
  try {
    httpServer.close();
    wss.clients.forEach((c) => c.terminate());
    await producer.disconnect();
    await redis.disconnect();
    await prisma.$disconnect();
  } finally {
    process.exit(0);
  }
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start().catch((err) => {
  console.error("Server failed to start:", err);
  process.exit(1);
});
