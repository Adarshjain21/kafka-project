require("dotenv").config();
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");
const { Partitioners } = require("kafkajs");
const { kafka } = require("./client");
const { prisma, startListener, NOTIFY_CHANNEL, serializeRow } = require("./db");

const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const httpServer = http.createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

function broadcast(obj) {
  const payload = JSON.stringify(obj);
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

app.get("/healthz", (_req, res) => res.json({ ok: true }));

app.get("/readyz", async (_req, res) => {
  try {
    await prisma.$queryRaw`SELECT 1`;
    res.json({ ok: true });
  } catch (err) {
    res.status(503).json({ ok: false, error: err.message });
  }
});

app.post("/api/send", async (req, res) => {
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

app.get("/api/messages", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 500);
  try {
    const rows = await prisma.riderUpdate.findMany({
      orderBy: { receivedAt: "desc" },
      take: limit,
    });
    res.json({ messages: rows.map(serializeRow) });
  } catch (err) {
    console.error("Query failed:", err);
    res.status(500).json({ error: err.message });
  }
});

async function start() {
  await prisma.$connect();
  await producer.connect();
  console.log("Kafka producer connected");

  startListener({
    channel: NOTIFY_CHANNEL,
    onPayload: (payload) => {
      try {
        const row = JSON.parse(payload);
        broadcast({ type: "message", row });
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
