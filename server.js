require("dotenv").config();
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");
const { Partitioners } = require("kafkajs");
const { kafka } = require("./client");
const { pool, initSchema, NOTIFY_CHANNEL } = require("./db");

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
    try {
      ws.ping();
    } catch {}
  }
}, 30000);

wss.on("close", () => clearInterval(heartbeat));

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
    const { rows } = await pool.query(
      `SELECT id, rider_name, location, partition, topic, received_at
         FROM rider_updates
        ORDER BY received_at DESC
        LIMIT $1`,
      [limit]
    );
    res.json({ messages: rows });
  } catch (err) {
    console.error("Query failed:", err);
    res.status(500).json({ error: err.message });
  }
});

async function startListener() {
  const listener = await pool.connect();
  listener.on("notification", (msg) => {
    if (msg.channel !== NOTIFY_CHANNEL) return;
    try {
      const row = JSON.parse(msg.payload);
      broadcast({ type: "message", row });
    } catch (err) {
      console.error("Bad NOTIFY payload:", err.message);
    }
  });
  listener.on("error", (err) => {
    console.error("Listener client error:", err.message);
  });
  await listener.query(`LISTEN ${NOTIFY_CHANNEL}`);
  console.log(`Listening on Postgres channel "${NOTIFY_CHANNEL}"`);
}

async function start() {
  await initSchema();
  await producer.connect();
  console.log("Kafka producer connected");

  await startListener();

  httpServer.listen(PORT, () => {
    console.log(`UI running at http://localhost:${PORT}`);
    console.log(`WebSocket at ws://localhost:${PORT}/ws`);
  });
}

start().catch((err) => {
  console.error("Server failed to start:", err);
  process.exit(1);
});
