# Kafka Project + Neon Postgres + Live Web UI

A Node.js **Kafka** producer/consumer demo for `rider-updates`, with messages persisted to **Neon Postgres** and streamed to a **web UI** over **WebSockets** in real time.

```
Browser UI ──POST /api/send──▶ Express ──▶ Kafka (rider-updates)
    ▲                                            │
    │                                            ▼
    │                                        consumer.js
    │                                            │
    │                                            ▼
    │                                      Neon Postgres
    │                                            │
    │                                    pg_notify(channel)
    │                                            │
    └──── WebSocket (ws://.../ws) ◀── LISTEN ────┘
```

## Stack

- **Node.js** + **Express 5** (HTTP + static UI)
- **KafkaJS** producer/consumer against a local Kafka broker (Docker)
- **Neon Postgres** for persistence, with `LISTEN`/`NOTIFY` for live fan-out
- **`ws`** WebSocket server for pushing new rows to the browser instantly

---

## Quick start (web UI flow)

### 1. Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Node.js](https://nodejs.org/) (v18+ recommended)
- A free [Neon](https://console.neon.tech) Postgres project

### 2. Configure Neon

Create a project in the Neon console, copy the connection string, then:

```bash
cp .env.example .env
```

Edit `.env` and set:

```
DATABASE_URL=postgres://USER:PASSWORD@ep-xxxxx.<region>.aws.neon.tech/<db>?sslmode=require
PORT=3000   # optional, defaults to 3000
```

### 3. Start Kafka

```bash
docker-compose up -d
```

This brings up Zookeeper + Kafka in the background.

### 4. Install dependencies

```bash
npm install
```

### 5. Create the Kafka topic (one time)

```bash
npm run admin
```

This creates the `rider-updates` topic with **2 partitions**. If the topic already exists, the script deletes and recreates it.

### 6. Run the consumer

In its own terminal — this persists each message to Postgres and emits a `NOTIFY`:

```bash
npm run consumer
```

Optionally pass a consumer group name:

```bash
node consumer.js my-group
```

### 7. Run the UI

In another terminal:

```bash
npm run ui
```

Open <http://localhost:3000>. Send a rider update from the form — new rows stream into the table live via WebSocket (no polling), and are also loaded from Postgres on page load via `GET /api/messages`.

---

## How partitioning works

The producer routes messages to partitions by the `location` field:

| `location`        | Partition |
| ----------------- | --------- |
| `north` (any case) | `0`       |
| anything else      | `1`       |

This applies in both the web UI (`POST /api/send`) and the CLI producer.

---

## HTTP + WebSocket API

| Method | Path            | Description                                                        |
| ------ | --------------- | ------------------------------------------------------------------ |
| POST   | `/api/send`     | Body: `{ name, location }` — produces to the `rider-updates` topic |
| GET    | `/api/messages` | Query: `?limit=50` (max 500) — returns recent rows, newest first   |
| WS     | `/ws`           | Pushes `{ type: "message", row }` for every new row                |

---

## Original CLI flow

If you want to use the raw Kafka CLI scripts (no UI, no DB), you still can. Note the CLI consumer **does not** write to Postgres — only `server.js` + `consumer.js` do.

### 1. Clone and start Kafka

```bash
git clone https://github.com/lifeShadow59/kafka-project.git
cd kafka-project
```

On Linux/macOS, export your LAN IP if Kafka's advertised listener needs it:

```bash
export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)
docker-compose up -d
```

Install deps:

```bash
npm install
```

### 2. Create the topic

```bash
node admin.js
```

Creates `rider-updates` with 2 partitions.

### 3. Run the producer

```bash
node producer.js
```

At the `>` prompt, type two space-separated words: a rider name and a location, e.g.

```
> alice north
> bob south
```

`north` → partition 0, anything else → partition 1.

### 4. Run the consumer

```bash
node consumer.js [GROUP_ID]
```

`GROUP_ID` is optional and defaults to `rider-ui-group`. The consumer subscribes to `rider-updates` and logs each message.

---

## Scripts

| Script           | What it does                                        |
| ---------------- | --------------------------------------------------- |
| `npm run admin`  | (Re)create the `rider-updates` topic                |
| `npm run producer` | Interactive CLI producer                          |
| `npm run consumer` | Kafka consumer that persists to Postgres + NOTIFY |
| `npm run ui`     | Express + WebSocket server (the web UI)             |

---

## Cleaning up

Stop and remove the Docker containers:

```bash
docker-compose down
```

The Neon database is managed in the Neon console — delete the project there if you don't need it anymore.

---

## Customization

- **Kafka config** — tweak `docker-compose.yml` for ports, advertised listeners, etc. The broker address is set in `client.js` (default `localhost:9092`).
- **Topic / partitions** — change `TOPIC` and `NUM_PARTITIONS` in `admin.js`.
- **Schema** — the `rider_updates` table is created automatically by `db.js` (`initSchema`). Edit the DDL there if you need more columns.
- **UI** — the frontend lives in `public/index.html`.

---

## Troubleshooting

- **`DATABASE_URL is not set`** — copy `.env.example` to `.env` and paste your Neon connection string.
- **Consumer can't reach Kafka** — make sure `docker-compose up -d` succeeded and port `9092` is reachable.
- **No live updates in the UI** — check the browser console for the `/ws` WebSocket connection, and make sure the consumer is running (it's what triggers `pg_notify`).
- See the [Docker](https://docs.docker.com/) and [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html) docs for broader help.

---

## Contributors

- Adarsh Jain

Issues and PRs welcome. Happy Kafka-ing!
