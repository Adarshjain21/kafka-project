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

> See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full data-flow diagram, component responsibilities, and scaling notes.

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

Edit `.env` and set **both** URLs (Prisma convention):

```
# POOLED endpoint — hostname contains "-pooler". Used by Prisma Client for all queries.
DATABASE_URL=postgres://USER:PASSWORD@ep-xxxxx-pooler.<region>.aws.neon.tech/<db>?sslmode=require

# DIRECT endpoint — hostname does NOT contain "-pooler". Used by:
#   - prisma migrate   (schema changes)
#   - the LISTEN client in db.js (LISTEN doesn't work through the pooler)
DIRECT_URL=postgres://USER:PASSWORD@ep-xxxxx.<region>.aws.neon.tech/<db>?sslmode=require

PORT=3000   # optional, default 3000
```

> **Why two URLs?** Neon's pooler (PgBouncer in transaction mode) breaks `LISTEN`
> — notifications silently never arrive. We use the **direct** URL for the one
> dedicated LISTEN connection and for migrations, and the **pooled** URL for all
> Prisma Client queries so you don't burn Neon's low direct-connection cap.

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

### 5b. Run Prisma migrations (one time, and after every schema change)

```bash
npm run migrate
```

Runs `prisma migrate deploy`, which applies every unapplied migration under
`prisma/migrations/` to the DIRECT URL in order. Prisma tracks applied
migrations in a `_prisma_migrations` table it manages for you.

To open a GUI table browser for your Neon DB: `npm run studio`.

> ⚠️ The initial migration **drops the old `rider_updates` table** before
> recreating it with the new schema + AFTER-INSERT trigger. Any existing demo
> rows are discarded.

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

| Script                | What it does                                                  |
| --------------------- | ------------------------------------------------------------- |
| `npm run migrate`     | `prisma migrate deploy` — apply pending migrations            |
| `npm run migrate:dev` | `prisma migrate dev` — create & apply a new migration locally |
| `npm run studio`      | Open Prisma Studio (DB browser GUI)                           |
| `npm run admin`       | (Re)create the `rider-updates` topic                          |
| `npm run producer`    | Interactive CLI producer                                      |
| `npm run consumer`    | Kafka consumer; idempotent INSERT (trigger fires NOTIFY)      |
| `npm run ui`          | Express + WebSocket server (the web UI)                       |

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
- **Schema** — the `rider_updates` table and the AFTER-INSERT trigger live in `prisma/schema.prisma` and `prisma/migrations/`. Change the model, then `npm run migrate:dev -- --name <what-you-changed>` to generate a new migration file. Never edit an already-applied migration in place.
- **UI** — the frontend lives in `public/index.html`.

---

## Troubleshooting

- **`DATABASE_URL is not set`** — copy `.env.example` to `.env` and paste your Neon connection string (direct endpoint — no `-pooler` in the hostname).
- **Consumer can't reach Kafka** — make sure `docker-compose up -d` succeeded and port `9092` is reachable.
- **No live updates in the UI** — three likely causes, in order:
  1. The consumer isn't running. The AFTER-INSERT trigger only fires when a row is actually inserted.
  2. `DIRECT_URL` is missing or points at the Neon **pooler** endpoint. `LISTEN` silently receives nothing through PgBouncer in transaction mode.
  3. You skipped `npm run migrate`, so the trigger that fires the NOTIFY doesn't exist yet.
- **"relation \"rider_updates\" does not exist"** — run `npm run migrate`.
- **"PrismaClient did not initialize yet. Please run `prisma generate`"** — `npm install` should run `prisma generate` automatically via the `postinstall` script. If it didn't, run it manually: `npx prisma generate`.

---

## Known limits / what's next

This project implements several production-grade DB patterns (idempotent inserts, trigger-based atomic NOTIFY, reconnecting LISTEN, two-pool split, versioned migrations, SSL verify-full, graceful shutdown, readiness probe). What's deliberately **not** done — add before real production:

- **Retention / partitioning** — `rider_updates` grows unbounded. Convert to a `PARTITION BY RANGE (received_at)` monthly scheme and schedule partition rotation + archival (pg_cron or an external job).
- **Auth / rate limiting** — `/api/send` and `/ws` are unauthenticated.
- **TLS termination** — plain `http` / `ws` locally; terminate TLS at a reverse proxy or load balancer.
- **Horizontal scale** — multiple `server.js` instances work (each has its own LISTEN, Postgres delivers NOTIFY to all), but you'll want sticky sessions or a shared pub/sub if you move away from Postgres LISTEN.
- **Observability** — add Prometheus metrics, structured logs with request IDs, and Kafka consumer-lag monitoring.
- **Secrets** — move off `.env` to a real secret manager (AWS SSM, Vault, Doppler) and rotate.
- See the [Docker](https://docs.docker.com/) and [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html) docs for broader help.

---

## Contributors

- Adarsh Jain

Issues and PRs welcome. Happy Kafka-ing!
