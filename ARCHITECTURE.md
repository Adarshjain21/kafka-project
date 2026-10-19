# Architecture

End-to-end view of how a rider update flows from the browser through Kafka to Postgres and back to every open UI in real time.

---

## Full data flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              BROWSER (UI)                                │
│                                                                          │
│   ┌──────────────────┐              ┌──────────────────────────────┐    │
│   │   Send update    │              │   Live messages table        │    │
│   │   [ name     ]   │              │   rider | loc | part | time  │    │
│   │   [ location ▼]  │              │   ──────┼─────┼──────┼─────  │    │
│   │   [ Send ]       │              │   tony  │north│  0   │ ...   │    │
│   └────────┬─────────┘              └──────────────▲───────────────┘    │
│            │                                        │                    │
│            │ fetch POST                             │ WebSocket onmessage│
└────────────┼────────────────────────────────────────┼────────────────────┘
             │                                        │
             │ /api/send                              │ ws://host/ws
             │ { name, location }                     │ { type:"message",
             ▼                                        │   row:{...} }
┌──────────────────────────────────────────┐          │
│         server.js  (Express + ws)        │          │
│  ┌────────────────────────┐              │          │
│  │ POST /api/send         │              │          │
│  │   producer.send(...)   │              │          │
│  └──────────┬─────────────┘              │          │
│             │                            │          │
│             │ KafkaJS producer           │          │
│             ▼                            │          │
└─────────────┼────────────────────────────┘          │
              │                                       │
              ▼                                       │
     ┌────────────────────────────────────┐           │
     │   KAFKA  topic: rider-updates      │           │
     │   ┌──────────┐  ┌──────────┐       │           │
     │   │ Part 0   │  │ Part 1   │       │           │
     │   │ (north)  │  │ (south,  │       │           │
     │   │          │  │  east..) │       │           │
     │   └────┬─────┘  └────┬─────┘       │           │
     └────────┼─────────────┼─────────────┘           │
              │             │                         │
              └──────┬──────┘                         │
                     │ consumer.subscribe()           │
                     ▼                                │
     ┌─────────────────────────────────────┐          │
     │           consumer.js                │          │
     │   eachMessage({ topic, partition,   │          │
     │                 message }) =>       │          │
     │     prisma.riderUpdate.createMany({ │          │
     │       data: [...],                  │          │
     │       skipDuplicates: true          │          │
     │     })                              │          │
     └──────────────┬──────────────────────┘          │
                    │ Prisma Client (pooled URL)      │
                    ▼                                 │
     ┌──────────────────────────────────────────────┐ │
     │              NEON POSTGRES                   │ │
     │  ┌────────────────────────────────────────┐  │ │
     │  │ TABLE rider_updates                    │  │ │
     │  │   id          BIGSERIAL                │  │ │
     │  │   rider_name  TEXT                     │  │ │
     │  │   location    TEXT                     │  │ │
     │  │   partition   INTEGER                  │  │ │
     │  │   topic       TEXT                     │  │ │
     │  │   kafka_offset BIGINT                  │  │ │
     │  │   received_at TIMESTAMPTZ              │  │ │
     │  │   UNIQUE (topic, partition, offset)    │  │ │
     │  └────────────────────────────────────────┘  │ │
     │                                              │ │
     │  TRIGGER rider_updates_notify AFTER INSERT:  │ │
     │    pg_notify('rider_updates_channel', row)   │ │
     │                                              │ │
     │  NOTIFY rider_updates_channel ──┐            │ │
     └─────────────────────────────────┼────────────┘ │
                                       │ pg LISTEN    │
                                       │ (direct URL) │
                                       ▼              │
     ┌─────────────────────────────────────┐          │
     │   server.js  (LISTEN handler)        │          │
     │   listener.on('notification', msg => │          │
     │     broadcast({                      │          │
     │       type: 'message',               │          │
     │       row:  JSON.parse(msg.payload)  │          │
     │     })                               │          │
     │   )                                  │          │
     └──────────────┬──────────────────────┘          │
                    │                                 │
                    │ ws.send() to every open client  │
                    └─────────────────────────────────┘
```

---

## Processes

| Process                        | Role                                                                                   |
| ------------------------------ | -------------------------------------------------------------------------------------- |
| `node admin.js`                | One-time. Creates / recreates the `rider-updates` Kafka topic with 2 partitions.       |
| `node consumer.js`             | Long-running. Reads from Kafka → idempotent INSERT via Prisma. Trigger fires NOTIFY.   |
| `node server.js` (`npm run ui`)| Long-running. HTTP + WebSocket + Kafka producer + a dedicated Postgres LISTEN client.  |

All three can run on separate machines; they communicate only via Kafka and Postgres.

---

## Two "live" paths meet at the server

**Write path** (user clicks Send):
```
Browser → HTTP POST → Express → KafkaJS producer → Kafka topic
```

**Buffered path** (always decoupled):
```
Kafka topic → consumer.js → Prisma createMany → Postgres INSERT
```

**Read path** (push, no polling):
```
Postgres AFTER INSERT trigger → pg_notify → LISTEN client in server.js → WebSocket broadcast → every open browser tab
```

---

## Why each piece

| Component          | What it buys you                                                                                       | What breaks if removed                                   |
| ------------------ | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------- |
| **Kafka**          | Durable, replayable buffer. Producers and consumers scale independently. New consumer groups replay history. | Backpressure disappears; a slow DB stalls the UI's POST. |
| **Partitions**     | Ordering guarantee *within* a partition + horizontal scale. We key by `location` (north → 0, else → 1).   | All messages serialize through one broker thread.        |
| **Postgres (Neon)**| Source of truth. SQL, transactions, indexes, point-in-time restore.                                       | History lives only in Kafka's retention window.          |
| **Prisma Client**  | Typed model, migrations, straightforward CRUD. Lives on the pooled Neon endpoint.                        | Hand-written SQL + ad-hoc schema drift.                  |
| **LISTEN/NOTIFY**  | Push updates to the app the instant a row commits — zero polling. One dedicated direct-URL connection.    | UI must poll (burn DB queries and lag updates).          |
| **AFTER INSERT trigger** | Atomic: NOTIFY fires in the same transaction as the INSERT. No gap where the row persists but no one hears about it. | Need application-level 2-phase code; drops on crash.     |
| **WebSocket (`ws`)** | Bi-directional, low-latency fan-out to every open browser.                                              | SSE works too but WS is standard for live tracking UIs.  |

---

## Database setup specifics

### Two Neon connection URLs

Neon offers two endpoints per project:

| Env var        | Hostname     | Used by                                     |
| -------------- | ------------ | ------------------------------------------- |
| `DATABASE_URL` | `*-pooler.*` | Prisma Client — all CRUD queries            |
| `DIRECT_URL`   | `*.neon.tech`| `prisma migrate` + the LISTEN client in `db.js` |

**Why both:** Neon's pooler (PgBouncer in transaction mode) silently drops `LISTEN`. Our write path and reads go through the pooler for scale; the one LISTEN connection has to use the direct endpoint. `prisma migrate` also needs direct (it acquires session-level advisory locks).

### Idempotent inserts

```sql
UNIQUE (topic, partition, kafka_offset)
```

Kafka is at-least-once — on consumer restart it can redeliver the same message. `prisma.riderUpdate.createMany({ skipDuplicates: true })` compiles to `INSERT ... ON CONFLICT DO NOTHING` on that unique index. The trigger only fires when a row is **actually** inserted, so the WebSocket never broadcasts duplicates either.

### Atomic NOTIFY

```sql
CREATE TRIGGER rider_updates_notify
  AFTER INSERT ON rider_updates
  FOR EACH ROW EXECUTE FUNCTION notify_rider_update();
```

The trigger fires inside the INSERT's transaction. Postgres delivers the NOTIFY only on COMMIT, so a crashed INSERT also loses the NOTIFY — the two are consistent by construction.

### Reconnecting LISTEN client

`db.js :: startListener` opens one dedicated `pg.Client` on `DIRECT_URL`, calls `LISTEN rider_updates_channel`, and re-establishes itself with exponential backoff (500ms → 15s cap) if the connection errors or ends. Without that, one network blip would silently stop live updates forever.

### Schema managed by Prisma

- `prisma/schema.prisma` — model definition (snake_case in DB ↔ camelCase in TS).
- `prisma/migrations/<timestamp>_<name>/migration.sql` — raw SQL migrations, including the trigger (which Prisma's schema doesn't model natively).
- `prisma.config.ts` — holds the `datasource.url` (Prisma 7 convention; no URL in schema).
- `npm run migrate` → `prisma migrate deploy` applies pending migrations.

---

## Load profile (one open browser tab, idle)

| Resource          | Cost                                                                 |
| ----------------- | -------------------------------------------------------------------- |
| HTTP requests     | **0 / sec** (no polling)                                             |
| WebSocket         | 1 held connection, 1 ping every 30s                                  |
| DB queries        | **0 / sec** while idle                                               |
| Per message sent  | 1 Kafka produce + 1 Kafka consume + 1 INSERT + 1 NOTIFY + 1 WS push per connected tab |

Compare to the earlier polling design: 1 req/sec per tab, 2 DB queries each — ~86,000 wasted queries/day per idle tab.

---

## Scaling notes (what breaks first and what to add)

| Scale pressure                | What happens                                        | Fix                                                                                 |
| ----------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Many `server.js` instances    | Each instance has its own LISTEN → Postgres NOTIFY reaches all of them → every WS client still gets every event. Works out of the box. | Nothing (short-term). Long term: move fan-out to Redis pub/sub or Kafka to avoid LISTEN-connection count. |
| Many consumers                | Scale by adding instances to the same `groupId` — Kafka rebalances partitions. | Nothing — already works.                                                            |
| High insert rate              | NOTIFY payload has an 8 KB limit; row count on `rider_updates` grows unbounded. | Partition the table by `received_at`; schedule archival (`pg_cron` or external cron). |
| Many browsers                 | WS fan-out is O(n) per message; LISTEN connection count stays at 1 per server instance.  | Add server instances behind a load balancer with sticky sessions.                   |
| "Nearby riders" queries       | Table scan on `(lat, lng)`.                         | Add Redis GEO (`GEOADD` / `GEOSEARCH`) as the hot path; keep Postgres for durable history. |

---

## File map

| File                                      | Role                                                                          |
| ----------------------------------------- | ----------------------------------------------------------------------------- |
| `client.js`                               | KafkaJS client — one shared broker config                                     |
| `admin.js`                                | Topic management (create / recreate)                                          |
| `producer.js`                             | CLI producer (readline loop) — predates the UI                                |
| `consumer.js`                             | Kafka → Postgres via Prisma                                                   |
| `server.js`                               | Express + WebSocket + Kafka producer + Postgres LISTEN                        |
| `db.js`                                   | Prisma Client singleton + reconnecting `startListener` + `serializeRow`       |
| `prisma/schema.prisma`                    | Model definitions                                                             |
| `prisma/migrations/*/migration.sql`       | Raw SQL migrations (including the trigger)                                    |
| `prisma.config.ts`                        | Prisma 7 datasource URL + migrate paths                                       |
| `public/index.html`                       | UI — form + live table over WebSocket                                         |
| `docker-compose.yml`                      | Local Kafka + Zookeeper                                                       |
| `.env` (gitignored) / `.env.example`      | `DATABASE_URL`, `DIRECT_URL`, `PORT`                                          |
