# Full architecture — Kafka + Postgres + Redis + WebSocket

## 1. The three durability tiers

Each system owns one job. No overlap, no fighting.

| Tier       | System      | Purpose                                       | Lifetime     |
| ---------- | ----------- | --------------------------------------------- | ------------ |
| **Log**    | Kafka       | Ordered, replayable, partitioned event stream | Days (retention) |
| **Store**  | Postgres    | Durable source of truth, SQL queries, dedup   | Forever      |
| **Cache**  | Redis       | Sub-ms hot reads, dedup, rate limit, pub/sub  | TTL-bounded  |

If Redis dies the system keeps working. If Postgres dies the consumer stops committing offsets and Kafka replays when it's back. If Kafka dies producers fail fast and nothing is lost on disk.

---

## 2. End-to-end flow of one message

```
┌─────────┐  HTTP POST /api/send
│ Browser │ ────────────────────┐
└────┬────┘                     │  { name, location }
     │                          ▼
     │                   ┌──────────────┐
     │           ┌──────►│   server.js  │
     │ WS /ws    │       │              │
     │           │       │ 1. Redis Lua │  rateLimit({rider}:rl:send:<ip>)
     │           │       │              │     ├─ allow  → continue
     │           │       │              │     └─ deny   → 429 + Retry-After
     │           │       │ 2. producer  │
     │           │       │    .send()   │
     │           │       └──────┬───────┘
     │           │              │
     │           │              ▼
     │           │       ┌──────────────┐
     │           │       │    Kafka     │  topic: rider-updates
     │           │       │              │  partition = location==='north' ? 0 : 1
     │           │       └──────┬───────┘
     │           │              │
     │           │              ▼
     │           │       ┌──────────────┐
     │           │       │  consumer.js │  group: rider-ui-group
     │           │       │              │
     │           │       │ 3. Postgres  │  createMany(skipDuplicates:true)
     │           │       │    INSERT    │  unique (topic,partition,offset)
     │           │       │              │     ├─ count=1 → new  → step 4
     │           │       │              │     └─ count=0 → dup  → stop
     │           │       │              │
     │           │       │ 4. Redis Lua │  ingestRider (ATOMIC, one script):
     │           │       │              │     ├─ SET NX dedup key  (TTL 24h)
     │           │       │              │     ├─ ZADD feed sorted set
     │           │       │              │     ├─ ZREMRANGEBYRANK  → trim to N
     │           │       │              │     ├─ HSET state:<name>
     │           │       │              │     ├─ ZADD state:index
     │           │       │              │     ├─ HINCRBY metrics
     │           │       │              │     └─ PUBLISH {rider}:events
     │           │       └──────┬───────┘
     │           │              │
     │           │   ┌──────────┴──────────┐
     │           │   │                     │
     │           │   ▼                     ▼
     │           │ ┌───────────┐    ┌──────────────┐
     │           │ │  Postgres │    │    Redis     │
     │           │ │  trigger  │    │   pub/sub    │
     │           │ │  NOTIFY   │    │              │
     │           │ └─────┬─────┘    └──────┬───────┘
     │           │       │ LISTEN          │ SUBSCRIBE
     │           │       └────────┬────────┘
     │           │                ▼
     │           │       ┌────────────────┐
     │           │       │   server.js    │  broadcast(row)
     │           │       │                │  dedup by row.id
     │           └───────┤  wss.clients   │  (both paths carry same row)
     │                   │   .send(JSON)  │
     └───────────────────┤                │
                         └────────────────┘
```

---

## 3. Why each hop is there

### Hop 1 — Rate limit (Redis Lua)
`INCR` + `EXPIRE` in one atomic script. A naive two-step client would race on the first request of a window and leak keys that never expire. Fail-open: if Redis is down, the request still goes through — a Redis outage must not kill writes.

### Hop 2 — Kafka produce
`LegacyPartitioner` + explicit `partition` based on location. North → P0, everything else → P1. The partition key is **location**, so a given location's messages stay ordered.

### Hop 3 — Postgres INSERT (durable, idempotent)
Unique index `(topic, partition, kafka_offset)`. `createMany({skipDuplicates:true})` compiles to `INSERT … ON CONFLICT DO NOTHING`. Kafka can and will redeliver on consumer rebalance — the DB is the authoritative dedup. If this throws, the consumer re-throws so KafkaJS does **not** commit the offset and the message is retried.

### Hop 4 — Redis ingest (hot cache, atomic)
One Lua script does six things so no partial state is ever visible:

| Op                    | Key                       | Why                                   |
| --------------------- | ------------------------- | ------------------------------------- |
| `SET NX EX`           | `{rider}:dedup:<t:p:off>` | Short-circuit redelivery before any writes |
| `ZADD` + `ZREMRANGE`  | `{rider}:feed`            | Latest-N feed, O(log N), trimmed      |
| `HSET` + `EXPIRE`     | `{rider}:state:<name>`    | Current location per rider            |
| `ZADD`                | `{rider}:state:index`     | Rider directory sorted by last-seen   |
| `HINCRBY`             | `{rider}:metrics`         | total + per-partition counters        |
| `PUBLISH`             | `{rider}:events`          | WS fanout                             |

All keys share the `{rider}` hash-tag so Redis Cluster co-locates them in one slot — the Lua stays legal when you later move to Cluster.

### Hop 5 — Fanout to WebSocket (dual path)
Two independent broadcast paths both deliver the same row:
- **Primary**: Redis `SUBSCRIBE {rider}:events` — horizontally scalable (any server instance that subscribes gets every row).
- **Backup**: Postgres `LISTEN rider_updates_channel` — a trigger on `rider_updates` fires `NOTIFY` with the row payload. Keeps the UI alive if Redis is dead.

The server dedups by `row.id` with a bounded in-memory `Set` so clients see each row exactly once.

---

## 4. Component responsibilities (code map)

```
client.js         Kafka connection factory (brokers, clientId)
admin.js          Create/recreate topic with 2 partitions (dev-only)
producer.js       CLI producer (manual testing)
consumer.js       Kafka consumer group → Postgres → Redis ingest
db.js             Prisma client + pg LISTEN loop with reconnect/backoff
redis.js          ioredis client + 2 Lua scripts + helpers (ingest, rl,
                  getFeed, getRiders, getRider, getMetrics, subscribe)
server.js         Express + WS + Kafka producer + Redis-first reads
                  + rate limit + dual WS fanout
prisma/schema     RiderUpdate model with unique dedup index
public/index.html Dashboard — form, feed, rider state, metrics badges
docker-compose    kafka + zookeeper + kafka-ui + redis + redis-insight
```

---

## 5. HTTP surface

| Route                    | Reads from       | Notes                                            |
| ------------------------ | ---------------- | ------------------------------------------------ |
| `POST /api/send`         | —                | Rate-limited; produces to Kafka                  |
| `GET /api/messages`      | Redis → Postgres | Redis sorted set first; `?source=db` forces PG   |
| `GET /api/riders`        | Redis            | HGETALL per rider, pipelined in one round trip   |
| `GET /api/riders/:name`  | Redis            | One HGETALL                                      |
| `GET /api/stats`         | Redis            | Metrics hash                                     |
| `GET /healthz`           | —                | Liveness                                         |
| `GET /readyz`            | Both             | `{ ok, db, redis }` — 503 if either down         |
| `WS  /ws`                | pub/sub + NOTIFY | Broadcasts deduped by row id                     |

---

## 6. Failure modes

| What dies    | Effect                                                                 |
| ------------ | ---------------------------------------------------------------------- |
| Redis        | Rate limit fails open; `/api/messages` serves from PG with source badge; consumer logs "degraded" but still writes to PG; WS still works via NOTIFY. |
| Postgres     | Consumer re-throws → Kafka does not commit offset → redelivery after PG returns. Reads of `/api/messages` serve the Redis cache. |
| Kafka        | `POST /api/send` → 500. Consumer idle. Nothing lost (nothing produced yet). |
| Consumer     | Kafka buffers; on restart `fromBeginning:true` + DB unique index + Redis dedup absorb replay. |
| Server       | Horizontal: run multiple; all subscribe to Redis pub/sub, each serves WS clients. |

---

## 7. Scaling levers (already baked in)

- **Topic partitions**: increase in `admin.js` + add more consumer instances in the same group — Kafka rebalances automatically.
- **Server instances**: all pub/sub subscribers, stateless, behind a sticky-WS load balancer.
- **Redis Cluster**: hash-tags already in place; move `REDIS_URL` to a cluster endpoint and ioredis handles slot routing.
- **Streams instead of pub/sub**: swap `PUBLISH`/`SUBSCRIBE` for `XADD`/`XREADGROUP` when you need replayable WS fanout.
- **Prisma pool**: `PG_POOL_MAX` env + Neon pooler endpoint on `DATABASE_URL`.

That's the whole system — one write path, two read paths, three durability tiers, and every piece can fail without taking the rest down.
