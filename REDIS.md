# Redis in this project — the hot path alongside Kafka & Postgres

## Why Redis here at all?

Kafka is the durable log (replay, ordering, partitioned fan-out). Postgres is
the durable source of truth (unique index, SQL queries, long-term storage).
Redis sits in the middle as the **hot path**: everything that needs to be
_fast_ and _ephemeral_ — dedup checks, latest-state lookups, latest-N feeds,
rate limits, and WebSocket fanout across multiple server instances.

```
          ┌────────────┐
POST /send│   server   │──Kafka──►[rider-updates / 2 partitions]
          │  (Express) │                       │
          │  rate-lmt◄─┼──Redis Lua            │
          └────┬───────┘                       ▼
       ┌───────┴────────┐           ┌─────────────────┐
   WS  │ Redis SUBSCRIBE│◄─PUB──────│    consumer     │
       │ PG   LISTEN    │◄─NOTIFY───│                 │
       └────────────────┘           │ 1. INSERT PG    │  (durable)
                                    │ 2. Redis Lua    │  (atomic cache)
                                    │    - dedup SET  │
                                    │    - ZADD feed  │
                                    │    - HSET state │
                                    │    - HINCRBY m  │
                                    │    - PUBLISH    │
                                    └─────────────────┘
```

## Keys (all under the `{rider}` hash-tag so Redis Cluster can co-locate them)

| Key                        | Type        | Purpose                                     |
| -------------------------- | ----------- | ------------------------------------------- |
| `{rider}:dedup:<t:p:off>`  | string + TTL| Kafka-redelivery dedup, 24h TTL             |
| `{rider}:feed`             | sorted set  | Latest-N messages (score = received_at ms)  |
| `{rider}:state:<name>`     | hash        | Current state per rider (location, when)    |
| `{rider}:state:index`      | sorted set  | Rider directory sorted by last-seen         |
| `{rider}:metrics`          | hash        | Counters: total + per-partition             |
| `{rider}:rl:send:<ip>`     | string + TTL| Rate-limit window per client IP             |
| `{rider}:events`           | pub/sub     | WS fanout channel                           |

## Why Lua?

The consumer's ingest is five writes + one check that **must happen atomically**:
dedup, feed, state, index, metrics, publish. A `MULTI/EXEC` would also be
atomic but still needs the dedup result to decide whether to send the other
commands — that's two round trips. Lua is one round trip and the server is
single-threaded, so no other command can interleave.

The rate limit is the classic `INCR + EXPIRE` race: if two clients hit on the
very first request of a window, both might INCR to 1 and only the first will
EXPIRE — the key would then live forever. The Lua script makes INCR + EXPIRE
one atomic step.

See both scripts in [`redis.js`](./redis.js) under `defineCommand`.

## Dedup: why both Redis AND the Postgres unique index?

Belt and braces. The Redis SET NX short-circuits a redelivery storm (Kafka
can replay thousands of messages when a consumer group rebalances) **before**
the message touches Postgres, saving write throughput and keeping the AFTER
INSERT trigger quiet (no WS double-broadcast). But Redis is a cache — it
can get flushed, lose keys to LRU eviction, or be unavailable. The DB unique
index on `(topic, partition, kafka_offset)` is the authoritative dedup.
`createMany({ skipDuplicates: true })` turns a duplicate into a no-op, and
only when `inserted.count > 0` do we touch Redis.

## Graceful degradation

Every Redis-touching function in `redis.js` returns a safe default when
`isReady()` is false. The flow if Redis dies:

- **Producer** (`/api/send`): rate limit fails open → requests still succeed.
- **Consumer**: INSERT to PG still runs; it logs "Redis cache degraded" and
  moves on. No message is lost. The next successful ingest re-warms the cache.
- **Server** (`/api/messages`): falls back to Postgres and reports
  `"source": "postgres"` in the response (visible as a badge in the UI).
- **WebSocket**: Redis pub/sub silently stops delivering, PG NOTIFY keeps
  going. Clients still see new rows live.

## Running it

```bash
docker compose up -d        # kafka + zookeeper + redis + kafka-ui + redis-insight
npm install                 # picks up ioredis
npm run admin               # (re)create the Kafka topic
npm run consumer            # writes to PG + Redis
npm run ui                  # serves the dashboard at :3000
```

- Kafka UI:    http://localhost:8080
- Redis UI:    http://localhost:5540 (host = `redis`, port = `6379`)
- Dashboard:   http://localhost:3000
- Readiness:   http://localhost:3000/readyz → `{ ok, db, redis }`
- Stats:       http://localhost:3000/api/stats
- Riders:      http://localhost:3000/api/riders
- Feed source: `GET /api/messages?source=db` forces the Postgres path for
  comparison; without the flag you'll see `"source": "redis"` once the cache
  is warm.

## Where this goes next (production)

- **Separate ioredis instance per role** at a cluster scale — the subscriber
  connection already is; add one more for pipelined writes if the consumer
  becomes CPU-bound on serialization.
- **Redis Sentinel or Cluster** for HA. The `{rider}` hash-tag already keeps
  all Lua-touched keys slot-local, so Cluster migration needs no code change.
- **Streams instead of pub/sub** if you need replayable, consumer-group
  fanout to many WS servers. Pub/sub is fire-and-forget; a late subscriber
  misses messages. Streams give you persistence + XREADGROUP.
- **TLS + AUTH** via `rediss://user:pass@host:6379` in `REDIS_URL`.
- **Observability**: scrape `INFO`, publish `redis_up`, `cache_hit_ratio`
  (hits-from-redis vs falls-to-pg), and consumer `ingest_duration` to
  Prometheus.
