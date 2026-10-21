# Scaling

How to push this stack past its current ~3,200 req/sec ceiling, ordered by
**impact per effort**.

---

## Measured baseline

Local load tests with [`autocannon`](https://github.com/mcollina/autocannon)
against `POST /api/send` on a single-box dev setup:

| Concurrent users | Total req (30s) | Req/sec   | p50     | p99     | Errors |
| ---------------- | --------------- | --------- | ------- | ------- | ------ |
| 50               | 91,000          | **3,040** | 12 ms   | 55 ms   | 0      |
| 200              | 97,000          | **3,214** | 48 ms   | 152 ms  | 0      |
| 1000             | 80,000          | **2,645** | 292 ms  | 835 ms  | 0      |

**Reading this:** throughput caps out around 200 concurrent users. Beyond
that, extra concurrency only inflates latency (classic Little's Law — the
queue is forming, not draining faster). The bottleneck is the single Kafka
producer in `server.js` serializing one `producer.send()` per HTTP request.

---

## Fixes, ordered by impact per effort

### 1. Batch Kafka produces  _(biggest win, smallest change)_

Right now `/api/send` calls `producer.send()` with 1 message. Collect
messages in a buffer, flush every **10 ms** or every **100 messages**
(whichever comes first). One network round-trip to Kafka carries 100
messages instead of 1.

**Expected gain:** 5–10× throughput.

**Tradeoff:** client no longer knows if Kafka actually got the message. If
the server crashes before the flush, those messages are lost. Fine for
rider-updates; not fine for payments.

### 2. Lower producer durability

In `client.js`, set `acks: 1` (leader only) instead of `-1` (all replicas).

**Expected gain:** 30–50% lower produce latency.

**Tradeoff:** you lose a tiny bit of durability — if the leader dies before
replicating, that message is gone.

### 3. Horizontal scale the server

Run 3–4 `server.js` instances behind a load balancer (nginx, HAProxy, or a
cloud LB). Each one has its own Kafka producer + Postgres `LISTEN`
connection. Postgres NOTIFY still reaches all of them, so every connected
WebSocket client still gets every event.

**Expected gain:** roughly linear.

**Tradeoff:** more direct Postgres connections (`LISTEN` must use the direct
endpoint, not the pooler). Neon's free tier caps direct connections — watch
that.

### 4. More Kafka partitions + more consumers

`rider-updates` has 2 partitions → max 2 consumers in parallel. Bump to
**8–16 partitions** and run that many `consumer.js` instances in the same
`groupId`. Kafka rebalances the partition assignment automatically.

**Expected gain:** linear in the number of consumers, up to the partition
count.

**Tradeoff:** partition count can only ever go **up**, never down. Pick a
number you won't regret.

### 5. Scale Kafka itself

One broker in Docker is a single point of failure. Production should be
**3+ brokers** with `replication.factor=3` and `min.insync.replicas=2`.

**Expected gain:** durability + availability, not raw throughput.

**Tradeoff:** operational complexity. For managed options, consider Confluent
Cloud, Redpanda Cloud, or AWS MSK instead of self-hosting.

### 6. Postgres write throughput  _(pair with #1)_

The consumer does one `INSERT` per message via `createMany({ data: [one] })`.
Switch to `consumer.run({ eachBatch })` and build a single `createMany` of
~100 rows per batch. Turns 100 round trips into 1.

**Expected gain:** 10–50× consumer throughput (dominated by the network RTT
to Neon).

**Tradeoff:** on a crash mid-batch, up to 100 messages replay — but the
`UNIQUE (topic, partition, kafka_offset)` index + `skipDuplicates` makes
those replays safe no-ops.

### 7. Connection pool sizes

Prisma defaults are conservative. Once consumers are batching (fix #6),
tune `connection_limit` on the pooled URL. Example:

```
DATABASE_URL=postgres://...@...-pooler.neon.tech/db?sslmode=require&connection_limit=20
```

**Expected gain:** marginal in isolation, but unblocks the other fixes under
high concurrency.

**Tradeoff:** overshooting burns Neon's connection quota. Measure before
tuning.

---

## The honest answer

**Fix #1 and #6 first.** They're a day of work combined, don't touch
infrastructure, and will probably 10× capacity on their own:

- **#1** removes the per-request produce round-trip, letting the server
  push 10,000+ req/sec without breaking a sweat.
- **#6** lets the consumer actually keep up with that rate, so the Kafka
  backlog doesn't grow unbounded.

Everything else (horizontal scale, partition count, broker count) is
infrastructure — don't reach for it until the single-box numbers are
already strong.

---

## Related docs

- [ARCHITECTURE.md](./ARCHITECTURE.md) — how the pieces fit together
- [Readme.md](./Readme.md) — setup and quick start
