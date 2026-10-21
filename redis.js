require("dotenv").config();
const IORedis = require("ioredis");

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

const KEYS = {
  // {rider} hash-tag keeps every related key in the same Redis Cluster slot,
  // which is required for Lua scripts that touch multiple keys atomically.
  dedup: (uid) => `{rider}:dedup:${uid}`,
  feed: "{rider}:feed",
  state: (name) => `{rider}:state:${name}`,
  stateIndex: "{rider}:state:index",
  metrics: "{rider}:metrics",
  rateLimit: (bucket) => `{rider}:rl:${bucket}`,
  channel: "{rider}:events",
};

const FEED_MAX = parseInt(process.env.REDIS_FEED_MAX || "100", 10);
const DEDUP_TTL = parseInt(process.env.REDIS_DEDUP_TTL || "86400", 10);
const STATE_TTL = parseInt(process.env.REDIS_STATE_TTL || "604800", 10);

function makeClient(role) {
  const client = new IORedis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: 2,
    enableReadyCheck: true,
    // Exponential backoff with a 10s cap so a long outage doesn't lose events
    // to retry pile-up. ioredis queues commands while reconnecting; we also
    // mark `ready` false below so callers can short-circuit.
    retryStrategy: (times) => Math.min(1000 * Math.pow(2, times), 10_000),
    reconnectOnError: (err) => {
      const msg = err.message || "";
      return msg.includes("READONLY") || msg.includes("ETIMEDOUT");
    },
  });

  client.on("connect", () => console.log(`[redis:${role}] connecting...`));
  client.on("ready", () => console.log(`[redis:${role}] ready`));
  client.on("error", (err) => console.error(`[redis:${role}] error:`, err.message));
  client.on("end", () => console.warn(`[redis:${role}] connection ended`));
  client.on("reconnecting", (ms) =>
    console.warn(`[redis:${role}] reconnecting in ${ms}ms`)
  );
  return client;
}

// Primary connection: regular commands + PUBLISH.
const redis = makeClient("main");

// Dedicated subscriber connection — SUBSCRIBE puts a connection in
// subscriber-mode where normal commands are rejected, so it MUST be separate.
const subscriber = makeClient("sub");

/**
 * ingestRider: single atomic Lua script that owns the entire consumer hot path.
 *
 * - SET NX on the dedup key → short-circuits Kafka redeliveries before any
 *   other work. Returns 0 immediately for duplicates (so the consumer doesn't
 *   write to the DB or re-publish).
 * - ZADD + ZREMRANGEBYRANK on the feed sorted set → O(log N) latest-N cache.
 * - HSET on the per-rider state hash + ZADD on the state index → fast
 *   "online riders" and "state of rider X" reads.
 * - HINCRBY on the metrics hash → per-partition + total counters.
 * - PUBLISH on the events channel → fanout for every subscribed WS server.
 *
 * All five run atomically: no partial state is ever visible, no race between
 * dedup check and the writes, and because the keys share the {rider} hash-tag
 * this is Redis Cluster-safe.
 */
redis.defineCommand("ingestRider", {
  numberOfKeys: 5,
  lua: `
    local dedupKey = KEYS[1]
    local feedKey  = KEYS[2]
    local stateKey = KEYS[3]
    local indexKey = KEYS[4]
    local metrics  = KEYS[5]

    local dedupTtl = tonumber(ARGV[1])
    local feedMax  = tonumber(ARGV[2])
    local stateTtl = tonumber(ARGV[3])
    local score    = tonumber(ARGV[4])
    local member   = ARGV[5]
    local rider    = ARGV[6]
    local location = ARGV[7]
    local partition= ARGV[8]
    local topic    = ARGV[9]
    local offset   = ARGV[10]
    local receivedAt = ARGV[11]
    local channel  = ARGV[12]

    local isNew = redis.call('SET', dedupKey, '1', 'NX', 'EX', dedupTtl)
    if not isNew then
      return 0
    end

    redis.call('ZADD', feedKey, score, member)
    local overflow = redis.call('ZCARD', feedKey) - feedMax
    if overflow > 0 then
      redis.call('ZREMRANGEBYRANK', feedKey, 0, overflow - 1)
    end

    redis.call('HSET', stateKey,
      'rider_name', rider,
      'location', location,
      'partition', partition,
      'topic', topic,
      'kafka_offset', offset,
      'received_at', receivedAt
    )
    redis.call('EXPIRE', stateKey, stateTtl)
    redis.call('ZADD', indexKey, score, rider)

    redis.call('HINCRBY', metrics, 'total', 1)
    redis.call('HINCRBY', metrics, 'part:' .. partition, 1)
    redis.call('HSET', metrics, 'last_ts', receivedAt)

    redis.call('PUBLISH', channel, member)
    return 1
  `,
});

/**
 * rateLimit: atomic fixed-window counter.
 *
 * One round trip, no race between the GET and the SET+EXPIRE that a naive
 * client implementation would have. Returns {allowed, remaining, resetMs}.
 */
redis.defineCommand("rateLimit", {
  numberOfKeys: 1,
  lua: `
    local key   = KEYS[1]
    local limit = tonumber(ARGV[1])
    local wndw  = tonumber(ARGV[2])

    local count = redis.call('INCR', key)
    if count == 1 then
      redis.call('EXPIRE', key, wndw)
    end
    local ttl = redis.call('PTTL', key)
    if count > limit then
      return {0, 0, ttl}
    end
    return {1, limit - count, ttl}
  `,
});

async function connect() {
  await Promise.all([redis.connect(), subscriber.connect()]);
}

async function disconnect() {
  await Promise.all([redis.quit().catch(() => {}), subscriber.quit().catch(() => {})]);
}

function isReady() {
  return redis.status === "ready";
}

/**
 * Idempotent, atomic ingest. Returns true if the row was new, false if the
 * dedup key already existed (duplicate Kafka delivery) or Redis is unavailable
 * (caller falls back to the DB unique index as the source of truth).
 */
async function ingest(row) {
  if (!isReady()) return { newRow: false, degraded: true };
  const uid = `${row.topic}:${row.partition}:${row.kafka_offset}`;
  const member = JSON.stringify(row);
  const score = new Date(row.received_at).getTime();
  try {
    const res = await redis.ingestRider(
      KEYS.dedup(uid),
      KEYS.feed,
      KEYS.state(row.rider_name),
      KEYS.stateIndex,
      KEYS.metrics,
      DEDUP_TTL,
      FEED_MAX,
      STATE_TTL,
      score,
      member,
      row.rider_name,
      row.location,
      String(row.partition),
      row.topic,
      String(row.kafka_offset),
      row.received_at,
      KEYS.channel
    );
    return { newRow: res === 1, degraded: false };
  } catch (err) {
    console.error("[redis] ingest failed:", err.message);
    return { newRow: false, degraded: true };
  }
}

/**
 * Fixed-window rate limit. 10 req / 10s / IP is a sane demo default; tune
 * via REDIS_RATE_LIMIT / REDIS_RATE_WINDOW. Fail-open on Redis outage so a
 * Redis crash doesn't take the whole write path down.
 */
async function checkRateLimit(bucket, { limit = 10, windowSec = 10 } = {}) {
  if (!isReady()) return { allowed: true, remaining: limit, resetMs: 0, degraded: true };
  try {
    const [allowed, remaining, resetMs] = await redis.rateLimit(
      KEYS.rateLimit(bucket),
      limit,
      windowSec
    );
    return { allowed: allowed === 1, remaining, resetMs };
  } catch (err) {
    console.error("[redis] rate limit failed:", err.message);
    return { allowed: true, remaining: limit, resetMs: 0, degraded: true };
  }
}

async function getFeed(limit = 50) {
  if (!isReady()) return null;
  try {
    // ZREVRANGE returns highest-score first = newest first.
    const members = await redis.zrevrange(KEYS.feed, 0, limit - 1);
    return members.map((m) => JSON.parse(m));
  } catch (err) {
    console.error("[redis] getFeed failed:", err.message);
    return null;
  }
}

async function getRiders(limit = 100) {
  if (!isReady()) return null;
  try {
    const names = await redis.zrevrange(KEYS.stateIndex, 0, limit - 1);
    if (names.length === 0) return [];
    // Pipeline every HGETALL into one round trip.
    const pipeline = redis.pipeline();
    for (const n of names) pipeline.hgetall(KEYS.state(n));
    const results = await pipeline.exec();
    return results
      .map(([err, hash]) => (err || !hash || !hash.rider_name ? null : hash))
      .filter(Boolean);
  } catch (err) {
    console.error("[redis] getRiders failed:", err.message);
    return null;
  }
}

async function getRider(name) {
  if (!isReady()) return null;
  try {
    const hash = await redis.hgetall(KEYS.state(name));
    return hash && hash.rider_name ? hash : null;
  } catch (err) {
    console.error("[redis] getRider failed:", err.message);
    return null;
  }
}

async function getMetrics() {
  if (!isReady()) return null;
  try {
    const hash = await redis.hgetall(KEYS.metrics);
    const partitions = {};
    let total = 0;
    let lastTs = null;
    for (const [k, v] of Object.entries(hash)) {
      if (k === "total") total = Number(v);
      else if (k === "last_ts") lastTs = v;
      else if (k.startsWith("part:")) partitions[k.slice(5)] = Number(v);
    }
    return { total, partitions, last_ts: lastTs };
  } catch (err) {
    console.error("[redis] getMetrics failed:", err.message);
    return null;
  }
}

/**
 * Subscribe to the fanout channel. The handler receives the already-parsed
 * row — same shape the DB NOTIFY path produces. Returns an unsubscribe fn.
 */
function subscribe(onRow) {
  subscriber.subscribe(KEYS.channel).catch((err) => {
    console.error("[redis:sub] subscribe failed:", err.message);
  });
  const handler = (channel, payload) => {
    if (channel !== KEYS.channel) return;
    try {
      onRow(JSON.parse(payload));
    } catch (err) {
      console.error("[redis:sub] bad payload:", err.message);
    }
  };
  subscriber.on("message", handler);
  return () => {
    subscriber.off("message", handler);
    subscriber.unsubscribe(KEYS.channel).catch(() => {});
  };
}

async function ping() {
  try {
    const res = await redis.ping();
    return res === "PONG";
  } catch {
    return false;
  }
}

module.exports = {
  redis,
  subscriber,
  KEYS,
  connect,
  disconnect,
  isReady,
  ingest,
  checkRateLimit,
  getFeed,
  getRiders,
  getRider,
  getMetrics,
  subscribe,
  ping,
};
