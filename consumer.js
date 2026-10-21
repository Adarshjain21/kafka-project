const { kafka } = require("./client");
const { prisma } = require("./db");
const redis = require("./redis");

const group = process.argv[2] || "rider-ui-group";

async function init() {
  await redis.connect().catch((err) =>
    // Non-fatal: the consumer still writes to Postgres. Redis is the hot cache,
    // not the source of truth. ioredis will keep retrying in the background.
    console.warn("[consumer] Redis not reachable at start:", err.message)
  );

  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();
  console.log(`Consumer connected (group=${group})`);

  await consumer.subscribe({ topics: ["rider-updates"], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value.toString();
      const offset = message.offset;
      console.log(`${group}: [${topic}]: PART:${partition} OFF:${offset}: ${raw}`);

      let name = null;
      let location = null;
      try {
        const parsed = JSON.parse(raw);
        name = parsed.name ?? null;
        location = parsed.location ?? null;
      } catch {
        console.warn("Non-JSON message, skipping:", raw);
        return;
      }

      if (!name || !location) return;

      // --- Durable write (Postgres) -------------------------------------
      // Kafka may redeliver on consumer restart. createMany + skipDuplicates
      // uses INSERT ... ON CONFLICT DO NOTHING, keyed on
      // (topic, partition, kafka_offset). The AFTER INSERT trigger only
      // fires when a row is actually inserted → NOTIFY stays dedup'd too.
      let inserted;
      try {
        inserted = await prisma.riderUpdate.createMany({
          data: [
            {
              riderName: name,
              location,
              partition,
              topic,
              kafkaOffset: BigInt(offset),
            },
          ],
          skipDuplicates: true,
        });
      } catch (err) {
        // Throw → KafkaJS does NOT commit the offset → message is retried.
        console.error("INSERT failed, re-throwing for retry:", err.message);
        throw err;
      }

      // --- Hot cache (Redis) --------------------------------------------
      // Only update Redis when the DB confirmed this was a NEW row. On a
      // duplicate delivery, the Redis dedup key will usually also catch it,
      // but DB.count is the authoritative signal.
      //
      // Redis failures are logged but NOT thrown — otherwise a Redis outage
      // would stall the whole Kafka consumer group. The DB has the row,
      // the PG NOTIFY path still broadcasts, and the next ingest will
      // self-heal the cache via normal writes.
      if (inserted.count > 0) {
        const row = {
          rider_name: name,
          location,
          partition,
          topic,
          kafka_offset: offset,
          received_at: new Date().toISOString(),
        };
        const { degraded } = await redis.ingest(row);
        if (degraded) {
          console.warn(`[consumer] Redis cache degraded for OFF:${offset}`);
        }
      }
    },
  });

  const shutdown = async (sig) => {
    console.log(`\n${sig} received, draining consumer...`);
    try {
      await consumer.disconnect();
      await redis.disconnect();
      await prisma.$disconnect();
    } finally {
      process.exit(0);
    }
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

init().catch((err) => {
  console.error("Consumer failed to start:", err);
  process.exit(1);
});
