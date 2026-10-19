const { kafka } = require("./client");
const { prisma } = require("./db");

const group = process.argv[2] || "rider-ui-group";

async function init() {
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

      // Idempotent: Kafka may redeliver on consumer restart. createMany with
      // skipDuplicates uses INSERT ... ON CONFLICT DO NOTHING under the hood
      // (keyed on the (topic, partition, kafka_offset) unique index). The
      // AFTER INSERT trigger only fires when a row is actually inserted, so
      // the WS never broadcasts duplicates either.
      try {
        await prisma.riderUpdate.createMany({
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
        // Throw so KafkaJS does NOT commit the offset -> message is retried.
        console.error("INSERT failed, re-throwing for retry:", err.message);
        throw err;
      }
    },
  });
}

init().catch((err) => {
  console.error("Consumer failed to start:", err);
  process.exit(1);
});
