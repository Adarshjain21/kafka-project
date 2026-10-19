const { kafka } = require("./client");
const { pool, initSchema, NOTIFY_CHANNEL } = require("./db");

const group = process.argv[2] || "rider-ui-group";

async function init() {
  await initSchema();

  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();
  console.log(`Consumer connected (group=${group})`);

  await consumer.subscribe({ topics: ["rider-updates"], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value.toString();
      console.log(`${group}: [${topic}]: PART:${partition}: ${raw}`);

      let name = null;
      let location = null;
      try {
        const parsed = JSON.parse(raw);
        name = parsed.name ?? null;
        location = parsed.location ?? null;
      } catch (err) {
        console.warn("Non-JSON message, skipping DB write:", raw);
        return;
      }

      if (!name || !location) return;

      try {
        const { rows } = await pool.query(
          `INSERT INTO rider_updates (rider_name, location, partition, topic)
           VALUES ($1, $2, $3, $4)
           RETURNING id, rider_name, location, partition, topic, received_at`,
          [name, location, partition, topic]
        );
        const row = rows[0];
        await pool.query(`SELECT pg_notify($1, $2)`, [
          NOTIFY_CHANNEL,
          JSON.stringify(row),
        ]);
      } catch (err) {
        console.error("Failed to persist message:", err.message);
      }
    },
  });
}

init().catch((err) => {
  console.error("Consumer failed to start:", err);
  process.exit(1);
});
