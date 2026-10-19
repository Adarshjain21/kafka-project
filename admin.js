const { kafka } = require("./client");

const TOPIC = "rider-updates";
const NUM_PARTITIONS = 2;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitUntilAbsent(admin, topic, { tries = 20, delayMs = 500 } = {}) {
  for (let i = 0; i < tries; i++) {
    const topics = await admin.listTopics();
    if (!topics.includes(topic)) return;
    await sleep(delayMs);
  }
  throw new Error(`Timed out waiting for topic "${topic}" to be deleted`);
}

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting...");
  await admin.connect();
  console.log("Admin connection success");

  const existing = await admin.listTopics();
  console.log("Existing topics:", existing);

  if (existing.includes(TOPIC)) {
    console.log(`Deleting topic [${TOPIC}]`);
    try {
      await admin.deleteTopics({ topics: [TOPIC], timeout: 5000 });
      await waitUntilAbsent(admin, TOPIC);
      console.log(`Topic deleted [${TOPIC}]`);
    } catch (err) {
      console.error(`Failed to delete topic [${TOPIC}]:`, err.message);
      await admin.disconnect();
      process.exit(1);
    }
  }

  console.log(`Creating topic [${TOPIC}] with ${NUM_PARTITIONS} partitions`);
  const created = await admin.createTopics({
    waitForLeaders: true,
    topics: [{ topic: TOPIC, numPartitions: NUM_PARTITIONS }],
  });

  if (created) {
    console.log(`Topic created [${TOPIC}]`);
  } else {
    console.warn(
      `Topic [${TOPIC}] already exists — createTopics returned false. ` +
        "Check Kafka logs or run this script again."
    );
  }

  const after = await admin.listTopics();
  console.log("Topics now:", after);

  console.log("Disconnecting admin");
  await admin.disconnect();
}

init().catch(async (err) => {
  console.error("Admin failed:", err);
  process.exit(1);
});
