const { Kafka } = require("kafkajs");

exports.kafka = new Kafka({
  clientId: "kafka-project",
  brokers: ["localhost:9092"],
});
