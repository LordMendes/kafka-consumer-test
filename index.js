const { Kafka } = require("kafkajs");
const { config } = require("dotenv");
config();

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9093"],
});

const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topics: [process.env.ENTERPRISES_TOPIC_NAME],
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        {
          topic,
          partition,
          offset: message.offset,
          value: JSON.parse(message.value.toString(), null, 2),
        }
      );
    },
  });
};

run().catch(console.error);
