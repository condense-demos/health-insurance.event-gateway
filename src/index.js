const kafka = require("./kafka");
const { processApplicationEvent } = require("./processor/applicationProcessor");
const logger = require("./utils/logger");

const run = async () => {
  await kafka.consumer.connect();
  await kafka.producer.connect();

  await kafka.consumer.subscribe({
    topic: process.env.KAFKA_INPUT_TOPIC,
    fromBeginning: true,
  });

  await kafka.consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = JSON.parse(message.value.toString());
        logger.info(`Received message: ${JSON.stringify(event)}`);

        const processedResult = await processApplicationEvent(event);

        // Publish result to output topic or DLT
        // Commit offset
      } catch (error) {
        logger.error(`Error processing message: ${error.message}`);
        // Publish to DLT
      }
    },
  });
};

run().catch((e) => logger.error(`[example/consumer] ${e.message}`, e));
