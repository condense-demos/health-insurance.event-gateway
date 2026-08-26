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

        await kafka.sendMessage(process.env.KAFKA_OUTPUT_TOPIC, processedResult);
        // Commit offset
      } catch (error) {
        logger.error(`Error processing message: ${error.message}`);
        await kafka.sendMessage(process.env.KAFKA_DLT_TOPIC, { error: error.message, originalMessage: event });
      }
    },
  });
};

run().catch((e) => logger.error(`[example/consumer] ${e.message}`, e));
