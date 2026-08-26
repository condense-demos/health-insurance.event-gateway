const { Kafka } = require("kafkajs");
const config = require("./config");
const logger = require("./utils/logger");

const kafka = new Kafka({
  clientId: "kafka-processor",
  brokers: config.kafka.brokers,
  ssl: false,
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_SASL_PASSWORD,
  },
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: config.kafka.groupId });

const connectProducer = async () => {
  await producer.connect();
  logger.info("Kafka Producer connected");
};

const disconnectProducer = async () => {
  await producer.disconnect();
  logger.info("Kafka Producer disconnected");
};

const connectConsumer = async () => {
  await consumer.connect();
  logger.info("Kafka Consumer connected");
};

const disconnectConsumer = async () => {
  await consumer.disconnect();
  logger.info("Kafka Consumer disconnected");
};

const sendMessage = async (topic, message) => {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
    logger.info(`Message sent to topic ${topic}: ${JSON.stringify(message)}`);
  } catch (error) {
    logger.error(`Error sending message to topic ${topic}: ${error.message}`);
    throw error;
  }
};

module.exports = {
  producer,
  consumer,
  connectProducer,
  disconnectProducer,
  connectConsumer,
  disconnectConsumer,
  sendMessage,
};
