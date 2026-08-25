require("dotenv").config();
const fastify = require("fastify");
const { Kafka } = require("kafkajs");
const pino = require("pino");

// --- Configuration from Environment Variables ---
const KAFKA_BROKERS = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",")
  : ["localhost:9092"];
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "insurance.application.events";
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "http-to-kafka-gateway";
const FASTIFY_PORT = parseInt(process.env.portNumber || "3000", 10);
const LOG_LEVEL = process.env.LOG_LEVEL || "info";

// --- Logger Setup ---
const logger = pino({
  level: LOG_LEVEL,
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
      ignore: "pid,hostname",
    },
  },
});

const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;

// --- Kafka Setup ---
const kafkaConfig = {
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
    factor: 2,
    multiplier: 1.5,
    restartOnFailure: async () => true,
  },
};

if (KAFKA_USERNAME && KAFKA_PASSWORD) {
  kafkaConfig.sasl = {
    mechanism: "scram-sha-512",
    username: KAFKA_USERNAME,
    password: KAFKA_PASSWORD,
  };
  kafkaConfig.ssl = false;
}

const kafka = new Kafka(kafkaConfig);

const producer = kafka.producer();

// --- Fastify Server Setup ---
const server = fastify({
  logger: logger,
});

// --- Graceful Shutdown Handler ---
const shutdown = async (signal) => {
  server.log.info(`Received signal ${signal}. Starting graceful shutdown...`);
  try {
    await producer.disconnect();
    server.log.info("Kafka producer disconnected.");
    await server.close();
    server.log.info("Fastify server closed.");
    process.exit(0);
  } catch (error) {
    server.log.error(`Error during shutdown: ${error.message}`);
    process.exit(1);
  }
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// --- Routes ---
server.post("/", async (request, reply) => {
  const { eventId, eventType, applicationId, timestamp, payload } =
    request.body;

  server.log.info({ eventId, eventType, applicationId }, "Request received");

  // Validation
  if (!applicationId) {
    return reply.status(400).send({
      accepted: false,
      message: "Validation failed: applicationId must not be empty.",
    });
  }
  if (!eventType) {
    return reply.status(400).send({
      accepted: false,
      message: "Validation failed: eventType must not be empty.",
    });
  }
  if (!eventId) {
    return reply.status(400).send({
      accepted: false,
      message: "Validation failed: eventId must not be empty.",
    });
  }
  if (!timestamp) {
    return reply.status(400).send({
      accepted: false,
      message: "Validation failed: timestamp must not be empty.",
    });
  }

  const messagePayload = payload === undefined ? {} : payload;

  try {
    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [
        {
          key: applicationId,
          value: JSON.stringify({
            eventId,
            eventType,
            applicationId,
            timestamp,
            payload: messagePayload,
          }),
        },
      ],
    });

    server.log.info(
      { eventId, eventType, applicationId },
      "Event published to Kafka.",
    );

    reply.status(202).send({
      accepted: true,
      eventId,
      applicationId,
      eventType,
    });
  } catch (error) {
    server.log.error(
      { error: error.message, eventId, eventType, applicationId },
      "Kafka publication failed.",
    );
    reply.status(503).send({
      accepted: false,
      message: "Service unavailable: Failed to publish event to Kafka.",
      error: error.message,
    });
  }
});

// --- Server Start ---
const start = async () => {
  try {
    await producer.connect();
    server.log.info("Kafka producer connected.");

    await server.listen({ port: FASTIFY_PORT, host: "0.0.0.0" });
    server.log.info(`Server listening on ${server.server.address().port}`);
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
};

start();
