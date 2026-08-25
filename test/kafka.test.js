const { Kafka } = require("kafkajs");

// Mock kafkajs to check constructor arguments
jest.mock("kafkajs", () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn(() => ({ connect: jest.fn(), disconnect: jest.fn() })),
    consumer: jest.fn(() => ({
      connect: jest.fn(),
      disconnect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn(),
    })),
  })),
}));

describe("Kafka Client Configuration", () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    jest.resetModules(); // Most important - it clears the cache
    process.env = { ...OLD_ENV }; // Make a copy
  });

  afterAll(() => {
    process.env = OLD_ENV; // Restore old environment
  });

  test("should initialize Kafka with correct SASL configuration", () => {
    process.env.KAFKA_BROKERS = "localhost:9092";
    process.env.KAFKA_SASL_USERNAME = "testuser";
    process.env.KAFKA_SASL_PASSWORD = "testpassword";

    require("../src/kafka");

    expect(Kafka).toHaveBeenCalledWith({
      clientId: "kafka-processor",
      brokers: ["localhost:9092"],
      ssl: true,
      sasl: {
        mechanism: "scram-sha-512",
        username: "testuser",
        password: "testpassword",
      },
    });
  });

  test("should initialize Kafka without SASL if environment variables are not set", () => {
    process.env.KAFKA_BROKERS = "localhost:9092";
    // Do not set KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD

    require("../src/kafka");

    expect(Kafka).toHaveBeenCalledWith({
      clientId: "kafka-processor",
      brokers: ["localhost:9092"],
      // ssl and sasl should not be present if not explicitly set by the config, but our current implementation sets ssl: true always. This might be a point of improvement if SSL should only be enabled with SASL.
      // For now, testing that sasl is not configured if env vars are missing.
      ssl: true, // As per current implementation in kafka.js
    });
    // Further assertion to ensure sasl property is undefined or not present if not configured.
    const callArgs = Kafka.mock.calls[0][0];
    expect(callArgs.sasl).toBeUndefined();
  });
});
