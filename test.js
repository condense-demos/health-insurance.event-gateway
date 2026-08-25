const fastify = require('fastify');
const { Kafka } = require('kafkajs');
const pino = require('pino');

// Mock KafkaJS
const mockProducerSend = jest.fn();
const mockProducerConnect = jest.fn();
const mockProducerDisconnect = jest.fn();

// Create a spy for the Kafka constructor
const KafkaConstructorSpy = jest.fn(() => ({
  producer: jest.fn(() => ({
    connect: mockProducerConnect,
    disconnect: mockProducerDisconnect,
    send: mockProducerSend,
  })),
}));

jest.mock('kafkajs', () => ({
  Kafka: KafkaConstructorSpy,
}));

// Mock pino-pretty transport
jest.mock('pino', () => {
  const originalPino = jest.requireActual('pino');
  return jest.fn((options) => {
    const mockLogger = originalPino(options);
    mockLogger.info = jest.fn(); // Mock info method
    mockLogger.error = jest.fn(); // Mock error method
    return mockLogger;
  });
});

describe('HTTP to Kafka Gateway Service', () => {
  let server;
  let logger;
  let originalEnv;

  beforeAll(() => {
    originalEnv = process.env; // Save original env
  });

  beforeEach(async () => {
    // Reset process.env for each test
    process.env = { ...originalEnv };

    // Clear mocks before each test
    mockProducerSend.mockClear();
    mockProducerConnect.mockClear();
    mockProducerDisconnect.mockClear();
    pino.mockClear();
    KafkaConstructorSpy.mockClear();

    // Dynamically import server.js to reset its state for each test
    // This is important because fastify instance is created at module level
    jest.isolateModules(() => {
      server = require('./server');
      logger = pino(); // Get the mocked logger instance
    });
  });

  afterEach(async () => {
    if (server) {
      await server.close();
    }
  });

  afterAll(() => {
    process.env = originalEnv; // Restore original env
  });

  test('should accept a valid event and publish to Kafka', async () => {
    mockProducerSend.mockResolvedValueOnce(); // Kafka publish succeeds

    const event = {
      eventId: 'test-event-123',
      eventType: 'APPLICATION_CREATED',
      applicationId: 'APP-001',
      timestamp: new Date().toISOString(),
      payload: { key: 'value' },
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(202);
    expect(JSON.parse(response.payload)).toEqual({
      accepted: true,
      eventId: event.eventId,
      applicationId: event.applicationId,
      eventType: event.eventType,
    });
    expect(mockProducerSend).toHaveBeenCalledTimes(1);
    expect(mockProducerSend).toHaveBeenCalledWith({
      topic: process.env.KAFKA_TOPIC || 'insurance.application.events',
      messages: [
        {
          key: event.applicationId,
          value: JSON.stringify(event),
        },
      ],
    });
    expect(logger.info).toHaveBeenCalledWith(expect.anything(), 'Request received');
    expect(logger.info).toHaveBeenCalledWith(expect.anything(), 'Event published to Kafka.');
  });

  test('should return 400 if applicationId is missing', async () => {
    const event = {
      eventId: 'test-event-123',
      eventType: 'APPLICATION_CREATED',
      timestamp: new Date().toISOString(),
      payload: { key: 'value' },
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(400);
    expect(JSON.parse(response.payload)).toEqual({
      accepted: false,
      message: 'Validation failed: applicationId must not be empty.',
    });
    expect(mockProducerSend).not.toHaveBeenCalled();
  });

  test('should return 400 if eventType is missing', async () => {
    const event = {
      eventId: 'test-event-123',
      applicationId: 'APP-001',
      timestamp: new Date().toISOString(),
      payload: { key: 'value' },
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(400);
    expect(JSON.parse(response.payload)).toEqual({
      accepted: false,
      message: 'Validation failed: eventType must not be empty.',
    });
    expect(mockProducerSend).not.toHaveBeenCalled();
  });

  test('should return 400 if eventId is missing', async () => {
    const event = {
      eventType: 'APPLICATION_CREATED',
      applicationId: 'APP-001',
      timestamp: new Date().toISOString(),
      payload: { key: 'value' },
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(400);
    expect(JSON.parse(response.payload)).toEqual({
      accepted: false,
      message: 'Validation failed: eventId must not be empty.',
    });
    expect(mockProducerSend).not.toHaveBeenCalled();
  });

  test('should return 400 if timestamp is missing', async () => {
    const event = {
      eventId: 'test-event-123',
      eventType: 'APPLICATION_CREATED',
      applicationId: 'APP-001',
      payload: { key: 'value' },
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(400);
    expect(JSON.parse(response.payload)).toEqual({
      accepted: false,
      message: 'Validation failed: timestamp must not be empty.',
    });
    expect(mockProducerSend).not.toHaveBeenCalled();
  });

  test('should return 503 if Kafka publication fails', async () => {
    const kafkaError = new Error('Kafka connection failed');
    mockProducerSend.mockRejectedValueOnce(kafkaError); // Kafka publish fails

    const event = {
      eventId: 'test-event-456',
      eventType: 'APPLICATION_UPDATED',
      applicationId: 'APP-002',
      timestamp: new Date().toISOString(),
      payload: { status: 'approved' },
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(503);
    expect(JSON.parse(response.payload)).toEqual({
      accepted: false,
      message: 'Service unavailable: Failed to publish event to Kafka.',
      error: kafkaError.message,
    });
    expect(mockProducerSend).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(expect.anything(), 'Kafka publication failed.');
  });

  test('should default payload to an empty object if not provided', async () => {
    mockProducerSend.mockResolvedValueOnce();

    const event = {
      eventId: 'test-event-789',
      eventType: 'APPLICATION_DELETED',
      applicationId: 'APP-003',
      timestamp: new Date().toISOString(),
    };

    const response = await server.inject({
      method: 'POST',
      url: '/',
      payload: event,
      headers: { 'Content-Type': 'application/json' },
    });

    expect(response.statusCode).toBe(202);
    expect(mockProducerSend).toHaveBeenCalledTimes(1);
    const sentMessage = JSON.parse(mockProducerSend.mock.calls[0][0].messages[0].value);
    expect(sentMessage.payload).toEqual({});
  });

  // New test cases for Kafka authentication
  test('Kafka constructor should be called with SASL config when env vars are set', async () => {
    process.env.KAFKA_USERNAME = 'testuser';
    process.env.KAFKA_PASSWORD = 'testpassword';

    // Re-require server.js to pick up new env vars
    jest.isolateModules(() => {
      server = require('./server');
    });

    expect(KafkaConstructorSpy).toHaveBeenCalledTimes(1);
    const kafkaConfig = KafkaConstructorSpy.mock.calls[0][0];
    expect(kafkaConfig.sasl).toEqual({
      mechanism: 'scram-sha-512',
      username: 'testuser',
      password: 'testpassword',
    });
    expect(kafkaConfig.ssl).toBe(true);
  });

  test('Kafka constructor should be called without SASL config when env vars are not set', async () => {
    // Ensure username and password are not set
    delete process.env.KAFKA_USERNAME;
    delete process.env.KAFKA_PASSWORD;

    // Re-require server.js to pick up new env vars
    jest.isolateModules(() => {
      server = require('./server');
    });

    expect(KafkaConstructorSpy).toHaveBeenCalledTimes(1);
    const kafkaConfig = KafkaConstructorSpy.mock.calls[0][0];
    expect(kafkaConfig.sasl).toBeUndefined();
    expect(kafkaConfig.ssl).toBeUndefined();
  });
});
