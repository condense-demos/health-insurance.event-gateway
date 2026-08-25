const {
  processApplicationEvent,
} = require("../src/processor/applicationProcessor");
const StateRepository = require("../src/state/StateRepository");
const normalizer = require("../src/processor/normalizer");
const validator = require("../src/validation/validator");
const logger = require("../src/utils/logger");

// Mock dependencies
jest.mock("../src/state/StateRepository");
jest.mock("../src/processor/normalizer");
jest.mock("../src/validation/validator");
jest.mock("../src/utils/logger");

describe("applicationProcessor", () => {
  let mockStateRepository;
  let mockNormalizer;
  let mockValidator;
  let mockLogger;

  beforeEach(() => {
    // Reset mocks before each test
    StateRepository.mockClear();
    normalizer.normalizeTobacco.mockClear();
    normalizer.normalizeCurrencyString.mockClear();
    normalizer.calculateAge.mockClear();
    validator.validate.mockClear();
    logger.info.mockClear();
    logger.error.mockClear();
    logger.warn.mockClear();

    // Setup mock instances for each test
    mockStateRepository = {
      isEventProcessed: jest.fn(),
      getApplicationState: jest.fn(),
      saveApplicationState: jest.fn(),
      addProcessedEvent: jest.fn(),
    };
    // Mock the constructor to return our mock instance
    require("../src/state/InMemoryStateRepository").mockImplementation(
      () => mockStateRepository,
    );

    mockNormalizer = {
      normalizeTobacco: jest.fn((app) => app),
      normalizeCurrencyString: jest.fn((app) => app),
      calculateAge: jest.fn((app) => app),
    };
    Object.assign(normalizer, mockNormalizer);

    mockValidator = {
      validate: jest.fn(() => ({ status: "PASS", messages: [] })),
    };
    Object.assign(validator, mockValidator);

    mockLogger = {
      info: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
    };
    Object.assign(logger, mockLogger);
  });

  it("should process APPLICATION_CREATED event and create new state", async () => {
    const event = {
      eventId: "event1",
      applicationId: "app1",
      eventType: "APPLICATION_CREATED",
      payload: { name: "Test App" },
    };

    mockStateRepository.isEventProcessed.mockResolvedValue(false);
    mockStateRepository.getApplicationState.mockResolvedValue(null);

    const result = await processApplicationEvent(event);

    expect(mockStateRepository.isEventProcessed).toHaveBeenCalledWith("event1");
    expect(mockStateRepository.getApplicationState).toHaveBeenCalledWith(
      "app1",
    );
    expect(normalizer.normalizeTobacco).toHaveBeenCalled();
    expect(normalizer.normalizeCurrencyString).toHaveBeenCalled();
    expect(normalizer.calculateAge).toHaveBeenCalled();
    expect(validator.validate).toHaveBeenCalled();
    expect(mockStateRepository.saveApplicationState).toHaveBeenCalledWith(
      "app1",
      expect.objectContaining({
        name: "Test App",
        eventId: "event1",
        status: "CREATED",
        validation: { status: "PASS", messages: [] },
      }),
    );
    expect(mockStateRepository.addProcessedEvent).toHaveBeenCalledWith(
      "event1",
    );
    expect(result).toEqual(
      expect.objectContaining({
        name: "Test App",
        eventId: "event1",
        status: "CREATED",
        validation: { status: "PASS", messages: [] },
        processedEventId: "event1",
      }),
    );
  });

  it("should process HEALTH_QUESTIONS_COMPLETED event and update existing state", async () => {
    const initialApplicationState = {
      applicationId: "app1",
      name: "Test App",
      status: "CREATED",
      healthQuestions: [],
    };
    const event = {
      eventId: "event2",
      applicationId: "app1",
      eventType: "HEALTH_QUESTIONS_COMPLETED",
      payload: { healthQuestions: ["Q1", "Q2"] },
    };

    mockStateRepository.isEventProcessed.mockResolvedValue(false);
    mockStateRepository.getApplicationState.mockResolvedValue(
      initialApplicationState,
    );

    const result = await processApplicationEvent(event);

    expect(mockStateRepository.isEventProcessed).toHaveBeenCalledWith("event2");
    expect(mockStateRepository.getApplicationState).toHaveBeenCalledWith(
      "app1",
    );
    expect(normalizer.normalizeTobacco).toHaveBeenCalled();
    expect(normalizer.normalizeCurrencyString).toHaveBeenCalled();
    expect(normalizer.calculateAge).toHaveBeenCalled();
    expect(validator.validate).toHaveBeenCalled();
    expect(mockStateRepository.saveApplicationState).toHaveBeenCalledWith(
      "app1",
      expect.objectContaining({
        name: "Test App",
        eventId: "event2",
        status: "CREATED",
        healthQuestions: ["Q1", "Q2"],
        validation: { status: "PASS", messages: [] },
      }),
    );
    expect(mockStateRepository.addProcessedEvent).toHaveBeenCalledWith(
      "event2",
    );
    expect(result).toEqual(
      expect.objectContaining({
        name: "Test App",
        eventId: "event2",
        status: "CREATED",
        healthQuestions: ["Q1", "Q2"],
        validation: { status: "PASS", messages: [] },
        processedEventId: "event2",
      }),
    );
  });

  it("should process APPLICATION_UPDATED event and partially merge into existing state", async () => {
    const initialApplicationState = {
      applicationId: "app1",
      name: "Test App",
      status: "CREATED",
      address: "123 Main St",
    };
    const event = {
      eventId: "event3",
      applicationId: "app1",
      eventType: "APPLICATION_UPDATED",
      payload: { address: "456 Oak Ave", city: "New City" },
    };

    mockStateRepository.isEventProcessed.mockResolvedValue(false);
    mockStateRepository.getApplicationState.mockResolvedValue(
      initialApplicationState,
    );

    const result = await processApplicationEvent(event);

    expect(mockStateRepository.isEventProcessed).toHaveBeenCalledWith("event3");
    expect(mockStateRepository.getApplicationState).toHaveBeenCalledWith(
      "app1",
    );
    expect(normalizer.normalizeTobacco).toHaveBeenCalled();
    expect(normalizer.normalizeCurrencyString).toHaveBeenCalled();
    expect(normalizer.calculateAge).toHaveBeenCalled();
    expect(validator.validate).toHaveBeenCalled();
    expect(mockStateRepository.saveApplicationState).toHaveBeenCalledWith(
      "app1",
      expect.objectContaining({
        name: "Test App",
        eventId: "event3",
        status: "CREATED",
        address: "456 Oak Ave",
        city: "New City",
        validation: { status: "PASS", messages: [] },
      }),
    );
    expect(mockStateRepository.addProcessedEvent).toHaveBeenCalledWith(
      "event3",
    );
    expect(result).toEqual(
      expect.objectContaining({
        name: "Test App",
        eventId: "event3",
        status: "CREATED",
        address: "456 Oak Ave",
        city: "New City",
        validation: { status: "PASS", messages: [] },
        processedEventId: "event3",
      }),
    );
  });

  it("should ignore duplicate events", async () => {
    const event = {
      eventId: "event4",
      applicationId: "app2",
      eventType: "APPLICATION_CREATED",
      payload: { name: "Duplicate App" },
    };

    mockStateRepository.isEventProcessed.mockResolvedValue(true);

    const result = await processApplicationEvent(event);

    expect(mockStateRepository.isEventProcessed).toHaveBeenCalledWith("event4");
    expect(mockLogger.warn).toHaveBeenCalledWith(
      "Duplicate event ID event4 ignored.",
    );
    expect(mockStateRepository.getApplicationState).not.toHaveBeenCalled();
    expect(normalizer.normalizeTobacco).not.toHaveBeenCalled();
    expect(normalizer.normalizeCurrencyString).not.toHaveBeenCalled();
    expect(normalizer.calculateAge).not.toHaveBeenCalled();
    expect(validator.validate).not.toHaveBeenCalled();
    expect(mockStateRepository.saveApplicationState).not.toHaveBeenCalled();
    expect(mockStateRepository.addProcessedEvent).not.toHaveBeenCalled();
    expect(result).toBeNull();
  });

  it("should return null for unknown event types", async () => {
    const event = {
      eventId: "event5",
      applicationId: "app3",
      eventType: "UNKNOWN_EVENT",
      payload: { data: "some data" },
    };

    mockStateRepository.isEventProcessed.mockResolvedValue(false);

    const result = await processApplicationEvent(event);

    expect(mockStateRepository.isEventProcessed).toHaveBeenCalledWith("event5");
    expect(mockLogger.warn).toHaveBeenCalledWith(
      "Unknown event type: UNKNOWN_EVENT",
    );
    expect(result).toBeNull();
  });

  it("should log error and return null if HEALTH_QUESTIONS_COMPLETED event for non-existent application", async () => {
    const event = {
      eventId: "event6",
      applicationId: "app4",
      eventType: "HEALTH_QUESTIONS_COMPLETED",
      payload: { healthQuestions: ["Q1"] },
    };

    mockStateRepository.isEventProcessed.mockResolvedValue(false);
    mockStateRepository.getApplicationState.mockResolvedValue(null);

    const result = await processApplicationEvent(event);

    expect(mockStateRepository.isEventProcessed).toHaveBeenCalledWith("event6");
    expect(mockStateRepository.getApplicationState).toHaveBeenCalledWith(
      "app4",
    );
    expect(mockLogger.error).toHaveBeenCalledWith(
      "Application state not found for HEALTH_QUESTIONS_COMPLETED event for applicationId: app4",
    );
    expect(result).toBeNull();
  });
});
