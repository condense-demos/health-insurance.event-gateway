const normalizer = require("../processor/normalizer");
const validator = require("../validation/validator");
const StateRepository = require("../state/StateRepository");
const InMemoryStateRepository = require("../state/InMemoryStateRepository");
const logger = require("../utils/logger");

const stateRepository = new InMemoryStateRepository(); // Or inject a different implementation

const processApplicationEvent = async (event) => {
  logger.info(`Processing event with ID: ${event.eventId}`);

  // 14. duplicate eventId is ignored
  if (await stateRepository.isEventProcessed(event.eventId)) {
    logger.warn(`Duplicate event ID ${event.eventId} ignored.`);
    return null;
  }

  let applicationState = await stateRepository.getApplicationState(
    event.applicationId,
  );

  switch (event.eventType) {
    case "APPLICATION_CREATED":
      // 1. APPLICATION_CREATED creates canonical state
      applicationState = {
        ...event.payload,
        eventId: event.eventId,
        status: "CREATED",
      };
      break;
    case "HEALTH_QUESTIONS_COMPLETED":
      // 2. HEALTH_QUESTIONS_COMPLETED updates existing state
      if (!applicationState) {
        logger.error(
          `Application state not found for HEALTH_QUESTIONS_COMPLETED event for applicationId: ${event.applicationId}`,
        );
        return null; // Or handle as an error
      }
      applicationState = {
        ...applicationState,
        ...event.payload,
        eventId: event.eventId,
      };
      break;
    case "APPLICATION_UPDATED":
      // 3. APPLICATION_UPDATED performs partial merge
      if (!applicationState) {
        logger.error(
          `Application state not found for APPLICATION_UPDATED event for applicationId: ${event.applicationId}`,
        );
        return null; // Or handle as an error
      }
      applicationState = {
        ...applicationState,
        ...event.payload,
        eventId: event.eventId,
      };
      break;
    default:
      logger.warn(`Unknown event type: ${event.eventType}`);
      return null;
  }

  // Apply normalizations
  applicationState = normalizer.normalizeTobacco(applicationState);
  applicationState = normalizer.normalizeCurrencyString(applicationState);
  applicationState = normalizer.calculateAge(applicationState);

  // Validate the state
  const validationResult = validator.validate(applicationState);

  // Update state with validation results and save
  applicationState.validation = validationResult;
  await stateRepository.saveApplicationState(
    event.applicationId,
    applicationState,
  );
  await stateRepository.addProcessedEvent(event.eventId);

  // 15. one processed-case message is generated per valid application event
  return { ...applicationState, processedEventId: event.eventId };
};

module.exports = {
  processApplicationEvent,
};
