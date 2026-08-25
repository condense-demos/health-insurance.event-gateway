const StateRepository = require("./StateRepository");

class InMemoryStateRepository extends StateRepository {
  constructor() {
    super();
    this.applicationStates = new Map();
    this.processedEvents = new Set();
  }

  async getApplicationState(applicationId) {
    return this.applicationStates.get(applicationId);
  }

  async saveApplicationState(applicationId, state) {
    this.applicationStates.set(applicationId, state);
  }

  async isEventProcessed(eventId) {
    return this.processedEvents.has(eventId);
  }

  async addProcessedEvent(eventId) {
    this.processedEvents.add(eventId);
  }
}

module.exports = InMemoryStateRepository;
