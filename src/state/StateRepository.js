class StateRepository {
  async getApplicationState(applicationId) {
    throw new Error("Method not implemented");
  }

  async saveApplicationState(applicationId, state) {
    throw new Error("Method not implemented");
  }

  async isEventProcessed(eventId) {
    throw new Error("Method not implemented");
  }

  async addProcessedEvent(eventId) {
    throw new Error("Method not implemented");
  }
}

module.exports = StateRepository;
