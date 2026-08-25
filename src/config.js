const config = {
  kafka: {
    brokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(",")
      : [],
    inputTopic: process.env.KAFKA_INPUT_TOPIC,
    outputTopic: process.env.KAFKA_OUTPUT_TOPIC,
    deadLetterTopic: process.env.KAFKA_DEAD_LETTER_TOPIC || "dlt-topic",
    groupId: process.env.KAFKA_GROUP_ID,
  },
  product: {
    minAge: parseInt(process.env.PRODUCT_MIN_AGE || "18", 10),
    maxAge: parseInt(process.env.PRODUCT_MAX_AGE || "65", 10),
  },
  financial: {
    coverageIncomeRatioLimit: parseFloat(
      process.env.FINANCIAL_COVERAGE_RATIO_LIMIT || "5.0",
    ),
  },
  logLevel: process.env.LOG_LEVEL || "info",
};

module.exports = config;
