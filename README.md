# Kafka Processor

This project is a Node.js application designed to process Kafka messages. It consumes messages from an input topic, processes them according to defined business logic, and publishes results to an output topic.

## Features

- **Kafka Integration**: Consumes and produces messages to Apache Kafka.
- **Event Processing**: Handles various event types including `APPLICATION_CREATED`, `HEALTH_QUESTIONS_COMPLETED`, and `APPLICATION_UPDATED`.
- **Data Normalization**: Normalizes data fields such as tobacco usage, currency strings, and calculates age.
- **Validation Rules**: Applies a series of validation rules including completeness, consent, financial checks, product age limits, and state product support.
- **State Management**: Maintains canonical state for applications using an in-memory repository.
- **Error Handling**: Implements mechanisms for handling invalid messages, duplicate events, and generating warnings/failures.

## Project Structure

```
.
├── Dockerfile
├── README.md
├── package.json
├── .gitignore
├── env_variables.json
└── src/
    ├── index.js                     # Main application entry point
    ├── kafka.js                     # Kafka consumer and producer setup
    ├── config.js                    # Application configuration
    ├── processor/
    │   ├── applicationProcessor.js  # Core business logic for processing events
    │   └── normalizer.js            # Data normalization logic
    ├── state/
    │   ├── StateRepository.js       # Interface for state management
    │   └── InMemoryStateRepository.js # In-memory state repository implementation
    ├── validation/
    │   ├── validator.js             # Orchestrates validation rules
    │   └── rules/
    │       ├── completeness.js      # Completeness validation rule
    │       ├── consent.js           # Consent validation rule
    │       ├── financial.js         # Financial validation rule
    │       ├── productAge.js        # Product age validation rule
    │       └── stateProduct.js      # State product validation rule
    └── utils/
        ├── number.js                # Utility functions for numbers
        ├── date.js                  # Utility functions for dates
        └── logger.js                # Logging utility
```

## Setup and Running

### Prerequisites

- Node.js (version 18 or later)
- Docker (for containerized deployment)
- Kafka cluster

### Environment Variables

Configure the application using the `env_variables.json` file or by setting environment variables directly.

| Variable Name                    | Description                                     | Type         | Mandatory | Default Value |
| :------------------------------- | :---------------------------------------------- | :----------- | :-------- | :------------ |
| `KAFKA_BROKERS`                  | Comma-separated list of Kafka broker addresses  | String       | Yes       |               |
| `KAFKA_INPUT_TOPIC`              | Kafka topic for input messages                  | Input Topic  | Yes       |               |
| `KAFKA_OUTPUT_TOPIC`             | Kafka topic for processed messages              | Output Topic | Yes       |               |
| `KAFKA_DEAD_LETTER_TOPIC`        | Kafka topic for messages that failed processing | Output Topic | No        | `dlt-topic`   |
| `KAFKA_GROUP_ID`                 | Consumer group ID                               | String       | Yes       |               |
| `LOG_LEVEL`                      | Application logging level                       | String       | No        | `info`        |
| `PRODUCT_MIN_AGE`                | Minimum allowed age for a product               | Integer      | No        | `18`          |
| `PRODUCT_MAX_AGE`                | Maximum allowed age for a product               | Integer      | No        | `65`          |
| `FINANCIAL_COVERAGE_RATIO_LIMIT` | Maximum allowed coverage income ratio           | Float        | No        | `5.0`         |

### Local Development

1.  **Install dependencies**:
    ```bash
    npm install
    ```
2.  **Run the application**:
    ```bash
    npm start
    ```

### Docker

1.  **Build the Docker image**:
    ```bash
    docker build -t kafka-processor .
    ```
2.  **Run the Docker container**:
    ```bash
    docker run -d --name kafka-processor \
      -e KAFKA_BROKERS="your_kafka_brokers" \
      -e KAFKA_INPUT_TOPIC="input-topic" \
      -e KAFKA_OUTPUT_TOPIC="output-topic" \
      -e KAFKA_GROUP_ID="my-group" \
      kafka-processor
    ```

## Testing

To run tests:

```bash
npm test
```
