# HTTP to Kafka Gateway Service

This service acts as an HTTP gateway to publish events to a Kafka topic. It's built with Node.js, Fastify, and KafkaJS.

## Features

- Receives HTTP POST requests with a specific JSON envelope.
- Validates incoming requests.
- Publishes the event payload to a Kafka topic, using `applicationId` as the Kafka message key.
- Provides structured logging using Pino.
- Implements graceful shutdown for both the Fastify server and Kafka producer.

## Getting Started

### Prerequisites

- Node.js (v18 or higher)
- Docker (for containerized deployment)
- Kafka cluster accessible from the environment

### Environment Variables

The service is configured using environment variables:

- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses (e.g., `localhost:9092`). Default: `localhost:9092`
- `KAFKA_TOPIC`: The Kafka topic to publish events to. Default: `insurance.application.events`
- `KAFKA_CLIENT_ID`: Client ID for the Kafka producer. Default: `https://github.com/condense-demos/health-insurance.event-gateway`
- `FASTIFY_PORT`: The port on which the Fastify server will listen. Default: `3000`
- `LOG_LEVEL`: Logging level (e.g., `info`, `debug`, `error`). Default: `info`

### Installation (Local)

1. Clone the repository:

   ```bash
   git clone <repository_url>
   cd https://github.com/condense-demos/health-insurance.event-gateway
   ```

2. Install dependencies:

   ```bash
   npm install
   ```

3. Run the service:
   ```bash
   npm start
   ```

### Docker

1. Build the Docker image:

   ```bash
   docker build -t https://github.com/condense-demos/health-insurance.event-gateway .
   ```

2. Run the Docker container (replace with your Kafka broker details):
   ```bash
   docker run -p 3000:3000 -e KAFKA_BROKERS=your_kafka_broker:9092 -e KAFKA_TOPIC=your_kafka_topic https://github.com/condense-demos/health-insurance.event-gateway
   ```

## API Endpoint

### POST /

Publishes an event to Kafka.

**Request Body (JSON Envelope):**

```json
{
  "eventId": "<uuid>",
  "eventType": "<request eventType>",
  "applicationId": "<path applicationId>",
  "timestamp": "<UTC ISO timestamp>",
  "payload": {...}
}
```

**Validation:**

- `applicationId` must not be empty.
- `eventType` must not be empty.
- `eventId` must not be empty.
- `timestamp` must not be empty.
- `payload` defaults to `{}` if not provided.

**Responses:**

- **202 Accepted:** Event successfully accepted for publishing.

  ```json
  {
    "accepted": true,
    "eventId": "...",
    "applicationId": "...",
    "eventType": "..."
  }
  ```

- **400 Bad Request:** Malformed request or validation failure.

  ```json
  {
    "accepted": false,
    "message": "Validation failed: ..."
  }
  ```

- **503 Service Unavailable:** Failed to publish event to Kafka.
  ```json
  {
    "accepted": false,
    "message": "Service unavailable: ...",
    "error": "..."
  }
  ```

## Example `curl` command:

```bash
curl -X POST -H "Content-Type: application/json" -d \
'{ "eventId": "$(uuidgen)", "eventType": "APPLICATION_CREATED", "applicationId": "APP-10482", "timestamp": "$(date -u "+%Y-%m-%dT%H:%M:%SZ")", "payload": { "applicant": "Jane Smith", "age": 52, "product": "20_YEAR_TERM", "faceAmount": 1000000, "income": 150000, "tobacco": "NON_SMOKER", "state": "IL", "consentReceived": true, "healthQuestionsComplete": false } }' \
http://localhost:3000/
```
