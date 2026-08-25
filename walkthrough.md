### Project Changes Walkthrough

This document outlines the changes made to the project during this session.

#### 1. `.env` File Creation

A `.env` file was created in the root of the workspace (`/home/condense/workspace/.env`). This file was generated based on the `env_variables.json` file, extracting each environment variable and its `default_value`.

The `.env` file is used to set environment variables for local development, allowing the application to be configured without modifying the code directly. Each line in the `.env` file corresponds to an environment variable in the format `KEY=VALUE`.

**File Path:** `/home/condense/workspace/.env`

**Content:**

```
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=insurance.application.events
KAFKA_CLIENT_ID=http-to-kafka-gateway
FASTIFY_PORT=3000
LOG_LEVEL=info
KAFKA_USERNAME=
KAFKA_PASSWORD=
```

#### 2. Kafka SASL-SCRAM-512 Authentication

The `server.js` file was modified to include logic for Kafka SASL-SCRAM-512 authentication. This involved:

- Adding `KAFKA_USERNAME` and `KAFKA_PASSWORD` to `env_variables.json` as new environment variables.
- Modifying the `Kafka` constructor in `server.js` to conditionally configure SASL and SSL based on the presence of `KAFKA_USERNAME` and `KAFKA_PASSWORD` environment variables.

#### 3. `.env` File Loading with `dotenv`

To ensure that the environment variables defined in `.env` are loaded into `process.env` at runtime, the `dotenv` package was integrated:

- `require('dotenv').config();` was added to the very top of `server.js`.
- The `dotenv` package was added as a dependency in `package.json`.
- `npm install` was run to install the new dependency.

This ensures that the application can correctly read configuration from the `.env` file.

#### 4. Updated Test File (`test.js`)

The `test.js` file was updated to include new test cases for Kafka authentication. The `jest.mock('kafkajs')` was modified to spy on the `Kafka` constructor, and tests were added to verify that:

- The `Kafka` constructor is called with `sasl` and `ssl: true` when `KAFKA_USERNAME` and `KAFKA_PASSWORD` environment variables are set.
- The `Kafka` constructor is called without `sasl` and `ssl: true` when these environment variables are not set.
