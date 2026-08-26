# Walkthrough: Adding Kafka Producer Logic

This walkthrough details the changes made to introduce Kafka message publishing capabilities to the application.

## 1. `src/kafka.js`

- A new asynchronous function `sendMessage(topic, message)` was added to handle publishing messages to a Kafka topic. This function stringifies the message to JSON before sending.
- Error handling was included in `sendMessage` to catch and log any errors during the `producer.send` operation.
- The `sendMessage` function was added to `module.exports` to make it accessible to other parts of the application.

```javascript
// ...existing code...

const sendMessage = async (topic, message) => {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
    logger.info(`Message sent to topic ${topic}: ${JSON.stringify(message)}`);
  } catch (error) {
    logger.error(`Error sending message to topic ${topic}: ${error.message}`);
    throw error;
  }
};

module.exports = {
  producer,
  consumer,
  connectProducer,
  disconnectProducer,
  connectConsumer,
  disconnectConsumer,
  sendMessage,
};
```

## 2. `test/kafka.test.js`

- The `test/kafka.test.js` file was updated to include a new test suite for the `sendMessage` function.
- Mocking for `producer.send` was added to the `kafkajs` mock.
- Test cases were created to verify:
  - Successful message sending: Ensures `producer.send` is called with the correct arguments and `logger.info` is invoked.
  - Error handling: Verifies that errors during `producer.send` are caught, logged by `logger.error`, and re-thrown.

```javascript
// ...existing Kafka Client Configuration tests...

describe("Kafka Producer", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    logger.info = jest.fn();
    logger.error = jest.fn();
  });

  test("sendMessage should send a message to the specified topic", async () => {
    const topic = "test-topic";
    const message = { id: 1, value: "test" };
    producer.send.mockResolvedValueOnce();

    await sendMessage(topic, message);

    expect(producer.send).toHaveBeenCalledWith({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
    expect(logger.info).toHaveBeenCalledWith(
      `Message sent to topic ${topic}: ${JSON.stringify(message)}`,
    );
  });

  test("sendMessage should log an error and re-throw if sending fails", async () => {
    const topic = "test-topic";
    const message = { id: 1, value: "test" };
    const errorMessage = "Failed to send message";
    producer.send.mockRejectedValueOnce(new Error(errorMessage));

    await expect(sendMessage(topic, message)).rejects.toThrow(errorMessage);
    expect(logger.error).toHaveBeenCalledWith(
      `Error sending message to topic ${topic}: ${errorMessage}`,
    );
  });
});
```

## How to run tests

To run the tests, execute the following command in your terminal:

```bash
npx jest test/kafka.test.js
```

# Walkthrough: Kafka Producer Logic Integration

This walkthrough details the changes made to integrate Kafka producer logic into the `index.js` file, along with necessary environment variable updates and test cases.

## 1. Modifications in `src/index.js`

The `src/index.js` file, which contains the main Kafka consumer logic, was updated to include calls to the `kafka.sendMessage` function for producing data to Kafka topics.

- **Output Topic Integration (Line 22 equivalent):**
  After an application event is successfully processed by `processApplicationEvent`, the `processedResult` is now sent to the `KAFKA_OUTPUT_TOPIC`.

  ```javascript
          const processedResult = await processApplicationEvent(event);

          await kafka.sendMessage(process.env.KAFKA_OUTPUT_TOPIC, processedResult);
          // Commit offset
  ```

- **Dead Letter Topic (DLT) Integration (Line 26 equivalent):**
  In the event of an error during message processing within the `try-catch` block, the error message and the original event are now sent to the `KAFKA_DLT_TOPIC`.

  ```javascript
          logger.error(`Error processing message: ${error.message}`);
          await kafka.sendMessage(process.env.KAFKA_DLT_TOPIC, { error: error.message, originalMessage: event });
  ```

## 2. `env_variables.json` Update

The `env_variables.json` file was updated to align the environment variable name used in the code for the Dead Letter Topic. `KAFKA_DEAD_LETTER_TOPIC` was renamed to `KAFKA_DLT_TOPIC`.

## 3. Test File (`test/index.test.js`)

A new test file, `test/index.test.js`, was created to ensure the new Kafka producer logic functions as expected. This file uses Jest for testing and mocks the Kafka consumer/producer, application processor, and logger.

The test cases cover:
- **Successful Processing:** Verifies that a message is correctly processed and the result is sent to the `KAFKA_OUTPUT_TOPIC`.
- **Error Handling with DLT:** Verifies that if an error occurs during processing, the error details and the original message are sent to the `KAFKA_DLT_TOPIC`.

To run these tests, you would typically use the command:

```bash
npx jest test/index.test.js
```

