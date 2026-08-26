const { run } = require('../src/index');
const kafka = require('../src/kafka');
const { processApplicationEvent } = require('../src/processor/applicationProcessor');
const logger = require('../src/utils/logger');

jest.mock('../src/kafka', () => ({
  consumer: {
    connect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn(),
  },
  producer: {
    connect: jest.fn(),
  },
  sendMessage: jest.fn(),
}));

jest.mock('../src/processor/applicationProcessor', () => ({
  processApplicationEvent: jest.fn(),
}));

jest.mock('../src/utils/logger', () => ({
  info: jest.fn(),
  error: jest.fn(),
}));

describe('Kafka Consumer', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    process.env.KAFKA_INPUT_TOPIC = 'input-topic';
    process.env.KAFKA_OUTPUT_TOPIC = 'output-topic';
    process.env.KAFKA_DLT_TOPIC = 'dlt-topic';
  });

  it('should process a message successfully and send to output topic', async () => {
    const mockMessage = { value: Buffer.from(JSON.stringify({ id: '123', data: 'test' })) };
    const mockProcessedResult = { id: '123', processedData: 'test_processed' };

    kafka.consumer.run.mockImplementationOnce(async ({ eachMessage }) => {
      await eachMessage({ topic: 'input-topic', partition: 0, message: mockMessage });
    });
    processApplicationEvent.mockResolvedValueOnce(mockProcessedResult);

    await run();

    expect(kafka.consumer.connect).toHaveBeenCalledTimes(1);
    expect(kafka.producer.connect).toHaveBeenCalledTimes(1);
    expect(kafka.consumer.subscribe).toHaveBeenCalledWith({
      topic: 'input-topic',
      fromBeginning: true,
    });
    expect(processApplicationEvent).toHaveBeenCalledWith({ id: '123', data: 'test' });
    expect(kafka.sendMessage).toHaveBeenCalledWith('output-topic', mockProcessedResult);
    expect(logger.info).toHaveBeenCalledWith(`Received message: ${JSON.stringify({ id: '123', data: 'test' })}`);
    expect(logger.info).toHaveBeenCalledTimes(1);
    expect(logger.error).not.toHaveBeenCalled();
    expect(kafka.sendMessage).toHaveBeenCalledTimes(1);
  });

  it('should handle errors during message processing and send to DLT', async () => {
    const mockMessage = { value: Buffer.from(JSON.stringify({ id: '123', data: 'test' })) };
    const mockError = new Error('Processing failed');

    kafka.consumer.run.mockImplementationOnce(async ({ eachMessage }) => {
      await eachMessage({ topic: 'input-topic', partition: 0, message: mockMessage });
    });
    processApplicationEvent.mockRejectedValueOnce(mockError);

    await run();

    expect(kafka.consumer.connect).toHaveBeenCalledTimes(1);
    expect(kafka.producer.connect).toHaveBeenCalledTimes(1);
    expect(kafka.consumer.subscribe).toHaveBeenCalledWith({
      topic: 'input-topic',
      fromBeginning: true,
    });
    expect(processApplicationEvent).toHaveBeenCalledWith({ id: '123', data: 'test' });
    expect(logger.error).toHaveBeenCalledWith(`Error processing message: ${mockError.message}`);
    expect(kafka.sendMessage).toHaveBeenCalledWith('dlt-topic', {
      error: mockError.message,
      originalMessage: { id: '123', data: 'test' },
    });
    expect(logger.info).toHaveBeenCalledWith(`Received message: ${JSON.stringify({ id: '123', data: 'test' })}`);
    expect(logger.info).toHaveBeenCalledTimes(1);
    expect(kafka.sendMessage).toHaveBeenCalledTimes(1);
  });
});
