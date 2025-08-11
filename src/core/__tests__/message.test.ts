import { MessageImpl } from '../message';
import { MessageStatus, ValidationError } from '../types';

describe('Message', () => {
  const TEST_TOPIC = 'test-topic';

  it('should create a message with default values', () => {
    const payload = 'test message';
    const message = new MessageImpl(payload, { topicName: TEST_TOPIC });

    expect(message.payload).toBe(payload);
    expect(message.id).toBeDefined();
    expect(message.topicName).toBe(TEST_TOPIC);
    expect(message.partition).toBe(0);
    expect(message.offset).toBe(0);
    expect(message.status).toBe(MessageStatus.PENDING);
    expect(message.timestamp).toBeLessThanOrEqual(Date.now());
  });

  it('should create a message with custom options', () => {
    const payload = 'test message';
    const options = {
      id: 'custom-id',
      topicName: TEST_TOPIC,
      partition: 1,
      offset: 5,
      timestamp: Date.now(),
      status: MessageStatus.ACKNOWLEDGED
    };
    
    const message = new MessageImpl(payload, options);
    
    expect(message.payload).toBe(payload);
    expect(message.id).toBe(options.id);
    expect(message.topicName).toBe(options.topicName);
    expect(message.partition).toBe(options.partition);
    expect(message.offset).toBe(options.offset);
    expect(message.status).toBe(options.status);
    expect(message.timestamp).toBe(options.timestamp);
  });

  it('should throw error for empty payload', () => {
    expect(() => new MessageImpl('', { topicName: TEST_TOPIC })).toThrow(ValidationError);
    expect(() => new MessageImpl('  ', { topicName: TEST_TOPIC })).toThrow(ValidationError);
  });

  it('should throw error for missing topic name', () => {
    expect(() => new MessageImpl('test message')).toThrow(ValidationError);
    expect(() => new MessageImpl('test message', { topicName: '' })).toThrow(ValidationError);
    expect(() => new MessageImpl('test message', { topicName: '   ' })).toThrow(ValidationError);
  });

  it('should handle message status changes correctly', () => {
    const message = new MessageImpl('test message', { topicName: TEST_TOPIC });
    
    expect(message.isPending()).toBe(true);
    expect(message.isAcknowledged()).toBe(false);
    expect(message.isFailed()).toBe(false);

    message.acknowledge();
    expect(message.isPending()).toBe(false);
    expect(message.isAcknowledged()).toBe(true);
    expect(message.isFailed()).toBe(false);

    message.fail();
    expect(message.isPending()).toBe(false);
    expect(message.isAcknowledged()).toBe(false);
    expect(message.isFailed()).toBe(true);
  });
});