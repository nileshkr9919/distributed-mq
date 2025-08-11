import { Queue } from '../queue';
import { MessageImpl } from '../message';
import { MessageStatus } from '../types';

describe('Queue', () => {
  let queue: Queue;
  const TEST_TOPIC = 'test-topic';

  beforeEach(() => {
    queue = new Queue();
    queue.createTopic({ name: TEST_TOPIC, partitions: 2 });
  });

  it('should enqueue and dequeue messages in FIFO order', () => {
    const msg1 = new MessageImpl('first message', { topicName: TEST_TOPIC });
    const msg2 = new MessageImpl('second message', { topicName: TEST_TOPIC });
    
    queue.enqueue(msg1);
    queue.enqueue(msg2);
    
    expect(queue.dequeue(TEST_TOPIC)).toBe(msg1);
    expect(queue.dequeue(TEST_TOPIC)).toBe(msg2);
    expect(queue.dequeue(TEST_TOPIC)).toBeUndefined();
  });

  it('should handle multiple partitions correctly', () => {
    const msg1 = new MessageImpl('partition 0 message', { topicName: TEST_TOPIC, partition: 0 });
    const msg2 = new MessageImpl('partition 1 message', { topicName: TEST_TOPIC, partition: 1 });
    const msg3 = new MessageImpl('another partition 1 message', { topicName: TEST_TOPIC, partition: 1 });
    
    queue.enqueue(msg1);
    queue.enqueue(msg2);
    queue.enqueue(msg3);
    
    expect(queue.getPartitions(TEST_TOPIC)).toEqual([0, 1]);
    expect(queue.getPartitionSize(TEST_TOPIC, 0)).toBe(1);
    expect(queue.getPartitionSize(TEST_TOPIC, 1)).toBe(2);
    
    expect(queue.dequeue(TEST_TOPIC, 1)).toBe(msg2);
    expect(queue.getPartitionSize(TEST_TOPIC, 1)).toBe(1);
  });

  it('should assign correct offsets to messages', () => {
    const msg1 = new MessageImpl('first message', { topicName: TEST_TOPIC, partition: 0 });
    const msg2 = new MessageImpl('second message', { topicName: TEST_TOPIC, partition: 0 });
    const msg3 = new MessageImpl('message in partition 1', { topicName: TEST_TOPIC, partition: 1 });
    
    queue.enqueue(msg1);
    queue.enqueue(msg2);
    queue.enqueue(msg3);
    
    expect(msg1.offset).toBe(0);
    expect(msg2.offset).toBe(1);
    expect(msg3.offset).toBe(0); // First message in partition 1
  });

  it('should handle message acknowledgment', () => {
    const msg = new MessageImpl('test message', { topicName: TEST_TOPIC });
    queue.enqueue(msg);
    
    queue.acknowledgeMessage(msg.id);
    expect(msg.isAcknowledged()).toBe(true);
  });

  it('should provide correct queue statistics', () => {
    const msg1 = new MessageImpl('pending message', { topicName: TEST_TOPIC, partition: 0 });
    const msg2 = new MessageImpl('acknowledged message', { topicName: TEST_TOPIC, partition: 0 });
    const msg3 = new MessageImpl('failed message', { topicName: TEST_TOPIC, partition: 1 });
    
    queue.enqueue(msg1);
    queue.enqueue(msg2);
    queue.enqueue(msg3);
    
    queue.acknowledgeMessage(msg2.id);
    msg3.fail();
    
    const stats = queue.getStats();
    expect(stats.totalMessages).toBe(3);
    expect(stats.acknowledgedMessages).toBe(1);
    expect(stats.pendingMessages).toBe(1);
    expect(stats.failedMessages).toBe(1);

    const topicStats = stats.messagesByTopic.get(TEST_TOPIC)!;
    expect(topicStats.get(0)).toBe(2);
    expect(topicStats.get(1)).toBe(1);
  });

  it('should handle peek operation correctly', () => {
    const msg1 = new MessageImpl('first message', { topicName: TEST_TOPIC });
    const msg2 = new MessageImpl('second message', { topicName: TEST_TOPIC });
    
    queue.enqueue(msg1);
    queue.enqueue(msg2);
    
    expect(queue.peek(TEST_TOPIC)).toBe(msg1);
    expect(queue.getPartitionSize(TEST_TOPIC, 0)).toBe(2); // Peek shouldn't remove the message
    
    queue.dequeue(TEST_TOPIC);
    expect(queue.peek(TEST_TOPIC)).toBe(msg2);
  });

  it('should clear all messages and reset state', () => {
    const msg1 = new MessageImpl('message 1', { topicName: TEST_TOPIC, partition: 0 });
    const msg2 = new MessageImpl('message 2', { topicName: TEST_TOPIC, partition: 1 });
    
    queue.enqueue(msg1);
    queue.enqueue(msg2);
    
    queue.clear();
    
    expect(queue.isEmpty()).toBe(true);
    expect(queue.getTopics()).toHaveLength(0);
    expect(queue.getPartitionSize(TEST_TOPIC, 0)).toBe(0);
    expect(queue.getPartitionSize(TEST_TOPIC, 1)).toBe(0);
  });

  it('should manage topics correctly', () => {
    const topic1 = queue.createTopic({ name: 'topic1', partitions: 1 });
    const topic2 = queue.createTopic({ name: 'topic2', partitions: 2 });

    expect(queue.getTopics()).toContain('topic1');
    expect(queue.getTopics()).toContain('topic2');
    expect(queue.getTopic('topic1')).toBe(topic1);
    expect(queue.getPartitions('topic1')).toEqual([0]);
    expect(queue.getPartitions('topic2')).toEqual([0, 1]);
  });
});