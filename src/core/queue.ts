import { Message, MessageImpl } from './message';
import { Topic } from './topic';
import { QueueError, TopicError, TopicOptions } from './types';

export interface QueueStats {
  totalMessages: number;
  messagesByTopic: Map<string, Map<number, number>>;
  acknowledgedMessages: number;
  pendingMessages: number;
  failedMessages: number;
}

export class Queue {
  private readonly topics: Map<string, Topic>;
  private readonly messages: Map<string, Map<number, Message[]>>;
  private readonly offsets: Map<string, Map<number, number>>;
  
  constructor() {
    this.topics = new Map();
    this.messages = new Map();
    this.offsets = new Map();
  }

  createTopic(options: TopicOptions): Topic {
    const { name } = options;
    if (this.topics.has(name)) {
      throw new TopicError(`Topic ${name} already exists`);
    }

    const topic = new Topic(options);
    this.topics.set(name, topic);
    this.messages.set(name, new Map());
    this.offsets.set(name, new Map());
    
    return topic;
  }

  getTopic(name: string): Topic {
    const topic = this.topics.get(name);
    if (!topic) {
      throw new TopicError(`Topic ${name} does not exist`);
    }
    return topic;
  }

  private ensureTopicPartition(topicName: string, partition: number): void {
    const topic = this.getTopic(topicName);
    topic.validatePartition(partition);

    const topicMessages = this.messages.get(topicName)!;
    const topicOffsets = this.offsets.get(topicName)!;

    if (!topicMessages.has(partition)) {
      topicMessages.set(partition, []);
      topicOffsets.set(partition, 0);
    }
  }

  enqueue(message: Message): void {
    this.ensureTopicPartition(message.topicName, message.partition);

    const topicMessages = this.messages.get(message.topicName)!;
    const topicOffsets = this.offsets.get(message.topicName)!;
    
    const partitionMessages = topicMessages.get(message.partition)!;
    const currentOffset = topicOffsets.get(message.partition)!;
    
    message.offset = currentOffset;
    partitionMessages.push(message);
    topicOffsets.set(message.partition, currentOffset + 1);
  }

  dequeue(topicName: string, partition: number = 0): Message | undefined {
    this.ensureTopicPartition(topicName, partition);
    
    const topicMessages = this.messages.get(topicName)!;
    const partitionMessages = topicMessages.get(partition)!;
    
    if (partitionMessages.length === 0) {
      return undefined;
    }

    return partitionMessages.shift();
  }

  peek(topicName: string, partition: number = 0): Message | undefined {
    this.ensureTopicPartition(topicName, partition);
    
    const topicMessages = this.messages.get(topicName)!;
    const partitionMessages = topicMessages.get(partition)!;
    
    if (partitionMessages.length === 0) {
      return undefined;
    }

    return partitionMessages[0];
  }

  acknowledgeMessage(messageId: string): void {
    for (const [topicName, topicMessages] of this.messages) {
      for (const messages of topicMessages.values()) {
        const message = messages.find(m => m.id === messageId);
        if (message) {
          message.acknowledge();
          return;
        }
      }
    }
    throw new QueueError(`Message with id ${messageId} not found`);
  }

  getStats(): QueueStats {
    let totalMessages = 0;
    let acknowledgedMessages = 0;
    let pendingMessages = 0;
    let failedMessages = 0;
    const messagesByTopic = new Map<string, Map<number, number>>();

    for (const [topicName, topicMessages] of this.messages) {
      const topicStats = new Map<number, number>();
      messagesByTopic.set(topicName, topicStats);

      for (const [partition, messages] of topicMessages) {
        const partitionCount = messages.length;
        topicStats.set(partition, partitionCount);
        totalMessages += partitionCount;

        for (const message of messages) {
          if (message.isAcknowledged()) {
            acknowledgedMessages++;
          } else if (message.isFailed()) {
            failedMessages++;
          } else {
            pendingMessages++;
          }
        }
      }
    }

    return {
      totalMessages,
      messagesByTopic,
      acknowledgedMessages,
      pendingMessages,
      failedMessages
    };
  }

  clear(): void {
    this.topics.clear();
    this.messages.clear();
    this.offsets.clear();
  }

  getTopics(): string[] {
    return Array.from(this.topics.keys());
  }

  getPartitions(topicName: string): number[] {
    return this.getTopic(topicName).getPartitions();
  }

  getPartitionSize(topicName: string, partition: number): number {
    this.ensureTopicPartition(topicName, partition);
    const topicMessages = this.messages.get(topicName)!;
    const partitionMessages = topicMessages.get(partition)!;
    return partitionMessages.length;
  }

  isEmpty(): boolean {
    if (this.topics.size === 0) {
      return true;
    }
    
    return Array.from(this.messages.values()).every(
      topicMessages => Array.from(topicMessages.values()).every(
        messages => messages.length === 0
      )
    );
  }
}