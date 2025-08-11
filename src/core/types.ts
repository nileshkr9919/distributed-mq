export enum MessageStatus {
  PENDING = 'PENDING',
  ACKNOWLEDGED = 'ACKNOWLEDGED',
  FAILED = 'FAILED'
}

export interface TopicOptions {
  name: string;
  partitions?: number;
}

export interface MessageOptions {
  id?: string;
  topicName?: string;
  partition?: number;
  offset?: number;
  timestamp?: number;
  status?: MessageStatus;
}

export class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export class QueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'QueueError';
  }
}

export class TopicError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TopicError';
  }
}