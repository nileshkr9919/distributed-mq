import { v4 as uuidv4 } from 'uuid';
import { MessageStatus, MessageOptions, ValidationError } from './types';

export interface Message {
  id: string;
  topicName: string;
  payload: string;
  timestamp: number;
  partition: number;
  offset: number;
  status: MessageStatus;
  
  acknowledge(): void;
  fail(): void;
  isAcknowledged(): boolean;
  isFailed(): boolean;
  isPending(): boolean;
}

export class MessageImpl implements Message {
  readonly id: string;
  readonly topicName: string;
  readonly payload: string;
  readonly timestamp: number;
  readonly partition: number;
  offset: number;
  status: MessageStatus;

  constructor(payload: string, options: MessageOptions = {}) {
    if (!payload || payload.trim().length === 0) {
      throw new ValidationError('Message payload cannot be empty');
    }

    if (!options.topicName || options.topicName.trim().length === 0) {
      throw new ValidationError('Topic name is required');
    }

    this.id = options.id || uuidv4();
    this.topicName = options.topicName;
    this.payload = payload;
    this.timestamp = options.timestamp || Date.now();
    this.partition = options.partition || 0;
    this.offset = options.offset || 0;
    this.status = options.status || MessageStatus.PENDING;
  }

  acknowledge(): void {
    this.status = MessageStatus.ACKNOWLEDGED;
  }

  fail(): void {
    this.status = MessageStatus.FAILED;
  }

  isAcknowledged(): boolean {
    return this.status === MessageStatus.ACKNOWLEDGED;
  }

  isFailed(): boolean {
    return this.status === MessageStatus.FAILED;
  }

  isPending(): boolean {
    return this.status === MessageStatus.PENDING;
  }
}