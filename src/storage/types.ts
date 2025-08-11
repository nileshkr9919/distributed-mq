import { MessageOptions } from '../core/types';

export interface StorageConfig {
  dataDir: string;
  segmentSize: number;  // Maximum size of a segment file in bytes
  indexInterval: number;  // Number of messages between index entries
  flushInterval: number; // Interval in ms to force fsync
}

export interface LogEntry {
  offset: number;
  timestamp: number;
  messageSize: number;
  position: number;  // Physical position in segment file
}

export interface IndexEntry {
  offset: number;     // Message offset
  position: number;   // Position in segment file
}

export interface SegmentInfo {
  baseOffset: number;
  startPosition: number;
  size: number;
  path: string;
}

export class StorageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StorageError';
  }
}

export interface StorageMessage extends MessageOptions {
  data: Buffer;
}

export interface SegmentMetadata {
  topic: string;
  partition: number;
  baseOffset: number;
  lastOffset: number;
  size: number;
  created: number;
  lastModified: number;
}