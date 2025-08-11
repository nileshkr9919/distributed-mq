import path from 'path';
import fs from 'fs/promises';
import { Log } from './log';
import { StorageError, IndexEntry, SegmentInfo, StorageMessage, SegmentMetadata } from './types';

export class Segment {
  private log: Log;
  private indexPath: string;
  private lastOffset: number;
  private size: number;
  private indexCache: Map<number, number>;

  constructor(
    private readonly baseDir: string,
    private readonly topic: string,
    private readonly partition: number,
    private readonly baseOffset: number,
    private readonly maxSize: number,
    private readonly indexInterval: number
  ) {
    const segmentName = `segment_${baseOffset}`;
    this.indexPath = path.join(baseDir, `${segmentName}.index`);
    const logPath = path.join(baseDir, `${segmentName}.log`);
    
    this.log = new Log(logPath);
    this.lastOffset = baseOffset;
    this.size = 0;
    this.indexCache = new Map();
  }

  async init(): Promise<void> {
    try {
      await fs.mkdir(this.baseDir, { recursive: true });
      await this.log.open();
      await this.loadIndex();
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to initialize segment: ${errorMessage}`);
    }
  }

  private async loadIndex(): Promise<void> {
    try {
      await fs.access(this.indexPath);
      const indexData = await fs.readFile(this.indexPath);
      const entries = this.parseIndexData(indexData);
      
      // Populate cache
      entries.forEach(entry => {
        this.indexCache.set(entry.offset, entry.position);
      });
      
      if (entries.length > 0) {
        this.lastOffset = entries[entries.length - 1].offset;
      }
    } catch (error: any) {
      if (error.code !== 'ENOENT') {
        const errorMessage = error instanceof Error ? error.message : String(error);
        throw new StorageError(`Failed to load index: ${errorMessage}`);
      }
      // Create new index file if it doesn't exist
      await fs.writeFile(this.indexPath, Buffer.alloc(0));
    }
  }

  private parseIndexData(data: Buffer): IndexEntry[] {
    const entries: IndexEntry[] = [];
    let position = 0;

    while (position < data.length) {
      const offset = data.readBigInt64BE(position);
      const filePosition = data.readBigInt64BE(position + 8);
      entries.push({
        offset: Number(offset),
        position: Number(filePosition)
      });
      position += 16; // 8 bytes for offset + 8 bytes for position
    }

    return entries;
  }

  async append(message: StorageMessage): Promise<void> {
    if (await this.isFull()) {
      throw new StorageError('Segment is full');
    }

    try {
      const logEntry = await this.log.append(message);
      this.lastOffset = message.offset!;
      this.size = await this.log.getSize();

      // Write index entry if needed
      if (message.offset! % this.indexInterval === 0) {
        await this.writeIndex(message.offset!, logEntry.position);
        this.indexCache.set(message.offset!, logEntry.position);
      }
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to append to segment: ${errorMessage}`);
    }
  }

  private async writeIndex(offset: number, position: number): Promise<void> {
    const buffer = Buffer.alloc(16);
    buffer.writeBigInt64BE(BigInt(offset), 0);
    buffer.writeBigInt64BE(BigInt(position), 8);
    await fs.appendFile(this.indexPath, buffer);
  }

  async findPosition(offset: number): Promise<{ position: number; size: number } | null> {
    // Check cache first
    const cachedPosition = this.indexCache.get(offset);
    if (cachedPosition !== undefined) {
      return { position: cachedPosition, size: 0 }; // Size will be determined during read
    }

    // Find nearest index entry before the requested offset
    const indexData = await fs.readFile(this.indexPath);
    const entries = this.parseIndexData(indexData);
    
    let nearestEntry: IndexEntry | null = null;
    for (const entry of entries) {
      if (entry.offset > offset) break;
      nearestEntry = entry;
    }

    if (!nearestEntry) return null;
    return { position: nearestEntry.position, size: 0 };
  }

  async read(offset: number): Promise<StorageMessage | null> {
    try {
      const position = await this.findPosition(offset);
      if (!position) return null;

      const messageBuffer = await this.log.read(position.position, 1024); // Read initial chunk
      const messageStr = messageBuffer.toString();
      const lines = messageStr.split('\n');
      
      for (const line of lines) {
        if (!line) continue;
        const [timestamp, msgOffset, size, message] = line.split(':');
        if (Number(msgOffset) === offset) {
          return JSON.parse(message);
        }
      }
      
      return null;
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to read message: ${errorMessage}`);
    }
  }

  async isFull(): Promise<boolean> {
    return this.size >= this.maxSize;
  }

  async close(): Promise<void> {
    await this.log.close();
  }

  getMetadata(): SegmentMetadata {
    return {
      topic: this.topic,
      partition: this.partition,
      baseOffset: this.baseOffset,
      lastOffset: this.lastOffset,
      size: this.size,
      created: 0, // Will be set from file stats
      lastModified: 0 // Will be set from file stats
    };
  }
}