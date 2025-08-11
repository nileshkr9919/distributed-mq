import path from 'path';
import fs from 'fs/promises';
import { Segment } from './segment';
import { StorageConfig, StorageError, StorageMessage, SegmentMetadata } from './types';

export class StorageManager {
  private activeSegments: Map<string, Segment> = new Map();
  private readonly config: StorageConfig;

  constructor(config: StorageConfig) {
    this.config = {
      dataDir: config.dataDir,
      segmentSize: config.segmentSize || 1024 * 1024 * 1024, // 1GB default
      indexInterval: config.indexInterval || 1000, // Every 1000 messages
      flushInterval: config.flushInterval || 1000 // 1 second default
    };
  }

  private getTopicPartitionPath(topic: string, partition: number): string {
    return path.join(this.config.dataDir, topic, String(partition));
  }

  private getSegmentKey(topic: string, partition: number): string {
    return `${topic}-${partition}`;
  }

  async init(): Promise<void> {
    try {
      await fs.mkdir(this.config.dataDir, { recursive: true });
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to initialize storage: ${errorMessage}`);
    }
  }

  async createTopicPartition(topic: string, partition: number): Promise<void> {
    const partitionPath = this.getTopicPartitionPath(topic, partition);
    try {
      await fs.mkdir(partitionPath, { recursive: true });
      // Initialize first segment
      await this.getOrCreateSegment(topic, partition, 0);
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to create topic partition: ${errorMessage}`);
    }
  }

  private async getOrCreateSegment(topic: string, partition: number, baseOffset: number): Promise<Segment> {
    const key = this.getSegmentKey(topic, partition);
    let segment = this.activeSegments.get(key);

    if (!segment || await segment.isFull()) {
      if (segment) {
        await segment.close();
      }

      segment = new Segment(
        this.getTopicPartitionPath(topic, partition),
        topic,
        partition,
        baseOffset,
        this.config.segmentSize,
        this.config.indexInterval
      );

      await segment.init();
      this.activeSegments.set(key, segment);
    }

    return segment;
  }

  async appendMessage(topic: string, partition: number, message: StorageMessage): Promise<void> {
    try {
      const key = this.getSegmentKey(topic, partition);
      let segment = this.activeSegments.get(key);

      if (!segment) {
        // Find the latest segment or create new one
        const baseOffset = await this.getLatestBaseOffset(topic, partition);
        segment = await this.getOrCreateSegment(topic, partition, baseOffset);
      }

      try {
        await segment.append(message);
      } catch (error) {
        if (error instanceof StorageError && error.message.includes('Segment is full')) {
          // Create new segment with next base offset
          const newBaseOffset = message.offset!;
          segment = await this.getOrCreateSegment(topic, partition, newBaseOffset);
          await segment.append(message);
        } else {
          throw error;
        }
      }
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to append message: ${errorMessage}`);
    }
  }

  async getMessage(topic: string, partition: number, offset: number): Promise<StorageMessage | null> {
    try {
      const segment = await this.findSegmentForOffset(topic, partition, offset);
      if (!segment) {
        return null;
      }
      return segment.read(offset);
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to get message: ${errorMessage}`);
    }
  }

  async getMessageRange(
    topic: string,
    partition: number,
    startOffset: number,
    endOffset: number
  ): Promise<StorageMessage[]> {
    const messages: StorageMessage[] = [];
    let currentOffset = startOffset;

    while (currentOffset < endOffset) {
      const message = await this.getMessage(topic, partition, currentOffset);
      if (!message) break;
      
      messages.push(message);
      currentOffset++;
    }

    return messages;
  }

  private async findSegmentForOffset(
    topic: string,
    partition: number,
    offset: number
  ): Promise<Segment | null> {
    const segments = await this.listSegments(topic, partition);
    
    for (const metadata of segments) {
      if (offset >= metadata.baseOffset && offset <= metadata.lastOffset) {
        return this.getOrCreateSegment(topic, partition, metadata.baseOffset);
      }
    }

    return null;
  }

  private async getLatestBaseOffset(topic: string, partition: number): Promise<number> {
    const segments = await this.listSegments(topic, partition);
    if (segments.length === 0) return 0;
    
    return Math.max(...segments.map(s => s.baseOffset));
  }

  async listSegments(topic: string, partition: number): Promise<SegmentMetadata[]> {
    const partitionPath = this.getTopicPartitionPath(topic, partition);
    try {
      const files = await fs.readdir(partitionPath);
      const segments: SegmentMetadata[] = [];

      for (const file of files) {
        if (!file.endsWith('.log')) continue;
        
        const baseOffset = parseInt(file.split('_')[1]);
        const stats = await fs.stat(path.join(partitionPath, file));
        
        const key = this.getSegmentKey(topic, partition);
        const activeSegment = this.activeSegments.get(key);
        
        if (activeSegment && activeSegment.getMetadata().baseOffset === baseOffset) {
          segments.push({
            ...activeSegment.getMetadata(),
            created: stats.birthtime.getTime(),
            lastModified: stats.mtime.getTime()
          });
        } else {
          const segment = new Segment(
            partitionPath,
            topic,
            partition,
            baseOffset,
            this.config.segmentSize,
            this.config.indexInterval
          );
          await segment.init();
          
          segments.push({
            ...segment.getMetadata(),
            created: stats.birthtime.getTime(),
            lastModified: stats.mtime.getTime()
          });
          
          await segment.close();
        }
      }

      return segments.sort((a, b) => a.baseOffset - b.baseOffset);
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to list segments: ${errorMessage}`);
    }
  }

  async deleteSegment(topic: string, partition: number, baseOffset: number): Promise<void> {
    const partitionPath = this.getTopicPartitionPath(topic, partition);
    const segmentPrefix = `segment_${baseOffset}`;

    try {
      // Remove from active segments if present
      const key = this.getSegmentKey(topic, partition);
      const activeSegment = this.activeSegments.get(key);
      if (activeSegment?.getMetadata().baseOffset === baseOffset) {
        await activeSegment.close();
        this.activeSegments.delete(key);
      }

      // Delete log and index files
      await Promise.all([
        fs.unlink(path.join(partitionPath, `${segmentPrefix}.log`)),
        fs.unlink(path.join(partitionPath, `${segmentPrefix}.index`))
      ]);
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to delete segment: ${errorMessage}`);
    }
  }

  async close(): Promise<void> {
    const closePromises = Array.from(this.activeSegments.values()).map(segment => segment.close());
    await Promise.all(closePromises);
    this.activeSegments.clear();
  }
}