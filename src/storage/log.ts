import fs from 'fs/promises';
import { open, FileHandle } from 'fs/promises';
import { StorageError, LogEntry, StorageMessage } from './types';
import path from 'path';

export class Log {
  private fileHandle: FileHandle | null = null;
  private writePosition: number = 0;
  private lastSync: number = Date.now();

  constructor(
    private readonly filePath: string,
    private readonly flushInterval: number = 1000
  ) {}

  async open(): Promise<void> {
    try {
      await fs.mkdir(path.dirname(this.filePath), { recursive: true });
      this.fileHandle = await open(this.filePath, 'a+');
      const stats = await this.fileHandle.stat();
      this.writePosition = stats.size;
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to open log file: ${errorMessage}`);
    }
  }

  async append(message: StorageMessage): Promise<LogEntry> {
    if (!this.fileHandle) {
      throw new StorageError('Log file not opened');
    }

    const timestamp = Date.now();
    const messageBuffer = Buffer.from(JSON.stringify(message));
    const messageSize = messageBuffer.length;

    // Format: <timestamp>:<offset>:<size>:<message>
    const entry = `${timestamp}:${message.offset}:${messageSize}:${messageBuffer}\n`;
    const entryBuffer = Buffer.from(entry);

    try {
      const position = this.writePosition;
      await this.fileHandle.write(entryBuffer, 0, entryBuffer.length, position);
      this.writePosition += entryBuffer.length;

      // Force fsync based on interval
      if (Date.now() - this.lastSync >= this.flushInterval) {
        await this.fileHandle.sync();
        this.lastSync = Date.now();
      }

      return {
        offset: message.offset!,
        timestamp,
        messageSize,
        position
      };
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to append message: ${errorMessage}`);
    }
  }

  async read(position: number, size: number): Promise<Buffer> {
    if (!this.fileHandle) {
      throw new StorageError('Log file not opened');
    }

    const buffer = Buffer.alloc(size);
    try {
      const { bytesRead } = await this.fileHandle.read(buffer, 0, size, position);
      if (bytesRead !== size) {
        throw new StorageError('Incomplete read');
      }
      return buffer;
    } catch (error: any) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new StorageError(`Failed to read message: ${errorMessage}`);
    }
  }

  async close(): Promise<void> {
    if (this.fileHandle) {
      await this.fileHandle.sync();
      await this.fileHandle.close();
      this.fileHandle = null;
    }
  }

  async sync(): Promise<void> {
    if (this.fileHandle) {
      await this.fileHandle.sync();
      this.lastSync = Date.now();
    }
  }

  async getSize(): Promise<number> {
    if (!this.fileHandle) {
      throw new StorageError('Log file not opened');
    }
    const stats = await this.fileHandle.stat();
    return stats.size;
  }
}