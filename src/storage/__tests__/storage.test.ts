import path from 'path';
import fs from 'fs/promises';
import { StorageManager } from '../manager';
import { StorageConfig, StorageMessage } from '../types';
import { MessageStatus } from '../../core/types';

describe('Storage System', () => {
  let storageManager: StorageManager;
  const testDir = path.join(__dirname, '../../.tmp/test-storage');
  const config: StorageConfig = {
    dataDir: testDir,
    segmentSize: 1024 * 1024, // 1MB for testing
    indexInterval: 5, // Index every 5 messages for testing
    flushInterval: 100
  };

  beforeEach(async () => {
    // Clean up test directory
    try {
      await fs.rm(testDir, { recursive: true, force: true });
    } catch (error) {
      // Ignore if directory doesn't exist
    }
    storageManager = new StorageManager(config);
    await storageManager.init();
  });

  afterEach(async () => {
    await storageManager.close();
    try {
      await fs.rm(testDir, { recursive: true, force: true });
    } catch (error) {
      // Ignore cleanup errors
    }
  });

  describe('Topic Partition Management', () => {
    it('should create topic partition directory structure', async () => {
      await storageManager.createTopicPartition('test-topic', 0);
      
      const partitionPath = path.join(testDir, 'test-topic', '0');
      const exists = await fs.stat(partitionPath)
        .then(() => true)
        .catch(() => false);
      
      expect(exists).toBe(true);
    });

    it('should handle multiple partitions', async () => {
      await storageManager.createTopicPartition('test-topic', 0);
      await storageManager.createTopicPartition('test-topic', 1);
      
      const partition0 = await fs.stat(path.join(testDir, 'test-topic', '0'));
      const partition1 = await fs.stat(path.join(testDir, 'test-topic', '1'));
      
      expect(partition0.isDirectory()).toBe(true);
      expect(partition1.isDirectory()).toBe(true);
    });
  });

  describe('Message Operations', () => {
    const testMessages: StorageMessage[] = [
      {
        id: '1',
        topicName: 'test-topic',
        partition: 0,
        offset: 0,
        timestamp: Date.now(),
        status: MessageStatus.PENDING,
        data: Buffer.from('message 1')
      },
      {
        id: '2',
        topicName: 'test-topic',
        partition: 0,
        offset: 1,
        timestamp: Date.now(),
        status: MessageStatus.PENDING,
        data: Buffer.from('message 2')
      }
    ];

    beforeEach(async () => {
      await storageManager.createTopicPartition('test-topic', 0);
    });

    it('should append and retrieve messages', async () => {
      // Append messages
      await storageManager.appendMessage('test-topic', 0, testMessages[0]);
      await storageManager.appendMessage('test-topic', 0, testMessages[1]);

      // Retrieve messages
      const message1 = await storageManager.getMessage('test-topic', 0, 0);
      const message2 = await storageManager.getMessage('test-topic', 0, 1);

      expect(message1).toMatchObject({
        id: testMessages[0].id,
        offset: testMessages[0].offset
      });
      expect(message2).toMatchObject({
        id: testMessages[1].id,
        offset: testMessages[1].offset
      });
    });

    it('should handle message range queries', async () => {
      // Append messages
      await Promise.all(testMessages.map(msg => 
        storageManager.appendMessage('test-topic', 0, msg)
      ));

      // Retrieve range
      const messages = await storageManager.getMessageRange('test-topic', 0, 0, 2);
      
      expect(messages).toHaveLength(2);
      expect(messages[0]).toMatchObject({
        id: testMessages[0].id,
        offset: testMessages[0].offset
      });
      expect(messages[1]).toMatchObject({
        id: testMessages[1].id,
        offset: testMessages[1].offset
      });
    });
  });

  describe('Segment Management', () => {
    it('should create new segment when size limit is reached', async () => {
      await storageManager.createTopicPartition('test-topic', 0);
      
      // Create messages that will exceed segment size
      const largeData = Buffer.alloc(config.segmentSize / 2);
      const messages: StorageMessage[] = [
        {
          id: '1',
          topicName: 'test-topic',
          partition: 0,
          offset: 0,
          timestamp: Date.now(),
          status: MessageStatus.PENDING,
          data: largeData
        },
        {
          id: '2',
          topicName: 'test-topic',
          partition: 0,
          offset: 1,
          timestamp: Date.now(),
          status: MessageStatus.PENDING,
          data: largeData
        }
      ];

      // Append messages
      await storageManager.appendMessage('test-topic', 0, messages[0]);
      await storageManager.appendMessage('test-topic', 0, messages[1]);

      // List segments
      const segments = await storageManager.listSegments('test-topic', 0);
      expect(segments.length).toBeGreaterThan(1);
    });

    it('should delete segments', async () => {
      await storageManager.createTopicPartition('test-topic', 0);
      
      // Append a message
      const message: StorageMessage = {
        id: '1',
        topicName: 'test-topic',
        partition: 0,
        offset: 0,
        timestamp: Date.now(),
        status: MessageStatus.PENDING,
        data: Buffer.from('test message')
      };
      
      await storageManager.appendMessage('test-topic', 0, message);
      
      // Delete segment
      await storageManager.deleteSegment('test-topic', 0, 0);
      
      // Verify segment files are gone
      ;
  });
});