# Distributed Message Queue Architecture

## Overview
This document outlines the architecture and implementation plan for a distributed message queue system with message persistence and FIFO ordering.

## Core Components

### Message
```typescript
interface Message {
  id: string;          // UUID
  payload: string;     // Message text content
  timestamp: number;   // Creation timestamp
  partition: number;   // Assigned partition
  offset: number;      // Position in log
  status: MessageStatus;
}
```

### Queue
- Manages message storage and retrieval
- Maintains FIFO ordering within partitions
- Handles message acknowledgments
- Manages partition assignment

### Storage Manager
- Manages append-only log files
- Handles log segmentation and cleanup
- Provides message persistence
- Maintains message indexes

### Network Manager
- Custom TCP protocol for node communication
- Handles node discovery and health checks
- Manages message routing
- Handles network failures

## Implementation Phases

### Phase 1: Core Message Queue (2-3 days)
- Message and Queue data structures
- File-based storage system with append-only logs
- Basic producer/consumer API
- Message acknowledgment system
- Unit tests for core functionality

### Phase 2: Networking Layer (2-3 days)
- Custom TCP protocol implementation
- Node discovery and health checking
- Message routing between nodes
- Network failure handling
- Integration tests for networking

### Phase 3: Distribution & Replication (3-4 days)
- Queue partitioning logic
- Leader election for partitions
- Message replication across nodes
- Consistency protocols
- Failover handling

### Phase 4: Durability & Recovery (2-3 days)
- Log compaction
- Recovery from node failures
- Message replay
- Crash recovery
- System-wide testing

## Project Structure

```
src/
├── core/
│   ├── message.ts       # Message data structure
│   ├── queue.ts         # Queue implementation
│   └── types.ts         # Common type definitions
├── storage/
│   ├── log.ts          # Log file management
│   ├── segment.ts      # Log segmentation
│   └── manager.ts      # Storage operations
├── network/
│   ├── protocol.ts     # Custom TCP protocol
│   ├── node.ts         # Node management
│   └── transport.ts    # Network transport
├── distribution/
│   ├── coordinator.ts  # Partition coordination
│   ├── replication.ts  # Message replication
│   └── leader.ts       # Leader election
├── api/
│   ├── producer.ts     # Producer API
│   └── consumer.ts     # Consumer API
└── utils/
    ├── logger.ts       # Logging utility
    └── config.ts       # Configuration management
```

## Technical Specifications

### Storage Format
- Append-only log files with format: `<timestamp>:<id>:<partition>:<length>:<message>`
- Each partition has its own log file
- Log segmentation for efficient cleanup
- Index files for quick message lookup

### Network Protocol
```
[Header - 8 bytes][Type - 1 byte][Length - 4 bytes][Payload]
Types:
- 0x01: Message
- 0x02: Ack
- 0x03: Heartbeat
- 0x04: Replication
- 0x05: Election
```

### Fault Tolerance
- Leader election using RAFT consensus
- Message replication (configurable factor)
- Automatic failover
- Message checksums for integrity