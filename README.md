# distributed-mq

A message log written to understand the storage layer of a queue from the inside, rather than to
be deployed. Topics, partitions, per-partition offsets, acknowledgement tracking, and an
append-only segmented file store.

## What is built

**Single node.** Everything below runs in one process.

| Area | Status |
| --- | --- |
| Topics and partition assignment | built |
| Per-partition FIFO ordering and offsets | built |
| Message acknowledgement and status tracking | built |
| Append-only log with segmentation | built |
| Networking between nodes | designed, not built |
| Replication and leader election | designed, not built |
| Log compaction and crash recovery | designed, not built |

The name is aspirational and the table is the correction: **this is not a distributed system
today.** [`docs/architecture.md`](docs/architecture.md) is the full design — a custom binary TCP
protocol, RAFT leader election, configurable replication factor, checksums — and phases 2 through
4 of it are unwritten.

## What was interesting to build

**Offsets belong to the partition, not the message.** The first cut stored a read cursor on the
consumer, which makes replay easy and makes two consumers on one partition incoherent. Moving the
offset into the partition means a consumer is a position rather than a state machine, which is
what lets a restart resume rather than re-read.

**Segmentation is what makes deletion possible.** An append-only log cannot delete a record in
place, so retention has to operate on whole segments. That constraint decides the file format
before anything else does — the segment boundary has to be findable without parsing the segment.

## Layout

```
src/core/      message, topic, queue, shared types
src/storage/   append-only log, segment handling, storage manager
```

## Running it

```bash
npm install
npm test        # unit tests for message, queue and storage
npm run build
```

~1,300 lines of TypeScript. Tests cover the message lifecycle, queue semantics and the storage
layer.
