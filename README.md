# ⚡ PulseMQ

**Distributed, high-throughput event streaming broker built from scratch in Java 21.**

PulseMQ is a lightweight distributed message broker inspired by Apache Kafka. It persists event streams in segmented, immutable commit logs on disk and delivers ordered, high-speed pub/sub messaging over a custom binary TCP protocol.

---

## 🏛️ System Architecture

PulseMQ is designed with a zero-dependency core leveraging modern Java 21 features and Linux/Windows systems engineering primitives:

- **Segmented Append-Only Storage Engine (`pulse-storage`)**:
  - Topics are partitioned into immutable log segments (`.log`) indexed by sparse offset indexes (`.index`) and time indexes (`.timeindex`).
  - Sparse index binary search achieves $O(\log N)$ file offset resolution without caching entire indexes in JVM heap.
  - Zero-copy data transmission directly from disk to network sockets using OS `sendfile` / Java NIO `FileChannel.transferTo()`.
- **Custom Binary Network Protocol (`pulse-protocol`)**:
  - Length-prefixed binary framing via Netty, minimizing overhead and payload inflation.
  - Native support for `PRODUCE`, `FETCH`, `METADATA`, `JOIN_GROUP`, `HEARTBEAT`, `OFFSET_COMMIT`, and `OFFSET_FETCH`.
- **Asynchronous Execution via Java 21 Virtual Threads (`pulse-broker`)**:
  - Non-blocking network multiplexing with request handlers executing on lightweight virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`).
- **Dynamic Consumer Groups & Rebalancing (`pulse-broker`)**:
  - Server-side group coordinator tracking heartbeats and membership.
  - Durable offset persistence backed by internal committed logs (`__consumer_offsets`).
- **Embedded Raft Consensus (`pulse-consensus`)**:
  - KRaft-style embedded consensus engine managing leader election, metadata log replication, and quorum commits.
- **Client SDKs (`pulse-client`)**:
  - Asynchronous, non-blocking `PulseProducer` and `PulseConsumer` APIs.

---

## 📦 Project Structure

```text
PulseMQ/
├── pom.xml
├── pulse-common/      # Record format, RecordBatch, ByteUtils (CRC32C/Varints), ErrorCodes
├── pulse-storage/     # SparseOffsetIndex, TimeIndex, LogSegment, PartitionLog, LogManager
├── pulse-protocol/    # Binary framing, request/response codecs, Netty server/client handlers
├── pulse-consensus/   # Embedded Raft consensus module (RaftNode, RPCs, StateMachine)
├── pulse-broker/      # PulseServer, PulseRequestHandler, ReplicaManager, GroupCoordinator
└── pulse-client/      # PulseProducer, PulseConsumer, NetworkClient connection pool
```

---

## 🚀 Quickstart

### Prerequisites
- Java 21 LTS (or later)
- Apache Maven 3.9+

### Build and Test
```bash
mvn clean test
```

### Produce & Consume Example

#### 1. Producing Messages
```java
try (PulseProducer producer = new PulseProducer("127.0.0.1", 9092, "order-producer", (short) 1)) {
    ProducerRecord record = new ProducerRecord("orders", "order-101", "{ \"amount\": 99.50 }");
    RecordMetadata meta = producer.send(record).get();
    System.out.println("Message written to offset: " + meta.offset());
}
```

#### 2. Consuming Messages
```java
try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", 9092, "order-consumers")) {
    consumer.subscribe("orders");

    while (true) {
        List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord record : records) {
            System.out.printf("Received [offset=%d]: %s -> %s%n",
                record.offset(), record.keyAsString(), record.valueAsString());
        }
        consumer.commitSync();
    }
}
```
