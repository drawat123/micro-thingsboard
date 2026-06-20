# Project Development Notes — Part 3

> Continuation of `notes-2.md`. Covers Phase 3.3.1+ — Docker Compose, Kafka deep
> internals (KRaft, listeners, partitions, acks), the Spring Kafka producer, and
> the universal distributed-systems concept of **backpressure**.

---

## Table of Contents

1. [Docker Compose for Local Infrastructure](#1-docker-compose-for-local-infrastructure)
2. [Kafka Setup in KRaft Mode](#2-kafka-setup-in-kraft-mode)
3. [Kafka Listeners — The Trickiest Config](#3-kafka-listeners--the-trickiest-config)
4. [Kafka CLI Workflow](#4-kafka-cli-workflow)
5. [Partition Assignment & Keying](#5-partition-assignment--keying)
6. [Spring Kafka Producer](#6-spring-kafka-producer)
7. [Acks — The Durability Dial](#7-acks--the-durability-dial)
8. [Backpressure — A Universal Concept](#8-backpressure--a-universal-concept)
9. [Producer Buffering & Failure Modes](#9-producer-buffering--failure-modes)
10. [Architecture Habits Reinforced](#10-architecture-habits-reinforced)

---

# 1. Docker Compose for Local Infrastructure

## What It Is

Docker Compose is a tool for **declarative multi-container orchestration**. You describe the services you
want (Kafka, Redis, Postgres, etc.) in a YAML file; Compose starts them, networks them, manages them.

Same as `docker run` but declarative — committed alongside the code, reproducible across machines.

## Basic Lifecycle

```bash
docker compose up -d              # start all services in background
docker compose logs -f kafka      # follow logs for one service
docker compose ps                 # see running containers
docker compose down               # stop and remove everything
```

## Why This Beats Native Installation

| Concern | `brew install` | Docker Compose |
|---|---|---|
| Cleanliness | Pollutes system | Containers isolated |
| Reproducibility | Depends on Mac state | Same image, every machine |
| Version pinning | Hard | `image: foo:7.5.3` is exact |
| Production parity | Different from prod | Real deployments run containers |
| Side-by-side versions | Painful | Trivial |

For dev infrastructure (databases, brokers, caches), **Docker Compose is the modern default.**

## Key YAML Concepts

```yaml
services:
  kafka:                                  # service name (= internal DNS name)
    image: confluentinc/cp-kafka:7.5.3    # exact image + version
    container_name: kafka-dev             # friendly name in `docker ps`
    ports:
      - "9092:9092"                       # host:container port mapping
    environment:
      KAFKA_NODE_ID: 1                    # env vars passed to container
      KAFKA_PROCESS_ROLES: 'broker,controller'
```

**Service names become DNS names inside the Docker network.** So another container in the same Compose
file can reach Kafka at hostname `kafka` (or `kafka-dev` if you set `container_name`). From the host
machine, you reach it via the published port (`localhost:9092`).

## Comments In Compose Files Are Architecture

A well-commented `docker-compose.yml` is self-documenting infrastructure. Six months from now, you (or a
teammate) should be able to read it and understand every choice without external lookup.

**Architect's habit:** explain *why* each non-obvious environment variable exists, what production
typically differs on, and what would change for security/scale.

---

# 2. Kafka Setup in KRaft Mode

## ZooKeeper Is Going Away

Old Kafka required **two services**: Kafka brokers + a separate **ZooKeeper** cluster for coordination
(leader election, metadata, configs).

Modern Kafka (3.x+) introduced **KRaft mode** — Kafka uses its own Raft-based consensus protocol for
coordination. **One service instead of two.** ZooKeeper-based clusters are being deprecated.

For new projects: **always use KRaft.** No more ZooKeeper.

## Combined vs Separate Roles

In KRaft, each node has one or more roles:

* **`broker`** — handles producer/consumer traffic
* **`controller`** — manages cluster metadata (topic configs, partition assignments, ISR lists)

Two deployment modes:

* **Combined** (`KAFKA_PROCESS_ROLES: 'broker,controller'`) — same process does both. **Used for dev.**
* **Separate** — dedicated controller nodes, dedicated broker nodes. **Used in production** for isolation
  and independent scaling.

## Cluster ID

KRaft requires a permanent cluster identifier. Kafka stores it on disk and verifies at startup.

```yaml
CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
```

* Any valid base64 UUID works
* In production: generate via `kafka-storage random-uuid` per environment
* For dev: a known-good value from tutorials is fine

## Single-Node Tuning

Kafka's internal topics (`__consumer_offsets`, `__transaction_state`) **default to replication factor 3**
— designed for production clusters. On a single-broker dev cluster, Kafka will wait forever for
non-existent replicas. Override:

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

Also useful for dev:
```yaml
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

Default is 3 seconds — Kafka waits before assigning partitions to a new consumer group, hoping more
consumers will join. For dev iteration, `0` gives instant feedback. **In production, keep the 3s default**
— it avoids rebalance storms when consumers start in quick succession.

---

# 3. Kafka Listeners — The Trickiest Config

## The Two Concepts

Two distinct settings:

| Setting | Question it answers |
|---|---|
| `KAFKA_LISTENERS` | "What sockets does the broker BIND to?" |
| `KAFKA_ADVERTISED_LISTENERS` | "What addresses does it TELL clients to use?" |

**These can be different.** The broker can listen on one address (inside the container) and advertise
another (e.g., `localhost` for host machines).

## Why This Matters for Docker

When a client connects to Kafka, the broker tells the client: *"actually, talk to me at THIS address."*
That address comes from `KAFKA_ADVERTISED_LISTENERS`.

For local Docker:
* Other containers inside the Docker network reach Kafka at the service name (e.g., `kafka-dev:29092`)
* Your Mac reaches Kafka at `localhost:9092`

Both need to be advertised correctly. **If you set this wrong, clients get "connection refused" or DNS
errors even though the broker is up.**

## The Dual-Listener Pattern

```yaml
KAFKA_LISTENERS:
  'PLAINTEXT://0.0.0.0:29092,
   CONTROLLER://0.0.0.0:29093,
   PLAINTEXT_HOST://0.0.0.0:9092'

KAFKA_ADVERTISED_LISTENERS:
  'PLAINTEXT://kafka-dev:29092,
   PLAINTEXT_HOST://localhost:9092'
```

Three listeners, three purposes:

| Name | Port | Purpose |
|---|---|---|
| `PLAINTEXT` | 29092 | Inter-broker traffic (between brokers if there were more) |
| `CONTROLLER` | 29093 | KRaft cluster metadata traffic |
| `PLAINTEXT_HOST` | 9092 | Host-facing (your Mac connects here) |

`CONTROLLER` is never advertised — clients don't talk to controllers directly.

`0.0.0.0` means "bind to all network interfaces inside the container."

## Required Companion Settings

```yaml
KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
  'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
```

* `INTER_BROKER_LISTENER_NAME` — which listener brokers use to talk to each other
* `CONTROLLER_LISTENER_NAMES` — which listener handles KRaft traffic
* `SECURITY_PROTOCOL_MAP` — what security each listener uses. **`PLAINTEXT` = no encryption.** Fine for
  local dev, **never acceptable in production** (use SSL or SASL_SSL).

## The Mental Model

Think of `LISTENERS` as **doors** the broker opens. Think of `ADVERTISED_LISTENERS` as **the map** the
broker hands to clients showing how to get to each door.

The map and the doors don't have to match exactly — but the map must be reachable from where the client
stands.

---

# 4. Kafka CLI Workflow

## Installation

```bash
brew install kafka
```

Installs `kafka-topics`, `kafka-console-producer`, `kafka-console-consumer`, and the full suite.

## Essential Commands

### Topic Management

```bash
# Create a topic with 3 partitions, replication factor 1
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic raw-telemetry \
  --partitions 3 --replication-factor 1

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic (partition layout, leader info)
kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic raw-telemetry
```

### Producing

```bash
# Plain producer (no keys)
kafka-console-producer --bootstrap-server localhost:9092 \
  --topic raw-telemetry

# Producer with keyed messages
kafka-console-producer --bootstrap-server localhost:9092 \
  --topic raw-telemetry \
  --property parse.key=true \
  --property key.separator=":"

# Then type:  mac-edge-01:hello
# Key = "mac-edge-01", value = "hello"
```

### Consuming

```bash
# Consume from beginning, show all metadata
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw-telemetry \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true \
  --property print.value=true \
  --from-beginning

# Consume in a specific group (for work-sharing tests)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw-telemetry \
  --group "test-group"
```

## The Append-Only Log Cuts Both Ways

Kafka is durable. **Bad experimental data sticks around just like good data.**

If you mistype a producer command and send the wrong format (e.g., forget `parse.key=true` and end up
with `null` keys + weird values), those messages remain in the topic. Replay-from-beginning replays your
bugs too.

**Senior engineering posture:** when an experiment produces unexpected output, *don't gloss over it*.
Investigate. Every unexpected line in CLI output is a clue.

---

# 5. Partition Assignment & Keying

## How Kafka Decides Which Partition a Message Lands On

For each message you produce:

| Producer behavior | Partition assigned via |
|---|---|
| Set a key | `hash(key) % partition_count` (deterministic) |
| No key | Round-robin or sticky (random for our purposes) |

**Same key → always same partition.** This is the foundation of per-key ordering.

## Why Keying Matters for Telemetry

If we key by `agentId`:
* All readings from `mac-edge-01` always land on the **same partition**
* Order is preserved for that device (Kafka guarantees order within a partition)
* Different devices land on potentially different partitions → parallelism preserved

**Per-device ordering, cross-device parallelism.** Exactly what you want for telemetry.

## Partitions Are the Ceiling on Parallelism

Within a consumer group, **a single partition can only be read by one consumer at a time**. This is how
Kafka guarantees in-order delivery.

* 3 partitions → max 3 consumers actively processing
* 4th consumer joins → it sits idle (no partition to assign)
* 10 partitions → up to 10 consumers can scale out

**Rule of thumb:** `partitions = max consumers you'll ever want × 2 (safety margin)`

**Why this matters:** resharding a Kafka topic in production is operationally painful (create new topic,
dual-write, migrate consumers). Better to over-partition at design time.

For our project: 3 partitions for `raw-telemetry`. Enough for parallel consumers, not silly admin overhead.

## Extracting the Agent ID

Our `SensorReading.sensorId()` is structured like `mac-edge-01.cpu.load.percent`. To use as a Kafka key,
extract just the device portion:

```java
String sensorId = reading.sensorId();
int firstDot = sensorId.indexOf('.');
if (firstDot < 0) {
    log.warn("Cannot extract agentId from sensorId: {}", sensorId);
    return;
}
String agentId = sensorId.substring(0, firstDot);
```

**Always guard the parsing.** `substring(0, -1)` throws. **Boundaries are exactly where defensive coding
pays off** — fail-fast or fail-safe, never silently miskey.

A better long-term fix is to add `agentId` as a first-class field on `SensorReading`. Right now the agent
ID is implicit in the sensor ID — a leaky abstraction. Revisit when device-level tracking matters.

---

# 6. Spring Kafka Producer

## The Three Beans

A Spring Kafka producer setup has three layers:

```
Your code: kafkaTemplate.send(...)
              ↓
KafkaTemplate (Spring bean — convenience wrapper)
              ↓
ProducerFactory (Spring bean — holds config, connection pool)
              ↓
KafkaProducer (Apache Kafka client — talks to broker)
              ↓
Kafka broker
```

You configure the top two; Spring/Kafka assemble the rest.

## Dependency

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

**Notice this is NOT a Spring Boot starter** (no `spring-boot-starter-` prefix). Spring Kafka pre-dates
the starter convention but works the same way — auto-configures when present on classpath. Version
inherited from Spring Boot BOM.

## The Configuration Class

```java
@Configuration
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaProducerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ProducerFactory<String, byte[]> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.acks());
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, byte[]> kafkaTemplate(ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
```

## Use ProducerConfig Constants, Not Raw Strings

```java
configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ...);   // ✓
configProps.put("bootstrap.servers", ...);                       // ✗
```

If Kafka renames a property in a future major version, the constant fails to compile. String keys are
fragile; constants compile-fail when broken. **Defensive habit worth keeping.**

## Picking Serializers

| Type | Serializer |
|---|---|
| String key | `StringSerializer` |
| Byte array value | `ByteArraySerializer` (no-op pass-through) |
| Java object (JSON) | `JsonSerializer` (not what we want — loses Protobuf schema benefits) |

**For our pipeline:** value is `byte[]` — Protobuf bytes from `proto.toByteArray()`. The producer hands
them through unchanged. No double-encoding.

## The Send API

```java
kafkaTemplate.send(topic, value);                  // no key → random partition
kafkaTemplate.send(topic, key, value);             // keyed → hashed
kafkaTemplate.send(producerRecord);                // full control
```

The middle one is the canonical telemetry pattern.

## Send Is Async

`send()` returns a `CompletableFuture<SendResult<K, V>>`. **It does not block.** Actual delivery happens
in a background thread managed by Kafka's producer.

**Attach a callback for failures:**

```java
kafkaTemplate.send(topic, key, value)
    .whenComplete((result, ex) -> {
        if (ex != null) {
            log.warn("Failed to send to Kafka: {}", ex.getMessage());
        }
        // Don't log success — too noisy
    });
```

**Why async matters:** a sync call would slow your ingestion to Kafka's commit speed. We want pipeline
throughput. The trade-off: you don't know immediately whether send succeeded.

**Log failures only.** Success in telemetry is the boring path; success logging at thousands of msgs/sec
would crush your logging infrastructure.

## Every Config Knob Must Actually Do Something

If you add `acks` to your `KafkaProperties` record but forget to put it in the `configProps` map, you have
a **dead config key**. Future you sets `acks: "all"` in YAML expecting production durability, but the
producer still uses default `acks=1`.

**Dead config is worse than no config** — it implies control that doesn't exist. Verify every property
flows from YAML → record → actual Kafka config.

---

# 7. Acks — The Durability Dial

The most important reliability knob in any Kafka producer.

| acks setting | Meaning | Latency | Loss risk |
|---|---|---|---|
| `0` | Fire and forget. Don't wait for broker. | Lowest | High — broker crash = message lost |
| `1` | Wait for leader broker to acknowledge. | Medium | Medium — leader could die before replicating |
| `all` (or `-1`) | Wait for all in-sync replicas. | Highest | Lowest |

## The `acks=1` Failure Window

The precise timing of message loss with `acks=1`:

1. Leader broker receives the message
2. Leader writes it to its local log (which is **technically just the OS page cache**)
3. Leader sends "Success" ACK back to producer
4. Producer marks future as complete
5. **CRITICAL MOMENT:** Immediately after sending ACK — but before any follower replicates — the leader
   broker crashes
6. The cluster promotes a follower (which never had the message)
7. Message is **permanently lost**, even though your code believes it was safely stored

## Why Even `acks=all` Isn't Bulletproof

"Local log" means OS page cache by default — not fsync'd to disk. Even with `acks=all`, if all replicas
crash simultaneously before fsync (rare but possible), data is lost.

The truly paranoid use: `acks=all` + `flush.messages=1` + replication factor 3+. Each adds latency.

## Choosing Acks for Your Use Case

| Use case | Setting |
|---|---|
| High-volume metrics where some loss is OK | `0` or `1` |
| Most telemetry | `1` (our default) |
| Important events (alerts, billing) | `all` |
| Financial transactions | `all` + idempotence + transactions |

**For our project:** `acks=1`. Telemetry is already loss-tolerant via MQTT QoS 0. Going higher would cost
latency without proportional safety gain in single-broker dev.

**Make it configurable** so production can opt for `acks=all` without code change.

---

# 8. Backpressure — A Universal Concept

## The Definition

**Backpressure is the mechanism by which a slow consumer signals "stop, I can't keep up" to a fast
producer.**

## The Factory Analogy

Imagine an assembly line:
* **Worker A** assembles parts at 10/second
* **Worker B** packages them at 3/second
* Parts pile up between them

After 10 seconds: 100 parts produced, 30 packaged, 70 stuck in a growing pile.
After 1 minute: 600 parts in a pile overflowing onto the floor.

The factory needs a way to **slow Worker A** when the pile is high. That signal is **backpressure**.

## In Software

**When downstream is slow, the upstream component must either slow down, buffer, or drop.**

This applies to:
* TCP (sliding-window flow control)
* Reactive streams (`Flux.onBackpressureBuffer(...)`)
* MQTT (broker QoS levels)
* Kafka (producer buffer + `max.block.ms`)
* HTTP/2 (stream-level flow control)
* Database connection pools (queue + timeout)

**Every distributed system has backpressure mechanisms — either explicit, or implicit, or absent (which
means data loss under load).**

## Recognizing Buffers in a System

The architectural skill: **for every stage in your pipeline, ask "what fills this buffer and what drains
it?"**

Buffers are where backpressure lives. Identifying them is identifying the system's pressure points.

## A Pipeline Mapped With Buffers

For our MQTT → Backend → Kafka pipeline:

```
Edge Agent
   │
   ▼
[Paho client outbox]   ← Buffer 1 (in-memory on Edge Agent)
   │
   ▼  TCP
Mosquitto broker
   │
   ▼
[Spring Integration channel]   ← Buffer 2 (in backend)
   │
   ▼
TelemetryIngestionService.ingest()   ← Synchronous, no buffer here
   │
   ▼
[Kafka RecordAccumulator]   ← Buffer 3 (32MB in backend)
   │
   ▼  TCP
Kafka broker
```

**Four distinct buffers. Each has a different failure mode.**

## How Backpressure Propagates In Our System

What happens if **Kafka is slow** (broker overloaded, not dead):

1. `kafkaTemplate.send()` is async, so `ingest()` returns immediately
2. Messages pile up in the `RecordAccumulator` (Buffer 3)
3. MQTT thread free to handle next message
4. Spring Integration delivers next MQTT message → `ingest()` → another `send()`
5. Pile grows in Buffer 3
6. Eventually Buffer 3 fills

**Then what?**

`send()` no longer returns immediately. It **blocks** the calling thread until either:
* Space frees up (Kafka catches up), OR
* `max.block.ms` (default 60s) elapses → `TimeoutException`

**This blocking IS backpressure.** It propagates backward:

```
Kafka slow
    ↓ (Buffer 3 fills)
RecordAccumulator full
    ↓ (send() blocks)
MQTT handler thread blocks
    ↓ (Spring Integration can't deliver next message)
Mosquitto's send buffer fills
    ↓ (Mosquitto can't push to subscriber)
Edge Agent's Paho buffer eventually fills
    ↓
Edge Agent drops messages OR blocks publish()
```

**The system *is* self-stabilizing** — slowness propagates back to the source. Eventually the data
producer feels the pressure.

## Self-Stabilizing vs Risky Pipelines

**Buffer sizing determines graceful vs catastrophic degradation:**

* **Big buffers** = smooth degradation under transient slowness; high latency when full
* **Small buffers** = fast feedback; more dropping under load

**A well-designed pipeline degrades gracefully under load.** A poorly-designed one either crashes
(out-of-memory) or silently drops without alerting.

Our pipeline degrades because:
* Kafka producer blocks when buffer fills (propagates backward)
* MQTT QoS 0 drops messages when delivery queues overflow (controlled loss at the broker)
* Edge Agent's Paho buffer eventually fills

**At QoS 0**, we *accept* loss as the design trade-off.
**At QoS 1+**, this would behave differently — Mosquitto holds messages until ack'd, propagating
pressure further back.

## Why Backpressure Matters

It's one of the foundational concepts of distributed systems. **Most production incidents trace back to
broken or absent backpressure** — a downstream service slows down, the upstream keeps producing, memory
exhausts, the upstream crashes, the outage cascades.

**The architect's eye scans every system for "where is the backpressure?"** If you can't name it, you
have a hidden failure mode.

---

# 9. Producer Buffering & Failure Modes

## The Kafka Producer Internals

When you call `kafkaTemplate.send()`, the message goes into:

1. **`RecordAccumulator`** — an in-memory buffer (default 32MB via `buffer.memory`)
2. The **Sender thread** (background I/O) drains batches from the accumulator and delivers to the broker

This is what makes `send()` non-blocking. But it also means **messages can sit in memory before reaching
the broker.**

## Three Specific Loss Scenarios

With our current setup, messages can be lost in three ways even when `send()` returned "ok":

### 1. JVM Crash

If the Spring Boot backend crashes (OOM, `kill -9`, hardware failure) while messages are sitting in the
`RecordAccumulator`, **they vanish from memory and are lost forever.**

Mitigation: graceful shutdown lets the producer drain its buffer. Don't `kill -9`. Spring handles this if
you let the context shut down cleanly via SIGTERM.

### 2. Delivery Timeout (`delivery.timeout.ms`)

By default, Kafka gives a message 2 minutes to successfully send. If the broker is down longer than 2
minutes:

* The producer **gives up**
* It expires the batch
* Your `CompletableFuture` completes with an exception
* Your `.whenComplete()` handler logs a warning

**The message is dropped** unless you implement application-level recovery (DLQ, retry topic, persistent
local outbox).

### 3. Buffer Exhaustion (`max.block.ms`)

If the 32MB buffer fills up completely, `send()` stops being async — it **blocks** the calling thread.

If it blocks for longer than `max.block.ms` (default 60 seconds), it throws `TimeoutException`. The
exception propagates up, and unless your code does something specific (queue elsewhere, alert), the
message is dropped.

## Implications for Our Pipeline

We accept these losses because:
* MQTT QoS 0 already implies "loss-tolerant"
* For telemetry, individual sample loss is statistically irrelevant
* Critical events would use a different (durable) channel

**Production-grade pipelines for critical data** add:
* A **dead-letter queue** for failed sends
* **Producer idempotence** (`enable.idempotence=true`) to prevent duplicates on retry
* **Transactions** for cross-topic atomicity
* **Persistent local outbox** for crash recovery (e.g., backed by SQLite or a local file)

We don't need any of this for telemetry. **Know it exists; reach for it when the data demands it.**

---

# 10. Architecture Habits Reinforced

## Every Config Knob Must Wire Through

If you declare a property in YAML and add a field to `@ConfigurationProperties`, **you must also use it
somewhere**. Otherwise you have a dead control surface — config that implies behavior it doesn't deliver.

A test:
* Trace every property from YAML → record → consumer code
* If any link is missing, either delete the property or wire it up

This is a special case of: **don't accumulate code that doesn't earn its place.**

## Boundaries Need Defensive Code

Anywhere data crosses from "code you control" to "code/data you don't":
* String parsing (`substring`, `indexOf`)
* Deserialization
* External API responses
* User input

**Guard or validate.** The same `substring()` call that's safe internal code becomes a bug at a boundary.

Pattern:
```java
int firstDot = sensorId.indexOf('.');
if (firstDot < 0) {
    log.warn("Unexpected format: {}", sensorId);
    return;   // fail-safe
}
String agentId = sensorId.substring(0, firstDot);
```

## Successful Operations Don't Need Logging

For high-throughput operations (Kafka sends, MQTT publishes, queue offers), log **only failures**.

* Failure rate: low → logging is reasonable volume
* Success rate: high → logging crushes infrastructure

Successful Kafka sends at 10K/sec = 10K log lines/sec. Don't.

## Append-Only Stores Are Honest

Kafka, event sourcing, audit logs — all append-only. The honesty cuts both ways:
* Good data preserved forever
* **Bad data preserved forever too**

You can't easily "delete that experimental message I sent by mistake." You can ignore it via consumer
offsets, but it remains in the log.

**Production implication:** be deliberate about what enters an append-only store. Once in, always in.

## Package Layout Mirrors Architecture

When the project grew, we naturally moved:
* `MqttBrokerProperties`, `KafkaProperties` → `config/`
* `TelemetryIngestionService` → `service/`
* `KafkaProducerConfig` → `config/`

**Package structure is documentation.** A teammate opening `backend/service/` immediately knows: business
logic. Opening `backend/config/`: wiring. **Zero mental overhead.**

Don't put 30 files in one package. Don't create empty packages for the future. **Grow structure when
structure earns its place.**

## One Data Plane At a Time

When you're building a system that will eventually have multiple message flows:
* Telemetry: device → backend → storage
* Commands: backend → device
* Events: device → backend (asynchronous alerts)

**Build them as separate planes.** Don't try to design pub/sub for both directions at once.

Each plane has its own:
* Volume characteristics
* Reliability requirements (QoS, acks, retries)
* Latency tolerance
* Consumer count

**Mixing them in a single design produces something mediocre for both.**

Our project: telemetry plane first (Phases 3.1–3.6), then command plane (Phase 3.7).

## "I Don't Know" Is the Most Valuable Phrase

When asked about backpressure, the honest "I don't know what that means" was more valuable than guessing.

**Senior engineers say "I don't know" without ego.** It's:
* Faster than pretending
* Lets the conversation skip ahead to the actual learning
* Builds trust (you're calibrated)

Never bluff. Whatever you don't know now, you'll learn — and remembering "this was the thing I didn't
know" is itself useful metadata for future-you.

---

*Part 3 of the project notes. See `notes.md` for foundations (Maven, Java, concurrency, networking,
Protobuf) and `notes-2.md` for Phase 3.1+ (multi-module Maven, Spring Integration, MQTT, career growth).*
