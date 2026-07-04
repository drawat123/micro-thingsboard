# Micro-ThingsBoard: Complete Project Roadmap

> A learning-driven journey from plain Java fundamentals to a production-shaped, distributed telemetry system.
> Every phase builds on the previous — no concept introduced before it's needed.

---

## Architecture (End State)

```
┌──────────────────────────────────────────────────────────────────────┐
│                          EDGE AGENT (Plain Java 21)                   │
│                                                                       │
│  OSHI ──► Collector ──► Agent ──► Publisher ──► MQTT Client           │
│                                                            │          │
└────────────────────────────────────────────────────────────┼──────────┘
                                                             │ MQTT
                                                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     MQTT-TRANSPORT (Spring Boot)                       │
│                                                                       │
│  MQTT Broker ──► Ingestion (decode) ──► Kafka Producer                │
│                                                │                      │
└────────────────────────────────────────────────┼──────────────────────┘
                                                 │
                                                 ▼
                                          Kafka Topic
                                          (raw-telemetry)
                                                 │
                          ┌──────────────────────┴───────────┐
                          ▼                                   ▼
┌────────────────────────────────────────────┐   ┌────────────────────┐
│      TELEMETRY-PROCESSOR (Spring Boot)       │   │      Alerting      │
│                                              │   │      Consumer      │
│  @KafkaListener → decode → cache + persist   │   │   (future, its     │
│         │                    │               │   │  own consumer      │
│         ▼                    ▼               │   │  group)            │
│    Redis Cache          Cassandra            │   └────────────────────┘
│    (hot reads)         (time-series)         │
└────────────────────────────────────────────┘
                          │
                          ▼
                ┌──────────────────────────┐
                │   REST API Layer (Query)  │
                └──────────────┬────────────┘
                               │ HTTP
                               ▼
                     Dashboard / External Clients

Shape A: a single telemetry-processor consumes raw-telemetry and fans each
reading into both the hot cache (Redis) and time-series store (Cassandra).
Alerting starts as a separate future consumer group; persistence may later be
split out of the processor into its own consumer if load/failure isolation
demands it (a deliberate "split when it hurts" refactor, not upfront).
```

---

## ✅ Phase 1: Foundations (Complete)

Plain Java 21 Edge Agent — collecting real OS metrics on a local machine.

| Step | Topic                            | Concepts Learned                                                   |
|------|----------------------------------|--------------------------------------------------------------------|
| 1.1  | Maven project setup              | Archetypes, POM, lifecycle, conventions                            |
| 1.2  | Modern POM for Java 21           | Properties, version pinning, dependency hygiene                    |
| 1.3  | Domain modeling: `SensorReading` | Records, compact constructors, fail-fast validation, immutability  |
| 1.4  | Live OS metrics via OSHI         | `OperatingSystemMXBean` vs OSHI trade-offs, CPU tick semantics     |
| 1.5  | Thread-safe collector            | `synchronized`, internal vs documented thread safety               |
| 1.6  | Scheduled sampling               | `ScheduledExecutorService`, fixed-rate vs fixed-delay              |
| 1.7  | Graceful lifecycle               | Shutdown hooks, LIFO order, `Thread.currentThread().join()`        |
| 1.8  | Lock-free start guard            | `AtomicBoolean.compareAndSet`, CAS at the hardware level           |
| 1.9  | SLF4J + Logback                  | Structured logging, parameter placeholders, log levels             |

**Status:** ✅ Edge Agent samples live CPU/memory metrics every 2 seconds with a clean lifecycle.

---

## ✅ Phase 2: Distributed System Fundamentals (Complete)

Build a real wire protocol; transmit telemetry across processes.

### Phase 2.1 — Plain-Java TCP Smoke Server

| Step  | Topic                            | Concepts Learned                                                |
|-------|----------------------------------|-----------------------------------------------------------------|
| 2.1.1 | `ServerSocket`, `Socket`         | TCP as a byte stream, message-boundary problem                  |
| 2.1.2 | Blocking `accept()` and `read()` | Head-of-line blocking, the C10K problem                         |
| 2.1.3 | Try-with-resources for sockets   | File descriptor leaks, `CLOSE_WAIT` vs `TIME_WAIT`              |
| 2.1.4 | Multi-line TCP echo loop         | Backpressure invisible at this level — felt firsthand           |

### Phase 2.2 — Edge Agent Sends Data

| Step  | Topic                          | Concepts Learned                                                |
|-------|--------------------------------|-----------------------------------------------------------------|
| 2.2.1 | Wire format design (JSON v1)   | Locale-safe formatting, NaN/Infinity handling, escape ordering  |
| 2.2.2 | Producer-consumer with queue   | `BlockingQueue`, `ArrayBlockingQueue`, drop-on-full policy      |
| 2.2.3 | Bulkhead isolation             | Why direct calls between sampler and sender are catastrophic    |
| 2.2.4 | `TelemetrySender` v1           | `PrintWriter` + `checkError()` footgun, persistent connections  |
| 2.2.5 | End-to-end integration         | LIFO lifecycle, fail-fast startup, JSON over the wire           |

### Phase 2.3 — Resilience

| Step  | Topic                          | Concepts Learned                                                |
|-------|--------------------------------|-----------------------------------------------------------------|
| 2.3.1 | Reconnect supervisory loop     | Connect → drain → reconnect pattern, infinite outer loop        |
| 2.3.2 | Exponential backoff            | Why naive retry causes "thundering herd"                        |
| 2.3.3 | Jitter                         | Spreading retries randomly to avoid synchronized waves          |
| 2.3.4 | Bounded vs unbounded retry     | Fail-fast at startup, never-give-up at runtime                  |
| 2.3.5 | "Shoot the channel" shutdown   | Closing socket to unblock in-progress I/O from another thread   |

### Phase 2.4 — Protobuf Binary Wire Format

| Step  | Topic                          | Concepts Learned                                                |
|-------|--------------------------------|-----------------------------------------------------------------|
| 2.4.1 | Why Protobuf (real reason)     | Schema evolution, not just speed/size                           |
| 2.4.2 | Toolchain setup                | Maven Protobuf plugin, generated sources, `.proto` files        |
| 2.4.3 | Anti-Corruption Layer pattern  | `SensorReadingCodec` — keep generated types out of the domain   |
| 2.4.4 | Length-prefix framing          | `writeDelimitedTo` / `parseDelimitedFrom`, varint encoding      |
| 2.4.5 | End-to-end Protobuf cutover    | Trust boundaries, `IllegalArgumentException` at the edge        |

### Concurrency Deep Dives (Threaded Through Phase 2)

| Session | Topic                          | Outcome                                                        |
|---------|--------------------------------|----------------------------------------------------------------|
| C-1     | Threads 101                    | Felt the C10K cliff: 4,070 platform threads = OOM              |
| C-2     | Java Memory Model + `volatile` | Felt the JIT optimization that hangs forever                   |
| C-3     | Virtual Threads                | Spawned 1,000,000 virtual threads in 14 seconds, no sweat      |

**Status:** ✅ Real distributed system in plain Java: binary Protobuf over TCP, virtual-thread server, supervised reconnect.

---

## 🔄 Phase 3: Backend Services (In Progress)

Introduce Spring Boot for backend components, add MQTT for device transport,
add Kafka for internal event streaming, and grow the persistence + query layers.

### ✅ Phase 3.1 — Spring Boot Foundations (Complete)

| Step  | Topic                            | Concepts Introduced                                              |
|-------|----------------------------------|------------------------------------------------------------------|
| 3.1.1 | Hello, Spring Boot               | `@SpringBootApplication`, reflection-based DI, lifecycle         |
| 3.1.2 | Spring Integration TCP receiver  | `@Configuration`, `@Bean`, IntegrationFlow, EIP vocabulary       |
| 3.1.3 | Service-layer refactor           | `@Service`, constructor injection, `@Component` scanning         |
| 3.1.4 | Application properties           | `application.yml`, `@ConfigurationProperties`, relaxed binding   |

### ✅ Phase 3.1.5 — MQTT (Replace Raw TCP) (Complete)

| Step    | Topic                              | Concepts Introduced                                            |
|---------|------------------------------------|----------------------------------------------------------------|
| 3.1.5.1 | Run a local MQTT broker + CLI      | Mosquitto via Docker, pub/sub, retained messages, wildcards    |
| 3.1.5.2 | Edge Agent: Paho publisher         | MQTT QoS, topic design, `setAutomaticReconnect`                |
| 3.1.5.3 | Backend Spring Integration MQTT    | `Mqttv5PahoMessageDrivenChannelAdapter`, sticky headers, SpEL  |

### ✅ Phase 3.2 — Multi-Module Refactoring (Complete)

| Step  | Topic                            | Concepts Introduced                                          |
|-------|----------------------------------|--------------------------------------------------------------|
| 3.2.1 | Extract `common` Maven module    | Multi-module POMs, shared `.proto`, shared domain types      |
| 3.2.2 | Eliminate code duplication       | Single source of truth for `SensorReading`, codec, proto     |
| 3.2.3 | Build orchestration              | Parent POM, `<dependencyManagement>`, build order            |
| 3.2.4 | Stable Dependencies Principle    | Framework-free `common`, Dependency Inversion in practice    |

### 🔄 Phase 3.3 — Kafka Pipeline (In Progress)

| Step  | Topic                               | Concepts Introduced                                            | Status |
|-------|-------------------------------------|----------------------------------------------------------------|--------|
| 3.3.1 | Kafka in Docker + CLI               | KRaft mode, listeners, partitions, consumer groups, keys       | ✅     |
| 3.3.2 | Spring Kafka producer               | `KafkaTemplate`, serializers, async sends, acks, backpressure  | ✅     |
| 3.3.3 | Kafka consumer module (`telemetry-processor`) | `@KafkaListener`, `ConsumerFactory`, offset commits            | 🔄     |
| 3.3.4 | Error handling + DLQ                | `DefaultErrorHandler`, retry topics, dead-letter queues        | ⏳     |

### ⏳ Phase 3.4 — Redis (Hot Cache)

| Step  | Topic                          | Concepts Introduced                                          |
|-------|--------------------------------|--------------------------------------------------------------|
| 3.4.1 | Why Redis for telemetry        | Latest-value caching, dashboard hot-path                     |
| 3.4.2 | Run Redis locally              | Docker container, basic CLI exploration                      |
| 3.4.3 | Spring Data Redis              | `RedisTemplate`, key design, TTLs                            |
| 3.4.4 | Cache-aside vs write-through   | Trade-offs, consistency, failure modes                       |
| 3.4.5 | Spring's `@Cacheable`          | Method-level caching abstraction                             |

### ⏳ Phase 3.5 — Cassandra (Time-Series Persistence)

| Step  | Topic                                | Concepts Introduced                                          |
|-------|--------------------------------------|--------------------------------------------------------------|
| 3.5.1 | Why Cassandra over Postgres          | Write-heavy workloads, time-series patterns                  |
| 3.5.2 | Data modeling for queries            | Partition keys, clustering columns, denormalization          |
| 3.5.3 | Run Cassandra locally                | Docker, CQL shell, keyspace setup                            |
| 3.5.4 | Spring Data Cassandra                | Repositories, `@Table`, custom queries                       |
| 3.5.5 | Time-bucketing strategy              | Partitions per device per day, hot-spot avoidance            |
| 3.5.6 | (Optional) PostgreSQL for metadata   | Device registry, user accounts — relational data             |

### ⏳ Phase 3.6 — REST API for Queries

| Step  | Topic                        | Concepts Introduced                                          |
|-------|------------------------------|--------------------------------------------------------------|
| 3.6.1 | Spring Web fundamentals      | `@RestController`, `@RequestMapping`, request lifecycle      |
| 3.6.2 | DTOs vs domain objects       | Separation of API contract from internal model               |
| 3.6.3 | Query endpoints              | `/devices/{id}/metrics?from=...&to=...`                      |
| 3.6.4 | Validation                   | `@Valid`, `jakarta.validation` constraints                   |
| 3.6.5 | Exception handling           | `@ControllerAdvice`, consistent error responses              |
| 3.6.6 | Pagination + filtering       | `Pageable`, query parameters, response envelopes             |
| 3.6.7 | OpenAPI documentation        | `springdoc-openapi`, auto-generated Swagger UI               |

### ⏳ Phase 3.7 — Bidirectional Command Flow

Backend → device commands. Completes the control plane on top of the data plane.

| Step  | Topic                              | Concepts Introduced                                          |
|-------|------------------------------------|--------------------------------------------------------------|
| 3.7.1 | Command topic design               | `commands/{agentId}/{commandType}` structure                 |
| 3.7.2 | Edge Agent subscribes              | Paho subscribe path, command dispatcher                      |
| 3.7.3 | Device registry                    | Tracking known devices via retained `status/*` topics        |
| 3.7.4 | REST endpoint to trigger commands  | Backend → Kafka → Backend → MQTT → device                    |
| 3.7.5 | QoS 1 for commands                 | Why commands need higher guarantees than telemetry           |

---

## ⏳ Phase 4: Production Hardening

The system *works*. Now make it survivable.

### Phase 4.1 — Observability

| Step  | Topic                          | Concepts Introduced                                          |
|-------|--------------------------------|--------------------------------------------------------------|
| 4.1.1 | Micrometer metrics             | JVM metrics, HTTP metrics, custom business metrics           |
| 4.1.2 | Prometheus integration         | `/actuator/prometheus`, scrape config                        |
| 4.1.3 | Grafana dashboards             | Visualizing latency, throughput, error rates                 |
| 4.1.4 | Structured JSON logging        | Logback JSON encoder, log aggregation patterns               |
| 4.1.5 | Distributed tracing            | OpenTelemetry, trace IDs across services                     |
| 4.1.6 | Health checks                  | Spring Actuator `/health`, liveness vs readiness             |

### Phase 4.2 — Security

| Step  | Topic                          | Concepts Introduced                                          |
|-------|--------------------------------|--------------------------------------------------------------|
| 4.2.1 | TLS on MQTT                    | Certificates, mutual TLS for devices                         |
| 4.2.2 | API authentication             | JWT, Spring Security basics                                  |
| 4.2.3 | Authorization                  | Role-based access, device-scoped permissions                 |
| 4.2.4 | Secret management              | Externalized config, Vault basics                            |

### Phase 4.3 — Resilience Patterns

| Step  | Topic                              | Concepts Introduced                                          |
|-------|------------------------------------|--------------------------------------------------------------|
| 4.3.1 | Circuit breakers                   | Resilience4j, failure isolation                              |
| 4.3.2 | Rate limiting                      | Per-device throughput caps                                   |
| 4.3.3 | Bulkheading at the service level   | Thread pool isolation per downstream                         |
| 4.3.4 | Chaos testing                      | Kill -9 the broker, kill the DB, observe behavior            |

---

## ⏳ Phase 5: Deployment & Operations

From "runs on my Mac" to "runs in production."

### Phase 5.1 — Docker

| Step  | Topic                              | Concepts Introduced                                            |
|-------|------------------------------------|----------------------------------------------------------------|
| 5.1.1 | Dockerize the Edge Agent           | Multi-stage builds, JRE base images, layer caching             |
| 5.1.2 | Dockerize transport + processor    | Spring Boot's layered JAR format                               |
| 5.1.3 | Full-stack Docker Compose          | agent + mqtt-transport + telemetry-processor + MQTT + Kafka + Redis + Cassandra |
| 5.1.4 | Image optimization                 | Distroless images, GraalVM native-image basics                 |

### Phase 5.2 — Kubernetes (Optional, If Interest)

| Step  | Topic                              | Concepts Introduced                                          |
|-------|------------------------------------|--------------------------------------------------------------|
| 5.2.1 | Pods, Deployments, Services        | The Kubernetes mental model                                  |
| 5.2.2 | ConfigMaps and Secrets             | Externalized configuration                                   |
| 5.2.3 | Horizontal Pod Autoscaling         | Scaling consumers based on Kafka lag                         |
| 5.2.4 | Stateful services                  | StatefulSets for Cassandra/Kafka                             |
| 5.2.5 | Local cluster (kind or minikube)   | Running the whole stack on your Mac                          |

### Phase 5.3 — CI/CD

| Step  | Topic                          | Concepts Introduced                                          |
|-------|--------------------------------|--------------------------------------------------------------|
| 5.3.1 | GitHub Actions for builds      | Workflow YAML, matrix builds, caching                        |
| 5.3.2 | Automated testing in CI        | Unit tests, integration tests via Testcontainers             |
| 5.3.3 | Image publishing               | Docker Hub or GitHub Container Registry                      |
| 5.3.4 | Deployment automation          | Tagging, release notes, semantic versioning                  |

---

## 🧪 Phase 6: Advanced Topics (Choose-Your-Own-Adventure)

After Phase 5, the system is real. From here, depth or breadth — your call.

### Optional Paths

**Path A: Real-Time Processing**
- Kafka Streams for windowed aggregations
- Anomaly detection on time-series data
- Real-time alerting pipelines

**Path B: Multi-Tenancy**
- Per-tenant isolation strategies
- Topic-per-tenant vs shared topics
- Cassandra keyspace design for tenants

**Path C: Native Image Performance**
- GraalVM native-image build
- Startup time / memory comparison
- AOT compilation trade-offs

**Path D: Reactive Programming**
- Spring WebFlux deep dive
- Project Reactor mental model
- Backpressure in reactive streams

**Path E: Event Sourcing**
- Replay-based architectures
- Event store design
- CQRS patterns

**Path F: Custom Netty MQTT Broker (Deep Dive)**
- Build MQTT broker from scratch on Netty
- Study ThingsBoard's transport layer architecture
- Binary protocol parsing, channel pipelines, at-scale connection management

---

## 🎯 Pedagogical Principles

The roadmap honors these rules:

1. **No concept introduced before it's needed.** Each tool earns its place by solving a problem you've already felt.
2. **Plain Java first, frameworks second.** Master the primitives. Then understand what the framework is hiding.
3. **Tiny digestible steps.** One concept per session, fully internalized, before moving on.
4. **Iterative hardening.** First make it work, then make it survive failure, then make it operational.
5. **Architectural reasoning over recipes.** Every choice has a documented "why," not just a "how."
6. **Real artifacts at every stage.** No throwaway code — everything composes into the final system.
7. **No copy-paste code.** Skeletons and reasoning are provided; the implementation is written by hand.

---

## 📊 Time Estimate

Rough wall-clock hours of focused work per phase:

| Phase                          | Estimated Hours  | Cumulative |
|--------------------------------|------------------|------------|
| Phase 1 (foundations)          | ~6               | 6          |
| Phase 2 (distributed system)   | ~15              | 21         |
| Phase 3 (Spring + ecosystem)   | ~35              | 56         |
| Phase 4 (production hardening) | ~15              | 71         |
| Phase 5 (deployment)           | ~10              | 81         |
| Phase 6 (advanced topics)      | ~10–20 per path  | 91+        |

**By end of Phase 5: a portfolio-grade distributed system with real architectural depth.**

---

## 🚦 Status Tracker

| Phase   | Status         |
|---------|----------------|
| 1       | ✅ Complete    |
| 2.1     | ✅ Complete    |
| 2.2     | ✅ Complete    |
| 2.3     | ✅ Complete    |
| 2.4     | ✅ Complete    |
| 3.1.1   | ✅ Complete    |
| 3.1.2   | ✅ Complete    |
| 3.1.3   | ✅ Complete    |
| 3.1.4   | ✅ Complete    |
| 3.1.5.1 | ✅ Complete    |
| 3.1.5.2 | ✅ Complete    |
| 3.1.5.3 | ✅ Complete    |
| 3.2     | ✅ Complete    |
| 3.3.1   | ✅ Complete    |
| 3.3.2   | ✅ Complete    |
| 3.3.3   | 🔄 In progress |
| 3.3.4   | ⏳ Next        |
| 3.4     | ⏳ Planned     |
| 3.5     | ⏳ Planned     |
| 3.6     | ⏳ Planned     |
| 3.7     | ⏳ Planned     |
| 4+      | ⏳ Future      |
| 5+      | ⏳ Future      |
| 6+      | ⏳ Optional    |

---

*This roadmap is a living document — phases may reorder or expand as the project evolves. The destination is fixed; the route adapts. Update the Status Tracker when a phase completes.*