# Project Development Notes — Part 2

> Continuation of `notes.md`. Covers Phase 3.1+ — multi-module Maven, externalized config,
> Spring Integration, MQTT migration, Kafka introduction, and architect-growth habits.

---

## Table of Contents

1. [Spring Fundamentals (Reformatted)](#1-spring-fundamentals-reformatted)
2. [Design Principles in Practice](#2-design-principles-in-practice)
3. [Multi-Module Maven](#3-multi-module-maven)
4. [Configuration Externalization](#4-configuration-externalization)
5. [Spring Integration & EIP](#5-spring-integration--eip)
6. [MQTT — The IoT Protocol](#6-mqtt--the-iot-protocol)
7. [Kafka — Event Streams](#7-kafka--event-streams)
8. [Engineering Practice Additions](#8-engineering-practice-additions)
9. [Career & Growth Notes](#9-career--growth-notes)

---

# 1. Spring Fundamentals (Reformatted)

## Inversion of Control (IoC)

Inversion of Control is a core architectural design principle where the control of object creation,
configuration, and lifecycle management is transferred from the application code to the framework itself.

You declare; Spring constructs.

## Spring vs. Spring Boot

|                        | Spring (Framework)                                                | Spring Boot                                            |
|------------------------|-------------------------------------------------------------------|--------------------------------------------------------|
| **What it is**         | The IoC/DI engine and its modules (Spring Web, Spring Data, etc.) | An opinionated wrapper *over* Spring                   |
| **What it provides**   | The ApplicationContext, DI, AOP, lots of building blocks          | Auto-configuration, embedded server, sensible defaults |
| **Typical setup time** | Hours of XML or Java config                                       | Minutes — just add starter dependencies                |

## How Spring Instantiates Beans

When we start a Spring Boot application:

```java
SpringApplication.run(MqttTransportApplication.class, args);
```

We pass a **Class object** (`MqttTransportApplication.class`), not an instance.

Spring uses **Java Reflection** to inspect the class and create an object at runtime. Simplified internals:

```java
public static void run(Class<?> primarySource, String... args) {
    // Inspect the class
    Constructor<?> constructor = primarySource.getDeclaredConstructor();

    // Create an instance
    Object instance = constructor.newInstance();

    // Register it in the container
    context.register(instance);

    // Continue bootstrapping...
}
```

This same mechanism is used for all Spring beans. Spring scans for annotations such as `@Service`,
`@Component`, and `@Repository`, inspects their constructors, creates the objects, injects required
dependencies, and stores them in the Application Context.

**Key takeaway:** We declare classes and dependencies; Spring uses reflection to instantiate and wire
them together. This is the core idea behind Dependency Injection (DI) and Inversion of Control (IoC).

## `@SpringBootApplication` — Three Annotations in One

```java
@SpringBootApplication =
@SpringBootConfiguration   // "this class is a config source"
  +@EnableAutoConfiguration   // "look at the classpath and configure things automatically"
  +@ComponentScan             // "find @Component classes in this package and below"
```

**`@EnableAutoConfiguration` is the heart of Spring Boot.** It reads your classpath, sees what's there,
and configures sensible beans automatically.

## Component Scanning

Spring Boot's component scan starts **from the package of the `@SpringBootApplication` class** and includes
all sub-packages. Put your main class in a root package; everything else in sub-packages.

```
com.divyam.telemetry.transport           ← MqttTransportApplication.java (main)
com.divyam.telemetry.transport.config    ← @Configuration classes
com.divyam.telemetry.transport.service   ← @Service classes
```

> **Note:** the module formerly called `backend` was renamed to `mqtt-transport` in the transport-layer refactor. Older prose in these notes may still say "backend" as a generic term for the server side.

## Why a Spring App Without Web Stays Alive (Or Doesn't)

The JVM exits when all non-daemon threads finish. Without `spring-boot-starter-web`, Spring doesn't spin
up a servlet container; the app boots, configures, and exits.

**Anything that keeps the JVM alive (a TCP server, an MQTT subscriber, a Kafka consumer) creates non-daemon
threads.** That's what makes the app long-running.

---

# 2. Design Principles in Practice

## Single Responsibility Principle (Made Physical)

A class should have **one reason to change**. Don't mix:

* Network plumbing with business logic
* Configuration with implementation
* Wire-format details with domain rules

When unrelated concerns share the same file, you get merge conflicts and unclear ownership.

## Configuration Classes vs. Service Classes

* **Configuration files describe wiring.** Their `@Bean` methods produce other beans.
* **Service classes hold logic.** Their public methods do business work.

A class is `@Configuration` if its **primary export is bean definitions**.
A class is `@Service` if its **primary export is callable behavior**.

A configuration class with a constructor dependency is *still configuration* — the dependency is an input
to its bean definitions, not the class's primary contract.

### Why It Matters

* You can write a unit test for `TelemetryIngestionService` without spinning up a TCP server
* Tomorrow when we add Kafka, the same `TelemetryIngestionService` works behind both endpoints
* Each class has one reason to change

This separation is what enterprise Spring code looks like everywhere.

## Stereotype Annotations

Mechanically nearly identical (all meta-annotated with `@Component`), but signal different intent:

| Annotation        | Tells Spring            | Tells humans                                       |
|-------------------|-------------------------|----------------------------------------------------|
| `@Component`      | "Register me as a bean" | "Generic bean — no specific role"                  |
| `@Service`        | "Register me as a bean" | "Business logic lives here"                        |
| `@Repository`     | "Register me as a bean" | "Data access lives here" (+ exception translation) |
| `@Configuration`  | "Register me as a bean" | "I declare other beans via `@Bean` methods"        |
| `@RestController` | "Register me as a bean" | "HTTP endpoints live here"                         |

When you mark a class `@Service`, you're telling the next developer: *"this is where business behavior
lives, not configuration, not HTTP handling, not database queries."* Some tools (like Spring's AOP modules)
treat them differently — `@Repository` adds automatic exception translation, for example.

**Architect's heuristic:** Use the most specific annotation that fits. If it's business logic, `@Service`.
If it's data access, `@Repository`. If it's wiring, `@Configuration`. Generic `@Component` is a fallback
when nothing else fits.

## Dependency Injection — Constructor Injection Only

```java
// 1. Constructor injection — ✓ ONLY use this
@Service
public class MyService {
    private final OtherService other;

    public MyService(OtherService other) {
        this.other = other;
    }
}

// 2. Setter injection — ✗ avoid
@Service
public class MyService {
    private OtherService other;

    @Autowired
    public void setOther(OtherService other) {
        this.other = other;
    }
}

// 3. Field injection — ✗ avoid (still seen in tutorials, still wrong)
@Service
public class MyService {
    @Autowired
    private OtherService other;
}
```

### Why Constructor Injection Wins

1. **`final` fields** — the dependency can't be reassigned later. Genuine immutability.
2. **Fail-fast at construction** — if a required dependency is missing, error at startup, not at first
   invocation.
3. **Testability without Spring** — instantiate in a plain JUnit test with `new MyService(mockOther)`.
4. **No `@Autowired` needed** — Spring infers it from the single constructor (since Spring 4.3).
5. **Explicit dependencies** — the constructor signature is documentation.

## Builder Pattern — Why It Exists

Any time you have:

* Multiple optional pieces of configuration
* An order that matters
* A "complete" moment that produces a final object

**Reach for the builder pattern.** This is *Effective Java*, Item 2, in action.

### The Three Problems It Solves Simultaneously

1. **Evolution** — adding a 10th field doesn't break existing call sites (no constructor explosion)
2. **Optionality** — set only the fields you have; defaults for the rest
3. **Immutable updates** — `original.toBuilder().setX(newX).build()`

A constructor with 10 parameters is hostile to readers and brittle to change.

### Where You See It

Java's `HttpRequest.Builder`, Spring's `WebClient.Builder`, Lombok's `@Builder`, the Stream API's
`Stream.Builder`, Protobuf's generated classes, Spring Integration's `IntegrationFlow.from().transform().handle()`.

## Architectural Seam Test

The mark of clean architecture: **when one layer changes, the rest doesn't notice.**

When we migrated transport from TCP → MQTT:

* `SensorReading` — unchanged
* `SensorReadingCodec` — unchanged
* `SystemMetricsCollector` — unchanged
* `TelemetryAgent` — one line changed
* `TelemetrySender` (TCP) — replaced with `MqttTelemetryPublisher` (~60 lines)

**One layer changed completely. Everything else inherited from the architecture.** This is the payoff
of separation of concerns: business logic doesn't know what transport delivered the message.

## Stable Dependencies Principle

> **Stable dependencies should not depend on volatile ones.**

A `common` module containing domain records (rarely changes) should NOT depend on Spring (changes often,
has CVEs, deprecates APIs). Otherwise every framework upgrade forces a domain rebuild.

**Practical implication:** Shared modules are framework-free. They contain only:

* Domain records
* Codecs (anti-corruption layer)
* Pure utility classes
* `.proto` definitions

## Dependency Inversion (The "D" in SOLID)

Dependency arrows always point toward stability. Volatile concrete code → stable abstractions.

* Edge Agent → common ✅
* Backend → common ✅
* common → Backend ❌ (would pull entire web server into agent)
* Edge Agent ↔ Backend ❌ (circular, won't compile)

The Edge Agent doesn't know the Backend exists. The Backend doesn't know the Edge Agent exists. They
communicate only through `common`'s shared contracts.

## Informed Delegation

You wrote supervisor-with-backoff logic from scratch for the TCP `TelemetrySender`. **Eclipse Paho's
MQTT client provides the same logic built-in via `setAutomaticReconnect(true)`.**

When we migrated to MQTT, you deleted ~100 lines of supervisor code — not because your code was wrong,
but because you now *understood* what the library does and can trust it.

**This is the difference between using a library and being able to verify it.** Senior engineers don't
trust libraries because they have to; they trust them *after* understanding the work the library is doing.

---

# 3. Multi-Module Maven

For projects with multiple related artifacts (shared `common` lib + apps), use a **parent POM**.

## The Parent POM

A POM with `<packaging>pom</packaging>` (no JAR/WAR produced). Purely a coordinator.

```xml

<project>
    <groupId>com.divyam.telemetry</groupId>
    <artifactId>micro-thingsboard-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <modules>
        <module>common</module>
        <module>edge-agent</module>
        <module>mqtt-transport</module>
    </modules>

    <properties>
        <protobuf.version>4.28.2</protobuf.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>com.google.protobuf</groupId>
                <artifactId>protobuf-java</artifactId>
                <version>${protobuf.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

## `<dependencies>` vs `<dependencyManagement>`

| Section                  | What it does                                                         |
|--------------------------|----------------------------------------------------------------------|
| `<dependencies>`         | "I actually use this library, please add it to my classpath."        |
| `<dependencyManagement>` | "If anyone in this hierarchy uses this library, here's the version." |

The parent uses `<dependencyManagement>` to declare *available* versions. Children use `<dependencies>` to
*actually pull* the libraries — without specifying a version.

**Single source of truth**: change a version in the parent, every child gets it.

## Child Module POM

```xml

<project>
    <parent>
        <groupId>com.divyam.telemetry</groupId>
        <artifactId>micro-thingsboard-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>edge-agent</artifactId>
    <packaging>jar</packaging>

    <dependencies>
        <!-- No version — inherited from parent -->
        <dependency>
            <groupId>com.divyam.telemetry</groupId>
            <artifactId>common</artifactId>
        </dependency>
    </dependencies>
</project>
```

* `<relativePath>../pom.xml</relativePath>` points to parent
* No `<groupId>` or `<version>` — inherited
* Dependencies omit version — inherited from `<dependencyManagement>`

## Spring Boot BOM Import Pattern

When you can't use `spring-boot-starter-parent` (because you already have a custom parent), use BOM import:

```xml

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-dependencies</artifactId>
            <version>3.3.5</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Now any child can declare Spring starters without specifying versions. **This is the canonical workaround**
when you need Spring Boot's version management but already have your own parent POM.

## Toolchain Alignment

**Compiler and runtime versions must move together.**

This applies to:

* Protobuf compiler (`protoc`) ↔ Protobuf runtime (`protobuf-java`)
* TypeScript compiler ↔ TypeScript types library
* gRPC plugin ↔ gRPC runtime
* Java compiler bytecode target ↔ JVM version

Centralize both in parent POM's `<properties>` so they're physically the same value:

```xml

<properties>
    <protobuf.version>4.28.2</protobuf.version>
</properties>
```

And reference everywhere:

```xml

<protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
```

---

# 4. Configuration Externalization

## Layered Configuration Hierarchy

Modern apps resolve config through layers, later sources overriding earlier:

```
Priority (lowest → highest):
  1. Defaults in code (final fallback)
  2. application.yml in src/main/resources
  3. application-{profile}.yml (profile-specific)
  4. External application.yml in the working directory
  5. Environment variables (MQTT_BROKER_URL=...)
  6. JVM system properties (-Dmqtt.broker.url=...)
  7. Command-line arguments (--mqtt.broker.url=...)
```

**Why:** same artifact, environment-specific overrides. Dev local. Kubernetes via env vars. CI via CLI.
Code never knows. This is the foundation of 12-factor app deployment.

## Spring's `@ConfigurationProperties` (Recommended)

For grouped, type-safe configuration:

```yaml
mqtt:
  broker:
    url: tcp://localhost:1883
    clientId: backend-01
    topic: telemetry/#
```

```java

@ConfigurationProperties(prefix = "mqtt.broker")
public record MqttBrokerProperties(String url, String clientId, String topic) {
}
```

Enable in the main app:

```java

@SpringBootApplication
@EnableConfigurationProperties(MqttBrokerProperties.class)
public class MqttTransportApplication { ...
}
```

### Benefits Over `@Value`

* **Type-safe** — `port` is `int`, not a string parsed at every use
* **Grouped** — related settings in one record
* **Validated** — add `@Validated` + `jakarta.validation` annotations for fail-fast checks
* **IDE-friendly** — autocomplete in YAML via metadata processor

## `@Value` — For One-Off Values

```java

@Value("${server.tcp.port:5555}")
private int port;
```

Reads: *"give me `server.tcp.port`, defaulting to `5555`."*

**Anti-pattern when you have 5+ related knobs** — use `@ConfigurationProperties` instead.

## Relaxed Binding

Spring auto-translates between formats:

| Format          | Example             |
|-----------------|---------------------|
| YAML/properties | `mqtt.broker.url`   |
| Environment var | `MQTT_BROKER_URL`   |
| Command-line    | `--mqtt.broker.url` |
| System property | `-Dmqtt.broker.url` |

All four resolve to the same property. **Zero code on your side.**

## Plain Java Configuration (No Spring)

For modules without Spring (like the Edge Agent), implement layered resolution manually:

```java
private static String resolve(String key, Properties fileProps) {
    String fromSystem = System.getProperty(key);
    if (fromSystem != null) return fromSystem;

    String envKey = key.toUpperCase().replace('.', '_');
    String fromEnv = System.getenv(envKey);
    if (fromEnv != null) return fromEnv;

    String fromFile = fileProps.getProperty(key);
    if (fromFile == null) {
        throw new IllegalStateException("Required configuration missing: " + key);
    }
    return fromFile;
}
```

**Lesson:** ~70 lines of manual code reproduces what Spring's annotation gives in 1 line.

### Framework Trade-off

Spring's abstraction saves you boilerplate AND gives you battle-tested operational features
(profile-aware overrides, refresh, validation, IDE metadata).

The plain Java version gives you raw flexibility — you control every byte of the config pipeline.

**Frameworks trade flexibility for power.** When defaults match your needs, you win. When they don't,
fighting the framework costs more than writing it yourself. The architect's question:
*"Are my needs going to match the framework's opinions, today and tomorrow?"*

---

# 5. Spring Integration & EIP

## Enterprise Integration Patterns (EIP)

Spring Integration is based on **EIP** — a 20-year-old paradigm where applications are described as
**pipelines of message flows**. Same vocabulary used by Apache Camel, Mule, IBM MQ.

## From Loops to Pipelines

**Imperative networking code (plain Java):**

```
while (true):
    socket = accept()
    spawn virtual thread:
        while (data available):
            parse the next message
            log it
```

You write control flow. Loops, threads, lifecycle — all hand-managed.

**Spring Integration:**

```
[Inbound Adapter (TCP / MQTT / Kafka)]
     ↓ produces "messages"
[Transform 1: deserialize]
     ↓
[Transform 2: convert to domain]
     ↓
[Handle: do something]
```

You describe a pipeline. Spring runs the loops and threads internally. Each stage is a bean.

## Building an IntegrationFlow

```java

@Bean
public IntegrationFlow inboundFlow(InboundAdapter adapter) {
    return IntegrationFlow.from(adapter)
            .transform(payload -> deserialize(payload))
            .transform(payload -> toDomainObject(payload))
            .handle(message -> service.process(message))
            .get();
}
```

### Methods

* **`from(...)`** — where messages enter (the source)
* **`transform(...)`** — converts payload from one shape to another (preserves headers)
* **`handle(...)`** — does something with each message (often terminal)
* **`enrichHeaders(...)`** — adds/modifies headers without touching payload
* **`get()`** — terminator that builds the flow

## The `Message<T>` Envelope

Every message in a flow is wrapped in `Message<T>`: a **payload** plus **headers** (metadata).

```
Message<byte[]>
├── payload: byte[] (the actual data)
└── headers: Map<String, Object>
    ├── id
    ├── timestamp
    ├── mqtt_receivedTopic
    └── ...
```

## Sticky Headers — A Crucial Property

**When `transform()` produces a new payload, Spring Integration builds a new `Message` with that payload
but copies the original message's headers.**

Headers **survive transforms by default**. Payloads change; metadata persists.

This means: extract metadata into headers early in the flow, and it's available at every later stage.

This is genuinely **a good architectural property of Spring Integration**: routing/metadata stays with
the message as it flows through stages, even when the payload type changes radically.

## Reading Headers vs. Payload

| Method form                          | What the lambda receives |
|--------------------------------------|--------------------------|
| `.transform(payload -> ...)`         | Payload only             |
| `.handle((payload, headers) -> ...)` | Both payload and headers |

**To use the two-argument form on `.handle()`:**

```java
.handle((GenericHandler<MyType>)(payload,headers)->{
String topic = (String) headers.get("mqtt_receivedTopic");
// do work
    return null;   // terminal handler returns null
            })
```

The explicit cast is needed because Java's lambda type inference can't pick between `.handle()` overloads.

## Enriching Headers with SpEL

Use `enrichHeaders` to add metadata before the payload type changes:

```java
.from(adapter)
.

enrichHeaders(spec ->spec.

headerExpression("mqtt_qos","payload.qos"))
        .

transform(MqttMessage::getPayload)   // payload type changes; headers persist
.

handle((payload, headers) ->{
int qos = (int) headers.get("mqtt_qos");
// ...
});
```

`"payload.qos"` is a **SpEL expression** — evaluated against the current message. Equivalent to calling
`payload.getQos()`.

SpEL expressions are **string-based** — no compile-time type checking. If you misspell, you find out at
runtime. The recurring cost of declarative frameworks: concise, but loss of type safety.

## Error Channels

Configure an explicit error channel for the adapter:

```java

@Bean
public MessageChannel errorChannel() {
    return new DirectChannel();
}

@Bean
public IntegrationFlow errorFlow() {
    return IntegrationFlow.from("errorChannel")
            .handle(message -> {
                if (message instanceof ErrorMessage em) {
                    Throwable cause = em.getPayload();
                    log.error("Flow failure: {}", cause.getMessage());
                }
            })
            .get();
}

// In the adapter setup:
adapter.

setErrorChannelName("errorChannel");
```

**Why:** by default, Spring Integration silently drops messages on exceptions. An explicit error channel
makes failures visible and routable (logs, metrics, DLQ).

## Why Stage Transforms Instead of One Big Handler

Stuffing everything into one `.handle()` is tempting but loses:

* Per-stage error handling
* Per-stage logging/tracing capability
* The ability to insert a transformation (e.g., validation, filtering) without restructuring
* Testability of individual stages

**Spring Integration's value proposition is staged pipelines.** Use them.

## `@Configuration` Classes with `@Bean` Methods

A new way to declare beans. Instead of `@Component` on a class (which says *"instantiate me"*), you write
a `@Configuration` class with `@Bean` methods that **manually construct and return objects** for Spring
to manage.

```java

@Configuration
public class MyConfig {

    @Bean
    public SomeThing aSomeThing() {
        return new SomeThing("custom-config");
    }
}
```

When Spring boots, it calls `aSomeThing()`, takes the returned object, and registers it.

**When to use which?**

* `@Component` — you own the class, simple instantiation
* `@Bean` in `@Configuration` — third-party class, complex construction, multiple configured instances

For Spring Integration, we use `@Bean` because the adapters and factories come from the framework — we
can't add `@Component` to library classes.

---

# 6. MQTT — The IoT Protocol

## The Mental Model: Pub/Sub via a Broker

```
                ┌─────────────────────┐
                │   THE BROKER        │
                │  (e.g., Mosquitto)  │
                │                     │
                │   topic: weather/temp │
                │   topic: telemetry/...│
                └─────┬─────┬─────┬───┘
                      │     │     │
              publishes│     │subscribes
                      │     │     │
              ┌───────▼─────▼─────▼────┐
              │                        │
         Device A    Device B    Dashboard
        (publisher)  (publisher) (subscriber)
```

**Three roles:**

* **Publishers** post messages to a *topic* (named bulletin board section)
* **Subscribers** listen to topics that interest them
* **The broker** receives every message and routes to subscribers

**Critical:** Publishers and subscribers **never know each other exist**. Total decoupling — devices
added a year from now will be heard by your dashboard without changing a single line of dashboard code.

## Why MQTT for IoT

MQTT was designed for satellite-monitored oil pipelines in 1999. **Constrained networks, intermittent
connectivity, tiny devices.** The exact environment IoT runs in today.

| Property                 | TCP raw | HTTP        | MQTT                  |
|--------------------------|---------|-------------|-----------------------|
| Persistent connection    | DIY     | Per-request | Yes, by design        |
| Header overhead/msg      | None    | ~200 bytes  | ~2 bytes              |
| Delivery guarantees      | None    | App-level   | Built-in QoS 0/1/2    |
| Auto-detect dead clients | No      | No          | Last Will & Testament |
| Fan-out (1 → N)          | No      | No          | Native via topics     |
| Retained messages        | No      | No          | Yes                   |

## Topic Structure Design

Topics are forward-slash-separated strings forming a hierarchy. Convention: design like URL paths.

**Topic design is a 5-year architectural commitment.** There's no `301 Moved Permanently`. Once devices
in the field are publishing to `telemetry/cpu`, you can't migrate them without either:

1. Updating every device (impossible for IoT)
2. Running a bridge that mirrors old topics to new ones
3. Living with the old name forever

**Example structure for Micro-ThingsBoard:**

```
telemetry/{deviceId}/{category}/{specificMetric}

Examples:
  telemetry/mac-edge-01/cpu/load.percent
  telemetry/mac-edge-01/mem/used.bytes
```

### Five Forces Shaping Topic Design

1. **Routability via wildcards** — multiple consumers express interest with a single subscription
2. **Cardinality awareness** — don't put timestamps/IDs in the topic (millions of topics is bad)
3. **Forward extensibility** — accommodate new metric types without restructuring
4. **Multi-tenancy future-proofing** — reserve top-level slot if you might ever multi-tenant
5. **Read at a glance** — optimize for 3 AM debugging, not byte savings

### Wildcards

| Wildcard | Matches                         | Example                                                                       |
|----------|---------------------------------|-------------------------------------------------------------------------------|
| `+`      | Exactly one level               | `telemetry/+/cpu/load.percent` matches one specific metric across all devices |
| `#`      | Any number of levels at the end | `telemetry/#` matches everything under that prefix                            |

**Critical asymmetry:** `+` works at any position. `#` only valid at the very end.

**Neither wildcard does in-level pattern-matching.** `mem*` is not a thing. A wildcard covers an entire
level or none of it.

### Subscription Examples

| Use case                          | Subscription                  |
|-----------------------------------|-------------------------------|
| Backend: see everything           | `telemetry/#`                 |
| Dashboard for one device          | `telemetry/mac-edge-01/#`     |
| All CPU metrics across devices    | `telemetry/+/cpu/+`           |
| All memory metrics for one device | `telemetry/mac-edge-01/mem/+` |

### Don't Put These in Topics

* ❌ The actual value (`telemetry/cpu/85.4`) — that's payload
* ❌ The timestamp (`telemetry/cpu/2026-06-04...`) — payload
* ❌ Sequence numbers — payload
* ❌ Spaces, special chars — keep lowercase alphanumeric + dots/hyphens

### Reserved Top-Level Slots

The top-level segment reserves space for sibling channels:

```
telemetry/{agentId}/...                ← device → backend (data plane)
commands/{agentId}/{commandType}       ← backend → device (control plane, future)
events/{agentId}/{eventType}           ← device → backend alerts (future)
status/{agentId}                       ← device heartbeat (future, retained)
```

**Past-you designing topics is planning for future-you adding bidirectional flows.**

## Quality of Service (QoS)

| QoS   | Meaning                                | Use case                                   |
|-------|----------------------------------------|--------------------------------------------|
| **0** | Fire and forget. Message may be lost.  | High-frequency telemetry where loss is OK  |
| **1** | At least once. May be duplicated.      | Important events; receiver handles dedup   |
| **2** | Exactly once. Slow, complex handshake. | Financial transactions, never use casually |

**For telemetry, QoS 0 is the norm.** Production IoT uses QoS 0 for telemetry, QoS 1 for control commands.

## Retained Messages

Publish with the retained flag, and the broker keeps the **last** message per topic — delivering it to
any future subscriber on first subscribe.

```bash
mosquitto_pub -h localhost -p 1883 -t "status/agent-A" -m "online" -r
```

Perfect for "last known state" use cases — dashboards loading the current value on page load.

## Last Will & Testament

The killer feature for IoT. When the client connects, it pre-registers a "will" message. If the broker
detects the client lost connection unexpectedly, **the broker publishes the will message on the client's
behalf**.

Use case: backend auto-detects device offline without polling.

## What Happens to Messages Without Subscribers

The broker accepts publishes independently of subscribers. Messages sent to a topic with no subscribers
are typically **discarded** after the broker finds no matching subscriptions.

**Exception:** retained messages. The broker keeps **one** per topic — the most recent — for late-arriving
subscribers. Everything else evaporates.

**The architectural lesson:** *MQTT is a routing fabric, not a queue.* If you need durable queuing
(replay, multiple consumers reading at different speeds, weeks of history) — that's **Kafka territory**.

## Eclipse Paho — The Java MQTT Client

The standard library. Two flavors:

* **`mqttv5`** — modern, supports MQTT 5.0 (preferred for new code)
* **`mqttv3`** — universal, supports MQTT 3.1.1

### What Paho Gives You (vs hand-written)

| Concern                        | Hand-written code          | Paho                          |
|--------------------------------|----------------------------|-------------------------------|
| Connect on startup             | `tryConnectWithBackoff`    | `client.connect()`            |
| Exponential backoff reconnect  | Hand-written supervisor    | `setAutomaticReconnect(true)` |
| Persistent connection          | Manual socket holding      | Default                       |
| Local outbox during disconnect | `BlockingQueue`            | Paho's internal queue         |
| Disk-persistent outbox         | N/A                        | `MqttDefaultFilePersistence`  |
| Detect dead broker             | `PrintWriter.checkError()` | Keep-alive heartbeats         |
| Graceful shutdown              | Manual interrupt + join    | `client.disconnect()`         |

### Connection Options Pattern

```java
MqttConnectionOptions options = new MqttConnectionOptions();
options.

setAutomaticReconnect(true);     // Paho's supervisor
options.

setCleanStart(true);
options.

setKeepAliveInterval(30);        // ping every 30s
options.

setConnectionTimeout(10);        // give up initial connect after 10s
```

## MQTT Broker Landscape

| Broker        | Origin                   | Sweet Spot                                   |
|---------------|--------------------------|----------------------------------------------|
| **Mosquitto** | Eclipse Foundation, 2009 | Lightweight, dev/learning, small deployments |
| **HiveMQ**    | HiveMQ GmbH, 2012        | Enterprise IoT, massive scale, paid features |
| **EMQX**      | EMQ, 2013                | High-throughput, clustering                  |
| **VerneMQ**   | Erlang-based             | Distributed, fault-tolerant                  |

For dev: **Mosquitto via Docker** is the standard choice — small, fast, no nonsense.

## Netty vs. MQTT Brokers (Clarification)

**Netty is NOT an MQTT broker.** It's a high-performance Java networking library.

Some MQTT brokers are **built using Netty internally** as their networking layer:

* HiveMQ — uses Netty
* Moquette — a Java MQTT broker built on Netty
* Mosquitto — written in C, doesn't use Netty
* EMQX — uses Erlang, not Netty

**ThingsBoard's transport layer is built on Netty directly.** They implement MQTT, HTTP, CoAP protocols
themselves on top of Netty for:

* Scale (hundreds of thousands of concurrent connections per node)
* Protocol unification (one networking foundation for all transports)
* Custom protocol quirks (non-standard auth, custom QoS interpretations)
* Single JVM footprint (no separate broker container)

For our scale, off-the-shelf Mosquitto + Paho + Spring Integration MQTT is the right choice. Custom
Netty brokers are a possible Phase 6 deep-dive.

---

# 7. Kafka — Event Streams

## What Kafka Solves That MQTT Cannot

MQTT delivers messages and forgets them. **Kafka:**

* Stores messages durably on disk (hours, days, years)
* Supports replay from any past offset
* Allows multiple independent consumers reading the same stream at different speeds
* Provides automatic load balancing within consumer groups

## MQTT vs Kafka — Different Tools

| Property        | MQTT                                         | Kafka                                       |
|-----------------|----------------------------------------------|---------------------------------------------|
| Primary purpose | Device-to-server messaging                   | Event stream backbone                       |
| Retention       | Drops messages once delivered                | Stores everything on disk                   |
| Consumer model  | Subscribers receive live messages            | Consumers can replay from any past offset   |
| Scale model     | Hundreds of thousands of clients             | Millions of msgs/sec through few partitions |
| Failure model   | Broker dies → in-flight messages may be lost | Replicated; data survives node failure      |
| Mental model    | Postal mailbox                               | Append-only log file                        |

**MQTT is a routing fabric. Kafka is a durable log.** Complementary, not interchangeable.

**The architecture:** MQTT for device ingestion, Kafka for backend-internal pipelines.

## Core Concepts

### Brokers and Topics

A Kafka **cluster** is one or more **brokers** (servers) storing data. Data is organized into **topics**
(named streams). Analogous to tables.

### Partitions

Each topic is split into **partitions** — independent append-only logs. This is Kafka's scaling unit.

```
Topic: raw-telemetry
├── Partition 0: [msg1, msg2, msg5, ...]
├── Partition 1: [msg3, msg6, msg8, ...]
└── Partition 2: [msg4, msg7, msg9, ...]
```

When publishing, the producer picks a partition based on the **message key**. Same key → same partition →
guaranteed ordering for that key.

#### Leaders and Followers:

* Replication & Leaders: For fault tolerance, partitions are copied across brokers. One broker acts as the "Leader" for
  a partition (handling all reads/writes), while others are "Followers" (quietly backing up the data in
  case the leader crashes).

**Architectural implication:** choose your key carefully.

* Key by `agentId` → all telemetry from one device is ordered (but devices may share partitions)
* No key → random distribution, no per-device ordering

### Producers, Consumers, Offsets

* **Producer**: sends messages to a topic
* **Consumer**: reads messages from a topic
* **Offset**: a number identifying each message's position in a partition

When a consumer reads, it tracks how far it's gotten via its **committed offset**. A new consumer can start
from offset 0 and replay everything — **the durable-log superpower**.

### Consumer Groups

Multiple consumer instances can share work via a **consumer group**. Kafka distributes partitions among
group members.

```
Consumer group "cassandra-writers"
   ├── Consumer A → reads partition 0
   ├── Consumer B → reads partition 1
   └── Consumer C → reads partition 2
```

* Add a 4th consumer → it sits idle (no partitions to assign)
* Remove one → another picks up its partition
* **Automatic, dynamic load balancing**

**Each consumer group has its own offsets.** So `cassandra-writers` can be at offset 1000 while
`alerting-consumers` is at offset 800 — both reading the same topic, at different speeds.

**This is the fan-out property MQTT couldn't give us.**

### Serialization

Kafka stores bytes. You choose the serialization. Use Protobuf for the same reasons as the wire protocol —
schema evolution.

## Kafka in KRaft Mode

Historically, Kafka relied on Zookeeper for:

* Cluster State: Keeping track of which brokers are part of the cluster.
* Controller Election: Choosing which broker is the "boss" (Controller) to manage partition states.
* Topic Metadata: Storing details about topics, partitions, and where their data lives.
* Leader Election: Deciding which broker handles reads/writes for a specific partition.

The Shift: Modern Kafka (version 3.3+) has introduced **KRaft** mode, which removes the need for Zookeeper entirely by
handling metadata internally.

* KRaft makes Kafka much easier to run (you only need to manage one type of server instead of two).
* KRaft allows Kafka to support millions of partitions, whereas Zookeeper struggled at scale.
* Most new deployments now run without Zookeeper.Single-broker Docker setups are now straightforward.

### Advertised Listeners (The Tricky Config)

`KAFKA_ADVERTISED_LISTENERS` tells clients how to reach the broker. Get it wrong and clients get
"connection refused" even though the broker is up.

For local Docker:

* Inside the container, brokers use the internal Docker network
* From the host, clients connect via `localhost:9092`
* Both need to be advertised correctly — typically two listeners (one internal, one external)

## Kafka CLI Tools

Install via Homebrew:

```bash
brew install kafka
```

### Common Commands

```bash
# Create a topic
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic raw-telemetry \
  --partitions 3 --replication-factor 1

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Publish messages
kafka-console-producer --bootstrap-server localhost:9092 --topic raw-telemetry

# Consume from beginning
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw-telemetry --from-beginning

# Consume in a consumer group
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw-telemetry --group "test-group"
```

## Architecture: Where Kafka Sits

```
              MQTT                              Kafka
   Edge   (live routing)   Backend   (durable storage)
   Agent ─────────────────► (Spring) ──────────────────► Topic
                                                          │
                                              ┌───────────┼───────────┐
                                              ▼           ▼           ▼
                                         Cassandra     Redis      Alerting
                                         consumer    consumer    consumer
```

The backend's job becomes: **receive from MQTT, persist to Kafka.** Everything downstream becomes a
separate Kafka consumer — scales independently, fails independently, evolves independently.

**This is event-driven architecture in practice.**

## One Data Plane at a Time

> **Build one data plane at a time. Don't try to build pub/sub for both directions simultaneously.**

Reason: each direction has different reliability requirements, different scale characteristics, different
consumers. Mixing them in a single design produces something that's mediocre for both.

* **Telemetry pipeline** = high volume, loss-tolerant (QoS 0), append-only, fan-out to many consumers
* **Command pipeline** = low volume, loss-intolerant (QoS 1+), request-response patterns, single-target

Different beasts. Built separately. Composed at the system level.

---

# 8. Engineering Practice Additions

## Self-Review Before Submitting

Before sending code for review, run through this mental checklist:

1. Does every comment/Javadoc accurately describe the current code?
2. Are there any hardcoded values that should be config?
3. Does each method do one thing, or is it doing too much?
4. If a teammate reads this in 6 months, what will confuse them?

**The skill of finding your own bugs before someone else does is what architects refine over years.**

## Verify Your Own Work

It's not enough that code compiles and runs — verify each behavior you added actually does what you
intended. If you add topic logging, confirm the topic is logged with a real value, not `null`.

## Pushing Back on a Reviewer Who's Wrong

When a reviewer makes a claim that contradicts what you've verified at runtime, **say so**. Evidence
beats authority.

The correct posture: *"I ran the code and the behavior is X, not Y. Here's the log line."* Direct,
evidence-based, no apology. **That's how senior engineers respond to reviewers who are wrong.**

Reasoning from general principles is useful, but it must be checked against runtime reality. **Reviewers
make mistakes. Trust the code, not the reviewer.**

## Git Discipline

### Commit at Stable Points

Commit working state before cleanup. Each commit should tell a story; each piece of work should be a
separate, reviewable, revertable unit.

**Architect's rule:** *"Commit at every stable point. Cleanup is a separate commit from features."*

### Commit Message Style (Title + Body)

```
Brief title in imperative mood, under 72 chars

Body explains WHAT changed and WHY (not HOW — the diff shows that).
Wrap at 72 chars. Use bullet points for multi-part changes.
```

**Architect's habit:** always `git diff` before committing. Reading the diff catches accidents — random
debug `println`, commented-out tests, unrelated whitespace changes.

**Reading the diff before committing is the single best habit in git.**

### Don't Commit Generated/Local Files

* `target/` — regenerated on every build
* `.idea/` — IDE-specific
* `*.log` — runtime artifacts
* `*.iml` — IntelliJ module files

Add to `.gitignore` before they're tracked. If already tracked, use `git rm --cached`.

## Naming Things

> *File names and class names are documentation.*

A file named `App.java` tells the reader nothing; a file named `EdgeAgent.java` tells the reader exactly
which program they're looking at. Small consistency wins matter in a multi-module repo.

## Optionality vs. YAGNI

When designing for the future, the question is:

> *Will my needs match the framework's opinions, today and tomorrow?*

* **Preserve optionality** when the cost is small (e.g., reserving a top-level MQTT topic slot for future
  command flow — zero cost today, painful to retrofit)
* **Don't pre-build** for hypothetical needs (premature abstraction is its own kind of pain)

## Lying Docs Are Worse Than No Docs

When you change a class's architecture, update the class-level Javadoc the same commit. Future-you reading
the doc will trust it.

Stale comments train teammates to ignore comments. Lingering FIXMEs train teammates to ignore FIXMEs.
Update or remove. Don't accumulate cruft.

---

# 9. Career & Growth Notes

## Architect Work vs. Senior Developer Work

The trap most engineers fall into: waiting for a *title change* before doing architect work. **Backwards.**
Inside companies, the path is:

1. Start doing architect-shaped work in your current role
2. Become visibly good at it over 6-18 months
3. The title catches up to the work, not the other way around

Companies promote people who are *already operating* at the next level. They don't promote you to a
level and *then* hope you grow into it.

## Concrete Habits to Start Doing

### Write Design Docs Before Writing Code

For features bigger than a day's work, write a 1-page doc:

* **Problem:** what are we solving?
* **Options considered:** 2-3 alternatives
* **Chosen approach:** which and why
* **Trade-offs:** what we give up
* **Open questions:** what we don't know yet

Send to a senior engineer **before** coding. Even if nobody asks.

Trains you to think before typing (the core architect skill). Creates artifacts of your design reasoning.
Surfaces issues at design time, not after a sprint of wasted code.

### Ask "What If?" Questions in Code Reviews

Beyond bug-spotting, ask architectural questions:

* "What happens if this network call times out at 30 seconds?"
* "Will this break if we deploy two instances behind a load balancer?"
* "This works for our current traffic — at 10x, where does it break first?"
* "What's the rollback plan if this goes wrong in production?"

These are the questions architects ask reflexively. **Asking them publicly trains your team to think
this way too — that's the multiplier effect of an architect.**

### Volunteer for Messy Cross-Cutting Problems

Every team has a *thing nobody wants to own*: flaky test suite, slow build, scary deployment, legacy
module, the integration with the awful third-party API.

**Volunteer for these.** They're undervalued because they're not feature work, but they're exactly where
architect-shaped impact lives. Fix the build → save 30 hours of team time per week. That's a level-up's
worth of impact in one project.

### Document Tribal Knowledge

* Service architecture diagrams
* Runbooks for common production issues
* Architectural Decision Records (ADRs)
* "How this system actually works" docs

These are **artifacts of architect work** that exist after you finish them. Future you, future teammates,
future tech leads all reference them. **This is the visible scope.**

### Connect Systems in Your Head

Most engineers see their team's services. Architects see *how their team's services interact with other
teams' services*. So:

* Read other teams' design docs
* Understand the data flow upstream and downstream of your service
* Attend cross-team architecture reviews even if you're not asked to
* When something breaks in production, trace it across team boundaries

You start being the person in meetings who says *"wait, doesn't that conflict with what the Payments team
is doing?"* That single behavior is what makes someone visible as architect material.

### Find a Mentor Already at the Level

Look around your company for an architect or staff engineer whose work you respect. Ask if they'd be
willing to do a 30-minute coffee chat once a month. Just listen and ask questions.

Most senior people **want** to mentor — they just don't get asked. The ask itself signals you're serious
about growth.

## Three Watch-Outs

### 1. "Architect Work" Doesn't Always Get Rewarded

Some companies say they want architects but actually want senior coders. If you start writing design docs
and nobody reads them, **that's a signal about your company's culture**, not your skill. Not every
environment supports architect growth equally.

### 2. Don't Lose Your Hands-On Edge

The best architects still write code. They don't write *production features* daily, but they prototype,
they investigate, they pair-program with juniors. If "becoming an architect" means you stop touching code
entirely, your technical depth atrophies and your designs drift from reality.

### 3. Architecture Is Not Just Technical

The hardest part of the architect role isn't picking the right database. It's **convincing five engineers
to agree on something they all have opinions about**. The technical skill is the entry ticket; the
influence skill is what makes architects effective.

## The "No Copy-Paste" Learning Contract

For deep architectural skill-building, the rule is:

| What's allowed                   | What's not                       |
|----------------------------------|----------------------------------|
| Read the architectural reasoning | Copy-paste working code          |
| Research libraries via Javadoc   | Receive complete implementations |
| Get skeletons with TODOs         | Get pre-built solutions          |
| Get code reviews                 | Skip the writing step            |

**Friction is the learning.** A phase that takes one session with copy-paste takes 2-3 with this contract.
The slower path produces durable understanding; the faster path produces beautiful guided tourism through
a codebase.

The gap between *recognizing* good architecture and *producing* it from a blank screen is the gap a
learner walks by *generating* code, not by *reading* code.

---

*This is part 2 of the project notes. See `notes.md` for foundational sections (Maven basics, Java
language features, concurrency, networking, reliability patterns, Protobuf, logging discipline).*
