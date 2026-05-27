# Project Development Notes — Micro-ThingsBoard

> A learning journal: a lightweight telemetry engine in plain Java 21 (Edge Agent) + Spring Boot (Backend).

**Project status:** Phase 2 complete. End-to-end Protobuf telemetry pipeline with virtual-thread server,
exponential-backoff reconnect, and producer-consumer bulkheading. Phase 3 (Spring Boot backend) up next.

---

## Table of Contents

1. [Build & Tooling](#1-build--tooling)
2. [Java Language Features](#2-java-language-features)
3. [Concurrency Fundamentals](#3-concurrency-fundamentals)
4. [Concurrency Primitives](#4-concurrency-primitives)
5. [Threads: Platform & Virtual](#5-threads-platform--virtual)
6. [Networking & Sockets](#6-networking--sockets)
7. [Reliability Patterns](#7-reliability-patterns)
8. [Distributed System Patterns](#8-distributed-system-patterns)
9. [Serialization (Protobuf)](#9-serialization-protobuf)
10. [Engineering Practice](#10-engineering-practice)
11. [Phase 3 (Future): Spring Boot](#11-phase-3-future-spring-boot)

---

# 1. Build & Tooling

## Maven Fundamentals

Think of Maven as a toolbox for building Java projects. It hammers nails. It does the job.

**Analogy:** Like IKEA furniture. Rigid instructions (XML), predictable, boring in a good way. The industry
standard in enterprise Java.

## Project Initialization

To scaffold a basic Java app:

```bash
mvn archetype:generate \
  -DgroupId=com.divyam.telemetry \
  -DartifactId=edge-agent \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DarchetypeVersion=1.5 \
  -DinteractiveMode=false
```

### Archetype Parameters

An **archetype** is a project template — a pre-built skeleton with folders, a starter `pom.xml`, and an example file.

* **groupId:** Reverse-domain name (e.g., `com.divyam.telemetry`). Acts as a Java "namespace."
* **artifactId:** The name of this specific project (e.g., `edge-agent`).
* **archetype:** The template name. We use Maven's official `quickstart` starter.

### The pom.xml

The **Project Object Model (POM)** is the build "recipe." It lists dependencies, Java versions, and plugins.
Think of it as a restaurant's master inventory and supplier list.

## Maven Lifecycle Commands

| Command                                                                              | Purpose                  |
|--------------------------------------------------------------------------------------|--------------------------|
| `mvn clean`                                                                          | Wipe old build output    |
| `mvn compile`                                                                        | Compile sources          |
| `mvn test`                                                                           | Run unit tests           |
| `mvn exec:java -Dexec.mainClass="com.divyam.telemetry.App"`                          | Run a main class         |
| `mvn clean compile exec:java -Dexec.mainClass="com.divyam.telemetry.App"`            | Combined dev loop        |

---

# 2. Java Language Features

## Records (Java 14+)

Records are immutable data carriers. The compiler auto-generates the constructor, accessors, `equals`,
`hashCode`, and `toString`.

```java
public record SensorReading(String sensorId, double value, Instant timestamp) {
    // Compact constructor — runs validation BEFORE field assignment
    public SensorReading {
        if (sensorId == null || sensorId.isBlank()) {
            throw new IllegalArgumentException("sensorId must not be blank");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
    }
}
```

**Key properties:**

* **Immutable by default** — no setters; ideal for concurrent code (thread-safe for free).
* **Compact constructor** — `public RecordName { ... }` (no parentheses) runs validation. Throws here
  prevent invalid objects from ever existing. *"Make illegal states unrepresentable."*
* **Accessor naming** — `record.field()`, not `record.getField()`.

## Modern Java 21 Idioms

* **`var`** — local variable type inference: `var executor = Executors.newVirtualThreadPerTaskExecutor();`
* **Pattern matching for `instanceof`**: `if (e instanceof IOException io) { ... }`
* **Text blocks** — triple-quoted multi-line strings.
* **`Thread.ofVirtual()` / `Thread.ofPlatform()`** — explicit thread kinds.

---

# 3. Concurrency Fundamentals

## The Core Problem: Shared Mutable State

The root cause of almost all concurrency bugs. Occurs when multiple threads have access to the same variable
(**Shared**), and at least one thread can modify it (**Mutable**).

* **Visibility:** One thread's changes might not be immediately visible to others due to CPU caching.
* **Atomicity:** Operations that look simple (like `count++`) are actually multiple steps
  (read-modify-write). If a thread is preempted between these steps, data can be lost.

## Race Conditions

A race condition occurs when the correctness of a program depends on the relative timing of multiple threads.

* **The "Check-Then-Act" Problem:** A thread checks a condition (`if (x == 10)`), but by the time it acts,
  another thread has changed `x`.
* **Increment Example:** Two threads incrementing a counter. Both read `10`, both increment to `11`, both
  write `11`. The counter should be `12`, but it remains `11`.

## The Java Memory Model (JMM) and `volatile`

A common bug: one thread updates a flag, but another thread never sees the change and loops forever.

### The "CPU Register" Trap (Visibility)

When the JIT compiler optimizes a loop like `while (running) {}`, it notices that the worker thread itself
never modifies `running`.

* **The Optimization:** "Since I never change this value, I'll cache it in a CPU register and skip reading
  it from main memory."
* **The Reality:** If another thread writes a new value to `running` in RAM, the worker — still looking at
  its local cached copy — will never see the update.

**The Post-it Analogy:**
Imagine you are a cook (the thread) following a recipe (the loop). You look at a Post-it note (the CPU
register) on your workstation. Even if the Head Chef writes a new instruction on the master chalkboard
(Main Memory), you won't see it because you never look away from your personal Post-it.

### The Solution: `volatile`

The `volatile` keyword tells the JVM: *"Every read and every write of this variable must go directly to
Main Memory. No caching, no reordering."*

| Feature           | What `volatile` Guarantees                                                 |
|:------------------|:---------------------------------------------------------------------------|
| **Visibility**    | A write by one thread is immediately visible to all other threads.         |
| **No Reordering** | The compiler/CPU cannot move instructions around the volatile read/write.  |
| **Atomicity**     | Reads/writes of the variable itself are atomic (even for `long`/`double`). |

### What `volatile` is NOT

1. **No Compound Atomicity:** `count++` is read-modify-write. `volatile` doesn't prevent interleaving
   between those steps.
2. **No Mutual Exclusion:** Two threads can still execute the same code block at the same time.
   It protects the **value**, not the **logic**.

### Architect's Heuristic

| Situation                                              | Tool                                      |
|--------------------------------------------------------|-------------------------------------------|
| Single flag toggled by one thread, read by many        | `volatile`                                |
| Counter incremented by many threads                    | `AtomicInteger` (NOT `volatile int`)      |
| Complex logic or multiple related variables            | `synchronized` or `ReentrantLock`         |

## Synchronization and Locks

Locks are the primary mechanism for serializing access to shared resources (**Mutual Exclusion**).

### The `synchronized` Keyword

* **Synchronized Methods:** Locks the entire instance (`this`) or the class object (for static methods).
  ```java
  public synchronized void increment() { count++; }
  ```
* **Synchronized Blocks:** More granular; locks a specific object. Often preferred because it reduces
  the time the lock is held.
  ```java
  synchronized (lockObject) { count++; }
  ```

**Important:** All `synchronized` methods on the same instance share **one lock**. Two methods protecting
unrelated state will still block each other. Use multiple named lock objects to allow independent locking.

---

# 4. Concurrency Primitives

## Lock-Free Concurrency: Compare-And-Swap (CAS)

`AtomicBoolean.compareAndSet(expected, new)` atomically:

1. Reads the current value
2. Compares it to `expected`
3. If equal, sets it to `new` and returns `true`
4. Otherwise, returns `false`

**All three steps happen as a single CPU instruction (CAS — Compare-And-Swap).**
This eliminates the race condition of "check then act":

```java
// ❌ Race condition possible
if (!started) { started = true; }   // two threads can both see false

// ✅ Atomic — only one thread succeeds
if (!started.compareAndSet(false, true)) {
    throw new IllegalStateException("Already started");
}
```

This is the foundation of lock-free programming. Used throughout `java.util.concurrent.atomic`
(`AtomicInteger`, `AtomicLong`, `AtomicReference`, etc.).

## BlockingQueue — Thread-Safe Producer/Consumer Communication

`BlockingQueue<E>` is a queue with two superpowers:
1. **Thread-safe by design** — multiple producers and consumers, no external locks needed
2. **Blocks naturally when full or empty** — perfect for backpressure

### Producer Methods (When Queue Is Full)

| Method                          | Behavior                                    |
|---------------------------------|---------------------------------------------|
| `add(e)`                        | Throws `IllegalStateException`              |
| `offer(e)`                      | Returns `false` (item dropped)              |
| `offer(e, timeout, unit)`       | Waits up to timeout, returns `false`        |
| `put(e)`                        | Blocks until space available                |

**Architect's rule:** At the edge, prefer `offer()` — dropping is better than blocking the sampler.

### Consumer Methods (When Queue Is Empty)

| Method                | Behavior                                                  |
|-----------------------|-----------------------------------------------------------|
| `take()`              | Blocks until item available (use on a virtual thread)     |
| `poll(timeout, unit)` | Returns `null` after timeout                              |

### Implementations

| Class                   | When to use                                              |
|-------------------------|----------------------------------------------------------|
| `ArrayBlockingQueue`    | **Bounded**, fixed capacity. Default for telemetry.     |
| `LinkedBlockingQueue`   | Optionally bounded. Slightly more allocation.            |
| `PriorityBlockingQueue` | When some items are more urgent than others.            |
| `SynchronousQueue`      | Capacity = 0. Direct hand-off between threads.          |

**Always prefer bounded queues** — unbounded ones are memory leaks waiting to happen.

## ScheduledExecutorService

The "metronome" of the Edge Agent. Fires tasks on a predictable schedule.

### Fixed Rate vs. Fixed Delay

Think of a runner on a track:

* **`scheduleAtFixedRate` (Strict Clock):** Start a new lap every 60 seconds. If Lap 1 takes 65 seconds,
  start Lap 2 immediately to "catch up." Prioritizes **frequency**.
* **`scheduleWithFixedDelay` (Rest Period):** Finish a lap, rest exactly 10 seconds, then start the next.
  Prioritizes the **gap** between tasks.

**The Edge Agent uses `scheduleWithFixedDelay`** because CPU tick calculations depend on a steady time gap.
With `fixedRate`, an overrunning cycle would trigger the next one immediately, causing erratic 0% spikes.

**Hidden danger of `scheduleAtFixedRate`:** if your task consistently overruns the period, the executor's
queue grows unbounded → eventual `OutOfMemoryError`.

### The Thread Pool (`corePoolSize`)

A thread pool is like a taxi stand. Reuse cars (threads) instead of building a new one for every ride.

* **`corePoolSize`:** Minimum number of taxis kept at the stand, even if idle.
* **For the Edge Agent:** `1` — only one recurring task.

### Handling Silent Failures

By default, if a scheduled task throws an uncaught exception, the `ScheduledExecutorService` **silently
stops** all future executions to prevent a broken task from spinning in an error loop.

* **The Problem:** Exception swallowed into a `Future`, background loop quietly vanishes.
* **The Fix:** Wrap the entire body in `try-catch (Throwable t)` to ensure the schedule survives.

### Why `main()` Must Block

The JVM exits when all "user" (non-daemon) threads finish. Executor threads are usually non-daemon by
default, but relying on this is fragile.

**Solution:** `Thread.currentThread().join()` in `main()` makes the lifecycle explicit and robust against
refactoring (e.g., someone later switching to daemon threads).

---

# 5. Threads: Platform & Virtual

## The Limits of Platform Threads

A **Platform Thread** is a thin wrapper around a native OS thread. This 1-to-1 relationship introduces a
hard physical ceiling.

### The Cost of a Thread

* **Stack Memory:** Usually 1–2 MB reserved per thread.
* **Kernel Bookkeeping:** The OS tracks and schedules every thread.

### The Hard Ceiling (The C10K Problem)

If you create a new thread per client connection, you hit a wall around a few thousand threads on a typical
laptop.

**The Failure Mode:** `pthread_create failed (EAGAIN)` or `java.lang.OutOfMemoryError: unable to create native thread`.

**The Lesson:** Traditional thread-per-connection cannot scale to tens of thousands of concurrent users.
Threads are expensive and finite. To scale, use **Thread Pools** or **Virtual Threads**.

## Virtual Threads (Project Loom)

A **Virtual Thread** is a `java.lang.Thread` object that isn't tied to a specific OS thread. The single
biggest improvement to Java concurrency since generics.

### The Core Idea

In the old world: 1 Java Thread = 1 OS Thread (permanent binding).
In the Virtual world: the JVM **mounts** a virtual thread onto a "carrier" OS thread only when it has work,
and **unmounts** it the moment it blocks (e.g., on I/O or sleep).

**The Delivery Truck Analogy:**
Carrier Threads = delivery trucks. Virtual Threads = packages.
You don't need a truck for every package. You put a package on a truck only while it's being moved.
A few trucks can deliver millions of packages over a day.

### Platform vs. Virtual

| Feature              | Platform Threads                | Virtual Threads                 |
|:---------------------|:--------------------------------|:--------------------------------|
| **Cost per Thread**  | ~1–2 MB stack + kernel overhead | ~few KB heap object             |
| **Max Count**        | ~4,000 (typical laptop)         | Millions                        |
| **Blocking Penalty** | OS thread stays idle/reserved   | Detaches from carrier instantly |
| **Cost when Idle**   | Full stack remains allocated    | Negligible                      |

### The Real Mental Model

> **Virtual Threads are not a "scale-to-millions" feature. They're a "blocking I/O without guilt" feature.**
>
> Anywhere your code does `read()`, `write()`, `sleep()`, `await()`, `take()` — Virtual Threads let you
> do it without paying the price of a parked OS thread.

### Three Critical Footguns

1. **`synchronized` causes "Pinning"** — if a virtual thread enters a `synchronized` block and then blocks
   on I/O, it pins itself to the carrier. Carrier can't be reused.
    * **Fix:** Use `ReentrantLock` instead of `synchronized` for I/O-heavy code.
    * **Note:** Lifted in Java 24+. Still applies on Java 21.

2. **ThreadLocal Memory Bloat** — `ThreadLocal` was designed for hundreds of threads. Millions of virtual
   threads × their own copies → heap exhaustion.
    * **Alternative:** **Scoped Values** (Java 21).

3. **Don't Pool Virtual Threads** — they're meant to be unbounded and cheap. Putting them in a
   `FixedThreadPool` reintroduces the very bottleneck they solve.
    * **Pattern:** `Executors.newVirtualThreadPerTaskExecutor()`.

### Implementation Patterns

```java
// 1. Drop-in replacement for ExecutorService (Best Practice)
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> handleRequest(req));
}

// 2. Single-use Virtual Thread
Thread.ofVirtual().start(() -> doWork());

// 3. Using a Factory (useful for naming threads in logs)
ThreadFactory factory = Thread.ofVirtual().name("client-", 0).factory();
ExecutorService executor = Executors.newThreadPerTaskExecutor(factory);
```

---

# 6. Networking & Sockets

## TCP: The Foundation of the Web

HTTP, REST APIs, WebSockets — all built on **TCP**.

### What TCP Promises

* **Reliable Delivery:** Bytes arrive — or the connection breaks loudly.
* **In-Order Delivery:** Bytes arrive in the order sent.
* **Connection-Oriented:** Both sides explicitly "handshake" to open and "wave" to close.

### What TCP Does NOT Promise (The "Byte Stream" Trap)

The most common source of distributed system bugs. **TCP is a byte stream, not a message stream.**

**The Water Pipe Analogy:**
Think of TCP as a water pipe. Pour a cup of red water, then a cup of blue water. The person at the other
end doesn't see "two cups" — they see a stream of purple-ish water.

* **No Message Boundaries:** If you write `"HELLO"` then `"WORLD"`, the receiver might read `"HELLOWORLD"`,
  or `"HEL"` then `"LOWORLD"`.
* **The Solution:** Design your own "envelope" — delimiters (`\n`) or length prefixes — to tell the
  receiver where one message ends.

## Sockets

A **Socket** is the OS's abstraction for a network endpoint. Like a file descriptor that reads/writes a
wire instead of a disk.

### `accept()` Is Blocking

When your code calls `accept()`, the thread pauses and waits for a client.

* **The Receptionist Analogy:** A receptionist at a desk. If no one walks through the door, they just sit
  there, using zero CPU. This is **efficient resource utilization**.
* **The Problem (Head-of-Line Blocking):** If the receptionist is busy helping one client fill out a long
  form (a blocking `read()`), they can't return to the door for the next client. New clients wait in the
  hallway (the OS kernel backlog).

### `read()` States

A critical distinction:

* **`read()` Blocking:** Client is still connected but has nothing more to say. Server waits for more.
* **`read()` returning `-1`:** **End of Stream (EOS)**.
    * Client sent a "Finish" (`FIN`) packet: *"I'm done sending, I'm hanging up."*
    * Server **must** break the loop and close immediately.

### Sockets and Try-With-Resources

Sockets encapsulate two distinct layers of resources:

1. **Java Heap Memory** — managed by the GC.
2. **Native OS File Descriptors (FD)** — strictly limited (often 1024 per process on macOS/Linux).

**Why try-with-resources is mandatory:** Forget to close, and the FD leaks even when the Java object is
GC'd.

* **FD Leakage:** `Too many open files` errors → server crashes or rejects new connections.
* **Port Exhaustion:** Sockets in `CLOSE_WAIT` (your bug) or `TIME_WAIT` (normal TCP) tie up local ports.
  Massive `CLOSE_WAIT` count = missing `socket.close()` somewhere.

### Persistent vs. Per-Message Sockets

Telemetry agents always use **persistent connections**. Per-message connections suffer:

| Aspect                   | Per-Message Socket          | Persistent Socket         |
|--------------------------|-----------------------------|---------------------------|
| Setup cost per message   | ~1–5ms (TCP handshake)      | 0 (already open)          |
| Server load              | Constant accept/close churn | Idle until data arrives   |
| TIME_WAIT exhaustion     | Real risk at high rates     | Not an issue              |
| Backpressure handling    | None                        | Natural via TCP flow ctrl |

---

# 7. Reliability Patterns

## Exponential Backoff with Jitter

When retrying a failed network operation, **the naive "retry immediately" approach is dangerous**.
With many clients, it causes the **thundering herd** — all clients hammer the server simultaneously.

### Exponential Backoff

Double the wait after each failure:

```
Attempt 1 fails → wait 1s
Attempt 2 fails → wait 2s
Attempt 3 fails → wait 4s
Attempt 4 fails → wait 8s
... capped at MAX_BACKOFF_MS (e.g., 30s)
```

### Jitter — Spread the Retries

Imagine 1,000 agents disconnecting at exactly 14:00:00. Without jitter, all 1,000 retry at 14:00:01, then
14:00:03, then 14:00:07 — slamming the server in synchronized waves. Same thundering herd, just delayed.

**Fix:** add randomness so retries spread across a window.

```java
long jittered = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff);
// random in [backoff/2, backoff*1.5)
```

### Putting It Together

```java
private boolean tryConnectWithBackoff(int maxAttempts) {
    long backoff = INITIAL_BACKOFF_MS;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            connect();
            return true;
        } catch (IOException e) {
            log.warn("Reconnect failed (attempt={}, backoff_ms={})", attempt, backoff);
        }
        long jittered = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff);
        Thread.sleep(jittered);
        backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }
    return false;
}
```

**This is the canonical pattern.** Used by AWS SDK, gRPC clients, every cloud library.

## Idempotency Guards for Lifecycle Methods

Lifecycle methods (`start`, `stop`, `close`) are called from multiple places — main flow, error paths,
shutdown hooks, tests. **They must be safe to call more than once.**

```java
public void start() {
    if (!started.compareAndSet(false, true)) {
        throw new IllegalStateException("Already started");
    }
    // ... actual startup
}

public void stop() {
    if (!started.compareAndSet(true, false)) {
        return;   // already stopped — no-op
    }
    // ... actual shutdown
}
```

**Heuristic:** Reject double-start (throw), make double-stop a no-op (return).
This is the pattern used by Spring's `Lifecycle`, Guava's `Service`.

## Graceful Shutdown

### LIFO Lifecycle

Whatever you start last, you stop first.

* **Start order:** sender → agent
* **Stop order:** agent → sender

Same rule for any layered system: database before app, app before load balancer.

### Shutdown Hooks

`Runtime.getRuntime().addShutdownHook(thread)` — JVM runs this before exit.

**Limits:**
* Not run on `kill -9` or JVM crash
* Should be fast (operators will force-kill slow shutdowns)
* Should be observable (log what you're doing)

### Interrupting Blocked Operations

| Operation                              | Responds to `interrupt()`?            |
|----------------------------------------|---------------------------------------|
| `Thread.sleep()`, `wait()`, `take()`   | ✅ Throws `InterruptedException`      |
| `socket.read()`, `socket.write()`      | ❌ Not unblocked by interrupt         |
| Blocked socket I/O                     | **Close the socket from outside**     |

**"Shoot the channel from outside" pattern:** to unblock blocked I/O, close the underlying channel from
another thread. The blocked operation throws `IOException`, breaking out of the call.

### Restoring Interrupt Status

When catching `InterruptedException` outside intentional consumption, re-set the flag:

```java
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    // ... handle exit
}
```

Upper layers need to know the thread was interrupted.

## Fail-Fast Startup

If a critical dependency is missing at startup, **don't limp along** — exit with a clear error and a
non-zero exit code. Automation (systemd, Kubernetes) will detect the failure.

```java
try {
    sender.start();
} catch (IOException e) {
    log.error("Cannot start sender (is server up?)", e);
    System.exit(1);
}
```

**Heuristic:** Always fail loud at startup. A silent agent that processes nothing is worse than no agent.

---

# 8. Distributed System Patterns

## Producer-Consumer with Bulkhead Isolation

Two threads communicate via a shared queue, never directly.

```
Producer (sampler) → [BlockingQueue] → Consumer (network sender)
        ↑                                       ↑
   never blocks               failures isolated here
```

**Why:**
* Producer shouldn't block on consumer
* Consumer failures shouldn't propagate back to producer
* The queue is a **bulkhead** — a wall isolating failures (the term comes from ship hulls)

**Naive alternative — DON'T do this:**
```java
// ❌ catastrophic for an Edge Agent
sampler.run() -> {
    readings = collector.collect();
    for (reading : readings) sender.send(reading);  // blocks on network!
}
```

Failure modes:
1. Slow network → late samples (timing drift)
2. Dead network → dead sampler (hangs forever)
3. No survival — samples lost during outage

**Queue solves all three.**

## Anti-Corruption Layer (ACL)

When integrating with an external system or framework (e.g., Protobuf-generated classes), keep the
framework's types **out of your domain code**.

```
┌──────────────┐   convert at boundary   ┌──────────────┐
│   Domain     │ ◄──────────────────────►│   Generated  │
│ SensorReading│        (Codec)          │   Proto      │
│   (record)   │                         │   (heavy)    │
└──────────────┘                         └──────────────┘
```

**The pattern:**
* Domain uses clean, immutable types (records)
* Wire/storage layer uses the framework's types
* A thin **codec** translates between them at the boundary

**Why:**
* Domain stays framework-agnostic — easy to swap Protobuf → Avro later
* Generated code's complexity doesn't leak into business logic
* Domain validation (fail-fast constructors) protects against malformed external data

**Origin:** Domain-Driven Design (Eric Evans). Foundational concept.

## Trust Boundaries

A **trust boundary** is the line between code you control and code/data you don't.

| Side                       | Posture                                              |
|----------------------------|------------------------------------------------------|
| **Inside the boundary**    | Validation failures = internal bugs. Fail loudly.    |
| **At the boundary**        | Validation failures = expected. Handle gracefully.   |

Same exception type, opposite semantics depending on *where* it's thrown.

**Examples:**
* HTTP endpoint receiving JSON → boundary (validate input, catch exceptions)
* Internal service method called from same JVM → inside (let exceptions propagate)
* Database read → boundary (data could be corrupted)
* Constructor of a domain object → inside (callers should pass valid data)

## Drop-Newest vs. Drop-Oldest Backpressure

When a queue fills up, two policies:

| Policy           | Effect                                                    |
|------------------|-----------------------------------------------------------|
| **Drop-newest**  | `offer()` returns false. Backend sees oldest data first. |
| **Drop-oldest**  | Push out old to make room for new. Backend sees current. |

**For telemetry, drop-oldest is often preferable** — you usually care about *current state*, not
complete history.

---

# 9. Serialization (Protobuf)

## Why Protobuf, Really

Most tutorials sell Protobuf as *"like JSON but smaller and faster."* Misleading. **The real reason
engineers reach for Protobuf is schema evolution** — changing your data format over years without breaking
existing systems.

Speed and size are bonuses.

## The JSON Time Bomb

A telemetry system shipping JSON faces these scenarios:
1. Old client sends to new backend — backend expects new field, NPEs
2. New client sends to old backend — old backend ignores new field silently
3. You rename a field — old backends miss it, data lost
4. You change `value` from double to object — parsers crash

**Every one of these is a schema evolution event. JSON has no rules. Protobuf does.**

## Field Tags

Each field in a `.proto` has a **tag number**:

```proto
syntax = "proto3";

message SensorReadingProto {
  string sensor_id = 1;
  double value = 2;
  int64 timestamp_millis = 3;
}
```

**The tag number is what's on the wire.** The name is for humans.

## Schema Evolution Rules

| Change                     | Safe?                                                          |
|----------------------------|----------------------------------------------------------------|
| Rename a field             | ✅ Wire is unchanged (tag is what matters)                      |
| Add a new field            | ✅ New tag number; old clients ignore (forward compatible)      |
| Remove a field             | ⚠️ Mark its tag `reserved`. **Never reuse the number.**         |
| Change a field's type      | ❌ Schema won't compile (caught at build, not runtime)          |

Forward and backward compatibility **by construction.**

## Framing — Protobuf Has No Delimiter

JSON-over-TCP used `\n` as message boundary. Protobuf bytes have no natural delimiter.

**Solution: `writeDelimitedTo` / `parseDelimitedFrom`**
* Prepends a varint-length prefix to each message
* Reader: reads varint, then reads exactly that many bytes, parses

Returns `null` at end-of-stream — same contract as `BufferedReader.readLine()`.

```java
// Sender
SensorReadingProto proto = codec.toProto(reading);
proto.writeDelimitedTo(outputStream);
outputStream.flush();

// Receiver
SensorReadingProto proto;
while ((proto = SensorReadingProto.parseDelimitedFrom(in)) != null) {
    SensorReading reading = codec.fromProto(proto);
    // ...
}
```

## Don't Let Generated Types Into Your Domain

Use the **Anti-Corruption Layer** (Section 8). The codec is the only place that knows both worlds.

## The Builder Pattern (And Why It Exists)

Protobuf's generated classes use the builder pattern obsessively:

```java
SomeMessage.newBuilder()
    .setFieldA(...)
    .setFieldB(...)
    .build();
```

**Why not just a constructor?** Three problems solved simultaneously:

1. **Evolution** — adding a 10th field doesn't break existing call sites
2. **Optionality** — set only the fields you have; defaults for the rest
3. **Immutable updates** — `original.toBuilder().setX(newX).build()`

A constructor with 10 parameters is hostile to readers and brittle to change.

**Used elsewhere:** Java's `HttpRequest.Builder`, `Stream.Builder`, Spring's `WebClient.Builder`,
Lombok's `@Builder`. *Effective Java*, Item 2.

## When Does the Size Reduction Actually Matter?

For a single agent sending 4 readings every 2 seconds, the bandwidth savings are negligible — TCP/IP
headers dominate. **Protobuf becomes meaningful at:**

* **Scale:** thousands of concurrent devices → MB/sec of savings
* **Constrained networks:** cellular IoT, LoRaWAN, satellite — every byte costs money
* **High-throughput pipelines:** millions of messages/sec, where parsing speed matters

**Architect's lesson:** evaluate optimizations at the scale they'll actually run.

---

# 10. Engineering Practice

## Structured Logging with SLF4J

### Parameter Placeholders — Use `{}`, Never `+`

```java
log.info("Connected to {}:{} (attempt={})", host, port, attempt);   // ✓
log.info("Connected to " + host + ":" + port);                       // ✗ (always builds the string)
```

The `+` version constructs the string **even when the log level is silenced** — wasted CPU.

### Always Pass Exception as Last Argument

```java
log.error("Operation failed", e);            // ✓ stack trace included
log.error("Operation failed: " + e);          // ✗ only the message
log.error("Operation failed: {}", e);         // ✗ only the message
```

### Use `key=value` Format for Searchability

```java
log.warn("Reconnect failed (attempt={}, backoff_ms={}, cause={})", attempt, backoff, e.getMessage());
```

Grep-friendly. Tools like Splunk and Datadog parse this format automatically.

### Log Levels

| Level    | When                                                          |
|----------|---------------------------------------------------------------|
| ERROR    | Something is broken; humans should be notified                |
| WARN     | Unexpected but recoverable (network blip, queue full)         |
| INFO     | Routine lifecycle events (start, stop, connect)               |
| DEBUG    | Detailed flow info; off in production                         |
| TRACE    | Very detailed; rare                                           |

### Logger Per Class — Always

```java
private static final Logger log = LoggerFactory.getLogger(MyClass.class);
```

**Common bug:** copy-pasting `LoggerFactory.getLogger(OtherClass.class)` from another file. Log lines will
appear to come from `OtherClass`, sending future debuggers down rabbit holes. **Always verify the class
reference matches the file.**

### Operational Logging Rule

> *"Could an on-call engineer at 3 AM act on this log alone?"*

If no, the log isn't doing its job. Include context: client ID, attempt number, error cause, etc.

## Testing Discipline

### Pure-Function Tests Are Cheap, Use Them Liberally

`WireFormat` and `SensorReadingCodec` are pure — same input → same output, no I/O. **Unit-test these
heavily.** They are the contract of your system, codified.

### Test Strictness Should Be Consistent

```java
// Loose (passes if "12.34xyz" appears)
assertTrue(line.contains("\"value\":12.34"));

// Strict (full contract)
assertEquals(expected, line);
```

**Inconsistent strictness hides regressions.** Either commit to byte-exact or be deliberately loose
everywhere.

### Characterization Tests

Tests that pin down *current* behavior — desirable or not. If a future refactor changes the behavior, the
test fires, forcing a deliberate conversation.

Example: precision-loss test on the codec — locks down that `Instant.now()` loses nanosecond precision when
round-tripped through Protobuf's millis. Whoever changes that has to update the test consciously.

## Code Hygiene

### Optimize Imports

`Cmd+Option+O` in IntelliJ. Run before every commit. Dead imports are dead code.

### Update Stale Javadoc Aggressively

> *Docs that lie are worse than no docs.*

When you change a class's architecture (e.g., single-threaded → multi-client), update the class-level
Javadoc the same commit. Future-you reading the doc will trust it.

### Iterative Hardening

Real production code is **never written perfect on the first draft.** The architect's skill is *seeing
failure modes after the first draft* and refining without panicking:

1. Naive working code
2. Identify failure modes
3. Refactor to robust code

This rhythm — write, review, harden — is how every senior engineer ships software.

---

# 11. Phase 3 (Future): Spring Boot

> **Note:** Spring Boot belongs ONLY in the backend server, never in the Edge Agent.
> The Edge Agent must remain plain Java to preserve the learning value of raw JVM internals.

## Analogy

* **Plain Spring:** Buying car parts separately. Assemble the engine, wire the electronics, configure the
  dashboard. Powerful but tedious.
* **Spring Boot:** Buying a Toyota Camry. Pre-installed, pre-configured. You just drive.

## Spring Boot Superpowers

1. **Embedded Server:** No separate Tomcat installation. The `.jar` *is* the server.
2. **Auto-configuration:** Configures your app based on the classpath.
3. **Starter Dependencies:** Feature-based bundles instead of hand-picking versions.

## Configuration (For Backend, Not Edge Agent)

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.5</version>
    <relativePath/>
</parent>
```

**Why use a parent?** Sensible defaults and version management — prevents "JAR hell."

## Essential Dependencies

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

* `spring-boot-starter`: Core foundation (logging, context, auto-config).
* `spring-boot-starter-test`: JUnit 5, Mockito, AssertJ bundled.

> Detailed Spring Boot notes will be added during Phase 3 as concepts are learned organically.

---

## Appendix: Project Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                         Edge Agent                            │
│                                                               │
│  ┌────────────────────┐                                       │
│  │ SystemMetrics      │  (OSHI: CPU, memory)                  │
│  │ Collector          │                                       │
│  └─────────┬──────────┘                                       │
│            │                                                  │
│  ┌─────────▼──────────┐                                       │
│  │ TelemetryAgent     │  (ScheduledExecutorService, every 2s) │
│  │ (sampler)          │                                       │
│  └─────────┬──────────┘                                       │
│            │ enqueue(reading)                                 │
│  ┌─────────▼──────────┐                                       │
│  │ BlockingQueue      │  (bulkhead — capacity 1000)           │
│  └─────────┬──────────┘                                       │
│            │                                                  │
│  ┌─────────▼──────────┐                                       │
│  │ TelemetrySender    │  (virtual thread supervisor:          │
│  │ (drain loop)       │   connect → drain → reconnect)        │
│  └─────────┬──────────┘                                       │
│            │ SensorReadingCodec.toProto(reading)              │
│            │ proto.writeDelimitedTo(socket)                   │
└────────────┼──────────────────────────────────────────────────┘
             │ TCP (binary Protobuf, length-delimited)
             │
┌────────────▼──────────────────────────────────────────────────┐
│                        SmokeServer                             │
│  (current: dev harness; future: Spring Boot ingestion)         │
│                                                                │
│  Acceptor thread → spawn virtual thread per client             │
│      → parseDelimitedFrom(in)                                  │
│      → SensorReadingCodec.fromProto(proto)                     │
│      → log domain SensorReading                                │
└────────────────────────────────────────────────────────────────┘
```
