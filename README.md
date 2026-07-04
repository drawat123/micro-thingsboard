# Micro-ThingsBoard

A learning project: a distributed telemetry engine built from the ground up in
plain Java 21 and Spring Boot. Devices publish system metrics over MQTT; a
transport layer normalizes them onto a Kafka pipeline for downstream processing.

The wire format is **Protobuf** everywhere. Device-to-server transport is
**MQTT** (Mosquitto); the server-internal pipeline is **Kafka** (KRaft mode).

## Modules

Multi-module Maven project (`common` is the framework-free shared library that
every other module depends on):

- `common/` — shared domain records, Protobuf definitions, and the codec
  (anti-corruption layer). No framework dependencies.
- `edge-agent/` — plain Java 21 (no Spring). Samples OS metrics via OSHI and
  publishes them to MQTT using Eclipse Paho.
- `mqtt-transport/` — Spring Boot. Subscribes to MQTT via Spring Integration,
  decodes to the domain model, and produces to Kafka. Holds no business logic —
  it terminates the protocol and hands off to the queue.
- `processor/` — Spring Boot Kafka consumer. Reads `raw-telemetry` and processes
  each reading. *(In progress — Phase 3.3.3.)*

## Architecture

```
edge-agent ──MQTT──► Mosquitto ──► mqtt-transport ──Kafka──► raw-telemetry ──► processor
 (plain Java)                       (Spring Boot)             (3 partitions)    (Spring Boot)
```

## Running locally

Prerequisites: Docker, JDK 21, Maven, and the Kafka CLI (`brew install kafka`).

1. **Build once** (generates Protobuf sources, installs `common` to your local
   `.m2` so the other modules resolve it):
   ```
   mvn install -DskipTests
   ```
2. **Start infrastructure** — Kafka (from repo root) and Mosquitto:
   ```
   docker compose up -d
   docker run -it --rm --name mosquitto-dev -p 1883:1883 \
     -v ~/mosquitto-dev/config:/mosquitto/config eclipse-mosquitto:2.0
   ```
3. **Create the Kafka topic** (3 partitions):
   ```
   kafka-topics --bootstrap-server localhost:9092 \
     --create --topic raw-telemetry --partitions 3 --replication-factor 1
   ```
4. **Run the services:**
   ```
   cd mqtt-transport && mvn spring-boot:run
   cd edge-agent && mvn clean compile exec:java -Dexec.mainClass="com.divyam.telemetry.EdgeAgent"
   ```

## Roadmap & notes

Full roadmap and current progress in [`docs/roadmap.md`](docs/roadmap.md);
development notes in `docs/notes*.md`.
