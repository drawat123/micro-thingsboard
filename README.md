# Micro-ThingsBoard

A learning project: a lightweight telemetry engine built in plain Java 21 (Edge Agent) and Spring Boot (Backend).

## Architecture

- `edge-agent/` — Plain Java agent that samples OS metrics and ships them over TCP using Protobuf
- `smoke-server/` — Minimal test harness server (placeholder for the real Spring Boot backend)

## Running locally

1. Start the smoke server: `cd smoke-server && mvn clean compile exec:java -Dexec.mainClass="com.divyam.telemetry.SmokeServer"`
2. Start the edge agent: `cd edge-agent && mvn clean compile exec:java -Dexec.mainClass="com.divyam.telemetry.App"`

## Status

Phase 2 complete: end-to-end Protobuf telemetry with virtual threads, exponential backoff reconnect, and producer-consumer bulkheading.
