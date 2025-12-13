# Trino → Arrow Flight Gateway

Spring Boot service that submits queries to Trino, converts results to Apache Arrow, and serves them over Arrow Flight.

## Features

- REST/Flight gateway for Trino query results.
- Arrow conversion pipeline with configurable parallelism.
- Health/actuator endpoints via Spring Boot.

## Requirements
- JDK 25+
- Python 3 with `pyarrow` (only for the sample client)
- Access to Trino with spooling enabled (see below)

## Quick start

```bash
./gradlew bootRun
# server starts on port 8081 by default
```

### Configuration

Main options live in `src/main/resources/application.yml`:
```yaml
server:
  port: 8081
gateway:
  trino:
    base-url: http://localhost:8080
    user: trino-arrow-gateway
    # Sets the request header `X-Trino-Query-Data-Encoding` on /v1/statement.
    # Use `json+zstd` (recommended) or `json`.
    query-data-encoding: json+zstd
  conversion:
    # Thread pool size used for spooled segment download/decode/convert work.
    parallelism: 8
    # Rows per Arrow record batch sent via Flight (per `listener.putNext()`).
    # batch-size: 1024
    # Limit how many segments are processed concurrently (defaults to `parallelism`).
    # max-in-flight-segments: 8
    # Limit how many Arrow batches are buffered per segment while streaming.
    # max-buffered-batches-per-segment: 4
```
Override via environment variables or a custom `application.yml` on the classpath.

## Trino spooling expectations

This gateway relies on Trino returning *spooled* results via `/v1/statement` polling:

- When spooling is enabled, Trino responses include a `data` object with:
  - `encoding`: `json` or `json+zstd`
  - `segments[]`: each with a downloadable `uri` and an `ackUri`
- The gateway downloads each `segments[].uri` and then calls `GET segments[].ackUri` (expects HTTP 200).
  The ack request is required to allow Trino to delete the spooled segment.
- Segment payload is expected to be JSON array-of-rows: `[[col1, col2, ...], ...]` (matching Trino column order).

If Trino does not return spooled `segments[]`, the Flight request fails (there is no local JSON fallback).

## Arrow Flight demo

Start the app, then run the example client:
```bash
cd python-client
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python flight_client_test.py
```
This connects to `grpc+tcp://localhost:31337`, requests a demo query, and prints the Arrow table.

### Flight host/port

The Flight server binds and advertises its address via `gateway.flight.*`:
```yaml
gateway:
  flight:
    enabled: true
    bind-host: 0.0.0.0
    advertise-host: localhost
    port: 31337
```
Clients should connect to `grpc+tcp://<advertise-host>:<port>`.

## Building & testing

```bash
./gradlew test
```

## Publishing coordinates

Artifacts use the group ID `io.github.koszti.trinoarrowgateway`, e.g.:
```
io.github.koszti.trinoarrowgateway:trino-arrow-gateway:<version>
```
