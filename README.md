# Trino → Arrow Flight Gateway

Spring Boot service that submits queries to Trino, converts results to Apache Arrow, and serves them over Arrow Flight.

## Features

- REST/Flight gateway for Trino query results.
- Arrow conversion pipeline with configurable parallelism.
- Health/actuator endpoints via Spring Boot.

## Requirements
- 
- JDK 25+
- Python 3 with `pyarrow` (only for the sample client)
- Access to Trino and (optionally) an S3 bucket for spooling

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
  spool:
    s3:
      bucket: my-dev-spool-bucket
      prefix: trino/results/
      region: eu-central-1
  conversion:
    parallelism: 8
```
Override via environment variables or a custom `application.yml` on the classpath.

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

## Building & testing

```bash
./gradlew test
```

## Publishing coordinates

Artifacts use the group ID `io.github.koszti.trinoarrowgateway`, e.g.:
```
io.github.koszti.trinoarrowgateway:trino-arrow-gateway:<version>
```
