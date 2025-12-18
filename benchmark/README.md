# Benchmarks

Commands below assume you run them from the `benchmark/` directory.

Notes:
- Benchmarks are run with a locally running MinIO to reduce network overhead as much as possible.
- Full results will be added in a tabular format soon; current observations: enabling spooling is ~3x faster, and spooling + Arrow Gateway is ~6x faster.

## Python (Flight + DB-API)

Create a virtualenv and install requirements (run once):

```bash
python -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -r requirements.txt
```

Trino Arrow Gateway (Arrow Flight):

```bash
TRINO_VERIFY=false .venv/bin/python flight_client_test.py "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

Trino Python client (DB-API):

```bash
TRINO_VERIFY=false .venv/bin/python dbapi_client_test.py "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

## Java (JDBC)

Build the runnable jar:

```bash
javac TrinoJdbcBenchmark.java
```

Run (you still need the Trino JDBC driver + dependencies on the classpath):

```bash
java -cp "/path/to/trino-jdbc-jar:." TrinoJdbcBenchmark "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

## Java (Arrow Flight)

`TrinoFlightBenchmark.java` mirrors `flight_client_test.py` (get_flight_info, do_get stream drain, schema + sample rows).

Build the runnable jar:

```bash
javac TrinoFlightBenchmark.java
```

Run (you still need the Apache Arrow Flight jars on the classpath, `arrow-flight-core`, `arrow-vector`, `arrow-memory-core`, etc.):

```bash
java -cp "TrinoFlightBenchmark.jar:/path/to/arrow-libs/*" TrinoJdbTrinoFlightBenchmark "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

## Shell (Trino CLI)

```bash
TRINO_CLI_BIN=/path/to/trino-cli ./trino_cli_test.sh "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```
