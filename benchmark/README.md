# Benchmarks

Commands below assume you run them from the `benchmark/` directory.

## Python (Flight + DB-API)

Create a virtualenv and install requirements (run once):

```bash
python -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -r requirements.txt
```

Trino Arrow Gateway (Arrow Flight):

```bash
.venv/bin/python flight_client_test.py "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

Trino Python client (DB-API):

```bash
.venv/bin/python dbapi_client_test.py "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

## Java (JDBC)

Build the runnable jar:

```bash
javac TrinoJdbcBenchmark.java
jar --create --file TrinoJdbcBenchmark.jar --main-class TrinoJdbcBenchmark -C out .
```

Run (you still need the Trino JDBC driver + dependencies on the classpath; easiest is using a Trino server tarball `lib/*`):

```bash
java -cp "/path/to/trino-jdbc-jar:." TrinoJdbcBenchmark "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```

## Shell (Trino CLI)

```bash
TRINO_CLI_BIN=/path/to/trino-cli ./trino_cli_test.sh "SELECT * FROM tpch.sf100.orders LIMIT 1_000_000"
```
