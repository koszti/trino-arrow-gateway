# Benchmarks

Commands below assume you run them from the `benchmark/` directory.

### Test System Configuration

| Component        | Specification | Details                       |
|------------------|---------------|-------------------------------|
| CPU              | AMD 5800X     | 8 Cores / 16 Threads          |
| Trino Workers    | 10 Nodes      | distributed trino cluster     |
| SQL Query        | SELECT *      | tpch.sf100.orders LIMIT {n}   |
| Spooling backend | minio         | to minimise network overhead  |

### End-to-End Fetch Benchmarks

Values represent the total time (in seconds) from query submission until the last row is received by the client.

| Protocol                 | Client / Method    | 1M Rows | 10M Rows | 100M Rows |
|--------------------------|--------------------|---------|----------|-----------|
| Non-Spooled              | Trino CLI          | 10      | 74       | 699       |
|                          | Trino JDBC         | 9       | 72       | 668       |
|                          | Trino Python DBAPI | 12      | 112      | 1067      |
| Spooling                 | Trino CLI          | 6       | 25       | 220       |
|                          | Trino JDBC         | 5       | 23       | 201       |
|                          | Trino Python DBAPI | 8       | 69       | 691       |
| Spooling + Arrow Gateway | Flight (Java)      | 6       | 39       | 388       |
|                          | Flight (Python)    | 7       | 40       | 401       |


Trino python connector with spooling protocol raised multiple issues:
* High RAM requirement
* Without sufficient memory, it ends up with OOM in the client side, causing queries to hang indefinitely or transition to an ABANDONED state.
* TCP ephemeral port exhaustion. Because spooling fetches many small files in rapid succession, the client can quickly exhaust available network ports:
  * Set `net.ipv4.ip_local_port_range` and `net.ipv4.tcp_tw_reuse` sysctl settings
  * Reduce the total number of network requests by increasing the segment sizes in Trino coordinator `config.properties`. 
  This forces Trino to generate fewer, larger files: `protocol.spooling.initial-segment-size` `protocol.spooling.max-segment-size`
  
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
