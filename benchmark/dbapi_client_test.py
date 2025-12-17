import os
import sys
import time

from trino.dbapi import connect

from bench_utils import dbapi_schema_summary, env_int, format_elapsed, print_kv, print_sample_rows_dbapi, print_section

EXAMPLE_SQL = "SELECT * FROM tpch.sf1.orders LIMIT 1000000"
FETCH_SIZE = 1000
PROGRESS_EVERY = 100_000
SAMPLE_N = 10

def env_verify(name: str, default: bool = True) -> bool | str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        return default

    lowered = value.lower()
    if lowered in {"0", "false", "no", "off"}:
        return False
    if lowered in {"1", "true", "yes", "on"}:
        return True

    if not os.path.isfile(value):
        raise ValueError(f"{name} points to a non-existent file: {value}")
    return value


def main():
    # Accept SQL from argv (or default to a small demo query)
    sql_text = sys.argv[1] if len(sys.argv) > 1 else EXAMPLE_SQL

    host = "localhost"
    port = 8080
    user = "trino-arrow-gateway-dbapi"
    http_scheme = "http"
    verify = env_verify("TRINO_VERIFY", default=True)
    sample_n = env_int("TRINO_SAMPLE_N", SAMPLE_N)

    print_section("Trino DB-API Request")
    print_kv("Endpoint", f"{http_scheme}://{host}:{port}")
    print_kv("User", user)
    print_kv("SQL", sql_text)
    print_kv("Fetch size", FETCH_SIZE)
    print_kv("Verify TLS", verify)
    print_kv("Sample rows", sample_n)

    conn = connect(
        host=host,
        port=port,
        user=user,
        http_scheme=http_scheme,
        verify=verify,
    )

    cursor = conn.cursor()
    cursor.arraysize = max(1, FETCH_SIZE)

    t0 = time.perf_counter()
    print_section("Action: execute + fetchmany loop")
    cursor.execute(sql_text)

    rows = 0
    columns = len(cursor.description) if cursor.description else None
    if cursor.description:
        print_kv("Schema", dbapi_schema_summary(cursor.description))
    last_report_at = 0
    sample_rows: list[tuple] = []

    while True:
        batch = cursor.fetchmany()
        if not batch:
            break

        if sample_n > 0 and len(sample_rows) < sample_n:
            remaining = sample_n - len(sample_rows)
            sample_rows.extend(tuple(r) for r in batch[:remaining])

        rows += len(batch)
        if PROGRESS_EVERY > 0 and rows - last_report_at >= PROGRESS_EVERY:
            elapsed = time.perf_counter() - t0
            print(f"- Progress: {rows} rows ({format_elapsed(elapsed)})")
            last_report_at = rows

    cursor.close()
    conn.close()

    t2 = time.perf_counter()
    print_section("Result Summary")
    print_kv("Rows", rows)
    print_kv("Columns", columns)
    print_kv("Elapsed", format_elapsed(t2 - t0))
    if sample_n > 0:
        print_section(f"Sample Data (first {min(sample_n, len(sample_rows))} rows)")
        print_sample_rows_dbapi(cursor.description, sample_rows, limit=sample_n)


if __name__ == "__main__":
    raise SystemExit(main())
