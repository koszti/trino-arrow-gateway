import os
import sys
import time

import pyarrow as pa
from trino.dbapi import connect

from bench_utils import env_int, format_elapsed, print_kv, print_sample_rows, print_section, schema_summary

EXAMPLE_SQL = "SELECT * FROM tpch.sf1.orders LIMIT 1000000"
FETCH_SIZE = 1000
PROGRESS_EVERY = 100_000
SAMPLE_N = 10


def _normalize_trino_type(type_code: object) -> str:
    if type_code is None:
        return ""
    if isinstance(type_code, str):
        return type_code
    name = getattr(type_code, "name", None)
    if isinstance(name, str) and name:
        return name
    return str(type_code)


def trino_type_to_arrow(trino_type: str) -> pa.DataType:
    t = (trino_type or "").strip().upper()
    if not t:
        return pa.string()

    if t.startswith("BIGINT"):
        return pa.int64()
    if t.startswith("INTEGER") or t == "INT":
        return pa.int32()
    if t.startswith("DOUBLE"):
        return pa.float64()
    if t.startswith("VARCHAR") or t.startswith("CHAR") or t.startswith("JSON"):
        return pa.string()
    if t.startswith("DATE"):
        return pa.date32()

    # Best-effort fallback for complex/unknown types (ARRAY/MAP/ROW, etc.)
    return pa.string()


def arrow_schema_from_description(description: object) -> pa.Schema | None:
    if not description:
        return None

    fields: list[pa.Field] = []
    for col in description:
        # PEP-249: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        name = col[0] if len(col) > 0 else None
        type_code = col[1] if len(col) > 1 else None
        trino_type = _normalize_trino_type(type_code)
        arrow_type = trino_type_to_arrow(trino_type)
        fields.append(pa.field(str(name), arrow_type, nullable=True, metadata={"trino:type": trino_type}))
    return pa.schema(fields)


def main():
    # Accept SQL from argv (or default to a small demo query)
    sql_text = sys.argv[1] if len(sys.argv) > 1 else EXAMPLE_SQL

    host = "localhost"
    port = 8080
    user = "trino-arrow-gateway-dbapi"
    http_scheme = "http"

    print_section("Trino DB-API Request")
    print_kv("Endpoint", f"{http_scheme}://{host}:{port}")
    print_kv("User", user)
    print_kv("SQL", sql_text)
    print_kv("Fetch size", FETCH_SIZE)
    sample_n = env_int("TRINO_SAMPLE_N", SAMPLE_N)

    conn = connect(
        host=host,
        port=port,
        user=user,
        http_scheme=http_scheme,
        verify=False,
    )

    cursor = conn.cursor()
    cursor.arraysize = max(1, FETCH_SIZE)

    t0 = time.perf_counter()
    print_section("Action: execute + fetchmany loop")
    cursor.execute(sql_text)

    rows = 0
    columns = None
    arrow_schema = arrow_schema_from_description(cursor.description)
    if arrow_schema is not None:
        print_kv("Arrow schema", schema_summary(arrow_schema))
        columns = len(arrow_schema)

    arrow_batches = 0
    arrow_bytes = 0
    last_report_at = 0
    sample_batches: list[pa.RecordBatch] = []
    sample_rows = 0

    while True:
        batch = cursor.fetchmany()
        if not batch:
            break

        # Build Arrow batches from the fetched DB-API rows (do not retain them; this is for benchmarking conversion).
        if arrow_schema is not None:
            cols = list(zip(*batch))
            arrays: list[pa.Array] = []
            for i, field in enumerate(arrow_schema):
                arrays.append(pa.array(cols[i], type=field.type))
            record_batch = pa.record_batch(arrays, schema=arrow_schema)
            arrow_batches += 1
            arrow_bytes += record_batch.nbytes
            if sample_rows < sample_n:
                need = sample_n - sample_rows
                sample_batches.append(record_batch.slice(0, need))
                sample_rows += min(need, record_batch.num_rows)

        rows += len(batch)
        if PROGRESS_EVERY > 0 and rows - last_report_at >= PROGRESS_EVERY:
            elapsed = time.perf_counter() - t0
            print(f"- Progress: {rows} rows ({format_elapsed(elapsed)}), arrow_batches={arrow_batches}")
            last_report_at = rows

    cursor.close()
    conn.close()

    t2 = time.perf_counter()
    print_section("Result Summary")
    print_kv("Rows", rows)
    print_kv("Columns", columns)
    print_kv("Arrow batches", arrow_batches)
    print_kv("Arrow bytes", arrow_bytes)
    print_kv("Elapsed", format_elapsed(t2 - t0))
    if arrow_schema is not None and sample_batches:
        print_section(f"Sample Data (first {sample_n} rows)")
        sample_table = pa.Table.from_batches(sample_batches, schema=arrow_schema)
        print_sample_rows(sample_table, limit=sample_n)


if __name__ == "__main__":
    raise SystemExit(main())
