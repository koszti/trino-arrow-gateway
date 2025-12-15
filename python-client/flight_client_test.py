import os
import sys
import json
import time

import pyarrow as pa
import pyarrow.flight as fl


EXAMPLE_SQL = "SELECT * FROM tpch.sf1.orders LIMIT 1000000"
SAMPLE_N = 10

def print_section(title: str) -> None:
    line = "=" * len(title)
    print(f"\n{title}\n{line}")

def format_elapsed(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    minutes = int(seconds // 60)
    rem = seconds - (minutes * 60)
    if minutes > 0:
        return f"{minutes}m {rem:.3f}s"
    return f"{rem:.3f}s"


def print_kv(key: str, value: object) -> None:
    print(f"- {key}: {value}")


def schema_summary(schema: pa.Schema) -> str:
    cols = ", ".join(f"{f.name}: {f.type}" for f in schema)
    return f"{len(schema)} columns ({cols})"


def print_sample_rows(table: pa.Table, limit: int = 10) -> None:
    if table.num_rows == 0:
        print("(no rows)")
        return

    head = table.slice(0, min(limit, table.num_rows))
    rows = head.to_pylist()
    if not rows:
        print("(no rows)")
        return

    columns = list(head.schema.names)
    cell_strings = {c: [str(r.get(c, "")) for r in rows] for c in columns}
    widths = {c: max(len(c), *(len(v) for v in cell_strings[c])) for c in columns}

    def fmt_row(values: list[str]) -> str:
        return " | ".join(v.ljust(widths[c]) for c, v in zip(columns, values))

    print(fmt_row(columns))
    print("-+-".join("-" * widths[c] for c in columns))
    for i in range(len(rows)):
        print(fmt_row([cell_strings[c][i] for c in columns]))


def main():
    endpoint = "grpc+tcp://localhost:31337"
    client = fl.FlightClient(endpoint)

    # Accept SQL from argv (or default to a small demo query)
    sql_text = sys.argv[1] if len(sys.argv) > 1 else EXAMPLE_SQL
    sql = sql_text.encode("utf-8")

    descriptor = fl.FlightDescriptor.for_command(sql)

    print_section("Flight Request")
    print_kv("Endpoint", endpoint)
    print_kv("SQL", sql_text)

    t0 = time.perf_counter()
    print_section("Action: get_flight_info")
    info = client.get_flight_info(descriptor)
    t1 = time.perf_counter()
    print_kv("Elapsed", format_elapsed(t1 - t0))
    print_kv("Schema", schema_summary(info.schema))

    ticket = info.endpoints[0].ticket

    print_section("Action: do_get + read_all")
    t2 = time.perf_counter()
    reader = client.do_get(ticket)
    table = reader.read_all()
    table = table.combine_chunks()
    t3 = time.perf_counter()
    print_kv("Elapsed", format_elapsed(t3 - t2))

    print_section("Result Summary")
    print_kv("Rows", table.num_rows)
    print_kv("Columns", table.num_columns)

    sample_n = 10
    print_section(f"Sample Data (first {sample_n} rows)")
    print_sample_rows(table, limit=sample_n)


if __name__ == "__main__":
    main()
