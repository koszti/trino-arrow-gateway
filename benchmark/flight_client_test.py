import os
import sys
import json
import time

import pyarrow as pa
import pyarrow.flight as fl

from bench_utils import format_elapsed, print_kv, print_sample_rows, print_section, schema_summary


EXAMPLE_SQL = "SELECT * FROM tpch.sf100.orders LIMIT 1000000"
SAMPLE_N = 10

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

    print_section(f"Sample Data (first {SAMPLE_N} rows)")
    print_sample_rows(table, limit=SAMPLE_N)


if __name__ == "__main__":
    main()
