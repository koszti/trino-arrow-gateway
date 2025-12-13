import os
import sys

import pyarrow as pa
import pyarrow.flight as fl


def main():
    endpoint = "grpc+tcp://localhost:31337"
    client = fl.FlightClient(endpoint)

    # Accept SQL from argv (or default to a small demo query)
    sql_text = "SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bobcat'"
    sql = sql_text.encode("utf-8")

    descriptor = fl.FlightDescriptor.for_command(sql)

    info = client.get_flight_info(descriptor)
    print(f"Flight endpoint: {endpoint}")
    print(f"SQL: {sql_text}")
    print("Schema:")
    print(info.schema)

    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)
    table = reader.read_all()

    print("Table:")
    print(table.combine_chunks().to_string())


if __name__ == "__main__":
    main()
