import pyarrow as pa
import pyarrow.flight as fl


def main():
    # Connect to your gateway's Flight server
    client = fl.FlightClient("grpc+tcp://localhost:31337")

    # For now, keep this compatible with your fallback JSON (id BIGINT, name VARCHAR)
    sql = b"SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bobcat'"

    descriptor = fl.FlightDescriptor.for_command(sql)

    info = client.get_flight_info(descriptor)
    print("Schema:", info.schema)

    ticket = info.endpoints[0].ticket
    reader = client.do_get(ticket)
    table = reader.read_all()

    print("Table:")
    print(table.combine_chunks())


if __name__ == "__main__":
    main()
