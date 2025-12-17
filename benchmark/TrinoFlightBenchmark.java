import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public final class TrinoFlightBenchmark {
    private static final String DEFAULT_SQL = "SELECT * FROM tpch.sf100.orders LIMIT 1000000";
    private static final int DEFAULT_SAMPLE_N = 10;

    public static void main(String[] args) throws Exception {
        String endpoint = getenv("FLIGHT_ENDPOINT", "grpc+tcp://localhost:31337");
        String sqlText = args.length > 0 ? String.join(" ", args) : getenv("TRINO_SQL", DEFAULT_SQL);
        int sampleN = parseInt(getenv("TRINO_SAMPLE_N", Integer.toString(DEFAULT_SAMPLE_N)), DEFAULT_SAMPLE_N);

        HostPort hostPort = parseGrpcTcpEndpoint(endpoint);

        printSection("Flight Request");
        printKv("Endpoint", endpoint);
        printKv("SQL", sqlText);
        printKv("Sample rows", sampleN);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure(hostPort.host, hostPort.port)).build()) {
            FlightDescriptor descriptor = FlightDescriptor.command(sqlText.getBytes(java.nio.charset.StandardCharsets.UTF_8));

            printSection("Action: get_flight_info");
            long t0 = System.nanoTime();
            FlightInfo info = client.getInfo(descriptor);
            long t1 = System.nanoTime();
            printKv("Elapsed", format(Duration.ofNanos(t1 - t0)));
            printKv("Schema", schemaSummary(info.getSchema()));

            printSection("Action: do_get + read_all");
            long t2 = System.nanoTime();

            long rows = 0;
            int columns = info.getSchema().getFields().size();
            List<List<String>> sampleRows = new ArrayList<>();

            try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    int batchRows = root.getRowCount();
                    rows += batchRows;

                    if (sampleN > 0 && sampleRows.size() < sampleN) {
                        int remaining = sampleN - sampleRows.size();
                        int toTake = Math.min(remaining, batchRows);
                        for (int rowIndex = 0; rowIndex < toTake; rowIndex++) {
                            sampleRows.add(readRowStrings(root, columns, rowIndex));
                        }
                    }
                }
            }

            long t3 = System.nanoTime();
            printKv("Elapsed", format(Duration.ofNanos(t3 - t2)));

            printSection("Result Summary");
            printKv("Rows", rows);
            printKv("Columns", columns);

            if (sampleN > 0) {
                printSection("Sample Data (first " + Math.min(sampleN, sampleRows.size()) + " rows)");
                printSampleTable(info.getSchema(), sampleRows);
            }
        }
    }

    private static List<String> readRowStrings(VectorSchemaRoot root, int columns, int rowIndex) {
        List<String> cells = new ArrayList<>(columns);
        for (int colIndex = 0; colIndex < columns; colIndex++) {
            Object value = root.getVector(colIndex).getObject(rowIndex);
            cells.add(value == null ? "NULL" : String.valueOf(value));
        }
        return cells;
    }

    private static String schemaSummary(Schema schema) {
        List<Field> fields = schema.getFields();
        StringBuilder sb = new StringBuilder();
        sb.append(fields.size()).append(" columns (");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Field f = fields.get(i);
            sb.append(f.getName()).append(": ").append(f.getType());
        }
        sb.append(")");
        return sb.toString();
    }

    private static void printSampleTable(Schema schema, List<List<String>> rows) {
        List<Field> fields = schema.getFields();
        int columns = fields.size();
        if (columns == 0) {
            System.out.println("(no columns)");
            return;
        }
        if (rows.isEmpty()) {
            System.out.println("(no rows)");
            return;
        }

        List<String> headers = new ArrayList<>(columns);
        for (Field f : fields) {
            headers.add(f.getName());
        }

        int[] widths = new int[columns];
        for (int i = 0; i < columns; i++) {
            widths[i] = headers.get(i).length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < columns; i++) {
                String cell = i < row.size() ? row.get(i) : "";
                widths[i] = Math.max(widths[i], cell.length());
            }
        }

        System.out.println(formatRow(headers, widths));
        System.out.println(formatSeparator(widths));
        for (List<String> row : rows) {
            List<String> padded = new ArrayList<>(columns);
            for (int i = 0; i < columns; i++) {
                padded.add(i < row.size() ? row.get(i) : "");
            }
            System.out.println(formatRow(padded, widths));
        }
    }

    private static String formatRow(List<String> values, int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            String value = i < values.size() ? values.get(i) : "";
            sb.append(padRight(value, widths[i]));
        }
        return sb.toString();
    }

    private static String formatSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            if (i > 0) {
                sb.append("-+-");
            }
            sb.append("-".repeat(widths[i]));
        }
        return sb.toString();
    }

    private static String padRight(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return value + " ".repeat(width - value.length());
    }

    private static void printSection(String title) {
        System.out.println();
        System.out.println(title);
        System.out.println("=".repeat(title.length()));
    }

    private static void printKv(String key, Object value) {
        System.out.println("- " + key + ": " + value);
    }

    private static String getenv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null) {
            return defaultValue;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? defaultValue : trimmed;
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value.trim());
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private static String format(Duration duration) {
        long millis = duration.toMillis();
        long minutes = millis / 60_000;
        long seconds = (millis % 60_000) / 1_000;
        long ms = millis % 1_000;
        if (minutes > 0) {
            return minutes + "m" + seconds + "s";
        }
        if (seconds > 0) {
            return seconds + "." + String.format("%03d", ms) + "s";
        }
        return ms + "ms";
    }

    private static HostPort parseGrpcTcpEndpoint(String endpoint) {
        String prefix = "grpc+tcp://";
        if (!endpoint.startsWith(prefix)) {
            throw new IllegalArgumentException("Unsupported FLIGHT_ENDPOINT (expected grpc+tcp://host:port): " + endpoint);
        }
        String rest = endpoint.substring(prefix.length());
        int colon = rest.lastIndexOf(':');
        if (colon <= 0 || colon == rest.length() - 1) {
            throw new IllegalArgumentException("Invalid FLIGHT_ENDPOINT (expected grpc+tcp://host:port): " + endpoint);
        }
        String host = rest.substring(0, colon);
        int port = Integer.parseInt(rest.substring(colon + 1));
        return new HostPort(host, port);
    }

    private record HostPort(String host, int port) {}
}

