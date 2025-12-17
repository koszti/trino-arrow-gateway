import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class TrinoJdbcBenchmark {
    private static final String DEFAULT_SQL = "SELECT * FROM tpch.sf1.orders LIMIT 1000000";
    private static final int DEFAULT_SAMPLE_N = 10;

    public static void main(String[] args) throws Exception {
        String sql = args.length > 0 ? String.join(" ", args) : getenv("TRINO_SQL", DEFAULT_SQL);

        String jdbcUrl = getenv("TRINO_JDBC_URL", "jdbc:trino://localhost:8080");
        String user = getenv("TRINO_USER", "trino-arrow-gateway-jdbc");
        String password = System.getenv("TRINO_PASSWORD");
        int fetchSize = parseInt(getenv("TRINO_FETCH_SIZE", "1000"), 1000);
        int sampleN = parseInt(getenv("TRINO_SAMPLE_N", Integer.toString(DEFAULT_SAMPLE_N)), DEFAULT_SAMPLE_N);
        long progressEvery = parseLong(getenv("TRINO_PROGRESS_EVERY", "100000"), 100_000L);

        System.out.println("Trino JDBC Request");
        System.out.println("==================");
        System.out.println("- JDBC URL: " + jdbcUrl);
        System.out.println("- User: " + user);
        System.out.println("- SQL: " + sql);
        System.out.println("- Fetch size: " + fetchSize);
        System.out.println("- Sample rows: " + sampleN);
        System.out.println();

        Properties properties = new Properties();
        properties.setProperty("user", user);
        if (password != null && !password.isBlank()) {
            properties.setProperty("password", password);
        }

        long t0 = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement statement = connection.createStatement()) {
            statement.setFetchSize(Math.max(1, fetchSize));

            long t1 = System.nanoTime();
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                long t2 = System.nanoTime();

                ResultSetMetaData meta = resultSet.getMetaData();
                int columns = meta.getColumnCount();
                if (columns > 0) {
                    System.out.println("- Schema: " + schemaSummary(meta, columns));
                }

                long rows = 0;
                long lastReportAt = 0;
                List<List<String>> sampleRows = new ArrayList<>();
                while (resultSet.next()) {
                    if (sampleN > 0 && sampleRows.size() < sampleN) {
                        sampleRows.add(readRowStrings(resultSet, columns));
                    }
                    for (int i = 1; i <= columns; i++) {
                        resultSet.getObject(i);
                    }
                    rows++;

                    if (progressEvery > 0 && rows - lastReportAt >= progressEvery) {
                        Duration elapsed = Duration.ofNanos(System.nanoTime() - t2);
                        System.out.println("- Progress: " + rows + " rows (" + format(elapsed) + ")");
                        lastReportAt = rows;
                    }
                }

                long t3 = System.nanoTime();

                System.out.println();
                System.out.println("Result Summary");
                System.out.println("==============");
                System.out.println("- Rows: " + rows);
                System.out.println("- Columns: " + columns);
                System.out.println("- Connect time: " + format(Duration.ofNanos(t1 - t0)));
                System.out.println("- Execute time: " + format(Duration.ofNanos(t2 - t1)));
                System.out.println("- Fetch time: " + format(Duration.ofNanos(t3 - t2)));
                System.out.println("- Total time: " + format(Duration.ofNanos(t3 - t0)));

                if (sampleN > 0) {
                    System.out.println();
                    System.out.println("Sample Data (first " + Math.min(sampleN, sampleRows.size()) + " rows)");
                    System.out.println("=================================================");
                    printSampleTable(meta, columns, sampleRows);
                }
            }
        }
    }

    private static String schemaSummary(ResultSetMetaData meta, int columns) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(columns).append(" columns (");
        for (int i = 1; i <= columns; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            String name = meta.getColumnLabel(i);
            String type = meta.getColumnTypeName(i);
            sb.append(name).append(": ").append(type);
        }
        sb.append(")");
        return sb.toString();
    }

    private static List<String> readRowStrings(ResultSet resultSet, int columns) throws Exception {
        List<String> cells = new ArrayList<>(columns);
        for (int i = 1; i <= columns; i++) {
            Object value = resultSet.getObject(i);
            cells.add(value == null ? "NULL" : String.valueOf(value));
        }
        return cells;
    }

    private static void printSampleTable(ResultSetMetaData meta, int columns, List<List<String>> rows) throws Exception {
        if (columns <= 0) {
            System.out.println("(no columns)");
            return;
        }
        if (rows.isEmpty()) {
            System.out.println("(no rows)");
            return;
        }

        List<String> headers = new ArrayList<>(columns);
        for (int i = 1; i <= columns; i++) {
            headers.add(meta.getColumnLabel(i));
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

    private static long parseLong(String value, long defaultValue) {
        try {
            return Long.parseLong(value.trim());
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
}
