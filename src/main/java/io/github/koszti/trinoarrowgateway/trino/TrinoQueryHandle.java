package io.github.koszti.trinoarrowgateway.trino;

import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Lightweight handle for a submitted Trino query.
 */
public class TrinoQueryHandle
{
    private final String queryId;
    private final List<TrinoColumn> columns;
    private final Schema arrowSchema;
    private final String spoolEncoding;
    private final List<TrinoSpoolSegment> spoolSegments;

    public TrinoQueryHandle(String queryId,
            List<TrinoColumn> columns,
            Schema arrowSchema) {
        this(queryId, columns, arrowSchema, null, List.of());
    }

    public TrinoQueryHandle(String queryId,
            List<TrinoColumn> columns,
            Schema arrowSchema,
            String spoolEncoding,
            List<TrinoSpoolSegment> spoolSegments) {
        this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
        this.columns = List.copyOf(columns);
        this.arrowSchema = Objects.requireNonNull(arrowSchema, "arrowSchema must not be null");
        this.spoolEncoding = spoolEncoding;
        this.spoolSegments = List.copyOf(spoolSegments);
    }

    public String getQueryId() {
        return queryId;
    }

    public List<TrinoColumn> getColumns() {
        return columns;
    }
    public Schema getArrowSchema() {
        return arrowSchema;
    }

    public String getSpoolEncoding() {
        return spoolEncoding;
    }

    public List<TrinoSpoolSegment> getSpoolSegments() {
        return spoolSegments;
    }

    public record TrinoColumn(String name, String type) {}

    public record TrinoSpoolSegment(
            URI uri,
            URI ackUri,
            Long rowOffset,
            Long rowsCount,
            Long segmentSize,
            String expiresAt,
            String type,
            Map<String, String> headers
    ) {}
}
