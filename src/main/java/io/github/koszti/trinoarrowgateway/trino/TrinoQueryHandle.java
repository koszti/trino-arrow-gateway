package io.github.koszti.trinoarrowgateway.trino;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Objects;

/**
 * Lightweight handle for a submitted Trino query.
 */
public class TrinoQueryHandle
{
    private final String queryId;
    private final List<TrinoColumn> columns;
    private final Schema arrowSchema;

    public TrinoQueryHandle(String queryId,
            List<TrinoColumn> columns,
            Schema arrowSchema) {
        this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
        this.columns = List.copyOf(columns);
        this.arrowSchema = Objects.requireNonNull(arrowSchema, "arrowSchema must not be null");
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


    public record TrinoColumn(String name, String type) {}
}
