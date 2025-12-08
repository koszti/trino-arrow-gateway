package io.github.koszti.trinoarrowgateway.trino;

import io.github.koszti.trinoarrowgateway.config.GatewayTrinoProperties;
import io.github.koszti.trinoarrowgateway.trino.dto.TrinoStatementResponse;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class TrinoClientImpl implements TrinoClient
{
    private static final Logger log = LoggerFactory.getLogger(TrinoClientImpl.class);

    private final RestClient restClient;
    private final GatewayTrinoProperties trinoProps;

    public TrinoClientImpl(RestClient trinoRestClient,
            GatewayTrinoProperties trinoProps) {
        this.restClient = trinoRestClient;
        this.trinoProps = trinoProps;
    }

    @Override
    public TrinoQueryHandle submitQuery(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");

        log.debug("Submitting query to Trino: {}", sql);

        TrinoStatementResponse response = restClient
                .post()
                .uri("/v1/statement")
                .header("X-Trino-User", trinoProps.getUser())
                .body(sql)
                .retrieve()
                .body(TrinoStatementResponse.class);

        if (response == null || response.getId() == null) {
            throw new IllegalStateException("Trino /v1/statement returned no id");
        }

        String queryId = response.getId();
        String state = response.getStats() != null ? response.getStats().getState() : null;

        // Follow nextUri chain until FINISHED or FAILED/CANCELED
        int maxIterations = 10_000;   // safety guard
        int iterations = 0;
        long sleepMillis = 100L;

        while (state != null && !"FINISHED".equalsIgnoreCase(state)) {
            if ("FAILED".equalsIgnoreCase(state) || "CANCELED".equalsIgnoreCase(state)) {
                String msg = response.getError() != null ? response.getError().getMessage() : "(no error message)";
                throw new IllegalStateException("Trino query " + queryId + " is in state " + state + ": " + msg);
            }

            String nextUri = response.getNextUri();
            if (nextUri == null || nextUri.isEmpty()) {
                throw new IllegalStateException("Trino query " + queryId + " is in state " + state + " but nextUri is null");
            }

            if (iterations++ > maxIterations) {
                throw new IllegalStateException("Trino query " + queryId + " did not reach FINISHED after " + maxIterations + " polls");
            }

            try {
                // nextUri is usually absolute (http://trino:8080/v1/statement/...),
                // so we pass it as-is; RestClient will handle the full URI.
                response = restClient
                        .get()
                        .uri(nextUri)
                        .retrieve()
                        .body(TrinoStatementResponse.class);
            } catch (Exception e) {
                throw new IllegalStateException("Error polling Trino nextUri for query " + queryId, e);
            }

            if (response == null) {
                throw new IllegalStateException("Null response while polling Trino for query " + queryId);
            }

            if (!queryId.equals(response.getId())) {
                // Safety: Trino responses for a query should all carry the same id
                log.warn("Trino response id changed from {} to {}", queryId, response.getId());
            }

            state = response.getStats() != null ? response.getStats().getState() : null;

            // Optional: avoid hammering Trino too hard
            if (!"FINISHED".equalsIgnoreCase(state)) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while polling Trino for query " + queryId, ie);
                }
            }
        }

        // At this point we consider the query FINISHED.
        List<TrinoQueryHandle.TrinoColumn> cols =
                response.getColumns() == null
                        ? Collections.emptyList()
                        : response.getColumns().stream()
                        .map(c -> new TrinoQueryHandle.TrinoColumn(
                                c.getName(),
                                c.getType()
                        ))
                        .collect(Collectors.toList());

        Schema arrowSchema = toArrowSchema(response.getColumns());

        TrinoQueryHandle handle = new TrinoQueryHandle(response.getId(), cols, arrowSchema);

        log.info("Submitted Trino query. id={}, columns={}", handle.getQueryId(), cols.size());
        return handle;
    }

    private Schema toArrowSchema(List<TrinoStatementResponse.Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return new Schema(List.of());
        }

        List<Field> fields = columns.stream()
                .map(c -> toArrowField(c.getName(), c.getType()))
                .collect(Collectors.toList());

        return new Schema(fields);
    }

    private Field toArrowField(String name, String trinoType) {
        // Very minimal type mapping for now.
        String t = trinoType.toUpperCase(Locale.ROOT);

        if (t.startsWith("BIGINT")) {
            return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
        } else if (t.startsWith("INTEGER")) {
            return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
        } else if (t.startsWith("DOUBLE")) {
            return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        } else if (t.startsWith("VARCHAR") || t.startsWith("CHAR")) {
            return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
        } else if (t.startsWith("BOOLEAN")) {
            return new Field(name, FieldType.nullable(new ArrowType.Bool()), null);
        }

        throw new IllegalArgumentException("Unsupported Trino type for now: " + trinoType + " (column " + name + ")");
    }
}
