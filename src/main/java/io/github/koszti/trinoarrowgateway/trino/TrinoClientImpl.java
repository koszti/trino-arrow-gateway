package io.github.koszti.trinoarrowgateway.trino;

import io.github.koszti.trinoarrowgateway.config.GatewayTrinoProperties;
import io.github.koszti.trinoarrowgateway.spool.SpooledSegmentHeaders;
import io.github.koszti.trinoarrowgateway.trino.dto.TrinoStatementResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class TrinoClientImpl implements TrinoClient
{
    private static final Logger log = LoggerFactory.getLogger(TrinoClientImpl.class);

    private final RestClient restClient;
    private final GatewayTrinoProperties trinoProps;
    private final ObjectMapper objectMapper;

    public TrinoClientImpl(RestClient trinoRestClient,
            GatewayTrinoProperties trinoProps,
            ObjectMapper objectMapper) {
        this.restClient = trinoRestClient;
        this.trinoProps = trinoProps;
        this.objectMapper = objectMapper;
    }

    @Override
    public TrinoQueryHandle submitQuery(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");

        log.debug("Submitting query to Trino: {}", sql);

        Map<String, TrinoQueryHandle.TrinoSpoolSegment> segmentsByUri = new LinkedHashMap<>();
        String spoolEncoding = null;

        var request = restClient
                .post()
                .uri("/v1/statement")
                .header("X-Trino-User", trinoProps.getUser());

        String queryDataEncoding = trinoProps.getQueryDataEncoding();
        if (queryDataEncoding != null && !queryDataEncoding.isBlank()) {
            request = request.header("X-Trino-Query-Data-Encoding", queryDataEncoding);
        }

        TrinoStatementResponse response;
        try {
            response = request
                    .body(sql)
                    .retrieve()
                    .body(TrinoStatementResponse.class);
        } catch (Exception e) {
            throw new TrinoUnavailableException(trinoProps.getBaseUrl(), e);
        }

        if (response == null || response.getId() == null) {
            throw new IllegalStateException("Trino /v1/statement returned no id");
        }

        String queryId = response.getId();
        String state = response.getStats() != null ? response.getStats().getState() : null;
        spoolEncoding = updateSpoolState(response, segmentsByUri, spoolEncoding);

        // Follow nextUri chain until FINISHED or FAILED/CANCELED
        int maxIterations = 10_000;   // safety guard
        int iterations = 0;
        long sleepMillis = 100L;

        while (state != null && !"FINISHED".equalsIgnoreCase(state)) {
            if ("FAILED".equalsIgnoreCase(state) || "CANCELED".equalsIgnoreCase(state)) {
                String msg = response.getError() != null ? response.getError().getMessage() : "(no error message)";
                throw new TrinoQueryFailedException(queryId, state, msg);
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
                throw new TrinoUnavailableException(trinoProps.getBaseUrl(), e);
            }

            if (response == null) {
                throw new IllegalStateException("Null response while polling Trino for query " + queryId);
            }

            if (!queryId.equals(response.getId())) {
                // Safety: Trino responses for a query should all carry the same id
                log.warn("Trino response id changed from {} to {}", queryId, response.getId());
            }

            state = response.getStats() != null ? response.getStats().getState() : null;
            spoolEncoding = updateSpoolState(response, segmentsByUri, spoolEncoding);

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
        List<TrinoStatementResponse.Column> columns =
                response.getColumns() == null ? List.of() : response.getColumns();

        List<TrinoQueryHandle.TrinoColumn> cols = columns.stream()
                .map(c -> new TrinoQueryHandle.TrinoColumn(
                        c.getName(),
                        c.getType()
                ))
                .collect(Collectors.toList());

        Schema arrowSchema = toArrowSchema(columns);

        List<TrinoQueryHandle.TrinoSpoolSegment> segments = segmentsByUri.values().stream()
                .sorted(Comparator
                        .comparingLong((TrinoQueryHandle.TrinoSpoolSegment s) -> Optional.ofNullable(s.rowOffset()).orElse(Long.MAX_VALUE))
                        .thenComparing(s -> s.uri() != null ? s.uri().toString() : ""))
                .toList();

        TrinoQueryHandle handle = new TrinoQueryHandle(response.getId(), cols, arrowSchema, spoolEncoding, segments);

        log.info("Submitted Trino query. id={}, columns={}, spooledSegments={}",
                handle.getQueryId(), cols.size(), segments.size());
        return handle;
    }

    private String updateSpoolState(TrinoStatementResponse response,
            Map<String, TrinoQueryHandle.TrinoSpoolSegment> segmentsByUri,
            String spoolEncoding) {
        if (response == null || response.getData() == null) {
            return spoolEncoding;
        }

        JsonNode dataNode = response.getData();
        if (dataNode == null || !dataNode.isObject()) {
            // Inline results typically have "data" as an array-of-arrays; ignore here.
            return spoolEncoding;
        }

        TrinoStatementResponse.Data spoolData;
        try {
            spoolData = objectMapper.treeToValue(dataNode, TrinoStatementResponse.Data.class);
        } catch (Exception e) {
            log.warn("Unable to parse spooled data object from Trino response: {}", e.toString());
            return spoolEncoding;
        }

        if (spoolData.getEncoding() != null && !spoolData.getEncoding().isBlank()) {
            spoolEncoding = spoolData.getEncoding();
        }

        if (spoolData.getSegments() == null || spoolData.getSegments().isEmpty()) {
            return spoolEncoding;
        }

        for (TrinoStatementResponse.Segment s : spoolData.getSegments()) {
            if (s == null || s.getUri() == null || s.getUri().isBlank()) {
                continue;
            }

            URI uri;
            URI ackUri;
            try {
                uri = URI.create(s.getUri());
                ackUri = s.getAckUri() != null && !s.getAckUri().isBlank() ? URI.create(s.getAckUri()) : null;
            } catch (IllegalArgumentException e) {
                log.warn("Skipping invalid spooled segment URI(s): uri='{}' ackUri='{}'", s.getUri(), s.getAckUri());
                continue;
            }

            Long rowOffset = s.getMetadata() != null ? s.getMetadata().getRowOffset() : null;
            Long rowsCount = s.getMetadata() != null ? s.getMetadata().getRowsCount() : null;
            Long segmentSize = s.getMetadata() != null ? s.getMetadata().getSegmentSize() : null;
            String expiresAt = s.getMetadata() != null ? s.getMetadata().getExpiresAt() : null;
            String type = s.getType();
            Map<String, String> headers = SpooledSegmentHeaders.toSingleValueHeaders(s.getHeaders());

            segmentsByUri.putIfAbsent(uri.toString(), new TrinoQueryHandle.TrinoSpoolSegment(
                    uri,
                    ackUri,
                    rowOffset,
                    rowsCount,
                    segmentSize,
                    expiresAt,
                    type,
                    headers
            ));
        }

        return spoolEncoding;
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
        } else if (t.startsWith("DATE")) {
            return new Field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        }

        throw new IllegalArgumentException("Unsupported Trino type for now: " + trinoType + " (column " + name + ")");
    }
}
