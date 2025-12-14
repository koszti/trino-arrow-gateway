package io.github.koszti.trinoarrowgateway.trino.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * Minimal view of Trino's /v1/statement response.
 * We only care about id + columns for now.
 */
public class TrinoStatementResponse
{
    private String id;
    private List<Column> columns;
    private String nextUri;
    private Stats stats;
    private TrinoError error;
    /**
     * Trino's "data" field is polymorphic:
     * - When results are inlined, it's typically an array-of-arrays (rows).
     * - When results are spooled, it's an object (encoding + segments).
     * We keep it as JsonNode and interpret it where needed.
     */
    private JsonNode data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public String getNextUri() {
        return nextUri;
    }

    public void setNextUri(String nextUri) {
        this.nextUri = nextUri;
    }

    public Stats getStats() {
        return stats;
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public TrinoError getError() {
        return error;
    }

    public void setError(TrinoError error) {
        this.error = error;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }

    public static class Column {
        private String name;
        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    /**
     * Result payload, including spooled segments (when Trino uses spooling).
     */
    public static class Data {
        private String encoding; // e.g. "json+zstd"
        private List<Segment> segments;

        public String getEncoding() {
            return encoding;
        }

        public void setEncoding(String encoding) {
            this.encoding = encoding;
        }

        public List<Segment> getSegments() {
            return segments;
        }

        public void setSegments(List<Segment> segments) {
            this.segments = segments;
        }
    }

    public static class Segment {
        private String type;   // e.g. "spooled"
        private String uri;    // download URI (http(s)://...)
        private String ackUri; // ack URI (http(s)://...)
        private String data;   // inline segment payload (base64-encoded)
        private SegmentMetadata metadata;
        private Map<String, List<String>> headers;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public String getAckUri() {
            return ackUri;
        }

        public void setAckUri(String ackUri) {
            this.ackUri = ackUri;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public SegmentMetadata getMetadata() {
            return metadata;
        }

        public void setMetadata(SegmentMetadata metadata) {
            this.metadata = metadata;
        }

        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, List<String>> headers) {
            this.headers = headers;
        }
    }

    public static class SegmentMetadata {
        private Long segmentSize;
        private Long rowsCount;
        private String expiresAt;
        private Long rowOffset;

        public Long getSegmentSize() {
            return segmentSize;
        }

        public void setSegmentSize(Long segmentSize) {
            this.segmentSize = segmentSize;
        }

        public Long getRowsCount() {
            return rowsCount;
        }

        public void setRowsCount(Long rowsCount) {
            this.rowsCount = rowsCount;
        }

        public String getExpiresAt() {
            return expiresAt;
        }

        public void setExpiresAt(String expiresAt) {
            this.expiresAt = expiresAt;
        }

        public Long getRowOffset() {
            return rowOffset;
        }

        public void setRowOffset(Long rowOffset) {
            this.rowOffset = rowOffset;
        }
    }

    public static class Stats {
        private String state; // QUEUED, RUNNING, FINISHED, FAILED, CANCELED, ...

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    public static class TrinoError {
        private String message;
        private String errorType;
        private int errorCode;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getErrorType() {
            return errorType;
        }

        public void setErrorType(String errorType) {
            this.errorType = errorType;
        }

        public int getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(int errorCode) {
            this.errorCode = errorCode;
        }
    }
}
