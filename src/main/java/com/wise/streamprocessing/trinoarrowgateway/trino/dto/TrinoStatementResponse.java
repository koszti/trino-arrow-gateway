package com.wise.streamprocessing.trinoarrowgateway.trino.dto;

import java.util.List;

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
