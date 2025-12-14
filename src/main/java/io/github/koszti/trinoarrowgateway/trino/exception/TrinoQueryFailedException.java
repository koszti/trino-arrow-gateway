package io.github.koszti.trinoarrowgateway.trino.exception;

import java.util.Objects;

public class TrinoQueryFailedException extends RuntimeException {

    private final String queryId;
    private final String state;

    public TrinoQueryFailedException(String queryId, String state, String message) {
        super(message);
        this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
        this.state = state;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getState() {
        return state;
    }
}
