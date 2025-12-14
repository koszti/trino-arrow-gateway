package io.github.koszti.trinoarrowgateway.trino.exception;

import java.util.Objects;

public class TrinoUnavailableException extends RuntimeException {

    private final String baseUrl;

    public TrinoUnavailableException(String baseUrl, Throwable cause) {
        super(cause);
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null");
    }

    public String getBaseUrl() {
        return baseUrl;
    }
}
