package io.github.koszti.trinoarrowgateway.trino.exception;

public class TrinoRequestRejectedException extends RuntimeException {
    private final int statusCode;

    public TrinoRequestRejectedException(int statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
