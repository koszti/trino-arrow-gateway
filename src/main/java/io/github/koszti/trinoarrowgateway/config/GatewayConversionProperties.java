package io.github.koszti.trinoarrowgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.conversion")
public class GatewayConversionProperties {

    /**
     * Number of threads used for JSON → Arrow conversion.
     */
    private int parallelism = Runtime.getRuntime().availableProcessors();

    /**
     * Number of rows per Arrow record batch when streaming results to Flight.
     */
    private int batchSize = 1024;

    /**
     * Maximum number of spooled segments processed concurrently.
     * Defaults to {@link #parallelism}.
     */
    private Integer maxInFlightSegments;

    /**
     * Maximum number of Arrow record batches buffered per segment while streaming.
     * Bounds memory usage when conversion is faster than Flight streaming.
     */
    private int maxBufferedBatchesPerSegment = 4;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getBatchSize() {
        return Math.max(1, batchSize);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxInFlightSegments() {
        if (maxInFlightSegments == null) {
            return Math.max(1, parallelism);
        }
        return Math.max(1, maxInFlightSegments);
    }

    public void setMaxInFlightSegments(Integer maxInFlightSegments) {
        this.maxInFlightSegments = maxInFlightSegments;
    }

    public int getMaxBufferedBatchesPerSegment() {
        return Math.max(1, maxBufferedBatchesPerSegment);
    }

    public void setMaxBufferedBatchesPerSegment(int maxBufferedBatchesPerSegment) {
        this.maxBufferedBatchesPerSegment = maxBufferedBatchesPerSegment;
    }
}
