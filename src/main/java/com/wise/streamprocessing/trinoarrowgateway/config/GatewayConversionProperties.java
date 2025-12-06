package com.wise.streamprocessing.trinoarrowgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.conversion")
public class GatewayConversionProperties {

    /**
     * Number of threads used for JSON → Arrow conversion.
     */
    private int parallelism = Runtime.getRuntime().availableProcessors();

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
