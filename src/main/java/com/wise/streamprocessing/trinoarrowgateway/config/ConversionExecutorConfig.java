package com.wise.streamprocessing.trinoarrowgateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ConversionExecutorConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService conversionExecutor(GatewayConversionProperties props) {
        int threads = Math.max(1, props.getParallelism());
        return Executors.newFixedThreadPool(threads);
    }
}
