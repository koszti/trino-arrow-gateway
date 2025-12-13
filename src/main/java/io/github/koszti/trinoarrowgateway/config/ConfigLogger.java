package io.github.koszti.trinoarrowgateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ConfigLogger
        implements CommandLineRunner
{
    private static final Logger log = LoggerFactory.getLogger(ConfigLogger.class);

    private final GatewayTrinoProperties trinoProps;
    private final GatewayConversionProperties convProps;
    private final GatewayFlightProperties flightProps;

    public ConfigLogger(GatewayTrinoProperties trinoProps,
            GatewayConversionProperties convProps,
            GatewayFlightProperties flightProps) {
        this.trinoProps = trinoProps;
        this.convProps = convProps;
        this.flightProps = flightProps;
    }

    @Override
    public void run(String... args)
    {
        log.info("Trino base URL      : {}", trinoProps.getBaseUrl());
        log.info("Trino user          : {}", trinoProps.getUser());
        log.info("Trino data encoding : {}", trinoProps.getQueryDataEncoding());
        log.info("Flight bind         : {}:{}", flightProps.getBindHost(), flightProps.getPort());
        log.info("Flight advertise    : {}:{}", flightProps.getAdvertiseHost(), flightProps.getPort());
        log.info("Conversion threads  : {}", convProps.getParallelism());
        log.info("Arrow batch size    : {}", convProps.getBatchSize());
        log.info("In-flight segments  : {}", convProps.getMaxInFlightSegments());
        log.info("Batch buffer/segment: {}", convProps.getMaxBufferedBatchesPerSegment());
    }
}
