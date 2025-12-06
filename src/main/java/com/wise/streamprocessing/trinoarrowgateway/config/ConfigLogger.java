package com.wise.streamprocessing.trinoarrowgateway.config;

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
    private final GatewaySpoolS3Properties spoolProps;
    private final GatewayConversionProperties convProps;

    public ConfigLogger(GatewayTrinoProperties trinoProps,
            GatewaySpoolS3Properties spoolProps,
            GatewayConversionProperties convProps) {
        this.trinoProps = trinoProps;
        this.spoolProps = spoolProps;
        this.convProps = convProps;
    }

    @Override
    public void run(String... args)
    {
        log.info("Trino base URL      : {}", trinoProps.getBaseUrl());
        log.info("Trino user          : {}", trinoProps.getUser());
        log.info("Spool S3 bucket     : {}", spoolProps.getBucket());
        log.info("Spool S3 prefix     : {}", spoolProps.getPrefix());
        log.info("Conversion threads  : {}", convProps.getParallelism());
    }
}
