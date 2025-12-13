package io.github.koszti.trinoarrowgateway.config;

import io.github.koszti.trinoarrowgateway.convert.SpooledRowsToArrowConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConverterConfig {

    @Bean
    public SpooledRowsToArrowConverter spooledRowsToArrowConverter(BufferAllocator allocator) {
        return new SpooledRowsToArrowConverter(allocator);
    }
}
