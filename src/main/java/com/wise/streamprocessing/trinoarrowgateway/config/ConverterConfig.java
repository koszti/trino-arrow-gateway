package com.wise.streamprocessing.trinoarrowgateway.config;

import com.wise.streamprocessing.trinoarrowgateway.convert.JsonToArrowConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConverterConfig {

    @Bean
    public JsonToArrowConverter jsonToArrowConverter(BufferAllocator allocator) {
        return new JsonToArrowConverter(allocator);
    }
}
