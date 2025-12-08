package io.github.koszti.trinoarrowgateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class TrinoRestClientConfig
{
    /**
     * RestClient dedicated to talking to Trino.
     * Base URL is taken from GatewayTrinoProperties.
     */
    @Bean
    public RestClient trinoRestClient(RestClient.Builder builder,
            GatewayTrinoProperties trinoProps) {
        return builder
                .baseUrl(trinoProps.getBaseUrl())
                .build();
    }
}
