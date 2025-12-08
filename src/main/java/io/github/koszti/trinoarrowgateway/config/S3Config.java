package io.github.koszti.trinoarrowgateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3Config {

    @Bean
    public S3Client s3Client(GatewaySpoolS3Properties s3Props) {
        return S3Client.builder()
                .region(Region.of(s3Props.getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
}
