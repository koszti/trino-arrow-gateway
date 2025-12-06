package com.wise.streamprocessing.trinoarrowgateway;

import com.wise.streamprocessing.trinoarrowgateway.config.GatewayConversionProperties;
import com.wise.streamprocessing.trinoarrowgateway.config.GatewaySpoolS3Properties;
import com.wise.streamprocessing.trinoarrowgateway.config.GatewayTrinoProperties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
		GatewayTrinoProperties.class,
		GatewaySpoolS3Properties.class,
		GatewayConversionProperties.class
})
public class TrinoArrowGatewayApplication {

	public static void main(String[] args) {
		SpringApplication.run(TrinoArrowGatewayApplication.class, args);
	}

}
