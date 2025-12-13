package io.github.koszti.trinoarrowgateway;

import io.github.koszti.trinoarrowgateway.config.GatewayConversionProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayFlightProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayTrinoProperties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
		GatewayTrinoProperties.class,
		GatewayFlightProperties.class,
		GatewayConversionProperties.class
})
public class TrinoArrowGatewayApplication {

	public static void main(String[] args) {
		SpringApplication.run(TrinoArrowGatewayApplication.class, args);
	}

}
