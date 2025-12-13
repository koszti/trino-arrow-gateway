package io.github.koszti.trinoarrowgateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "gateway.flight.enabled=false")
class TrinoArrowGatewayApplicationTests {

	@Test
	void contextLoads() {
	}

}
