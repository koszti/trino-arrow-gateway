package io.github.koszti.trinoarrowgateway.flight;

import io.github.koszti.trinoarrowgateway.config.GatewayFlightProperties;
import jakarta.annotation.PreDestroy;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlightServerConfig {

    private FlightServer flightServer;

    @Bean(destroyMethod = "close")
    public BufferAllocator rootAllocator() {
        // For now, max memory. Later we can make this configurable.
        return new RootAllocator(Long.MAX_VALUE);
    }

    @Bean
    @ConditionalOnProperty(prefix = "gateway.flight", name = "enabled", havingValue = "true", matchIfMissing = true)
    public FlightServer flightServer(BufferAllocator allocator,
            GatewayFlightProperties flightProps,
            TrinoFlightProducer producer) throws Exception {
        Location location = Location.forGrpcInsecure(flightProps.getBindHost(), flightProps.getPort());

        this.flightServer = FlightServer.builder(allocator, location, producer)
                .build();
        flightServer.start();
        return flightServer;
    }

    @PreDestroy
    public void shutdown() throws Exception {
        if (flightServer != null) {
            flightServer.close();
        }
    }
}
