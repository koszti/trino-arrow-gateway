package io.github.koszti.trinoarrowgateway.flight;

import jakarta.annotation.PreDestroy;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
    public FlightServer flightServer(BufferAllocator allocator,
            DemoFlightProducer producer) throws Exception {
        Location location = Location.forGrpcInsecure("0.0.0.0", 31337);

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
