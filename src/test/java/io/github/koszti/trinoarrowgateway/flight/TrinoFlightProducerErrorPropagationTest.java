package io.github.koszti.trinoarrowgateway.flight;

import io.github.koszti.trinoarrowgateway.config.GatewayConversionProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayFlightProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayTrinoProperties;
import io.github.koszti.trinoarrowgateway.convert.SpooledRowsToArrowConverter;
import io.github.koszti.trinoarrowgateway.spool.HttpSpooledSegmentClient;
import io.github.koszti.trinoarrowgateway.trino.InMemoryQueryRegistry;
import io.github.koszti.trinoarrowgateway.trino.TrinoClient;
import io.github.koszti.trinoarrowgateway.trino.TrinoQueryFailedException;
import io.github.koszti.trinoarrowgateway.trino.TrinoUnavailableException;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrinoFlightProducerErrorPropagationTest {

    @Test
    void getFlightInfo_returnsTrinoSqlFailureToClient() {
        TrinoClient trinoClient = sql -> {
            throw new TrinoQueryFailedException("q1", "FAILED", "line 1:15: Table 'tpch.sf1.regionx' does not exist");
        };

        try (RootAllocator allocator = new RootAllocator()) {
            var executor = Executors.newSingleThreadExecutor();
            try {
                TrinoFlightProducer producer = new TrinoFlightProducer(
                        allocator,
                        trinoClient,
                        new InMemoryQueryRegistry(),
                        new GatewayTrinoProperties(),
                        new GatewayFlightProperties(),
                        new HttpSpooledSegmentClient(),
                        new SpooledRowsToArrowConverter(allocator),
                        executor,
                        new GatewayConversionProperties()
                );

                FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () ->
                        producer.getFlightInfo(null, FlightDescriptor.command("SELECT *".getBytes(StandardCharsets.UTF_8))));

                assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
                assertTrue(e.getMessage().contains("Trino query failed"));
                assertTrue(e.getMessage().contains("Table 'tpch.sf1.regionx' does not exist"));
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void getFlightInfo_returnsUnavailableWhenTrinoCannotBeReached() {
        TrinoClient trinoClient = sql -> {
            throw new TrinoUnavailableException("http://localhost:8081", new RuntimeException("connection refused"));
        };

        try (RootAllocator allocator = new RootAllocator()) {
            var executor = Executors.newSingleThreadExecutor();
            try {
                TrinoFlightProducer producer = new TrinoFlightProducer(
                        allocator,
                        trinoClient,
                        new InMemoryQueryRegistry(),
                        new GatewayTrinoProperties(),
                        new GatewayFlightProperties(),
                        new HttpSpooledSegmentClient(),
                        new SpooledRowsToArrowConverter(allocator),
                        executor,
                        new GatewayConversionProperties()
                );

                FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () ->
                        producer.getFlightInfo(null, FlightDescriptor.command("SELECT *".getBytes(StandardCharsets.UTF_8))));

                assertEquals(FlightStatusCode.UNAVAILABLE, e.status().code());
                assertTrue(e.getMessage().contains("Trino is unavailable"));
                assertTrue(e.getMessage().contains("http://localhost:8081"));
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
