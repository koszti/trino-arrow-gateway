package io.github.koszti.trinoarrowgateway.flight;

import io.github.koszti.trinoarrowgateway.config.GatewayConversionProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayFlightProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayTrinoProperties;
import io.github.koszti.trinoarrowgateway.convert.SpooledRowsToArrowConverter;
import io.github.koszti.trinoarrowgateway.spool.HttpSpooledSegmentClient;
import io.github.koszti.trinoarrowgateway.trino.InMemoryQueryRegistry;
import io.github.koszti.trinoarrowgateway.trino.TrinoClient;
import io.github.koszti.trinoarrowgateway.trino.TrinoQueryHandle;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrinoFlightProducerInlineSegmentTest {

    private static final class CapturingServerStreamListener implements FlightProducer.ServerStreamListener {
        private VectorSchemaRoot root;
        private final List<List<Object>> rows = new ArrayList<>();
        private Throwable error;
        private boolean completed;

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider dictionaryProvider, IpcOption option) {
            this.root = root;
        }

        @Override
        public void putNext() {
            int rowCount = root.getRowCount();
            List<FieldVector> vectors = root.getFieldVectors();
            for (int i = 0; i < rowCount; i++) {
                List<Object> row = new ArrayList<>(vectors.size());
                for (FieldVector vector : vectors) {
                    row.add(vector.getObject(i));
                }
                rows.add(row);
            }
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void error(Throwable ex) {
            this.error = ex;
        }

        @Override
        public void completed() {
            this.completed = true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable runnable) {
            // no-op
        }
    }

    private static final class FailingHttpSpooledSegmentClient extends HttpSpooledSegmentClient {
        @Override
        public FetchedSegment fetch(URI uri, URI ackUri, Map<String, String> headers) {
            throw new AssertionError("fetch should not be called for inline segments");
        }

        @Override
        public void ack(URI ackUri, Map<String, String> headers) {
            throw new AssertionError("ack should not be called for inline segments");
        }
    }

    @Test
    void getStream_streamsInlineSegment() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        byte[] inlineJson = "[[1],[2],[3]]".getBytes(StandardCharsets.UTF_8);
        TrinoQueryHandle.TrinoSpoolSegment seg = new TrinoQueryHandle.TrinoSpoolSegment(
                URI.create("inline://trino/q1/0"),
                null,
                0L,
                3L,
                (long) inlineJson.length,
                null,
                "inline",
                Map.of(),
                inlineJson
        );

        TrinoQueryHandle handle = new TrinoQueryHandle(
                "q1",
                List.of(new TrinoQueryHandle.TrinoColumn("id", "bigint")),
                schema,
                "json+zstd",
                List.of(seg)
        );

        InMemoryQueryRegistry registry = new InMemoryQueryRegistry();
        registry.register(handle);

        TrinoClient unusedClient = sql -> {
            throw new UnsupportedOperationException();
        };

        try (RootAllocator allocator = new RootAllocator()) {
            var executor = Executors.newSingleThreadExecutor();
            try {
                TrinoFlightProducer producer = new TrinoFlightProducer(
                        allocator,
                        unusedClient,
                        registry,
                        new GatewayTrinoProperties(),
                        new GatewayFlightProperties(),
                        new FailingHttpSpooledSegmentClient(),
                        new SpooledRowsToArrowConverter(allocator),
                        executor,
                        new GatewayConversionProperties()
                );

                CapturingServerStreamListener listener = new CapturingServerStreamListener();
                producer.getStream(null, new Ticket("q1".getBytes(StandardCharsets.UTF_8)), listener);

                assertNull(listener.error);
                assertTrue(listener.completed);
                assertEquals(List.of(
                        List.of(1L),
                        List.of(2L),
                        List.of(3L)
                ), listener.rows);
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
