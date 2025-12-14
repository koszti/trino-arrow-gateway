package io.github.koszti.trinoarrowgateway.flight;

import io.github.koszti.trinoarrowgateway.config.GatewayTrinoProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayConversionProperties;
import io.github.koszti.trinoarrowgateway.config.GatewayFlightProperties;
import io.github.koszti.trinoarrowgateway.convert.SpooledRowsToArrowConverter;
import io.github.koszti.trinoarrowgateway.spool.HttpSpooledSegmentClient;
import io.github.koszti.trinoarrowgateway.trino.QueryRegistry;
import io.github.koszti.trinoarrowgateway.trino.TrinoClient;
import io.github.koszti.trinoarrowgateway.trino.TrinoQueryHandle;
import io.github.koszti.trinoarrowgateway.trino.exception.TrinoQueryFailedException;
import io.github.koszti.trinoarrowgateway.trino.exception.TrinoRequestRejectedException;
import io.github.koszti.trinoarrowgateway.trino.exception.TrinoUnavailableException;
import com.github.luben.zstd.ZstdInputStream;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.PushbackInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Flight producer that:
 *  - Interprets descriptor.command as SQL
 *  - Submits SQL to Trino and gets a queryId + Arrow schema
 *  - Uses ticket to carry queryId
 *  - In getStream, downloads Trino spooled segments and streams Arrow batches
 */
@Component
public class TrinoFlightProducer extends NoOpFlightProducer {
    private static final Logger log = LoggerFactory.getLogger(TrinoFlightProducer.class);

    private final BufferAllocator allocator;
    private final Location location;
    private final TrinoClient trinoClient;
    private final QueryRegistry queryRegistry;
    private final GatewayTrinoProperties trinoProps;
    private final HttpSpooledSegmentClient spooledSegmentClient;
    private final SpooledRowsToArrowConverter spooledRowsToArrowConverter;
    private final ExecutorService conversionExecutor;
    private final GatewayConversionProperties conversionProps;
    @SuppressWarnings("unused")
    private final GatewayFlightProperties flightProps;

    public TrinoFlightProducer(BufferAllocator allocator,
            TrinoClient trinoClient,
            QueryRegistry queryRegistry,
            GatewayTrinoProperties trinoProps,
            GatewayFlightProperties flightProps,
            HttpSpooledSegmentClient spooledSegmentClient,
            SpooledRowsToArrowConverter spooledRowsToArrowConverter,
            ExecutorService conversionExecutor,
            GatewayConversionProperties conversionProps) {
        this.allocator = allocator;
        this.flightProps = flightProps;
        this.location = Location.forGrpcInsecure(flightProps.getAdvertiseHost(), flightProps.getPort());
        this.trinoClient = trinoClient;
        this.queryRegistry = queryRegistry;
        this.trinoProps = trinoProps;
        this.spooledSegmentClient = spooledSegmentClient;
        this.spooledRowsToArrowConverter = spooledRowsToArrowConverter;
        this.conversionExecutor = conversionExecutor;
        this.conversionProps = conversionProps;
    }

    private static final class SegmentItem {
        final ArrowRecordBatch batch;
        final Throwable error;
        final boolean end;

        private SegmentItem(ArrowRecordBatch batch, Throwable error, boolean end) {
            this.batch = batch;
            this.error = error;
            this.end = end;
        }

        static SegmentItem batch(ArrowRecordBatch batch) {
            return new SegmentItem(batch, null, false);
        }

        static SegmentItem error(Throwable t) {
            return new SegmentItem(null, t, false);
        }

        static SegmentItem end() {
            return new SegmentItem(null, null, true);
        }
    }

    private record SegmentPipe(TrinoQueryHandle.TrinoSpoolSegment segment,
            BlockingQueue<SegmentItem> queue,
            Future<?> future) {}

    private static void put(BlockingQueue<SegmentItem> queue, SegmentItem item) {
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueueing Arrow batch", e);
        }
    }

    private static boolean isJsonEncoding(String encoding) {
        return encoding == null || encoding.isBlank() || "json".equalsIgnoreCase(encoding);
    }

    private static boolean isJsonZstdEncoding(String encoding) {
        return "json+zstd".equalsIgnoreCase(encoding);
    }

    private static InputStream maybeDecodeZstd(InputStream raw, boolean zstdPreferred) throws IOException {
        if (!zstdPreferred) {
            return raw;
        }

        // Zstandard frames start with the magic bytes: 28 B5 2F FD
        PushbackInputStream in = new PushbackInputStream(raw, 4);
        byte[] header = in.readNBytes(4);
        if (header.length > 0) {
            in.unread(header);
        }

        boolean looksZstd = header.length == 4
                && (header[0] & 0xFF) == 0x28
                && (header[1] & 0xFF) == 0xB5
                && (header[2] & 0xFF) == 0x2F
                && (header[3] & 0xFF) == 0xFD;

        // Trino may sometimes label payload as json+zstd even when it is plain JSON.
        if (!looksZstd) {
            return in;
        }

        return new ZstdInputStream(in);
    }

    private static void fail(ServerStreamListener listener, CallStatus status, String message) {
        listener.error(status.withDescription(message).toRuntimeException());
    }

    private static Throwable rootCause(Throwable t) {
        if (t == null) {
            return null;
        }
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static String safeMessage(Throwable t) {
        if (t == null) {
            return "(null)";
        }
        String msg = t.getMessage();
        if (msg == null || msg.isBlank()) {
            return t.getClass().getSimpleName();
        }
        return msg;
    }

    private static FlightRuntimeException toStreamFailure(String queryId, Throwable t) {
        if (t instanceof FlightRuntimeException fre) {
            return fre;
        }
        Throwable root = rootCause(t);
        String rootMsg = safeMessage(root != null ? root : t);
        String msg = "Failed to stream spooled results for queryId=" + queryId + ": " + rootMsg;
        return CallStatus.INTERNAL.withDescription(msg).withCause(t).toRuntimeException();
    }

    private static String unsupportedEncodingMessage(String encoding) {
        return "Unsupported Trino spooled encoding: " + encoding + " (supported: json, json+zstd)";
    }

    @Override
    public FlightInfo getFlightInfo(FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        byte[] cmd = descriptor.getCommand();
        if (cmd == null) {
            throw new IllegalArgumentException("Only command descriptors (SQL) are supported for now");
        }

        String sql = new String(cmd, StandardCharsets.UTF_8);
        log.info("getFlightInfo: received SQL: {}", sql);

        TrinoQueryHandle handle;
        try {
            handle = trinoClient.submitQuery(sql);
        } catch (TrinoRequestRejectedException e) {
            String msg = "Trino rejected query submission (HTTP " + e.getStatusCode() + "): " + e.getMessage();
            log.info("Flight SQL rejected by Trino: {}", msg);
            throw CallStatus.INVALID_ARGUMENT.withDescription(msg).withCause(e).toRuntimeException();
        } catch (TrinoQueryFailedException e) {
            String msg = "Trino query failed (queryId=" + e.getQueryId() + "): " + e.getMessage();
            log.info("Flight SQL failed: {}", msg);
            throw CallStatus.INVALID_ARGUMENT.withDescription(msg).withCause(e).toRuntimeException();
        } catch (IllegalArgumentException e) {
            String msg = "Unsupported query result schema: " + e.getMessage();
            log.info("Flight SQL schema unsupported: {}", msg);
            throw CallStatus.INVALID_ARGUMENT.withDescription(msg).withCause(e).toRuntimeException();
        } catch (TrinoUnavailableException e) {
            String msg = String.format(
                    "Trino is unavailable at %s. Start Trino or update gateway.trino.base-url.",
                    e.getBaseUrl());
            log.warn("Unable to submit query to Trino for Flight request: {}", msg, e);
            throw CallStatus.UNAVAILABLE.withDescription(msg).withCause(e).toRuntimeException();
        } catch (Exception e) {
            String msg = "Unexpected error while submitting query to Trino: " + e.getMessage();
            log.warn(msg, e);
            throw CallStatus.INTERNAL.withDescription(msg).withCause(e).toRuntimeException();
        }
        queryRegistry.register(handle);

        Schema schema = handle.getArrowSchema();

        // Ticket encodes the Trino queryId
        Ticket ticket = new Ticket(handle.getQueryId().getBytes(StandardCharsets.UTF_8));
        FlightEndpoint endpoint = new FlightEndpoint(ticket, location);

        return new FlightInfo(
                schema,
                descriptor,
                Collections.singletonList(endpoint),
                /* bytes */ -1,
                /* records */ -1
        );
    }

    @Override
    public void getStream(FlightProducer.CallContext context,
            Ticket ticket,
            FlightProducer.ServerStreamListener listener)
    {
        String queryId = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        log.info("getStream: queryId={}", queryId);

        TrinoQueryHandle handle = queryRegistry.get(queryId);
        if (handle == null) {
            fail(listener, CallStatus.NOT_FOUND, "Unknown queryId: " + queryId);
            return;
        }

        Schema schema = handle.getArrowSchema();

        if (handle.getSpoolSegments().isEmpty()) {
            String msg = "Trino did not return spooled segments for queryId=" + queryId +
                    ". Ensure Trino spooling is enabled and X-Trino-Query-Data-Encoding requests spooling.";
            fail(listener, CallStatus.INVALID_ARGUMENT, msg);
            return;
        }

        String encoding = handle.getSpoolEncoding();
        if (!isJsonEncoding(encoding) && !isJsonZstdEncoding(encoding)) {
            fail(listener, CallStatus.INVALID_ARGUMENT, unsupportedEncodingMessage(encoding));
            return;
        }

        try {
            streamSpooledSegments(handle, schema, encoding, listener);
        } catch (Throwable t) {
            log.warn("getStream failed for queryId={}: {}", queryId, safeMessage(t), t);
            listener.error(toStreamFailure(queryId, t));
        }
    }

    private void streamSpooledSegments(TrinoQueryHandle handle,
            Schema schema,
            String encoding,
            FlightProducer.ServerStreamListener listener) throws Exception {
        int batchSize = conversionProps.getBatchSize();
        int maxInFlightSegments = conversionProps.getMaxInFlightSegments();
        int maxBufferedBatchesPerSegment = conversionProps.getMaxBufferedBatchesPerSegment();

        Semaphore inFlight = new Semaphore(maxInFlightSegments);
        boolean isJsonZstd = isJsonZstdEncoding(encoding);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VectorLoader loader = new VectorLoader(root);
            listener.start(root);

            List<SegmentPipe> pipes = handle.getSpoolSegments().stream()
                    .map(segment -> {
                        BlockingQueue<SegmentItem> queue = new ArrayBlockingQueue<>(maxBufferedBatchesPerSegment);

                        Future<?> future = conversionExecutor.submit(() -> {
                            boolean acquired = false;
                            try {
                                inFlight.acquire();
                                acquired = true;

                                URI uri = segment.uri();
                                URI ackUri = segment.ackUri();
                                var headers = segment.headers();
                                byte[] inlineData = segment.inlineData();

                                if (inlineData != null) {
                                    try (InputStream raw = new ByteArrayInputStream(inlineData);
                                            InputStream decoded = maybeDecodeZstd(raw, isJsonZstd)) {
                                        spooledRowsToArrowConverter.convertStreaming(decoded, schema, batchSize,
                                                batch -> put(queue, SegmentItem.batch(batch)));
                                    }
                                } else {
                                    try (HttpSpooledSegmentClient.FetchedSegment fetched = spooledSegmentClient.fetch(uri, ackUri, headers)) {
                                        try (InputStream decoded = maybeDecodeZstd(fetched.body(), isJsonZstd)) {
                                            spooledRowsToArrowConverter.convertStreaming(decoded, schema, batchSize,
                                                    batch -> put(queue, SegmentItem.batch(batch)));
                                        }
                                    }
                                }

                                if (inlineData == null) {
                                    spooledSegmentClient.ack(ackUri, headers);
                                }
                                put(queue, SegmentItem.end());
                            } catch (Throwable t) {
                                Throwable wrapped = t;
                                try {
                                    wrapped = new RuntimeException(
                                            "Spooled segment processing failed (uri=" + segment.uri() + "): " + safeMessage(t),
                                            t);
                                } catch (Exception ignored) {
                                }
                                put(queue, SegmentItem.error(wrapped));
                                put(queue, SegmentItem.end());
                            } finally {
                                if (acquired) {
                                    inFlight.release();
                                }
                            }
                        });

                        return new SegmentPipe(segment, queue, future);
                    })
                    .toList();

            try {
                for (SegmentPipe pipe : pipes) {
                    drainSegmentPipe(pipe, root, loader, listener);
                }
                listener.completed();
            } catch (Exception e) {
                cancelAndDrain(pipes);
                throw e;
            }
        }
    }

    private static void drainSegmentPipe(SegmentPipe pipe,
            VectorSchemaRoot root,
            VectorLoader loader,
            FlightProducer.ServerStreamListener listener) throws Exception {
        while (true) {
            SegmentItem item = pipe.queue.take();
            if (item.error != null) {
                throw new RuntimeException(
                        "Spooled segment failed (uri=" + pipe.segment.uri() + "): " + safeMessage(item.error),
                        item.error);
            }
            if (item.end) {
                return;
            }
            try (ArrowRecordBatch batch = item.batch) {
                root.clear();
                loader.load(batch);
                listener.putNext();
            }
        }
    }

    private static void cancelAndDrain(List<SegmentPipe> pipes) {
        for (SegmentPipe pipe : pipes) {
            pipe.future.cancel(true);
            pipe.queue.forEach(it -> {
                if (it != null && it.batch != null) {
                    try {
                        it.batch.close();
                    } catch (Exception ignored) {
                    }
                }
            });
            pipe.queue.clear();
        }
    }
}
