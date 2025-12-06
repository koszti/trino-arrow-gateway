package com.wise.streamprocessing.trinoarrowgateway.flight;

import com.wise.streamprocessing.trinoarrowgateway.convert.JsonToArrowConverter;
import com.wise.streamprocessing.trinoarrowgateway.spool.ParallelSpoolJsonArrowReader;
import com.wise.streamprocessing.trinoarrowgateway.trino.QueryRegistry;
import com.wise.streamprocessing.trinoarrowgateway.trino.TrinoClient;
import com.wise.streamprocessing.trinoarrowgateway.trino.TrinoQueryHandle;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Flight producer that:
 *  - Interprets descriptor.command as SQL
 *  - Submits SQL to Trino and gets a queryId + Arrow schema
 *  - Uses ticket to carry queryId
 *  - In getStream, uses queryId as S3 relative prefix for spooled JSON
 *  - Falls back to /sample-results.jsonl if no spooled data is found
 * NOTE: JsonToArrowConverter is currently hard-coded for (id BIGINT, name VARCHAR),
 * so in "no spool" fallback path you should only run queries that match that schema.
 */
@Component
public class DemoFlightProducer extends NoOpFlightProducer {
    private static final Logger log = LoggerFactory.getLogger(DemoFlightProducer.class);

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private final BufferAllocator allocator;
    private final Location location;
    private final ParallelSpoolJsonArrowReader spoolReader;
    private final JsonToArrowConverter fallbackConverter;
    private final TrinoClient trinoClient;
    private final QueryRegistry queryRegistry;

    public DemoFlightProducer(BufferAllocator allocator,
            ParallelSpoolJsonArrowReader spoolReader,
            JsonToArrowConverter fallbackConverter,
            TrinoClient trinoClient,
            QueryRegistry queryRegistry) {
        this.allocator = allocator;
        this.location = Location.forGrpcInsecure("0.0.0.0", 31337);
        this.spoolReader = spoolReader;
        this.fallbackConverter = fallbackConverter;
        this.trinoClient = trinoClient;
        this.queryRegistry = queryRegistry;
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

        // Submit query to Trino
        TrinoQueryHandle handle = trinoClient.submitQuery(sql);
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
            listener.error(new IllegalStateException("Unknown queryId: " + queryId));
            return;
        }

        Schema schema = handle.getArrowSchema();

        List<ArrowRecordBatch> batches = null;
        boolean usedSpool = false;

        try {
            // Use queryId as relative prefix in S3 (bucket/prefix/<queryId>/...)
            batches = spoolReader.readAll(queryId, schema, DEFAULT_BATCH_SIZE);
            usedSpool = !batches.isEmpty();
        } catch (Exception e) {
            log.warn("SpoolReader failed for queryId '{}', will fall back to local JSON: {}",
                    queryId, e.toString());
        }

        if (!usedSpool) {
            log.info("No spooled data found for queryId '{}', falling back to /sample-results.jsonl", queryId);
            try (InputStream is = getClass().getResourceAsStream("/sample-results.jsonl")) {
                if (is == null) {
                    listener.error(new IllegalStateException("sample-results.jsonl not found on classpath"));
                    return;
                }
                batches = fallbackConverter.convert(is, schema, DEFAULT_BATCH_SIZE);
            } catch (Exception e) {
                listener.error(e);
                return;
            }
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VectorLoader loader = new VectorLoader(root);
            listener.start(root);

            for (ArrowRecordBatch batch : batches) {
                try (batch) {
                    root.clear();
                    loader.load(batch);
                    listener.putNext();
                }
            }

            listener.completed();
        }
        catch (Exception e) {
            listener.error(e);
        }
    }
}
