package io.github.koszti.trinoarrowgateway.spool;

import io.github.koszti.trinoarrowgateway.convert.JsonToArrowConverter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

/**
 * Reads all spool objects under a prefix in parallel, converting JSON -> ArrowRecordBatch.
 * For now:
 *  - Returns all batches as a List<ArrowRecordBatch> (ordered by S3 key).
 *  - Parallelism is controlled by the conversionExecutor (threads = gateway.conversion.parallelism).
 * Later:
 *  - We'll adapt this to stream batches directly to Flight instead of materializing them all.
 */
@Component
public class ParallelSpoolJsonArrowReader {

    private static final Logger log = LoggerFactory.getLogger(ParallelSpoolJsonArrowReader.class);

    private final SpoolSource spoolSource;
    private final JsonToArrowConverter converter;
    private final ExecutorService conversionExecutor;

    public ParallelSpoolJsonArrowReader(SpoolSource spoolSource,
            JsonToArrowConverter converter,
            ExecutorService conversionExecutor) {
        this.spoolSource = spoolSource;
        this.converter = converter;
        this.conversionExecutor = conversionExecutor;
    }

    private record OrderedResult(int order, List<ArrowRecordBatch> batches) {}

    /**
     * List all spool objects under {@code relativePrefix}, process them in parallel, and
     * return all ArrowRecordBatches in deterministic object order.
     *
     * @param relativePrefix prefix under gateway.spool.s3.prefix, e.g. "query123/"
     * @param schema         Arrow schema used by the converter
     * @param batchSize      rows per batch
     */
    public List<ArrowRecordBatch> readAll(String relativePrefix,
            Schema schema,
            int batchSize) throws IOException {
        List<SpoolObject> objects = spoolSource.listObjects(relativePrefix);

        if (objects.isEmpty()) {
            log.info("No spool objects found for prefix '{}'", relativePrefix);
            return List.of();
        }

        // Sort objects by key to have deterministic order
        objects.sort(Comparator.comparing(SpoolObject::getKey));

        log.info("Found {} spool objects under prefix '{}'", objects.size(), relativePrefix);

        ExecutorCompletionService<OrderedResult> completionService =
                new ExecutorCompletionService<>(conversionExecutor);

        // Submit one task per object
        for (int i = 0; i < objects.size(); i++) {
            final int order = i;
            final SpoolObject obj = objects.get(i);

            completionService.submit(() -> {
                log.debug("Starting conversion for object {} (order {})", obj.getKey(), order);
                try (InputStream is = spoolSource.openObject(obj)) {
                    List<ArrowRecordBatch> batches = converter.convert(is, schema, batchSize);
                    log.debug("Finished conversion for object {} (order {}), {} batches", obj.getKey(), order, batches.size());
                    return new OrderedResult(order, batches);
                } catch (IOException e) {
                    log.error("Error converting object {}: {}", obj.getKey(), e.getMessage());
                    throw e;
                }
            });
        }

        List<OrderedResult> orderedResults = new ArrayList<>(objects.size());

        // Collect all results
        for (int i = 0; i < objects.size(); i++) {
            try {
                Future<OrderedResult> future = completionService.take();
                OrderedResult result = future.get();
                orderedResults.add(result);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for conversion tasks", e);
            } catch (ExecutionException e) {
                throw new IOException("Conversion task failed", e.getCause());
            }
        }

        // Sort by 'order' to restore object order
        orderedResults.sort(Comparator.comparingInt(OrderedResult::order));

        // Flatten batches into a single list
        List<ArrowRecordBatch> allBatches = new ArrayList<>();
        for (OrderedResult r : orderedResults) {
            allBatches.addAll(r.batches());
        }

        log.info("Parallel readAll for prefix '{}' produced {} batches", relativePrefix, allBatches.size());
        return allBatches;
    }
}
