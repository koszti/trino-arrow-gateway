package io.github.koszti.trinoarrowgateway.spool;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Profile("parallel-spool-smoke")
@Component
public class ParallelReaderSmokeRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ParallelReaderSmokeRunner.class);

    private final ParallelSpoolJsonArrowReader reader;
    private final BufferAllocator allocator;

    public ParallelReaderSmokeRunner(ParallelSpoolJsonArrowReader reader,
            BufferAllocator allocator) {
        this.reader = reader;
        this.allocator = allocator;
    }

    @Override
    public void run(String... args) throws Exception {
        // Assuming your spool files under gateway.spool.s3.prefix/demo/...
        String relativePrefix = "demo";

        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        List<ArrowRecordBatch> batches = reader.readAll(relativePrefix, schema, 1024);

        int totalRows = 0;

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VectorLoader loader = new VectorLoader(root);

            for (ArrowRecordBatch batch : batches) {
                root.clear();
                loader.load(batch);
                totalRows += root.getRowCount();

                // demo log:
                if (root.getRowCount() > 0) {
                    var idVector = root.getVector("id");
                    var nameVector = root.getVector("name");
                    var firstId = idVector.getObject(0);
                    var firstNameBytes = (byte[]) nameVector.getObject(0);
                    String firstName = new String(firstNameBytes, StandardCharsets.UTF_8);
                    log.info("Batch: rows={}, first=(id={}, name={})",
                            root.getRowCount(), firstId, firstName);
                }

                batch.close();
            }
        }

        log.info("Parallel reader smoke test: total rows = {}", totalRows);
    }
}
