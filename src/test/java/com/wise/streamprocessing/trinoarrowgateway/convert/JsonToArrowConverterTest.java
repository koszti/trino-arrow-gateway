package com.wise.streamprocessing.trinoarrowgateway.convert;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonToArrowConverterTest
{
    private static BufferAllocator allocator;

    @BeforeAll
    static void setUpAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    static void tearDownAllocator() {
        allocator.close();
    }

    @Test
    void convertsJsonLinesToArrowBatches() throws Exception {
        // Given: a simple schema matching the JSON (id BIGINT, name VARCHAR)
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        JsonToArrowConverter converter = new JsonToArrowConverter(allocator);

        try (InputStream is = getClass().getResourceAsStream("/sample-results.jsonl")) {
            assertNotNull(is, "sample-results.jsonl not found on classpath");

            List<ArrowRecordBatch> batches = converter.convert(is, schema, 2); // 2 rows per batch

            // We had 4 rows in the file, so we expect 2 batches of 2 rows each
            assertEquals(2, batches.size(), "expected 2 batches");

            // Reconstruct a VectorSchemaRoot from batches to inspect data
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                VectorLoader loader = new VectorLoader(root);

                int totalRows = 0;

                for (ArrowRecordBatch batch : batches) {
                    root.clear();
                    loader.load(batch);

                    int rowCount = root.getRowCount();
                    totalRows += rowCount;

                    BigIntVector idVector = (BigIntVector) root.getVector("id");
                    VarCharVector nameVector = (VarCharVector) root.getVector("name");

                    for (int i = 0; i < rowCount; i++) {
                        long id = idVector.get(i);
                        String name = new String(nameVector.get(i), StandardCharsets.UTF_8);
                        assertTrue(id > 0);
                        assertNotNull(name);
                    }

                    batch.close();
                }

                assertEquals(4, totalRows, "expected 4 total rows across batches");
            }
        }
    }
}
