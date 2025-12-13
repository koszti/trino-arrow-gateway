package io.github.koszti.trinoarrowgateway.convert;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpooledRowsToArrowConverterTest {

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
    void convertsArrayOfRowsToArrowBatches() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("c1", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("c2", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("c3", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("c4", FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null),
                new Field("c5", FieldType.nullable(new ArrowType.Bool()), null)
        ));

        String json = """
                [
                  ["a", 1, 10, 1.5, true],
                  ["b", 2, 20, 2.5, false],
                  ["c", 3, 30, 3.5, true]
                ]
                """;

        SpooledRowsToArrowConverter converter = new SpooledRowsToArrowConverter(allocator);
        List<ArrowRecordBatch> batches = converter.convert(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                schema,
                2
        );

        assertEquals(2, batches.size());

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VectorLoader loader = new VectorLoader(root);
            int total = 0;

            for (ArrowRecordBatch batch : batches) {
                root.clear();
                loader.load(batch);
                total += root.getRowCount();

                assertTrue(root.getRowCount() > 0);

                VarCharVector c1 = (VarCharVector) root.getVector("c1");
                IntVector c2 = (IntVector) root.getVector("c2");
                BigIntVector c3 = (BigIntVector) root.getVector("c3");
                Float8Vector c4 = (Float8Vector) root.getVector("c4");
                BitVector c5 = (BitVector) root.getVector("c5");

                assertEquals(root.getRowCount(), c2.getValueCount());
                assertEquals(root.getRowCount(), c3.getValueCount());
                assertEquals(root.getRowCount(), c4.getValueCount());
                assertEquals(root.getRowCount(), c5.getValueCount());

                batch.close();
            }

            assertEquals(3, total);
        }
    }
}

