package io.github.koszti.trinoarrowgateway.convert;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Converts Trino's "data" row format (array-of-arrays) into ArrowRecordBatches.
 * <p>
 * Expected JSON shape:
 * <pre>
 *   [
 *     ["val1", 123, true],
 *     ["val2", 456, false]
 *   ]
 * </pre>
 * <p>
 * Note: decoding (e.g. zstd) is handled by the caller.
 */
public class SpooledRowsToArrowConverter {

    private final BufferAllocator allocator;
    private final JsonFactory jsonFactory = new JsonFactory();

    public SpooledRowsToArrowConverter(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");
    }

    public List<ArrowRecordBatch> convert(InputStream inputStream,
            Schema schema,
            int batchSize) throws IOException {
        List<ArrowRecordBatch> batches = new ArrayList<>();
        convertStreaming(inputStream, schema, batchSize, batches::add);
        return batches;
    }

    public void convertStreaming(InputStream inputStream,
            Schema schema,
            int batchSize,
            Consumer<ArrowRecordBatch> consumer) throws IOException {
        Objects.requireNonNull(inputStream, "inputStream must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");

        try (JsonParser parser = jsonFactory.createParser(inputStream);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VectorUnloader unloader = new VectorUnloader(root);
            root.allocateNew();

            JsonToken token = parser.nextToken();
            if (token == null) {
                return;
            }

            if (token != JsonToken.START_ARRAY) {
                throw new IOException("Expected START_ARRAY for rows, got " + token);
            }

            int rowIndex = 0;

            while (true) {
                token = parser.nextToken();
                if (token == JsonToken.END_ARRAY) {
                    break;
                }
                if (token != JsonToken.START_ARRAY) {
                    throw new IOException("Expected START_ARRAY for row, got " + token);
                }

                writeRow(parser, rowIndex, schema, root);
                rowIndex++;

                if (rowIndex == batchSize) {
                    root.setRowCount(rowIndex);
                    consumer.accept(unloader.getRecordBatch());
                    root.clear();
                    root.allocateNew();
                    rowIndex = 0;
                }
            }

            if (rowIndex > 0) {
                root.setRowCount(rowIndex);
                consumer.accept(unloader.getRecordBatch());
            }
        }
    }

    private void writeRow(JsonParser parser,
            int rowIndex,
            Schema schema,
            VectorSchemaRoot root) throws IOException {
        // parser is positioned at START_ARRAY of the row.
        List<Field> fields = schema.getFields();

        for (int colIndex = 0; colIndex < fields.size(); colIndex++) {
            JsonToken valueToken = parser.nextToken();
            if (valueToken == JsonToken.END_ARRAY) {
                // Short row; remaining columns are null.
                break;
            }

            Field field = fields.get(colIndex);
            FieldVector vector = root.getVector(field.getName());
            if (vector == null) {
                throw new IOException("Missing vector for field: " + field.getName());
            }

            if (valueToken == JsonToken.VALUE_NULL) {
                vector.setNull(rowIndex);
                continue;
            }

            ArrowType type = field.getType();
            switch (type.getTypeID()) {
                case Int -> {
                    ArrowType.Int intType = (ArrowType.Int) type;
                    long v = parser.getLongValue();
                    if (intType.getBitWidth() == 64) {
                        ((org.apache.arrow.vector.BigIntVector) vector).setSafe(rowIndex, v);
                    } else if (intType.getBitWidth() == 32) {
                        ((org.apache.arrow.vector.IntVector) vector).setSafe(rowIndex, (int) v);
                    } else {
                        throw new IOException("Unsupported INT bitWidth: " + intType.getBitWidth() + " for " + field.getName());
                    }
                }
                case FloatingPoint -> {
                    double v = parser.getDoubleValue();
                    ((org.apache.arrow.vector.Float8Vector) vector).setSafe(rowIndex, v);
                }
                case Utf8 -> {
                    String s = parser.getValueAsString();
                    ((org.apache.arrow.vector.VarCharVector) vector).setSafe(rowIndex, s.getBytes(StandardCharsets.UTF_8));
                }
                case Bool -> {
                    boolean b = parser.getBooleanValue();
                    ((org.apache.arrow.vector.BitVector) vector).setSafe(rowIndex, b ? 1 : 0);
                }
                case Date -> {
                    String s = parser.getValueAsString();
                    long days = LocalDate.parse(s).toEpochDay();
                    ((org.apache.arrow.vector.DateDayVector) vector).setSafe(rowIndex, (int) days);
                }
                default -> throw new IOException("Unsupported Arrow type " + type + " for field " + field.getName());
            }
        }

        // consume the END_ARRAY for the row
        JsonToken end = parser.nextToken();
        if (end != JsonToken.END_ARRAY) {
            throw new IOException("Expected END_ARRAY after row values, got " + end);
        }
    }
}
