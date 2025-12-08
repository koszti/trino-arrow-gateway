package io.github.koszti.trinoarrowgateway.convert;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Prototype JSON -> Arrow converter
 * - Assumes line-delimited JSON objects (one per line).
 * - Supports a simple schema with BIGINT and UTF8 fields (id, name) for now.
 * - Returns a list of ArrowRecordBatch objects.
 * Later we will:
 * - support more Trino types
 * - stream batches instead of materializing all of them
 */
public class JsonToArrowConverter
{
    private final BufferAllocator allocator;
    private final JsonFactory jsonFactory = new JsonFactory();

    public JsonToArrowConverter(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Convert a line-delimited JSON stream into ArrowRecordBatches.
     *
     * @param inputStream source of JSON lines
     * @param schema      Arrow schema to use
     * @param batchSize   number of rows per batch (e.g. 1024)
     */
    public List<ArrowRecordBatch> convert(InputStream inputStream,
            Schema schema,
            int batchSize) throws IOException {

        List<ArrowRecordBatch> batches = new ArrayList<>();

        try (JsonParser parser = jsonFactory.createParser(inputStream);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            VectorUnloader unloader = new VectorUnloader(root);

            int rowIndex = 0;

            BigIntVector idVector = (BigIntVector) getVector(root, "id");
            VarCharVector nameVector = (VarCharVector) getVector(root, "name");

            root.allocateNew();

            JsonToken token = parser.nextToken();
            while (token != null) {
                if (token == JsonToken.START_OBJECT) {
                    // Parse one JSON object = one row
                    readOneRow(parser, rowIndex, idVector, nameVector);
                    rowIndex++;

                    if (rowIndex == batchSize) {
                        root.setRowCount(rowIndex);
                        ArrowRecordBatch batch = unloader.getRecordBatch();
                        batches.add(batch);

                        // Prepare for next batch
                        root.clear();
                        root.allocateNew();
                        rowIndex = 0;

                        idVector = (BigIntVector) getVector(root, "id");
                        nameVector = (VarCharVector) getVector(root, "name");
                    }

                    // parser is positioned at END_OBJECT now
                }

                token = parser.nextToken();
            }

            // Flush last partial batch
            if (rowIndex > 0) {
                root.setRowCount(rowIndex);
                ArrowRecordBatch batch = unloader.getRecordBatch();
                batches.add(batch);
            }
        }

        return batches;
    }

    private void readOneRow(JsonParser parser,
            int rowIndex,
            BigIntVector idVector,
            VarCharVector nameVector) throws IOException {
        // parser is currently at START_OBJECT
        // move to first field
        JsonToken token = parser.nextToken();
        while (token != JsonToken.END_OBJECT && token != null) {
            if (token == JsonToken.FIELD_NAME) {
                String fieldName = parser.getCurrentName();
                token = parser.nextToken(); // move to value

                if ("id".equals(fieldName)) {
                    long value = parser.getLongValue();
                    idVector.setSafe(rowIndex, value);
                } else if ("name".equals(fieldName)) {
                    String value = parser.getValueAsString();
                    nameVector.setSafe(rowIndex, value.getBytes(StandardCharsets.UTF_8));
                } else {
                    // Unknown field: skip its value
                    parser.skipChildren();
                }
            }
            token = parser.nextToken();
        }
    }

    private org.apache.arrow.vector.FieldVector getVector(VectorSchemaRoot root, String name) {
        org.apache.arrow.vector.FieldVector vector = root.getVector(name);
        if (vector == null) {
            throw new IllegalStateException("Schema does not contain field: " + name);
        }
        return vector;
    }
}
