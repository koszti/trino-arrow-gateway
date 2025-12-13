package io.github.koszti.trinoarrowgateway.trino.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrinoStatementResponseDeserializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void deserializesInlineDataArray() throws Exception {
        String json = """
                {
                  "id": "q1",
                  "columns": [{"name":"id","type":"bigint"}],
                  "data": [[1],[2]],
                  "stats": {"state": "RUNNING"},
                  "nextUri": "http://trino/v1/statement/q1/1"
                }
                """;

        TrinoStatementResponse resp = objectMapper.readValue(json, TrinoStatementResponse.class);
        assertEquals("q1", resp.getId());
        assertNotNull(resp.getData());
        assertTrue(resp.getData().isArray());
    }

    @Test
    void deserializesSpooledDataObject() throws Exception {
        String json = """
                {
                  "id": "q2",
                  "columns": [{"name":"id","type":"bigint"}],
                  "data": {
                    "encoding": "json+zstd",
                    "segments": [
                      {
                        "type": "spooled",
                        "uri": "http://localhost:8080/v1/spooled/download/abc",
                        "ackUri": "http://localhost:8080/v1/spooled/ack/abc",
                        "metadata": { "rowOffset": 0, "rowsCount": 1, "segmentSize": 29, "expiresAt": "2025-12-12T22:40:54.218171619" }
                      }
                    ]
                  },
                  "stats": {"state": "RUNNING"},
                  "nextUri": "http://trino/v1/statement/q2/1"
                }
                """;

        TrinoStatementResponse resp = objectMapper.readValue(json, TrinoStatementResponse.class);
        assertEquals("q2", resp.getId());
        assertNotNull(resp.getData());
        assertTrue(resp.getData().isObject());

        TrinoStatementResponse.Data data = objectMapper.treeToValue(resp.getData(), TrinoStatementResponse.Data.class);
        assertEquals("json+zstd", data.getEncoding());
        assertNotNull(data.getSegments());
        assertEquals(1, data.getSegments().size());
        assertEquals("spooled", data.getSegments().getFirst().getType());
    }
}

