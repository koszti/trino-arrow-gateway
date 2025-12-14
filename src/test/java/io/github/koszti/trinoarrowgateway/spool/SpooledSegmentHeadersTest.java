package io.github.koszti.trinoarrowgateway.spool;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SpooledSegmentHeadersTest {
    @Test
    void keepsOnlySingleValueHeaders() {
        Map<String, List<String>> raw = Map.of(
                "X-One", List.of("v1"),
                "X-Multi", List.of("v1", "v2"),
                "X-Empty", List.of()
        );

        Map<String, String> flattened = SpooledSegmentHeaders.toSingleValueHeaders(raw);
        assertEquals(Map.of("X-One", "v1"), flattened);
    }
}

