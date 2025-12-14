package io.github.koszti.trinoarrowgateway.spool;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SpooledSegmentHeaders {
    private SpooledSegmentHeaders() {}

    /**
     * Trino provides optional segment headers as a map of headerName -> listOfValues.
     * Each header should have exactly one value; if multiple values are provided, the header is ignored.
     */
    public static Map<String, String> toSingleValueHeaders(Map<String, List<String>> rawHeaders) {
        if (rawHeaders == null || rawHeaders.isEmpty()) {
            return Map.of();
        }

        Map<String, String> flattened = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : rawHeaders.entrySet()) {
            if (entry == null) {
                continue;
            }
            String name = entry.getKey();
            List<String> values = entry.getValue();
            if (name == null || name.isBlank() || values == null || values.size() != 1) {
                continue;
            }
            String value = values.getFirst();
            if (value == null) {
                continue;
            }
            flattened.put(name, value);
        }
        return Map.copyOf(flattened);
    }
}

