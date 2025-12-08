package io.github.koszti.trinoarrowgateway.spool;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single spooled result object (e.g. a JSON file) in storage.
 */
public class SpoolObject
{
    private final String key;          // e.g. trino/results/query123/part-0001.json
    private final long sizeBytes;
    private final Instant lastModified;

    public SpoolObject(String key, long sizeBytes, Instant lastModified) {
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.sizeBytes = sizeBytes;
        this.lastModified = lastModified;
    }

    public String getKey() {
        return key;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    @Override
    public String toString() {
        return "SpoolObject{" +
                "key='" + key + '\'' +
                ", sizeBytes=" + sizeBytes +
                ", lastModified=" + lastModified +
                '}';
    }
}
