package io.github.koszti.trinoarrowgateway.spool;

import io.github.koszti.trinoarrowgateway.config.GatewaySpoolS3Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * S3-backed implementation of SpoolSource.
 * Uses bucket + prefix from GatewaySpoolS3Properties, then adds a relative prefix per query.
 */
@Component
public class S3SpoolSource
        implements SpoolSource
{
    private static final Logger log = LoggerFactory.getLogger(S3SpoolSource.class);

    private final S3Client s3Client;
    private final GatewaySpoolS3Properties s3Props;

    public S3SpoolSource(S3Client s3Client,
            GatewaySpoolS3Properties s3Props) {
        this.s3Client = s3Client;
        this.s3Props = s3Props;
    }

    @Override
    public List<SpoolObject> listObjects(String relativePrefix) throws IOException
    {
        String fullPrefix = normalizePrefix(s3Props.getPrefix()) + normalizePrefix(relativePrefix);
        log.debug("Listing S3 objects: bucket={}, prefix={}", s3Props.getBucket(), fullPrefix);

        List<SpoolObject> result = new ArrayList<>();

        String continuationToken = null;

        try {
            do {
                ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                        .bucket(s3Props.getBucket())
                        .prefix(fullPrefix)
                        .maxKeys(1000);

                if (continuationToken != null) {
                    reqBuilder.continuationToken(continuationToken);
                }

                ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());

                for (S3Object o : resp.contents()) {
                    String key = o.key();
                    long size = o.size();
                    Instant lastModified = o.lastModified() != null ? o.lastModified() : Instant.EPOCH;
                    result.add(new SpoolObject(key, size, lastModified));
                }

                continuationToken = resp.isTruncated() ? resp.nextContinuationToken() : null;
            }
            while (continuationToken != null);
        }
        catch (Exception e) {
            throw new IOException("Failed to list S3 objects for prefix " + fullPrefix, e);
        }

        return result;
    }

    @Override
    public InputStream openObject(SpoolObject object) throws IOException {
        log.debug("Opening S3 object: bucket={}, key={}", s3Props.getBucket(), object.getKey());

        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(s3Props.getBucket())
                .key(object.getKey())
                .build();

        try {
            ResponseInputStream<?> s3Stream = s3Client.getObject(req);
            // Let caller close it
            return s3Stream;
        } catch (Exception e) {
            throw new IOException("Failed to open S3 object " + object.getKey(), e);
        }
    }

    private String normalizePrefix(String p) {
        if (p == null || p.isEmpty()) return "";
        // Ensure prefix ends with /
        return p.endsWith("/") ? p : (p + "/");
    }
}
