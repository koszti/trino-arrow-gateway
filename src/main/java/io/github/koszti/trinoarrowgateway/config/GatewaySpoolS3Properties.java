package io.github.koszti.trinoarrowgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.spool.s3")
public class GatewaySpoolS3Properties {

    /**
     * Bucket where Trino spools query results.
     */
    private String bucket = "my-dev-spool-bucket";

    /**
     * Prefix under which query results are stored, e.g. trino/results/.
     */
    private String prefix = "trino/results/";

    /**
     * AWS region of the S3 bucket, e.g. eu-central-1.
     */
    private String region = "eu-central-1";

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
