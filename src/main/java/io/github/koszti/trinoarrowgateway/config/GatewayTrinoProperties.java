package io.github.koszti.trinoarrowgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.trino")
public class GatewayTrinoProperties {

    /**
     * Base URL of the Trino coordinator, e.g. http://localhost:8080
     */
    private String baseUrl = "http://localhost:8080";

    /**
     * Trino user to send in X-Trino-User header.
     */
    private String user = "trino-arrow-gateway";

    /**
     * Optional Trino query data encoding hint to request spooled/encoded results.
     * Example: "json+zstd" (paired with Trino's spooling protocol).
     */
    private String queryDataEncoding = "json+zstd";

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueryDataEncoding() {
        return queryDataEncoding;
    }

    public void setQueryDataEncoding(String queryDataEncoding) {
        this.queryDataEncoding = queryDataEncoding;
    }
}
