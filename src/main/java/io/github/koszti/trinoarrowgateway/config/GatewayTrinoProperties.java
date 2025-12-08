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
}
