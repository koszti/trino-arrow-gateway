package io.github.koszti.trinoarrowgateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.flight")
public class GatewayFlightProperties {

    /**
     * Enables the Arrow Flight server.
     */
    private boolean enabled = true;

    /**
     * Host/interface the Flight server binds to.
     * Use 0.0.0.0 to listen on all interfaces.
     */
    private String bindHost = "0.0.0.0";

    /**
     * Hostname advertised to Flight clients in the FlightInfo endpoint.
     * Typically "localhost" for local development or a DNS name in production.
     */
    private String advertiseHost = "localhost";

    /**
     * TCP port for the Flight server.
     */
    private int port = 31337;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getBindHost() {
        return bindHost;
    }

    public void setBindHost(String bindHost) {
        this.bindHost = bindHost;
    }

    public String getAdvertiseHost() {
        return advertiseHost;
    }

    public void setAdvertiseHost(String advertiseHost) {
        this.advertiseHost = advertiseHost;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

