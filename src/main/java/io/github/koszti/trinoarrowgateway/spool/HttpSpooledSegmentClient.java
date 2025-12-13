package io.github.koszti.trinoarrowgateway.spool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

/**
 * Downloads Trino spooled segments over HTTP(S) and acks them via an HTTP GET.
 * <p>
 * Ack must be called after the segment is successfully downloaded/consumed.
 */
@Component
public class HttpSpooledSegmentClient {

    private static final Logger log = LoggerFactory.getLogger(HttpSpooledSegmentClient.class);

    private final HttpClient httpClient;

    public HttpSpooledSegmentClient() {
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public FetchedSegment fetch(URI uri, URI ackUri) throws IOException {
        Objects.requireNonNull(uri, "uri must not be null");

        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMinutes(5))
                .GET()
                .build();

        HttpResponse<InputStream> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while downloading spooled segment: " + uri, e);
        } catch (Exception e) {
            throw new IOException("Failed to download spooled segment: " + uri, e);
        }

        int status = response.statusCode();
        if (status != 200) {
            try (InputStream body = response.body()) {
                // best-effort consume/close
            }
            throw new IOException("Spooled segment download failed: " + uri + " (HTTP " + status + ")");
        }

        return new FetchedSegment(uri, ackUri, response.body());
    }

    public void ack(URI ackUri) throws IOException {
        if (ackUri == null) {
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(ackUri)
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();

        HttpResponse<Void> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acking spooled segment: " + ackUri, e);
        } catch (Exception e) {
            throw new IOException("Failed to ack spooled segment: " + ackUri, e);
        }

        if (response.statusCode() != 200) {
            throw new IOException("Spooled segment ack failed: " + ackUri + " (HTTP " + response.statusCode() + ")");
        }
    }

    public record FetchedSegment(URI uri, URI ackUri, InputStream body) implements AutoCloseable {
        public FetchedSegment {
            Objects.requireNonNull(uri, "uri must not be null");
            Objects.requireNonNull(body, "body must not be null");
        }

        @Override
        public void close() throws IOException {
            body.close();
        }
    }
}

