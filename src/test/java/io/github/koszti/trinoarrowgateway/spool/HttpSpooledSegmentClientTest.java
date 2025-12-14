package io.github.koszti.trinoarrowgateway.spool;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpSpooledSegmentClientTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void downloadsAndAcks() throws Exception {
        AtomicBoolean acked = new AtomicBoolean(false);
        AtomicBoolean downloadSawHeaders = new AtomicBoolean(false);
        AtomicBoolean ackSawHeaders = new AtomicBoolean(false);

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/download", exchange -> {
            try {
                assertEquals("v1", exchange.getRequestHeaders().getFirst("X-Test"));
                downloadSawHeaders.set(true);
                respond(exchange, 200, "hello");
            } catch (Exception e) {
                exchange.close();
                throw new RuntimeException(e);
            }
        });
        server.createContext("/ack", exchange -> {
            acked.set(true);
            try {
                assertEquals("v1", exchange.getRequestHeaders().getFirst("X-Test"));
                ackSawHeaders.set(true);
                respond(exchange, 200, "ok");
            } catch (Exception e) {
                exchange.close();
                throw new RuntimeException(e);
            }
        });
        server.start();

        int port = server.getAddress().getPort();
        URI downloadUri = URI.create("http://127.0.0.1:" + port + "/download");
        URI ackUri = URI.create("http://127.0.0.1:" + port + "/ack");

        HttpSpooledSegmentClient client = new HttpSpooledSegmentClient();
        Map<String, String> headers = Map.of("X-Test", "v1");

        try (HttpSpooledSegmentClient.FetchedSegment seg = client.fetch(downloadUri, ackUri, headers)) {
            byte[] body = seg.body().readAllBytes();
            assertEquals("hello", new String(body, StandardCharsets.UTF_8));
        }

        client.ack(ackUri, headers);
        assertTrue(acked.get());
        assertTrue(downloadSawHeaders.get());
        assertTrue(ackSawHeaders.get());
    }

    private static void respond(HttpExchange exchange, int status, String body) throws Exception {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
        exchange.close();
    }
}
