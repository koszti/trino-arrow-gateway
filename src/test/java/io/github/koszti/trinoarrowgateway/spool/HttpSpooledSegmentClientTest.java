package io.github.koszti.trinoarrowgateway.spool;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/download", exchange -> {
            try {
                respond(exchange, 200, "hello");
            } catch (Exception e) {
                exchange.close();
                throw new RuntimeException(e);
            }
        });
        server.createContext("/ack", exchange -> {
            acked.set(true);
            try {
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

        try (HttpSpooledSegmentClient.FetchedSegment seg = client.fetch(downloadUri, ackUri)) {
            byte[] body = seg.body().readAllBytes();
            assertEquals("hello", new String(body, StandardCharsets.UTF_8));
        }

        client.ack(ackUri);
        assertTrue(acked.get());
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
