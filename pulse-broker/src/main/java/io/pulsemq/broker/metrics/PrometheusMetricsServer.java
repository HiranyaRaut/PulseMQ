package io.pulsemq.broker.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Lightweight, zero-dependency HTTP server exposing Prometheus /metrics, /healthz, and /readyz
 * endpoints over Java 21 Virtual Threads using the built-in JDK HTTP server.
 */
public class PrometheusMetricsServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsServer.class);

    private final String host;
    private final int requestedPort;
    private final BrokerMetrics brokerMetrics;
    private HttpServer server;
    private int boundPort;
    private volatile boolean running = false;

    public PrometheusMetricsServer(String host, int requestedPort, BrokerMetrics brokerMetrics) {
        this.host = host;
        this.requestedPort = requestedPort;
        this.brokerMetrics = brokerMetrics;
    }

    public synchronized void start() throws IOException {
        if (running) return;

        InetSocketAddress address = new InetSocketAddress(host, requestedPort);
        this.server = HttpServer.create(address, 0);

        // Serve HTTP requests asynchronously on lightweight virtual threads
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/healthz", new HealthHandler());
        server.createContext("/health", new HealthHandler());
        server.createContext("/readyz", new ReadyHandler());

        server.start();
        this.boundPort = server.getAddress().getPort();
        this.running = true;

        log.info("PulseMQ Prometheus Metrics Server started on http://{}:{}/metrics", host, boundPort);
    }

    public int boundPort() {
        return boundPort;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public synchronized void close() {
        if (!running) return;
        running = false;
        if (server != null) {
            server.stop(0);
            log.info("PulseMQ Prometheus Metrics Server stopped.");
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                String response = brokerMetrics.registry().exportPrometheusText();
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);

                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (Throwable t) {
                log.error("Error generating metrics response", t);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String json = String.format(
                    "{\"status\":\"UP\",\"brokerId\":%d,\"activeConnections\":%d}",
                    brokerMetrics.brokerId(),
                    brokerMetrics.activeConnectionsCount()
            );
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private class ReadyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] bytes = "OK\n".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
