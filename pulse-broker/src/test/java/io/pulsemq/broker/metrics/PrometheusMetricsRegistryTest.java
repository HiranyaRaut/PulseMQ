package io.pulsemq.broker.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrometheusMetricsRegistryTest {

    private MetricsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
    }

    @Test
    void testCounterOperationsAndFormatting() {
        Counter counter = registry.counter("pulsemq_messages_in_total", "Total count of messages ingested");

        counter.inc(10);
        assertThat(counter.get()).isEqualTo(10.0);

        Map<String, String> topicLabels = Map.of("topic", "orders");
        counter.inc(topicLabels, 50);
        assertThat(counter.get(topicLabels)).isEqualTo(50.0);

        assertThatThrownBy(() -> counter.inc(-1))
                .isInstanceOf(IllegalArgumentException.class);

        String text = registry.exportPrometheusText();
        assertThat(text)
                .contains("# HELP pulsemq_messages_in_total Total count of messages ingested")
                .contains("# TYPE pulsemq_messages_in_total counter")
                .contains("pulsemq_messages_in_total 10")
                .contains("pulsemq_messages_in_total{topic=\"orders\"} 50");
    }

    @Test
    void testGaugeOperationsAndFormatting() {
        Gauge gauge = registry.gauge("pulsemq_active_connections", "Number of active client connections");

        gauge.set(5.0);
        assertThat(gauge.get()).isEqualTo(5.0);

        gauge.inc(2.0);
        assertThat(gauge.get()).isEqualTo(7.0);

        gauge.dec(3.0);
        assertThat(gauge.get()).isEqualTo(4.0);

        Map<String, String> labels = Map.of("client", "worker-1");
        gauge.set(labels, 12.5);
        assertThat(gauge.get(labels)).isEqualTo(12.5);

        String text = registry.exportPrometheusText();
        assertThat(text)
                .contains("# HELP pulsemq_active_connections Number of active client connections")
                .contains("# TYPE pulsemq_active_connections gauge")
                .contains("pulsemq_active_connections 4")
                .contains("pulsemq_active_connections{client=\"worker-1\"} 12.5");
    }

    @Test
    void testDynamicCollectorAndLabelEscaping() {
        registry.registerDynamicCollector(() -> List.of(
                new MetricSample(
                        "pulsemq_custom_metric",
                        Map.of("path", "C:\\data\\test", "quote", "foo\"bar"),
                        42.0,
                        "Metric with special character labels",
                        MetricType.GAUGE
                )
        ));

        String text = registry.exportPrometheusText();
        assertThat(text)
                .contains("# HELP pulsemq_custom_metric Metric with special character labels")
                .contains("# TYPE pulsemq_custom_metric gauge")
                .contains("pulsemq_custom_metric{")
                .contains("path=\"C:\\\\data\\\\test\"")
                .contains("quote=\"foo\\\"bar\"")
                .contains(" 42");
    }
}
