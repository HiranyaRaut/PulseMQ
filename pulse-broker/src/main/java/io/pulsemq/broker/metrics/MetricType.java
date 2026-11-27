package io.pulsemq.broker.metrics;

/**
 * Metric type classifications conforming to Prometheus / OpenMetrics exposition standard.
 */
public enum MetricType {
    COUNTER,
    GAUGE,
    HISTOGRAM
}
