package io.pulsemq.broker.metrics;

import java.util.Collections;
import java.util.Map;

/**
 * An individual metric sample measurement with name, label dimensions, numerical value, and metadata.
 */
public record MetricSample(
        String name,
        Map<String, String> labels,
        double value,
        String help,
        MetricType type
) {
    public MetricSample(String name, double value, String help, MetricType type) {
        this(name, Collections.emptyMap(), value, help, type);
    }
}
