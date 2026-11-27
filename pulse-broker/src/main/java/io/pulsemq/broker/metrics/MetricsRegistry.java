package io.pulsemq.broker.metrics;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Supplier;

/**
 * Thread-safe registry that stores metrics and formats them in the Prometheus / OpenMetrics text exposition format.
 */
public class MetricsRegistry {

    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();
    private final List<Supplier<List<MetricSample>>> dynamicCollectors = new CopyOnWriteArrayList<>();

    public Counter counter(String name, String help) {
        return counters.computeIfAbsent(name, n -> new Counter(n, help));
    }

    public Gauge gauge(String name, String help) {
        return gauges.computeIfAbsent(name, n -> new Gauge(n, help));
    }

    public void registerDynamicCollector(Supplier<List<MetricSample>> collector) {
        dynamicCollectors.add(collector);
    }

    public void clear() {
        counters.clear();
        gauges.clear();
        dynamicCollectors.clear();
    }

    /**
     * Formats all registered metrics into the Prometheus text-based exposition format.
     */
    public String exportPrometheusText() {
        StringBuilder sb = new StringBuilder(4096);
        Map<String, List<MetricSample>> samplesByMetric = new TreeMap<>();

        // 1. Gather static counters
        for (Counter counter : counters.values()) {
            List<MetricSample> list = samplesByMetric.computeIfAbsent(counter.name(), k -> new ArrayList<>());
            for (Map.Entry<Map<String, String>, DoubleAdder> entry : counter.values().entrySet()) {
                list.add(new MetricSample(counter.name(), entry.getKey(), entry.getValue().sum(), counter.help(), MetricType.COUNTER));
            }
        }

        // 2. Gather static gauges
        for (Gauge gauge : gauges.values()) {
            List<MetricSample> list = samplesByMetric.computeIfAbsent(gauge.name(), k -> new ArrayList<>());
            for (Map.Entry<Map<String, String>, java.util.concurrent.atomic.AtomicReference<Double>> entry : gauge.values().entrySet()) {
                list.add(new MetricSample(gauge.name(), entry.getKey(), entry.getValue().get(), gauge.help(), MetricType.GAUGE));
            }
        }

        // 3. Gather dynamic samples
        for (Supplier<List<MetricSample>> collector : dynamicCollectors) {
            try {
                List<MetricSample> dynamicSamples = collector.get();
                if (dynamicSamples != null) {
                    for (MetricSample sample : dynamicSamples) {
                        samplesByMetric.computeIfAbsent(sample.name(), k -> new ArrayList<>()).add(sample);
                    }
                }
            } catch (Throwable ignored) {
                // Ignore collection errors in dynamic suppliers to preserve exporter stability
            }
        }

        // 4. Format into Prometheus text representation
        for (Map.Entry<String, List<MetricSample>> entry : samplesByMetric.entrySet()) {
            String metricName = entry.getKey();
            List<MetricSample> samples = entry.getValue();
            if (samples.isEmpty()) continue;

            MetricSample first = samples.getFirst();
            String help = first.help() != null ? first.help() : "";
            MetricType type = first.type() != null ? first.type() : MetricType.GAUGE;

            sb.append("# HELP ").append(metricName).append(" ").append(help).append("\n");
            sb.append("# TYPE ").append(metricName).append(" ").append(type.name().toLowerCase(Locale.ROOT)).append("\n");

            for (MetricSample sample : samples) {
                sb.append(sample.name());
                if (sample.labels() != null && !sample.labels().isEmpty()) {
                    sb.append("{");
                    boolean firstLabel = true;
                    Map<String, String> sortedLabels = new TreeMap<>(sample.labels());
                    for (Map.Entry<String, String> label : sortedLabels.entrySet()) {
                        if (!firstLabel) sb.append(",");
                        sb.append(label.getKey()).append("=\"").append(escapeLabelValue(label.getValue())).append("\"");
                        firstLabel = false;
                    }
                    sb.append("}");
                }
                sb.append(" ");
                formatValue(sb, sample.value());
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    private static void formatValue(StringBuilder sb, double value) {
        if (value == (long) value) {
            sb.append((long) value);
        } else {
            sb.append(value);
        }
    }

    private static String escapeLabelValue(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n");
    }
}
