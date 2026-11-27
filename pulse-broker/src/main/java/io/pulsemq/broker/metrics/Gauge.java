package io.pulsemq.broker.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A metric that represents a single numerical value that can arbitrarily go up and down.
 */
public class Gauge {

    private final String name;
    private final String help;
    private final Map<Map<String, String>, AtomicReference<Double>> values = new ConcurrentHashMap<>();

    public Gauge(String name, String help) {
        this.name = name;
        this.help = help;
    }

    public void set(double value) {
        set(Collections.emptyMap(), value);
    }

    public void set(Map<String, String> labels, double value) {
        values.computeIfAbsent(labels, k -> new AtomicReference<>(0.0)).set(value);
    }

    public void inc(double delta) {
        inc(Collections.emptyMap(), delta);
    }

    public void inc(Map<String, String> labels, double delta) {
        values.computeIfAbsent(labels, k -> new AtomicReference<>(0.0))
                .updateAndGet(prev -> prev + delta);
    }

    public void dec(double delta) {
        inc(Collections.emptyMap(), -delta);
    }

    public void dec(Map<String, String> labels, double delta) {
        inc(labels, -delta);
    }

    public double get() {
        return get(Collections.emptyMap());
    }

    public double get(Map<String, String> labels) {
        AtomicReference<Double> ref = values.get(labels);
        return ref != null ? ref.get() : 0.0;
    }

    public String name() {
        return name;
    }

    public String help() {
        return help;
    }

    public Map<Map<String, String>, AtomicReference<Double>> values() {
        return Collections.unmodifiableMap(values);
    }
}
