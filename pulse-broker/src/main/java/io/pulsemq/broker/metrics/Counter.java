package io.pulsemq.broker.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * A monotonically increasing cumulative counter metric with label support.
 */
public class Counter {

    private final String name;
    private final String help;
    private final Map<Map<String, String>, DoubleAdder> values = new ConcurrentHashMap<>();

    public Counter(String name, String help) {
        this.name = name;
        this.help = help;
    }

    public void inc() {
        inc(1.0);
    }

    public void inc(double amount) {
        inc(Collections.emptyMap(), amount);
    }

    public void inc(Map<String, String> labels, double amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("Counter increments must be non-negative: " + amount);
        }
        values.computeIfAbsent(labels, k -> new DoubleAdder()).add(amount);
    }

    public double get() {
        return get(Collections.emptyMap());
    }

    public double get(Map<String, String> labels) {
        DoubleAdder adder = values.get(labels);
        return adder != null ? adder.sum() : 0.0;
    }

    public String name() {
        return name;
    }

    public String help() {
        return help;
    }

    public Map<Map<String, String>, DoubleAdder> values() {
        return Collections.unmodifiableMap(values);
    }
}
