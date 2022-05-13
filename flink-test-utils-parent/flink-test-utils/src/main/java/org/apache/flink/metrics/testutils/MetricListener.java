/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.testutils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A MetricListener listens metric and group registration under the provided root metric group, and
 * stores them in an internal HashMap for fetching.
 */
public class MetricListener {

    // Constants
    public static final String DELIMITER = ".";
    public static final String ROOT_METRIC_GROUP_NAME = "rootMetricGroup";

    // Root metric group
    private final MetricGroup rootMetricGroup;

    // Map for storing registered metrics
    private final Map<String, Metric> metrics = new HashMap<>();

    public MetricListener() {
        TestingMetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setDelimiter(DELIMITER.charAt(0))
                        .setRegisterConsumer(
                                (metric, name, group) ->
                                        this.metrics.put(group.getMetricIdentifier(name), metric))
                        .build();

        this.rootMetricGroup = new GenericMetricGroup(registry, null, ROOT_METRIC_GROUP_NAME);
    }

    /**
     * Get the root metric group of this listener. Note that only metrics and groups registered
     * under this group will be listened.
     *
     * @return Root metric group
     */
    public MetricGroup getMetricGroup() {
        return this.rootMetricGroup;
    }

    /**
     * Get registered {@link Metric} with identifier relative to the root metric group.
     *
     * <p>For example, identifier of metric "myMetric" registered in group "myGroup" under root
     * metric group can be reached by identifier ("myGroup", "myMetric")
     *
     * @param identifier identifier relative to the root metric group
     * @return Optional registered metric
     */
    public <T extends Metric> Optional<T> getMetric(Class<T> metricType, String... identifier) {
        if (!metrics.containsKey(getActualIdentifier(identifier))) {
            return Optional.empty();
        }
        return Optional.of(metricType.cast(metrics.get(getActualIdentifier(identifier))));
    }

    /**
     * Get registered {@link Meter} with identifier relative to the root metric group.
     *
     * @param identifier identifier relative to the root metric group
     * @return Optional registered meter
     */
    public Optional<Meter> getMeter(String... identifier) {
        return getMetric(Meter.class, identifier);
    }

    /**
     * Get registered {@link Counter} with identifier relative to the root metric group.
     *
     * @param identifier identifier relative to the root metric group
     * @return Optional registered counter
     */
    public Optional<Counter> getCounter(String... identifier) {
        return getMetric(Counter.class, identifier);
    }

    /**
     * Get registered {@link Histogram} with identifier relative to the root metric group.
     *
     * @param identifier identifier relative to the root metric group
     * @return Optional registered histogram
     */
    public Optional<Histogram> getHistogram(String... identifier) {
        return getMetric(Histogram.class, identifier);
    }

    /**
     * Get registered {@link Gauge} with identifier relative to the root metric group.
     *
     * @param identifier identifier relative to the root metric group
     * @return Optional registered gauge
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<Gauge<T>> getGauge(String... identifier) {
        if (!metrics.containsKey(getActualIdentifier(identifier))) {
            return Optional.empty();
        } else {
            return Optional.of((Gauge<T>) metrics.get(getActualIdentifier(identifier)));
        }
    }

    private String getActualIdentifier(String... identifier) {
        return ROOT_METRIC_GROUP_NAME + DELIMITER + String.join(DELIMITER, identifier);
    }
}
