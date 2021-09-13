/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.function.Predicate;

/** Collection of methods to interact with Kafka's client metric system. */
@Internal
public class MetricUtil {

    /**
     * Tries to find the the Kafka {@link Metric} in the provided metrics.
     *
     * @return {@link Metric} which exposes continuous updates
     * @throws IllegalStateException if the metric is not part of the provided metrics
     */
    public static Metric getKafkaMetric(
            Map<MetricName, ? extends Metric> metrics, String metricGroup, String metricName) {
        return getKafkaMetric(
                metrics,
                e ->
                        e.getKey().group().equals(metricGroup)
                                && e.getKey().name().equals(metricName));
    }

    /**
     * Tries to find the the Kafka {@link Metric} in the provided metrics matching a given filter.
     *
     * @return {@link Metric} which exposes continuous updates
     * @throws IllegalStateException if no metric matches the given filter
     */
    public static Metric getKafkaMetric(
            Map<MetricName, ? extends Metric> metrics,
            Predicate<Map.Entry<MetricName, ? extends Metric>> filter) {
        return metrics.entrySet().stream()
                .filter(filter)
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot find Kafka metric matching current filter."));
    }

    /**
     * Ensures that the counter has the same value as the given Kafka metric.
     *
     * <p>Do not use this method for every record because {@link Metric#metricValue()} is an
     * expensive operation.
     *
     * @param from Kafka's {@link Metric} to query
     * @param to {@link Counter} to write the value to
     */
    public static void sync(Metric from, Counter to) {
        to.inc(((Number) from.metricValue()).longValue() - to.getCount());
    }
}
