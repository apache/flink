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

package org.apache.flink.metrics.reporter;

import org.apache.flink.annotation.Public;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Base interface for custom metric reporters. */
@Public
public abstract class AbstractReporter implements MetricReporter, CharacterFilter {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Map<Gauge<?>, String> gauges = new HashMap<>();
    protected final Map<Counter, String> counters = new HashMap<>();
    protected final Map<Histogram, String> histograms = new HashMap<>();
    protected final Map<Meter, String> meters = new HashMap<>();

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String name = group.getMetricIdentifier(metricName, this);

        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    counters.put((Counter) metric, name);
                    break;
                case GAUGE:
                    gauges.put((Gauge<?>) metric, name);
                    break;
                case HISTOGRAM:
                    histograms.put((Histogram) metric, name);
                    break;
                case METER:
                    meters.put((Meter) metric, name);
                    break;
                default:
                    log.warn(
                            "Cannot add unknown metric type {}. This indicates that the reporter "
                                    + "does not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    counters.remove(metric);
                    break;
                case GAUGE:
                    gauges.remove(metric);
                    break;
                case HISTOGRAM:
                    histograms.remove(metric);
                    break;
                case METER:
                    meters.remove(metric);
                    break;
                default:
                    log.warn(
                            "Cannot remove unknown metric type {}. This indicates that the reporter "
                                    + "does not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }
}
