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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * An abstract reporter with registry for metrics. It's same as {@link
 * org.apache.flink.metrics.reporter.AbstractReporter} but with generalized information of metric.
 *
 * @param <MetricInfo> Custom metric information type
 */
abstract class AbstractReporter<MetricInfo> implements MetricReporter {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Map<Gauge<?>, MetricInfo> gauges = new HashMap<>();
    protected final Map<Counter, MetricInfo> counters = new HashMap<>();
    protected final Map<Histogram, MetricInfo> histograms = new HashMap<>();
    protected final Map<Meter, MetricInfo> meters = new HashMap<>();
    protected final MetricInfoProvider<MetricInfo> metricInfoProvider;

    protected AbstractReporter(MetricInfoProvider<MetricInfo> metricInfoProvider) {
        this.metricInfoProvider = metricInfoProvider;
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final MetricInfo metricInfo = metricInfoProvider.getMetricInfo(metricName, group);
        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    counters.put((Counter) metric, metricInfo);
                    break;
                case GAUGE:
                    gauges.put((Gauge<?>) metric, metricInfo);
                    break;
                case HISTOGRAM:
                    histograms.put((Histogram) metric, metricInfo);
                    break;
                case METER:
                    meters.put((Meter) metric, metricInfo);
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
