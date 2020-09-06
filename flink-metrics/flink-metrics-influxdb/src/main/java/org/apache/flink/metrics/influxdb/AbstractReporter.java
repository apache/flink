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
 * An abstract reporter with registry for metrics.
 * It's same as {@link org.apache.flink.metrics.reporter.AbstractReporter} but with generalized information of metric.
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
			if (metric instanceof Counter) {
				counters.put((Counter) metric, metricInfo);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, metricInfo);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, metricInfo);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, metricInfo);
			} else {
				log.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.remove(metric);
			} else if (metric instanceof Gauge) {
				gauges.remove(metric);
			} else if (metric instanceof Histogram) {
				histograms.remove(metric);
			} else if (metric instanceof Meter) {
				meters.remove(metric);
			} else {
				log.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}
}
