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

package org.apache.flink.metrics;

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An abstract metrics class that helps creating metrics for components.
 */
public abstract class AbstractMetrics {
	private static final Metric GAUGE_PLACEHOLDER = (Gauge<Integer>) () -> -1;
	private final MetricGroup metricGroup;
	private final ConcurrentMap<String, Metric> metrics = new ConcurrentHashMap<>();

	// --------------- constructor ------------------------

	/**
	 * Construct the metrics based on the given {@link MetricDef}.
	 *
	 * @param metricGroup the metric group to register the metrics.
	 * @param metricDef the metric definition.
	 */
	public AbstractMetrics(MetricGroup metricGroup, MetricDef metricDef) {
		this.metricGroup = metricGroup;
		metricDef.definitions().values().forEach(
			definition -> createMetric(metricGroup, metricDef, definition.name, definition.spec));
	}

	// --------------------- public methods --------------------

	/**
	 * Get the gauge with given name.
	 *
	 * @param metricName the metric name of the gauge.
	 * @return the gauge if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public Gauge<?> getGauge(String metricName) {
		Gauge<?> gauge = get(metricName);
		if (gauge == GAUGE_PLACEHOLDER) {
			throw new IllegalStateException(metricName + " does not exist.");
		}
		return gauge;
	}

	/**
	 * Get the counter with given name.
	 *
	 * @param metricName the metric name of the counter.
	 * @return the counter if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public Counter getCounter(String metricName) {
		return get(metricName);
	}

	/**
	 * Get the meter with given name.
	 *
	 * @param metricName the metric name of the meter.
	 * @return the meter if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public Meter getMeter(String metricName) {
		return get(metricName);
	}

	/**
	 * Get the histogram with given name.
	 *
	 * @param metricName the metric name of the histogram.
	 * @return the histogram if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public org.apache.flink.metrics.Histogram getHistogram(String metricName) {
		return get(metricName);
	}

	/**
	 * Set the actual gauge.
	 *
	 * @param metricName the name of the gauge.
	 * @param gauge the actual gauge instance.
	 */
	public void setGauge(String metricName, Gauge<?> gauge) {
		metricGroup.gauge(metricName, gauge);
		addMetric(metricName, metricGroup.gauge(metricName, gauge));
	}

	/**
	 * Get a metric. It is a convenient method with adaptive types.
	 *
	 * @param metricName the name of the metric.
	 * @return the metric with the given name.
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(String metricName) {
		return (T) notNull(metrics.get(metricName), metricName + "does not exist.");
	}

	/**
	 * Get the name of all metrics.
	 *
	 * @return a set of all metric names.
	 */
	public Set<String> allMetricNames() {
		return Collections.unmodifiableSet(metrics.keySet());
	}

	/**
	 * Get the metric group in which this abstract metrics is defined.
	 *
	 * @return the metric group associated with this abstract metrics.
	 */
	public MetricGroup metricGroup() {
		return metricGroup;
	}

	// ------------------ private helper methods ---------------------

	private Metric createMetric(MetricGroup metricGroup,
								MetricDef metricDef,
								String metricName,
								MetricSpec spec) {
		// If a metric has been created, just use it.
		Metric metric = metrics.get(metricName);
		if (metric != null) {
			return metric;
		}

		// Create dependency metrics recursively.
		for (String dependencyMetric : spec.dependencies) {
			if (!metrics.containsKey(dependencyMetric)) {
				MetricDef.MetricInfo metricInfo = metricDef.definitions().get(dependencyMetric);
				if (metricInfo == null) {
					throw new IllegalStateException("Could not find metric definition for " + dependencyMetric
														+ " which is referred by " + metricName + " as a"
														+ " dependency metric.");
				}
				createMetric(metricGroup, metricDef, dependencyMetric, metricInfo.spec);
			}
		}

		// Create metric.
		switch (spec.type) {
			case COUNTER:
				if (spec instanceof MetricSpec.InstanceSpec) {
					return addMetric(
						metricName,
						metricGroup.counter(metricName,	(Counter) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					return addMetric(metricName, metricGroup.counter(metricName));
				}

			case METER:
				if (spec instanceof MetricSpec.InstanceSpec) {
					return addMetric(
						metricName,
						metricGroup.meter(metricName, (Meter) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					MetricSpec.MeterSpec meterSpec = (MetricSpec.MeterSpec) spec;
					String counterName = meterSpec.counterMetricName;
					Counter counter;
					if (counterName == null || counterName.isEmpty()) {
						// No separate counter metric is required.
						counter = new SimpleCounter();
					} else {
						// Separate counter metric has been created.
						counter = (Counter) metrics.get(counterName);
					}
					return addMetric(
						metricName,
						metricGroup.meter(metricName, new MeterView(counter, meterSpec.timeSpanInSeconds)));
				}

			case HISTOGRAM:
				if (spec instanceof MetricSpec.InstanceSpec) {
					return addMetric(
						metricName,
						metricGroup.histogram(metricName, (org.apache.flink.metrics.Histogram) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					return addMetric(metricName, metricGroup.histogram(
						metricName,
						new DropwizardHistogramWrapper(new Histogram(new ExponentiallyDecayingReservoir()))));
				}

			case GAUGE:
				if (spec instanceof MetricSpec.InstanceSpec) {
					return addMetric(
						metricName,
						(Gauge) metricGroup.gauge(metricName, (Gauge) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					return metrics.put(metricName, GAUGE_PLACEHOLDER);
				}

			default:
				throw new IllegalArgumentException("Unknown metric type " + spec.type);

		}
	}

	private Metric addMetric(String name, Metric metric) {
		return metrics.compute(name, (n, m) -> {
			if (m != null && m != GAUGE_PLACEHOLDER) {
				throw new IllegalStateException("Metric of name " + n + " already exists. The metric is " + m);
			}
			return metric;
		});
	}

	private static <T> T notNull(T obj, String errorMsg) {
		if (obj == null) {
			throw new IllegalArgumentException(errorMsg);
		} else {
			return obj;
		}
	}
}
