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
package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract {@link org.apache.flink.metrics.MetricGroup} that contains key functionality for adding metrics and groups.
 */
@Internal
public abstract class AbstractMetricGroup implements MetricGroup {
	private static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);
	protected final MetricRegistry registry;

	// all metrics that are directly contained in this group
	protected final Map<String, Metric> metrics = new HashMap<>();
	// all generic groups that are directly contained in this group
	protected final Map<String, MetricGroup> groups = new HashMap<>();

	public AbstractMetricGroup(MetricRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void close() {
		for (MetricGroup group : groups.values()) {
			group.close();
		}
		this.groups.clear();
		for (Map.Entry<String, Metric> metric : metrics.entrySet()) {
			registry.unregister(metric.getValue(), metric.getKey(), this);
		}
		this.metrics.clear();
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Scope
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Generates the full scope based on the default/configured format that applies to all metrics within this group.
	 *
	 * @return generated scope
	 */
	public abstract List<String> generateScope();

	/**
	 * Generates the full scope based on the given format that applies to all metrics within this group.
	 *
	 * @param format format string
	 * @return generated scope
	 */
	public abstract List<String> generateScope(Scope.ScopeFormat format);

	// -----------------------------------------------------------------------------------------------------------------
	// Metrics
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Counter counter(int name) {
		return counter("" + name);
	}

	@Override
	public Counter counter(String name) {
		Counter counter = new Counter();
		addMetric(name, counter);
		return counter;
	}

	@Override
	public <T> Gauge<T> gauge(int name, Gauge<T> gauge) {
		return gauge("" + name, gauge);
	}

	@Override
	public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
		addMetric(name, gauge);
		return gauge;
	}

	protected MetricGroup addMetric(String name, Metric metric) {
		if (!name.matches("[a-zA-Z0-9]*")) {
			throw new IllegalArgumentException("Metric names may not contain special characters.");
		}
		if (metrics.containsKey(name)) {
			LOG.warn("Detected metric name collision. This group already contains a group for the given group name. " +
				this.generateScope().toString() + "." + name);
		}
		if (groups.containsKey(name)) {
			LOG.warn("Detected metric name collision. This group already contains a group for the given metric name." +
				this.generateScope().toString() + ")." + name);
		}
		metrics.put(name, metric);
		registry.register(metric, name, this);
		return this;
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Groups
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public MetricGroup addGroup(int name) {
		return addGroup("" + name);
	}

	@Override
	public MetricGroup addGroup(String name) {
		if (metrics.containsKey(name)) {
			LOG.warn("Detected metric name collision. This group already contains a metric for the given group name."
				+ this.generateScope().toString() + "." + name);
		}
		if (!groups.containsKey(name)) {
			groups.put(name, new GenericMetricGroup(registry, this, name));
		}
		return groups.get(name);
	}
}
