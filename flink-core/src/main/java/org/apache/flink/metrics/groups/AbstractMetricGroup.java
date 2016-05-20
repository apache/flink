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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract {@link MetricGroup} that contains key functionality for adding metrics and groups.
 * 
 * <p><b>IMPORTANT IMPLEMENTATION NOTE</b>
 * 
 * <p>This class uses locks for adding and removing metrics objects. This is done to
 * prevent resource leaks in the presence of concurrently closing a group and adding
 * metrics and subgroups.
 * Since closing groups recursively closes the subgroups, the lock acquisition order must
 * be strictly from parent group to subgroup. If at any point, a subgroup holds its group
 * lock and calls a parent method that also acquires the lock, it will create a deadlock
 * condition.
 */
@Internal
public abstract class AbstractMetricGroup implements MetricGroup {

	/** shared logger */
	private static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);

	private static final String METRIC_NAME_REGEX = "[a-zA-Z0-9]*";
	
	/** The pattern that metric and group names have to match */
	private static final Pattern METRIC_NAME_PATTERN = Pattern.compile(METRIC_NAME_REGEX);

	// ------------------------------------------------------------------------

	/** The registry that this metrics group belongs to */
	protected final MetricRegistry registry;

	/** All metrics that are directly contained in this group */
	protected final Map<String, Metric> metrics = new HashMap<>();

	/** All metric subgroups of this group */
	protected final Map<String, MetricGroup> groups = new HashMap<>();

	/** Flag indicating whether this group has been closed */
	private volatile boolean closed;

	// ------------------------------------------------------------------------
	
	public AbstractMetricGroup(MetricRegistry registry) {
		this.registry = checkNotNull(registry);
	}

	// ------------------------------------------------------------------------
	//  Closing
	// ------------------------------------------------------------------------

	@Override
	public void close() {
		synchronized (this) {
			if (!closed) {
				closed = true;

				// close all subgroups
				for (MetricGroup group : groups.values()) {
					group.close();
				}
				groups.clear();

				// un-register all directly contained metrics
				for (Map.Entry<String, Metric> metric : metrics.entrySet()) {
					registry.unregister(metric.getValue(), metric.getKey(), this);
				}
				metrics.clear();
			}
		}
	}

	@Override
	public final boolean isClosed() {
		return closed;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//  Scope
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
	//  Metrics
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Counter counter(int name) {
		return counter(String.valueOf(name));
	}

	@Override
	public Counter counter(String name) {
		Counter counter = new Counter();
		addMetric(name, counter);
		return counter;
	}

	@Override
	public <T> Gauge<T> gauge(int name, Gauge<T> gauge) {
		return gauge(String.valueOf(name), gauge);
	}

	@Override
	public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
		addMetric(name, gauge);
		return gauge;
	}

	/**
	 * Adds the given metric to the group and registers it at the registry, if the group
	 * is not yet closed, and if no metric with the same name has been registered before.
	 * 
	 * @param name the name to register the metric under
	 * @param metric the metric to register
	 */
	protected void addMetric(String name, Metric metric) {
		Matcher nameMatcher = METRIC_NAME_PATTERN.matcher(name);
		if (!nameMatcher.matches()) {
			throw new IllegalArgumentException("Metric names may not contain special characters or spaces. " +
					"Allowed is: " + METRIC_NAME_REGEX);
		}

		// add the metric only if the group is still open
		synchronized (this) {
			if (!closed) {
				// immediately put without a 'contains' check to optimize the common case (no collition)
				// collisions are resolved later
				Metric prior = metrics.put(name, metric);

				// check for collisions with other metric names
				if (prior == null) {
					// no other metric with this name yet

					if (groups.containsKey(name)) {
						// we warn here, rather than failing, because metrics are tools that should not fail the
						// program when used incorrectly
						LOG.warn("Name collision: Adding a metric with the same name as a metric subgroup: '" +
								name + "'. Metric might not get properly reported. (" + generateScope() + ')');
					}

					registry.register(metric, name, this);
				}
				else {
					// we had a collision. put back the original value
					metrics.put(name, prior);
					
					// we warn here, rather than failing, because metrics are tools that should not fail the
					// program when used incorrectly
					LOG.warn("Name collision: Group already contains a Metric with the name '" +
							name + "'. Metric will not be reported. (" + generateScope() + ')');
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Groups
	// ------------------------------------------------------------------------

	@Override
	public MetricGroup addGroup(int name) {
		return addGroup(String.valueOf(name));
	}

	@Override
	public MetricGroup addGroup(String name) {
		synchronized (this) {
			if (!closed) {
				// adding a group with the same name as a metric creates problems in many reporters/dashboards
				// we warn here, rather than failing, because metrics are tools that should not fail the
				// program when used incorrectly
				if (metrics.containsKey(name)) {
					LOG.warn("Name collision: Adding a metric subgroup with the same name as an existing metric: '" +
							name + "'. Metric might not get properly reported. (" + generateScope() + ')');
				}

				MetricGroup newGroup = new GenericMetricGroup(registry, this, name);
				MetricGroup prior = groups.put(name, newGroup);
				if (prior == null) {
					// no prior group with that name
					return newGroup;
				} else {
					// had a prior group with that name, add the prior group back
					groups.put(name, prior);
					return prior;
				}
			}
			else {
				// return a non-registered group that is immediately closed already
				GenericMetricGroup closedGroup = new GenericMetricGroup(registry, this, name);
				closedGroup.close();
				return closedGroup;
			}
		}
	}
}
