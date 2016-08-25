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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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
 *
 * <p>An AbstractMetricGroup can be {@link #close() closed}. Upon closing, the group de-register all metrics
 * from any metrics reporter and any internal maps. Note that even closed metrics groups
 * return Counters, Gauges, etc to the code, to prevent exceptions in the monitored code.
 * These metrics simply do not get reported any more, when created on a closed group.
 * 
 * @param <A> The type of the parent MetricGroup
 */
public abstract class AbstractMetricGroup<A extends AbstractMetricGroup<?>> implements MetricGroup {

	/** shared logger */
	private static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);

	// ------------------------------------------------------------------------

	/** The parent group containing this group */
	protected final A parent;

	/** The map containing all variables and their associated values, lazily computed. */
	protected volatile Map<String, String> variables;
	
	/** The registry that this metrics group belongs to */
	protected final MetricRegistry registry;

	/** All metrics that are directly contained in this group */
	private final Map<String, Metric> metrics = new HashMap<>();

	/** All metric subgroups of this group */
	private final Map<String, AbstractMetricGroup> groups = new HashMap<>();

	/** The metrics scope represented by this group.
	 *  For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ]. */
	private final String[] scopeComponents;

	/** The metrics scope represented by this group, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper" */
	private String scopeString;

	/** Flag indicating whether this group has been closed */
	private volatile boolean closed;

	// ------------------------------------------------------------------------

	public AbstractMetricGroup(MetricRegistry registry, String[] scope, A parent) {
		this.registry = checkNotNull(registry);
		this.scopeComponents = checkNotNull(scope);
		this.parent = parent;
	}

	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized(this) {
				if (variables == null) {
					if (parent != null) {
						variables = parent.getAllVariables();
					} else { // this case should only be true for mock groups
						variables = new HashMap<>();
					}
				}
			}
		}
		return variables;
	}

	/**
	 * Gets the scope as an array of the scope components, for example
	 * {@code ["host-7", "taskmanager-2", "window_word_count", "my-mapper"]}
	 * 
	 * @see #getMetricIdentifier(String) 
	 */
	public String[] getScopeComponents() {
		return scopeComponents;
	}

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}
	 * 
	 * @param metricName metric name
	 * @return fully qualified metric name
	 */
	public String getMetricIdentifier(String metricName) {
		return getMetricIdentifier(metricName, null);
	}

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}
	 *
	 * @param metricName metric name
	 * @param filter character filter which is applied to the scope components if not null.
	 * @return fully qualified metric name
	 */
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		if (scopeString == null) {
			if (filter != null) {
				scopeString = ScopeFormat.concat(filter, registry.getDelimiter(), scopeComponents);
			} else {
				scopeString = ScopeFormat.concat(registry.getDelimiter(), scopeComponents);
			}
		}

		if (filter != null) {
			return scopeString + registry.getDelimiter() + filter.filterCharacters(metricName);
		} else {
			return scopeString + registry.getDelimiter() + metricName;
		}
	}
	
	// ------------------------------------------------------------------------
	//  Closing
	// ------------------------------------------------------------------------

	public void close() {
		synchronized (this) {
			if (!closed) {
				closed = true;

				// close all subgroups
				for (AbstractMetricGroup group : groups.values()) {
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

	public final boolean isClosed() {
		return closed;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//  Metrics
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Counter counter(int name) {
		return counter(String.valueOf(name));
	}

	@Override
	public Counter counter(String name) {
		return counter(name, new SimpleCounter());
	}
	
	@Override
	public <C extends Counter> C counter(int name, C counter) {
		return counter(String.valueOf(name), counter);
	}

	@Override
	public <C extends Counter> C counter(String name, C counter) {
		addMetric(name, counter);
		return counter;
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
		return gauge(String.valueOf(name), gauge);
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
		addMetric(name, gauge);
		return gauge;
	}

	@Override
	public <H extends Histogram> H histogram(int name, H histogram) {
		return histogram(String.valueOf(name), histogram);
	}

	@Override
	public <H extends Histogram> H histogram(String name, H histogram) {
		addMetric(name, histogram);
		return histogram;
	}

	/**
	 * Adds the given metric to the group and registers it at the registry, if the group
	 * is not yet closed, and if no metric with the same name has been registered before.
	 * 
	 * @param name the name to register the metric under
	 * @param metric the metric to register
	 */
	protected void addMetric(String name, Metric metric) {
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
								name + "'. Metric might not get properly reported. (" + scopeString + ')');
					}

					registry.register(metric, name, this);
				}
				else {
					// we had a collision. put back the original value
					metrics.put(name, prior);
					
					// we warn here, rather than failing, because metrics are tools that should not fail the
					// program when used incorrectly
					LOG.warn("Name collision: Group already contains a Metric with the name '" +
							name + "'. Metric will not be reported. (" + scopeString + ')');
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
							name + "'. Metric might not get properly reported. (" + scopeString + ')');
				}

				AbstractMetricGroup newGroup = new GenericMetricGroup(registry, this, name);
				AbstractMetricGroup prior = groups.put(name, newGroup);
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
