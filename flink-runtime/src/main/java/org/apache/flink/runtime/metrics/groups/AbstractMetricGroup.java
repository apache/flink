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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract {@link MetricGroup} that contains key functionality for adding metrics and groups.
 *
 * <p><b>IMPORTANT IMPLEMENTATION NOTE</b>
 *
 * <p>This class uses locks for adding and removing metrics objects. This is done to prevent
 * resource leaks in the presence of concurrently closing a group and adding metrics and subgroups.
 * Since closing groups recursively closes the subgroups, the lock acquisition order must be
 * strictly from parent group to subgroup. If at any point, a subgroup holds its group lock and
 * calls a parent method that also acquires the lock, it will create a deadlock condition.
 *
 * <p>An AbstractMetricGroup can be {@link #close() closed}. Upon closing, the group de-register all
 * metrics from any metrics reporter and any internal maps. Note that even closed metrics groups
 * return Counters, Gauges, etc to the code, to prevent exceptions in the monitored code. These
 * metrics simply do not get reported any more, when created on a closed group.
 *
 * @param <A> The type of the parent MetricGroup
 */
@Internal
public abstract class AbstractMetricGroup<A extends AbstractMetricGroup<?>> implements MetricGroup {

    protected static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);

    // ------------------------------------------------------------------------

    /** The parent group containing this group. */
    protected final A parent;

    /** The map containing all variables and their associated values, lazily computed. */
    protected volatile Map<String, String>[] variables;

    /** The registry that this metrics group belongs to. */
    protected final MetricRegistry registry;

    /** All metrics that are directly contained in this group. */
    private final Map<String, Metric> metrics = new HashMap<>();

    /** All metric subgroups of this group. */
    private final Map<String, AbstractMetricGroup> groups = new HashMap<>();

    /**
     * The metrics scope represented by this group. For example ["host-7", "taskmanager-2",
     * "window_word_count", "my-mapper" ].
     */
    private final String[] scopeComponents;

    /**
     * Array containing the metrics scope represented by this group for each reporter, as a
     * concatenated string, lazily computed. For example:
     * "host-7.taskmanager-2.window_word_count.my-mapper"
     */
    private final String[] scopeStrings;

    /**
     * The logical metrics scope represented by this group for each reporter, as a concatenated
     * string, lazily computed. For example: "taskmanager.job.task"
     */
    private String[] logicalScopeStrings;

    /** The metrics query service scope represented by this group, lazily computed. */
    protected QueryScopeInfo queryServiceScopeInfo;

    /** Flag indicating whether this group has been closed. */
    private volatile boolean closed;

    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public AbstractMetricGroup(MetricRegistry registry, String[] scope, A parent) {
        this.registry = checkNotNull(registry);
        this.scopeComponents = checkNotNull(scope);
        this.parent = parent;
        this.scopeStrings = new String[registry.getNumberReporters()];
        this.logicalScopeStrings = new String[registry.getNumberReporters()];
        this.variables = new Map[registry.getNumberReporters() + 1];
    }

    @Override
    public Map<String, String> getAllVariables() {
        return internalGetAllVariables(0, Collections.emptySet());
    }

    public Map<String, String> getAllVariables(int reporterIndex, Set<String> excludedVariables) {
        if (reporterIndex < 0 || reporterIndex >= logicalScopeStrings.length) {
            // invalid reporter index; either a programming mistake, or we try to retrieve variables
            // outside of a reporter
            reporterIndex = -1;
        }

        // offset cache location to account for general cache at position 0
        reporterIndex += 1;

        // if no variables are excluded (which is the default!) we re-use the general variables map
        // to save space
        return internalGetAllVariables(
                excludedVariables.isEmpty() ? 0 : reporterIndex, excludedVariables);
    }

    private Map<String, String> internalGetAllVariables(
            int cachingIndex, Set<String> excludedVariables) {
        if (variables[cachingIndex] == null) {
            Map<String, String> tmpVariables = new HashMap<>();

            putVariables(tmpVariables);
            excludedVariables.forEach(tmpVariables::remove);

            if (parent != null) { // not true for Job-/TaskManagerMetricGroup
                // explicitly call getAllVariables() to prevent cascading caching operations
                // upstream, to prevent
                // caching in groups which are never directly passed to reporters
                for (Map.Entry<String, String> entry : parent.getAllVariables().entrySet()) {
                    if (!excludedVariables.contains(entry.getKey())) {
                        tmpVariables.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            variables[cachingIndex] = tmpVariables;
        }
        return variables[cachingIndex];
    }

    /**
     * Enters all variables specific to this {@link AbstractMetricGroup} and their associated values
     * into the map.
     *
     * @param variables map to enter variables and their values into
     */
    protected void putVariables(Map<String, String> variables) {}

    /**
     * Returns the logical scope of this group, for example {@code "taskmanager.job.task"}.
     *
     * @param filter character filter which is applied to the scope components
     * @return logical scope
     */
    public String getLogicalScope(CharacterFilter filter) {
        return getLogicalScope(filter, registry.getDelimiter());
    }

    /**
     * Returns the logical scope of this group, for example {@code "taskmanager.job.task"}.
     *
     * @param filter character filter which is applied to the scope components
     * @return logical scope
     */
    public String getLogicalScope(CharacterFilter filter, char delimiter) {
        return getLogicalScope(filter, delimiter, -1);
    }

    /**
     * Returns the logical scope of this group, for example {@code "taskmanager.job.task"}.
     *
     * @param filter character filter which is applied to the scope components
     * @param delimiter delimiter to use for concatenating scope components
     * @param reporterIndex index of the reporter
     * @return logical scope
     */
    String getLogicalScope(CharacterFilter filter, char delimiter, int reporterIndex) {
        if (logicalScopeStrings.length == 0
                || (reporterIndex < 0 || reporterIndex >= logicalScopeStrings.length)) {
            return createLogicalScope(filter, delimiter);
        } else {
            if (logicalScopeStrings[reporterIndex] == null) {
                logicalScopeStrings[reporterIndex] = createLogicalScope(filter, delimiter);
            }
            return logicalScopeStrings[reporterIndex];
        }
    }

    protected String createLogicalScope(CharacterFilter filter, char delimiter) {
        final String groupName = getGroupName(filter);
        return parent == null
                ? groupName
                : parent.getLogicalScope(filter, delimiter) + delimiter + groupName;
    }

    /**
     * Returns the name for this group, meaning what kind of entity it represents, for example
     * "taskmanager".
     *
     * @param filter character filter which is applied to the name
     * @return logical name for this group
     */
    protected abstract String getGroupName(CharacterFilter filter);

    /**
     * Gets the scope as an array of the scope components, for example {@code ["host-7",
     * "taskmanager-2", "window_word_count", "my-mapper"]}.
     *
     * @see #getMetricIdentifier(String)
     */
    @Override
    public String[] getScopeComponents() {
        return scopeComponents;
    }

    /**
     * Returns the metric query service scope for this group.
     *
     * @param filter character filter
     * @return query service scope
     */
    public QueryScopeInfo getQueryServiceMetricInfo(CharacterFilter filter) {
        if (queryServiceScopeInfo == null) {
            queryServiceScopeInfo = createQueryServiceMetricInfo(filter);
        }
        return queryServiceScopeInfo;
    }

    /**
     * Creates the metric query service scope for this group.
     *
     * @param filter character filter
     * @return query service scope
     */
    protected abstract QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter);

    /**
     * Returns the fully qualified metric name, for example {@code
     * "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
     *
     * @param metricName metric name
     * @return fully qualified metric name
     */
    @Override
    public String getMetricIdentifier(String metricName) {
        return getMetricIdentifier(metricName, CharacterFilter.NO_OP_FILTER);
    }

    /**
     * Returns the fully qualified metric name, for example {@code
     * "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
     *
     * @param metricName metric name
     * @param filter character filter which is applied to the scope components if not null.
     * @return fully qualified metric name
     */
    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        return getMetricIdentifier(metricName, filter, -1, registry.getDelimiter());
    }

    /**
     * Returns the fully qualified metric name using the configured delimiter for the reporter with
     * the given index, for example {@code
     * "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
     *
     * @param metricName metric name
     * @param filter character filter which is applied to the scope components if not null.
     * @param reporterIndex index of the reporter whose delimiter should be used
     * @param delimiter delimiter to use
     * @return fully qualified metric name
     */
    public String getMetricIdentifier(
            String metricName, CharacterFilter filter, int reporterIndex, char delimiter) {
        Preconditions.checkNotNull(filter);

        metricName = filter.filterCharacters(metricName);
        if (scopeStrings.length == 0
                || (reporterIndex < 0 || reporterIndex >= scopeStrings.length)) {
            return ScopeFormat.concat(filter, delimiter, scopeComponents) + delimiter + metricName;
        } else {
            if (scopeStrings[reporterIndex] == null) {
                scopeStrings[reporterIndex] =
                        ScopeFormat.concat(filter, delimiter, scopeComponents);
            }
            return scopeStrings[reporterIndex] + delimiter + metricName;
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

    @Override
    public <M extends Meter> M meter(int name, M meter) {
        return meter(String.valueOf(name), meter);
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        addMetric(name, meter);
        return meter;
    }

    /**
     * Adds the given metric to the group and registers it at the registry, if the group is not yet
     * closed, and if no metric with the same name has been registered before.
     *
     * @param name the name to register the metric under
     * @param metric the metric to register
     */
    protected void addMetric(String name, Metric metric) {
        if (metric == null) {
            LOG.warn(
                    "Ignoring attempted registration of a metric due to being null for name {}.",
                    name);
            return;
        }
        // add the metric only if the group is still open
        synchronized (this) {
            if (!closed) {
                // immediately put without a 'contains' check to optimize the common case (no
                // collision)
                // collisions are resolved later
                Metric prior = metrics.put(name, metric);

                // check for collisions with other metric names
                if (prior == null) {
                    // no other metric with this name yet

                    if (groups.containsKey(name)) {
                        // we warn here, rather than failing, because metrics are tools that should
                        // not fail the
                        // program when used incorrectly
                        LOG.warn(
                                "Name collision: Adding a metric with the same name as a metric subgroup: '"
                                        + name
                                        + "'. Metric might not get properly reported. "
                                        + Arrays.toString(scopeComponents));
                    }

                    registry.register(metric, name, this);
                } else {
                    // we had a collision. put back the original value
                    metrics.put(name, prior);

                    // we warn here, rather than failing, because metrics are tools that should not
                    // fail the
                    // program when used incorrectly
                    LOG.warn(
                            "Name collision: Group already contains a Metric with the name '"
                                    + name
                                    + "'. Metric will not be reported."
                                    + Arrays.toString(scopeComponents));
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Groups
    // ------------------------------------------------------------------------

    @Override
    public MetricGroup addGroup(int name) {
        return addGroup(String.valueOf(name), ChildType.GENERIC);
    }

    @Override
    public MetricGroup addGroup(String name) {
        return addGroup(name, ChildType.GENERIC);
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        return addGroup(key, ChildType.KEY).addGroup(value, ChildType.VALUE);
    }

    private AbstractMetricGroup<?> addGroup(String name, ChildType childType) {
        synchronized (this) {
            if (!closed) {
                // adding a group with the same name as a metric creates problems in many
                // reporters/dashboards
                // we warn here, rather than failing, because metrics are tools that should not fail
                // the
                // program when used incorrectly
                if (metrics.containsKey(name)) {
                    LOG.warn(
                            "Name collision: Adding a metric subgroup with the same name as an existing metric: '"
                                    + name
                                    + "'. Metric might not get properly reported. "
                                    + Arrays.toString(scopeComponents));
                }

                AbstractMetricGroup newGroup = createChildGroup(name, childType);
                AbstractMetricGroup prior = groups.put(name, newGroup);
                if (prior == null) {
                    // no prior group with that name
                    return newGroup;
                } else {
                    // had a prior group with that name, add the prior group back
                    groups.put(name, prior);
                    return prior;
                }
            } else {
                // return a non-registered group that is immediately closed already
                GenericMetricGroup closedGroup = new GenericMetricGroup(registry, this, name);
                closedGroup.close();
                return closedGroup;
            }
        }
    }

    protected GenericMetricGroup createChildGroup(String name, ChildType childType) {
        switch (childType) {
            case KEY:
                return new GenericKeyMetricGroup(registry, this, name);
            default:
                return new GenericMetricGroup(registry, this, name);
        }
    }

    /**
     * Enum for indicating which child group should be created. `KEY` is used to create {@link
     * GenericKeyMetricGroup}. `VALUE` is used to create {@link GenericValueMetricGroup}. `GENERIC`
     * is used to create {@link GenericMetricGroup}.
     */
    protected enum ChildType {
        KEY,
        VALUE,
        GENERIC
    }
}
