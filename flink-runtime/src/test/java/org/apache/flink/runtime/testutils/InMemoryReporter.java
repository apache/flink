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

package org.apache.flink.runtime.testutils;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link MetricReporter} implementation that makes all reported metrics available for tests.
 *
 * <p>By default, metrics in the {@link InMemoryReporter} follow the general life-cycle of metrics
 * in a {@link org.apache.flink.runtime.metrics.util.TestReporter}; that is, task metrics will be
 * removed as soon as the task finishes etc. By using {@link #createWithRetainedMetrics()}, these
 * metrics will only be retained until the cluster is closed.
 *
 * <p>Note that at this time, there is not a strong guarantee that metrics from one job in test case
 * A cannot spill over to a job from test case B if both test cases use the same minicluster - even
 * when run sequentially. To ensure that assertions against metrics are stable, use rather unique
 * task and operator names and respective metric patterns.
 */
@Experimental
@ThreadSafe
public class InMemoryReporter implements MetricReporter {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryReporter.class);
    private static final String ID = "ID";
    private static final Map<UUID, InMemoryReporter> REPORTERS = new ConcurrentHashMap<>();

    private final Map<MetricGroup, Map<String, Metric>> metrics = new HashMap<>();
    private final UUID id;

    private final boolean retainMetrics;

    InMemoryReporter(boolean retainMetrics) {
        this.retainMetrics = retainMetrics;
        this.id = UUID.randomUUID();
        REPORTERS.put(id, this);
    }

    public static InMemoryReporter create() {
        return new InMemoryReporter(false);
    }

    public static InMemoryReporter createWithRetainedMetrics() {
        return new InMemoryReporter(true);
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {
        synchronized (this) {
            metrics.clear();
            REPORTERS.remove(id);
        }
    }

    public Map<String, Metric> getMetricsByIdentifiers(JobID jobId) {
        synchronized (this) {
            return getMetricStream(jobId).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }

    public Map<MetricGroup, Map<String, Metric>> getMetricsByGroup() {
        synchronized (this) {
            // create a deep copy to avoid concurrent modifications
            return metrics.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> new HashMap<>(e.getValue())));
        }
    }

    public Map<String, Metric> getMetricsByGroup(MetricGroup metricGroup) {
        synchronized (this) {
            // create a copy of the inner Map to avoid concurrent modifications
            return new HashMap<>(metrics.getOrDefault(metricGroup, Collections.emptyMap()));
        }
    }

    public Map<String, Metric> findMetrics(JobID jobId, String identifierPattern) {
        synchronized (this) {
            return getMetricStream(jobId, identifierPattern)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }

    public Optional<Metric> findMetric(JobID jobId, String patternString) {
        synchronized (this) {
            return getMetricStream(jobId, patternString).map(Entry::getValue).findFirst();
        }
    }

    public Set<MetricGroup> findGroups(String groupPattern) {
        synchronized (this) {
            return getGroupStream(groupPattern).collect(Collectors.toSet());
        }
    }

    public Optional<MetricGroup> findGroup(String groupPattern) {
        synchronized (this) {
            return getGroupStream(groupPattern).findFirst();
        }
    }

    public List<OperatorMetricGroup> findOperatorMetricGroups(JobID jobId, String operatorPattern) {
        Pattern pattern = Pattern.compile(operatorPattern);
        synchronized (this) {
            return metrics.keySet().stream()
                    .filter(
                            g ->
                                    g instanceof OperatorMetricGroup
                                            && pattern.matcher(getOperatorName(g)).find()
                                            && getJobId(g).equals(jobId.toString()))
                    .map(OperatorMetricGroup.class::cast)
                    .sorted(Comparator.comparing(this::getSubtaskId))
                    .collect(Collectors.toList());
        }
    }

    public List<Tuple3<MetricGroup, String, Metric>> findJobMetricGroups(
            JobID jobId, String metricPattern) {
        Pattern pattern = Pattern.compile(metricPattern);
        synchronized (this) {
            return metrics.entrySet().stream()
                    .filter(group -> Objects.equals(getJobId(group.getKey()), jobId.toString()))
                    .flatMap(
                            group ->
                                    group.getValue().entrySet().stream()
                                            .filter(
                                                    metric ->
                                                            pattern.matcher(metric.getKey()).find())
                                            .map(
                                                    metric ->
                                                            Tuple3.of(
                                                                    group.getKey(),
                                                                    metric.getKey(),
                                                                    metric.getValue())))
                    .collect(Collectors.toList());
        }
    }

    private String getSubtaskId(OperatorMetricGroup g) {
        return g.getAllVariables().get(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX);
    }

    private String getOperatorName(MetricGroup g) {
        return g.getAllVariables().get(ScopeFormat.SCOPE_OPERATOR_NAME);
    }

    private String getJobId(MetricGroup g) {
        return g.getAllVariables().get(ScopeFormat.SCOPE_JOB_ID);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        MetricGroup metricGroup = unwrap(group);
        LOG.debug("Registered {} @ {}", metricName, metricGroup);
        synchronized (this) {
            metrics.computeIfAbsent(metricGroup, dummy -> new HashMap<>()).put(metricName, metric);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        if (!retainMetrics) {
            synchronized (this) {
                MetricGroup metricGroup = unwrap(group);
                Map<String, Metric> registeredMetrics = metrics.get(metricGroup);
                if (registeredMetrics != null) {
                    registeredMetrics.remove(metricName);
                    if (registeredMetrics.isEmpty()) {
                        metrics.remove(metricGroup);
                    }
                }
            }
        }
    }

    private Stream<Entry<String, Metric>> getMetricStream(JobID jobID, String identifierPattern) {
        Pattern pattern = Pattern.compile(identifierPattern);
        return getMetricStream(jobID).filter(m -> pattern.matcher(m.getKey()).find());
    }

    private Stream<Entry<String, Metric>> getMetricStream(JobID jobId) {
        return metrics.entrySet().stream()
                .filter(gr -> Objects.equals(getJobId(gr.getKey()), jobId.toString()))
                .flatMap(this::getGroupMetricStream);
    }

    private Stream<MetricGroup> getGroupStream(String groupPattern) {
        Pattern pattern = Pattern.compile(groupPattern);
        return metrics.keySet().stream()
                .filter(
                        group ->
                                Arrays.stream(group.getScopeComponents())
                                        .anyMatch(scope -> pattern.matcher(scope).find()));
    }

    private Stream<SimpleEntry<String, Metric>> getGroupMetricStream(
            Entry<MetricGroup, Map<String, Metric>> groupMetrics) {
        return groupMetrics.getValue().entrySet().stream()
                .map(
                        nameMetric ->
                                new SimpleEntry<>(
                                        groupMetrics
                                                .getKey()
                                                .getMetricIdentifier(nameMetric.getKey()),
                                        nameMetric.getValue()));
    }

    private MetricGroup unwrap(MetricGroup group) {
        return group instanceof LogicalScopeProvider
                ? ((LogicalScopeProvider) group).getWrappedMetricGroup()
                : group;
    }

    public Configuration addToConfiguration(Configuration configuration) {
        MetricOptions.forReporter(configuration, "mini_cluster_resource_reporter")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, InMemoryReporter.Factory.class.getName())
                .setString("ID", id.toString());

        return configuration;
    }

    /** The factory for the {@link InMemoryReporter}. */
    public static class Factory implements MetricReporterFactory {

        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            String id = properties.getProperty(ID);
            checkState(
                    id != null,
                    "Reporter id not found. Did you use InMemoryReporter#addConfiguration?");
            return REPORTERS.get(UUID.fromString(id));
        }
    }
}
