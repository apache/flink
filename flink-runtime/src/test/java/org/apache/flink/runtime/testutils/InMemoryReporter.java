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
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link MetricReporter} implementation that makes all reported metrics available for tests.
 * Metrics remain registered even after the job finishes for easier assertions.
 *
 * <p>This class can only be accessed through {@link InMemoryReporterRule#getReporter()} to ensure
 * that test cases are properly isolated.
 *
 * <p>Note that at this time, there is not a strong guarantee that metrics from one job in test case
 * A cannot spill over to a job from test case B if both test cases use the same minicluster - even
 * when run sequentially. To ensure that assertions against metrics are stable, use rather unique
 * task and operator names and respective metric patterns.
 */
@Experimental
@ThreadSafe
public class InMemoryReporter implements MetricReporter {
    private static final ThreadLocal<InMemoryReporter> REPORTERS =
            ThreadLocal.withInitial(InMemoryReporter::new);

    static InMemoryReporter getInstance() {
        return REPORTERS.get();
    }

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryReporter.class);

    private final Map<MetricGroup, Map<String, Metric>> metrics = new HashMap<>();
    private final Set<MetricGroup> removedGroups = new HashSet<>();

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {
        synchronized (this) {
            metrics.clear();
            REPORTERS.remove();
        }
    }

    void applyRemovals() {
        synchronized (this) {
            metrics.keySet().removeAll(removedGroups);
            removedGroups.clear();
        }
    }

    public Map<String, Metric> getMetricsByIdentifiers() {
        synchronized (this) {
            return getMetricStream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
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
            return new HashMap<>(metrics.get(metricGroup));
        }
    }

    public Map<String, Metric> findMetrics(String identifierPattern) {
        synchronized (this) {
            return getMetricStream(identifierPattern)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }

    public Optional<Metric> findMetric(String patternString) {
        synchronized (this) {
            return getMetricStream(patternString).map(Entry::getValue).findFirst();
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

    public List<OperatorMetricGroup> findOperatorMetricGroups(String operatorPattern) {
        Pattern pattern = Pattern.compile(operatorPattern);
        synchronized (this) {
            return metrics.keySet().stream()
                    .filter(
                            g ->
                                    g instanceof OperatorMetricGroup
                                            && pattern.matcher(
                                                            g.getScopeComponents()[
                                                                    g.getScopeComponents().length
                                                                            - 2])
                                                    .find())
                    .map(OperatorMetricGroup.class::cast)
                    .sorted(
                            Comparator.comparing(
                                    g -> g.getScopeComponents()[g.getScopeComponents().length - 1]))
                    .collect(Collectors.toList());
        }
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
        synchronized (this) {
            removedGroups.add(unwrap(group));
        }
    }

    private Stream<Entry<String, Metric>> getMetricStream(String identifierPattern) {
        Pattern pattern = Pattern.compile(identifierPattern);
        return getMetricStream().filter(m -> pattern.matcher(m.getKey()).find());
    }

    private Stream<Entry<String, Metric>> getMetricStream() {
        return metrics.entrySet().stream().flatMap(this::getGroupMetricStream);
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

    /** The factory for the {@link InMemoryReporter}. */
    public static class Factory implements MetricReporterFactory {
        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return REPORTERS.get();
        }
    }
}
