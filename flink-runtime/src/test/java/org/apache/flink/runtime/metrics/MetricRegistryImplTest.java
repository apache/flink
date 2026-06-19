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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EventOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.events.Event;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.metrics.CollectingMetricsReporter.MetricGroupAndName;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.filter.DefaultReporterFilters;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestEventReporter;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.runtime.metrics.util.TestTraceReporter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.traces.reporter.TraceReporter;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterators;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link MetricRegistryImpl}. */
class MetricRegistryImplTest {

    private static final char GLOBAL_DEFAULT_DELIMITER = '.';

    @Test
    void testIsShutdown() throws Exception {
        MetricRegistryImpl metricRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        assertThat(metricRegistry.isShutdown()).isFalse();

        metricRegistry.closeAsync().get();

        assertThat(metricRegistry.isShutdown()).isTrue();
    }

    @Test
    void testMetricQueryServiceSetup() throws Exception {
        MetricRegistryImpl metricRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        assertThat(metricRegistry.getMetricQueryServiceGatewayRpcAddress()).isNull();

        metricRegistry.startQueryService(new TestingRpcService(), new ResourceID("mqs"));

        MetricQueryServiceGateway metricQueryServiceGateway =
                metricRegistry.getMetricQueryServiceGateway();
        assertThat(metricQueryServiceGateway).isNotNull();

        metricRegistry.register(
                new SimpleCounter(),
                "counter",
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup());

        boolean metricsSuccessfullyQueried = false;
        for (int x = 0; x < 10; x++) {
            MetricDumpSerialization.MetricSerializationResult metricSerializationResult =
                    metricQueryServiceGateway
                            .queryMetrics(Duration.ofSeconds(5))
                            .get(5, TimeUnit.SECONDS);

            if (metricSerializationResult.numCounters == 1) {
                metricsSuccessfullyQueried = true;
            } else {
                Thread.sleep(50);
            }
        }
        assertThat(metricsSuccessfullyQueried)
                .as("metrics query did not return expected result")
                .isTrue();
    }

    /**
     * Verifies that reporters implementing the Scheduled interface are regularly called to report
     * the metrics.
     */
    @Test
    void testReporterScheduling() throws Exception {
        MetricConfig config = new MetricConfig();
        config.setProperty("arg1", "hello");
        config.setProperty(MetricOptions.REPORTER_INTERVAL.key(), "50 MILLISECONDS");

        final ReportCountingReporter reporter = new ReportCountingReporter();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test", config, reporter)));

        long start = System.currentTimeMillis();

        // only start counting from now on
        reporter.resetCount();

        for (int x = 0; x < 10; x++) {
            Thread.sleep(100);
            int reportCount = reporter.getReportCount();
            long curT = System.currentTimeMillis();
            /**
             * Within a given time-frame T only T/500 reports may be triggered due to the interval
             * between reports. This value however does not not take the first triggered report into
             * account (=> +1). Furthermore we have to account for the mis-alignment between reports
             * being triggered and our time measurement (=> +1); for T=200 a total of 4-6 reports
             * may have been triggered depending on whether the end of the interval for the first
             * reports ends before or after T=50.
             */
            long maxAllowedReports = (curT - start) / 50 + 2;
            assertThat(maxAllowedReports)
                    .as("Too many reports were triggered.")
                    .isGreaterThanOrEqualTo(reportCount);
        }
        assertThat(reporter.getReportCount()).as("No report was triggered.").isGreaterThan(0);

        registry.closeAsync().get();
    }

    @Test
    void testReporterIntervalParsingErrorFallsBackToDefaultValue() throws Exception {
        MetricConfig config = new MetricConfig();
        // in a prior implementation the time amount was applied even if the time unit was invalid
        // in this case this would imply using 1 SECOND as the interval (seconds is the default)
        config.setProperty(MetricOptions.REPORTER_INTERVAL.key(), "1 UNICORN");

        final ManuallyTriggeredScheduledExecutorService manuallyTriggeredScheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test", config, new ReportCountingReporter())),
                        manuallyTriggeredScheduledExecutorService);
        try {
            Collection<ScheduledFuture<?>> scheduledTasks =
                    manuallyTriggeredScheduledExecutorService.getActiveScheduledTasks();
            ScheduledFuture<?> reportTask = Iterators.getOnlyElement(scheduledTasks.iterator());
            assertThat(reportTask.getDelay(TimeUnit.SECONDS))
                    .isEqualTo(MetricOptions.REPORTER_INTERVAL.defaultValue().getSeconds());
        } finally {
            registry.closeAsync().get();
        }
    }

    /** Reporter that exposes how often report() was called. */
    private static class ReportCountingReporter extends TestReporter implements Scheduled {
        private int reportCount = 0;

        @Override
        public void report() {
            reportCount++;
        }

        public int getReportCount() {
            return reportCount;
        }

        public void resetCount() {
            reportCount = 0;
        }
    }

    /** Verifies that reporters are notified of added/removed metrics. */
    @Test
    void testReporterNotifications() throws Exception {
        final NotificationCapturingMetricReporter reporter1 =
                new NotificationCapturingMetricReporter();
        final NotificationCapturingMetricReporter reporter2 =
                new NotificationCapturingMetricReporter();
        final NotificationCapturingEventReporter eventReporter1 =
                new NotificationCapturingEventReporter();
        final NotificationCapturingEventReporter eventReporter2 =
                new NotificationCapturingEventReporter();
        final NotificationCapturingSpanReporter spanReporter1 =
                new NotificationCapturingSpanReporter();
        final NotificationCapturingSpanReporter spanReporter2 =
                new NotificationCapturingSpanReporter();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test1", reporter1),
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test2", reporter2)),
                        Arrays.asList(
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "trace_test1", spanReporter1),
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "trace_test2", spanReporter2)),
                        Arrays.asList(
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "event_test1", eventReporter1),
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "event_test2", eventReporter2)));

        TaskManagerMetricGroup root =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        root.counter("rootCounter");
        root.addEvent(Event.builder(getClass(), "TestEvent"));
        root.addSpan(Span.builder(getClass(), "TestSpan"));

        assertThat(reporter1.getLastAddedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter1.getLastAddedMetricName()).hasValue("rootCounter");
        assertThat(eventReporter1.getLastAddedEvent().map(Event::getName)).hasValue("TestEvent");
        assertThat(spanReporter1.getLastAddedSpan().map(Span::getName)).hasValue("TestSpan");

        assertThat(reporter2.getLastAddedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter2.getLastAddedMetricName()).hasValue("rootCounter");
        assertThat(eventReporter2.getLastAddedEvent().map(Event::getName)).hasValue("TestEvent");
        assertThat(spanReporter2.getLastAddedSpan().map(Span::getName)).hasValue("TestSpan");

        root.close();

        assertThat(reporter1.getLastRemovedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter1.getLastRemovedMetricName()).hasValue("rootCounter");

        assertThat(reporter2.getLastRemovedMetric()).containsInstanceOf(Counter.class);
        assertThat(reporter2.getLastRemovedMetricName()).hasValue("rootCounter");

        registry.closeAsync().get();
    }

    /**
     * Reporter that exposes the name and metric instance of the last metric that was added or
     * removed.
     */
    private static class NotificationCapturingMetricReporter extends TestReporter {
        @Nullable private Metric addedMetric;
        @Nullable private String addedMetricName;

        @Nullable private Metric removedMetric;
        @Nullable private String removedMetricName;

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            addedMetric = metric;
            addedMetricName = metricName;
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            removedMetric = metric;
            removedMetricName = metricName;
        }

        public Optional<Metric> getLastAddedMetric() {
            return Optional.ofNullable(addedMetric);
        }

        public Optional<String> getLastAddedMetricName() {
            return Optional.ofNullable(addedMetricName);
        }

        public Optional<Metric> getLastRemovedMetric() {
            return Optional.ofNullable(removedMetric);
        }

        public Optional<String> getLastRemovedMetricName() {
            return Optional.ofNullable(removedMetricName);
        }
    }

    private static class NotificationCapturingEventReporter extends TestEventReporter {
        @Nullable private Event addedEvent;

        @Override
        public void notifyOfAddedEvent(Event event) {
            this.addedEvent = event;
        }

        public Optional<Event> getLastAddedEvent() {
            return Optional.ofNullable(addedEvent);
        }
    }

    private static class NotificationCapturingSpanReporter extends TestTraceReporter {
        @Nullable private Span addedSpan;

        @Override
        public void notifyOfAddedSpan(Span span) {
            this.addedSpan = span;
        }

        public Optional<Span> getLastAddedSpan() {
            return Optional.ofNullable(addedSpan);
        }
    }

    /** Verifies that the scope configuration is properly extracted. */
    @Test
    void testScopeConfig() {
        Configuration config = new Configuration();

        config.set(MetricOptions.SCOPE_NAMING_TM, "A");
        config.set(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
        config.set(MetricOptions.SCOPE_NAMING_TASK, "C");
        config.set(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

        ScopeFormats scopeConfig = ScopeFormats.fromConfig(config);

        assertThat(scopeConfig.getTaskManagerFormat().format()).isEqualTo("A");
        assertThat(scopeConfig.getTaskManagerJobFormat().format()).isEqualTo("B");
        assertThat(scopeConfig.getTaskFormat().format()).isEqualTo("C");
        assertThat(scopeConfig.getOperatorFormat().format()).isEqualTo("D");
    }

    @Test
    void testConfigurableDelimiter() throws Exception {
        Configuration config = new Configuration();
        config.set(MetricOptions.SCOPE_DELIMITER, "_");
        config.set(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D.E");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.fromConfiguration(config),
                        ReporterSetupBuilder.METRIC_SETUP_BUILDER.fromConfiguration(
                                config, DefaultReporterFilters::metricsFromConfiguration, null));

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        assertThat(tmGroup.getMetricIdentifier("name")).isEqualTo("A_B_C_D_E_name");

        registry.closeAsync().get();
    }

    @Test
    void testConfigurableDelimiterForReporters() throws Exception {
        MetricConfig config1 = new MetricConfig();
        config1.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "_");

        MetricConfig config2 = new MetricConfig();
        config2.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "-");

        MetricConfig config3 = new MetricConfig();
        config3.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "AA");

        MetricConfig traceConfig1 = new MetricConfig();
        traceConfig1.setProperty(TraceOptions.REPORTER_SCOPE_DELIMITER.key(), "_");

        MetricConfig traceConfig2 = new MetricConfig();
        traceConfig2.setProperty(TraceOptions.REPORTER_SCOPE_DELIMITER.key(), "-");

        MetricConfig traceConfig3 = new MetricConfig();
        traceConfig3.setProperty(TraceOptions.REPORTER_SCOPE_DELIMITER.key(), "AA");

        MetricConfig eventConfig1 = new MetricConfig();
        eventConfig1.setProperty(EventOptions.REPORTER_SCOPE_DELIMITER.key(), "_");

        MetricConfig eventConfig2 = new MetricConfig();
        eventConfig2.setProperty(EventOptions.REPORTER_SCOPE_DELIMITER.key(), "-");

        MetricConfig eventConfig3 = new MetricConfig();
        eventConfig3.setProperty(EventOptions.REPORTER_SCOPE_DELIMITER.key(), "AA");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test1", config1, new TestReporter()),
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test2", config2, new TestReporter()),
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test3", config3, new TestReporter())),
                        Arrays.asList(
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "traceTest1", traceConfig1, new TestTraceReporter()),
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "traceTest2", traceConfig2, new TestTraceReporter()),
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "traceTest3", traceConfig3, new TestTraceReporter())),
                        Arrays.asList(
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "eventTest1", eventConfig1, new TestEventReporter()),
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "eventTest2", eventConfig2, new TestEventReporter()),
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "eventTest3", eventConfig3, new TestEventReporter())));

        assertThat(registry.getDelimiter()).isEqualTo(GLOBAL_DEFAULT_DELIMITER);
        assertThat(registry.getDelimiter(0)).isEqualTo('_');
        assertThat(registry.getDelimiter(1)).isEqualTo('-');
        assertThat(registry.getDelimiter(2)).isEqualTo(GLOBAL_DEFAULT_DELIMITER);
        assertThat(registry.getDelimiter(3)).isEqualTo(GLOBAL_DEFAULT_DELIMITER);
        assertThat(registry.getDelimiter(-1)).isEqualTo(GLOBAL_DEFAULT_DELIMITER);

        List<MetricRegistryImpl.ReporterAndSettings<TraceReporter, SpanBuilder>> traceReporters =
                registry.getTraceReporters();
        assertThat(traceReporters.get(0).getSettings().getDelimiter()).isEqualTo('_');
        assertThat(traceReporters.get(1).getSettings().getDelimiter()).isEqualTo('-');
        assertThat(traceReporters.get(2).getSettings().getDelimiter())
                .isEqualTo(GLOBAL_DEFAULT_DELIMITER);

        List<MetricRegistryImpl.ReporterAndSettings<EventReporter, EventBuilder>> eventReporters =
                registry.getEventReporters();
        assertThat(traceReporters.get(0).getSettings().getDelimiter()).isEqualTo('_');
        assertThat(traceReporters.get(1).getSettings().getDelimiter()).isEqualTo('-');
        assertThat(traceReporters.get(2).getSettings().getDelimiter())
                .isEqualTo(GLOBAL_DEFAULT_DELIMITER);

        registry.closeAsync().get();
    }

    @Test
    void testConfigurableDelimiterForReportersInGroup() throws Exception {
        String name = "C";
        MetricConfig config1 = new MetricConfig();
        config1.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "_");

        MetricConfig config2 = new MetricConfig();
        config2.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "-");

        MetricConfig config3 = new MetricConfig();
        config3.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "AA");

        Configuration config = new Configuration();
        config.set(MetricOptions.SCOPE_NAMING_TM, "A.B");

        List<ReporterSetup> reporterConfigurations =
                Arrays.asList(
                        ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                "test1", config1, new CollectingMetricsReporter()),
                        ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                "test2", config2, new CollectingMetricsReporter()),
                        ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                "test3", config3, new CollectingMetricsReporter()),
                        ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                "test4", new CollectingMetricsReporter()));
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.fromConfiguration(config), reporterConfigurations);

        TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        group.counter(name);
        group.close();
        registry.closeAsync().get();

        for (ReporterSetup cfg : reporterConfigurations) {
            String delimiter =
                    cfg.getConfiguration()
                            .getProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key());
            if (delimiter == null || delimiter.equals("AA")) {
                // test3 reporter: 'AA' - not correct
                // for test4 reporter use global delimiter
                delimiter = String.valueOf(GLOBAL_DEFAULT_DELIMITER);
            }
            String expected =
                    (config.get(MetricOptions.SCOPE_NAMING_TM) + ".C").replaceAll("\\.", delimiter);
            CollectingMetricsReporter reporter = (CollectingMetricsReporter) cfg.getReporter();

            for (MetricGroupAndName groupAndName :
                    Arrays.asList(reporter.findAdded(name), reporter.findRemoved(name))) {
                assertThat(groupAndName.group.getMetricIdentifier(name)).isEqualTo(expected);
                assertThat(groupAndName.group.getMetricIdentifier(name, reporter))
                        .isEqualTo(expected);
            }
        }
    }

    /** Tests that the query actor will be stopped when the MetricRegistry is shut down. */
    @Test
    void testQueryActorShutdown() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        final RpcService rpcService = new TestingRpcService();

        registry.startQueryService(rpcService, null);

        MetricQueryService queryService = checkNotNull(registry.getQueryService());

        registry.closeAsync().get();

        queryService.getTerminationFuture().get();
    }

    @Test
    void testExceptionIsolation() throws Exception {
        final NotificationCapturingMetricReporter reporter1 =
                new NotificationCapturingMetricReporter();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test1", new FailingReporter()),
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test2", reporter1)));

        Counter metric = new SimpleCounter();
        registry.register(
                metric, "counter", new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertThat(reporter1.getLastAddedMetric()).hasValue(metric);
        assertThat(reporter1.getLastAddedMetricName()).hasValue("counter");

        registry.unregister(
                metric, "counter", new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertThat(reporter1.getLastRemovedMetric()).hasValue(metric);
        assertThat(reporter1.getLastRemovedMetricName()).hasValue("counter");

        registry.closeAsync().get();
    }

    /** Reporter that throws an exception when it is notified of an added or removed metric. */
    private static class FailingReporter extends TestReporter {
        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            throw new RuntimeException();
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            throw new RuntimeException();
        }
    }

    @Test
    void testMetricFiltering() {
        final String excludedMetricName = "excluded";
        final NotificationCapturingMetricReporter reporter =
                new NotificationCapturingMetricReporter();

        final Configuration reporterConfig = new Configuration();
        reporterConfig.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:*:counter"));
        reporterConfig.set(
                MetricOptions.REPORTER_EXCLUDES, Arrays.asList("*:" + excludedMetricName));

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetupBuilder.METRIC_SETUP_BUILDER.forReporter(
                                        "test",
                                        reporter,
                                        DefaultReporterFilters.metricsFromConfiguration(
                                                reporterConfig))));

        registry.register(
                new TestMeter(), "", new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertThat(reporter.getLastAddedMetric()).isEmpty();

        registry.register(
                new TestCounter(),
                excludedMetricName,
                new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertThat(reporter.getLastAddedMetric()).isEmpty();

        registry.register(
                new TestCounter(), "foo", new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertThat(reporter.getLastAddedMetric()).isNotEmpty();
    }

    @Test
    void testSpanFiltering() {
        final String includedGroupName = "foo";
        final String excludedSpanName = "excluded";
        final NotificationCapturingSpanReporter reporter = new NotificationCapturingSpanReporter();

        final Configuration reporterConfig = new Configuration();
        reporterConfig.set(
                TraceOptions.REPORTER_INCLUDES,
                Collections.singletonList(includedGroupName + ":*"));
        reporterConfig.set(
                TraceOptions.REPORTER_EXCLUDES, Collections.singletonList("*:" + excludedSpanName));

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "test",
                                        reporter,
                                        DefaultReporterFilters.tracesFromConfiguration(
                                                reporterConfig))),
                        Collections.emptyList());

        registry.addSpan(
                Span.builder(getClass(), "testSpan"),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, "bar"));

        assertThat(reporter.getLastAddedSpan()).isEmpty();

        registry.addSpan(
                Span.builder(getClass(), excludedSpanName),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, includedGroupName));

        assertThat(reporter.getLastAddedSpan()).isEmpty();

        registry.addSpan(
                Span.builder(getClass(), "foo"),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, includedGroupName));

        assertThat(reporter.getLastAddedSpan()).isNotEmpty();
    }

    @Test
    void testEventFiltering() {
        final String includedGroupName = "foo";
        final String excludedSpanName = "excluded";
        final NotificationCapturingEventReporter reporter =
                new NotificationCapturingEventReporter();

        final Configuration reporterConfig = new Configuration();
        reporterConfig.set(
                EventOptions.REPORTER_INCLUDES,
                Collections.singletonList(includedGroupName + ":*"));
        reporterConfig.set(
                EventOptions.REPORTER_EXCLUDES, Collections.singletonList("*:" + excludedSpanName));

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "test",
                                        reporter,
                                        DefaultReporterFilters.eventsFromConfiguration(
                                                reporterConfig))));

        registry.addEvent(
                Event.builder(getClass(), "testEvent"),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, "bar"));

        assertThat(reporter.getLastAddedEvent()).isEmpty();

        registry.addEvent(
                Event.builder(getClass(), excludedSpanName),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, includedGroupName));

        assertThat(reporter.getLastAddedEvent()).isEmpty();

        registry.addEvent(
                Event.builder(getClass(), "foo"),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, includedGroupName));

        assertThat(reporter.getLastAddedEvent()).isNotEmpty();
    }

    @Test
    void testSpanAdditionalVariables() {
        final NotificationCapturingSpanReporter reporter = new NotificationCapturingSpanReporter();

        final Configuration reporterConfig = new Configuration();

        Map<String, String> additionalVariables = Collections.singletonMap("foo", "bar");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                ReporterSetupBuilder.TRACE_SETUP_BUILDER.forReporter(
                                        "test",
                                        new MetricConfig(),
                                        reporter,
                                        DefaultReporterFilters.tracesFromConfiguration(
                                                reporterConfig),
                                        additionalVariables)),
                        Collections.emptyList());

        registry.addSpan(
                Span.builder(getClass(), "testEvent"),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, "testGroup"));

        Optional<Span> lastAddedSpan = reporter.getLastAddedSpan();

        assertThat(lastAddedSpan.get().getName()).isEqualTo("testEvent");
        assertThat(lastAddedSpan.get().getAttributes())
                .containsExactlyInAnyOrderEntriesOf(additionalVariables);
    }

    @Test
    void testEventAdditionalVariables() {
        final NotificationCapturingEventReporter reporter =
                new NotificationCapturingEventReporter();

        final Configuration reporterConfig = new Configuration();

        Map<String, String> additionalVariables = Collections.singletonMap("foo", "bar");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                ReporterSetupBuilder.EVENT_SETUP_BUILDER.forReporter(
                                        "test",
                                        new MetricConfig(),
                                        reporter,
                                        DefaultReporterFilters.eventsFromConfiguration(
                                                reporterConfig),
                                        additionalVariables)));

        registry.addEvent(
                Event.builder(getClass(), "testEvent"),
                new MetricGroupTest.DummyAbstractMetricGroup(registry, "testGroup"));

        Optional<Event> lastAddedEvent = reporter.getLastAddedEvent();

        assertThat(lastAddedEvent.get().getName()).isEqualTo("testEvent");
        assertThat(lastAddedEvent.get().getAttributes())
                .containsExactlyInAnyOrderEntriesOf(additionalVariables);
    }
}
