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

package org.apache.flink.metrics.otel;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OpenTelemetryMetricReporter} batching logic. */
class OpenTelemetryMetricReporterTest {

    private TestingOpenTelemetryMetricReporter reporter;
    private TestExporter testExporter;

    @BeforeEach
    void setUp() {
        testExporter = new TestExporter();
        reporter = new TestingOpenTelemetryMetricReporter(testExporter);
    }

    @AfterEach
    void tearDown() {
        reporter.close();
    }

    @Test
    void testReportingWithBatchingEnabled() {
        final MetricConfig config = new MetricConfig();
        config.put(OpenTelemetryReporterOptions.BATCH_SIZE.key(), 2);
        reporter.open(config);

        final MetricGroup group = new TestMetricGroup();
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter1", group);
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter2", group);
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter3", group);

        reporter.report();
        reporter.waitForLastReportToComplete();

        // 3 metrics with batch size 2 -> 2 batches (2 + 1)
        assertThat(testExporter.getExportedBatches()).hasSize(2);
        assertThat(testExporter.getExportedBatches().get(0)).hasSize(2);
        assertThat(testExporter.getExportedBatches().get(1)).hasSize(1);
    }

    @Test
    void testReportingWithBatchingDisabled() {
        final MetricConfig config = new MetricConfig();
        // Default batch.size=0 disables batching
        reporter.open(config);

        final MetricGroup group = new TestMetricGroup();
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter1", group);
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter2", group);
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter3", group);

        reporter.report();
        reporter.waitForLastReportToComplete();

        // All metrics in a single export call
        assertThat(testExporter.getExportedBatches()).hasSize(1);
        assertThat(testExporter.getExportedBatches().get(0)).hasSize(3);
    }

    @Test
    void testReportingWithNegativeBatchSize() {
        final MetricConfig config = new MetricConfig();
        config.put(OpenTelemetryReporterOptions.BATCH_SIZE.key(), -1);
        reporter.open(config);

        final MetricGroup group = new TestMetricGroup();
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter1", group);
        reporter.notifyOfAddedMetric(new SimpleCounter(), "counter2", group);

        reporter.report();
        reporter.waitForLastReportToComplete();

        // Negative batch size disables batching -> single export
        assertThat(testExporter.getExportedBatches()).hasSize(1);
        assertThat(testExporter.getExportedBatches().get(0)).hasSize(2);
    }

    @Test
    void testReportEmptyMetrics() {
        final MetricConfig config = new MetricConfig();
        reporter.open(config);

        reporter.report();

        // No metrics registered -> no export calls
        assertThat(testExporter.getExportedBatches()).isEmpty();
    }

    @Test
    void testReportingWithPartialBatchFailure() {
        final PartialFailureExporter failureExporter = new PartialFailureExporter(1);
        final TestingOpenTelemetryMetricReporter failureReporter =
                new TestingOpenTelemetryMetricReporter(failureExporter);

        final MetricConfig config = new MetricConfig();
        config.put(OpenTelemetryReporterOptions.BATCH_SIZE.key(), 2);
        failureReporter.open(config);

        final MetricGroup group = new TestMetricGroup();
        failureReporter.notifyOfAddedMetric(new SimpleCounter(), "counter1", group);
        failureReporter.notifyOfAddedMetric(new SimpleCounter(), "counter2", group);
        failureReporter.notifyOfAddedMetric(new SimpleCounter(), "counter3", group);

        failureReporter.report();
        failureReporter.waitForLastReportToComplete();
        failureReporter.close();

        // 2 batches attempted, first succeeds, second fails
        assertThat(failureExporter.getExportedBatches()).hasSize(2);
        assertThat(failureExporter.getExportedBatches().get(0)).hasSize(2);
        assertThat(failureExporter.getExportedBatches().get(1)).hasSize(1);
        assertThat(failureExporter.getSuccessCount()).isEqualTo(1);
        assertThat(failureExporter.getFailureCount()).isEqualTo(1);
    }

    private static final class TestingOpenTelemetryMetricReporter
            extends OpenTelemetryMetricReporter {

        private final MetricExporter testExporter;

        TestingOpenTelemetryMetricReporter(MetricExporter testExporter) {
            super(Clock.fixed(Instant.ofEpochMilli(1234), Clock.systemUTC().getZone()));
            this.testExporter = testExporter;
        }

        @Override
        protected MetricExporter createExporter(MetricConfig metricConfig) {
            return testExporter;
        }
    }

    /** A test {@link MetricExporter} that records all export calls. */
    private static final class TestExporter implements MetricExporter {

        private final List<List<MetricData>> exportedBatches =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            exportedBatches.add(new ArrayList<>(metrics));
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.DELTA;
        }

        List<List<MetricData>> getExportedBatches() {
            return exportedBatches;
        }
    }

    /** A test {@link MetricExporter} that fails after a specified number of successful exports. */
    private static final class PartialFailureExporter implements MetricExporter {

        private final List<List<MetricData>> exportedBatches =
                Collections.synchronizedList(new ArrayList<>());
        private final int failAfter;
        private int successCount;
        private int failureCount;

        PartialFailureExporter(final int failAfter) {
            this.failAfter = failAfter;
        }

        @Override
        public CompletableResultCode export(final Collection<MetricData> metrics) {
            exportedBatches.add(new ArrayList<>(metrics));
            if (exportedBatches.size() > failAfter) {
                failureCount++;
                return CompletableResultCode.ofFailure();
            }
            successCount++;
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(
                final InstrumentType instrumentType) {
            return AggregationTemporality.DELTA;
        }

        List<List<MetricData>> getExportedBatches() {
            return exportedBatches;
        }

        int getSuccessCount() {
            return successCount;
        }

        int getFailureCount() {
            return failureCount;
        }
    }

    static class TestMetricGroup extends UnregisteredMetricsGroup implements LogicalScopeProvider {

        @Override
        public String getLogicalScope(CharacterFilter characterFilter) {
            return "test.scope";
        }

        @Override
        public String getLogicalScope(CharacterFilter characterFilter, char c) {
            return "test.scope";
        }

        @Override
        public MetricGroup getWrappedMetricGroup() {
            return this;
        }
    }
}
