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

import java.time.Clock;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the OpenTelemetry metric reporter protocol configuration. */
public class OpenTelemetryMetricReporterProtocolTest
        extends AbstractOpenTelemetryReporterProtocolTest<OpenTelemetryMetricReporter> {

    private static final long TIME_MS = 1000;
    private static final String TEST_METRIC_NAME = "test.counter";
    private static final String EXPECTED_METRIC_NAME = "flink.test.test.counter";

    private TestMetricGroup metricGroup;

    @Override
    protected OpenTelemetryMetricReporter createReporter() {
        metricGroup = new TestMetricGroup();
        return new OpenTelemetryMetricReporter(
                Clock.fixed(Instant.ofEpochMilli(TIME_MS), Clock.systemUTC().getZone()));
    }

    @Override
    protected void closeReporter(OpenTelemetryMetricReporter reporter) {
        reporter.close();
    }

    @Override
    protected void setupAndReport(MetricConfig config) {
        reporter.open(config);
        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, TEST_METRIC_NAME, metricGroup);
        reporter.report();
        reporter.waitForLastReportToComplete();
    }

    @Override
    protected void assertReported() throws Exception {
        eventuallyConsumeJson(
                json -> assertThat(extractMetricNames(json)).contains(EXPECTED_METRIC_NAME));
    }

    static class TestMetricGroup extends UnregisteredMetricsGroup implements LogicalScopeProvider {

        @Override
        public String getLogicalScope(CharacterFilter characterFilter) {
            return "test";
        }

        @Override
        public String getLogicalScope(CharacterFilter characterFilter, char c) {
            return "test";
        }

        @Override
        public MetricGroup getWrappedMetricGroup() {
            return this;
        }
    }
}
