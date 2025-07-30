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
import org.apache.flink.util.TestLoggerExtension;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the OpenTelemetry metric reporter protocol configuration. */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetryMetricReporterProtocolTest extends OpenTelemetryTestBase {

    private static final long TIME_MS = 1000;
    private static final String TEST_METRIC_NAME = "test.counter";
    private static final String EXPECTED_METRIC_NAME = "flink.test.test.counter";

    private OpenTelemetryMetricReporter reporter;
    private TestMetricGroup metricGroup;

    @BeforeEach
    public void setUpEach() {
        reporter =
                new OpenTelemetryMetricReporter(
                        Clock.fixed(Instant.ofEpochMilli(TIME_MS), Clock.systemUTC().getZone()));
        metricGroup = new TestMetricGroup();
    }

    @AfterEach
    public void tearDownEach() {
        if (reporter != null) {
            try {
                reporter.close();
            } catch (Exception e) {
                // Ignore exceptions during cleanup
            }
        }
    }

    @Test
    public void testExplicitGrpcProtocol() throws Exception {
        MetricConfig config = createConfig("gRPC");
        setupAndReportMetric(config);
        assertMetricReported();
    }

    @Test
    public void testHttpProtocol() throws Exception {
        MetricConfig config = createConfig("HTTP");
        setupAndReportMetric(config);
        assertMetricReported();
    }

    @Test
    public void testHttpProtocolWithLowerCase() throws Exception {
        MetricConfig config = createConfig("http");
        setupAndReportMetric(config);
        assertMetricReported();
    }

    @Test
    public void testGrpcProtocolWithLowerCase() throws Exception {
        MetricConfig config = createConfig("grpc");
        setupAndReportMetric(config);
        assertMetricReported();
    }

    @Test
    public void testMissingProtocolThrowsException() {
        MetricConfig config = createMetricConfig();
        // Don't set protocol property, should throw exception

        assertInvalidProtocolException(() -> reporter.open(config));
    }

    @Test
    public void testInvalidProtocolThrowsException() {
        MetricConfig config = createMetricConfig();
        config.setProperty(
                OpenTelemetryReporterOptions.EXPORTER_PROTOCOL.key(), "INVALID_PROTOCOL");

        assertInvalidProtocolException(() -> reporter.open(config));
    }

    private void assertInvalidProtocolException(ThrowingCallable action) {
        assertThatThrownBy(action)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid exporter.protocol")
                .hasMessageContaining("gRPC")
                .hasMessageContaining("HTTP");
    }

    private MetricConfig createConfig(String protocol) {
        boolean isHttp = protocol.equalsIgnoreCase("HTTP");
        MetricConfig config = isHttp ? new MetricConfig() : createMetricConfig();
        if (isHttp) {
            config.setProperty(
                    OpenTelemetryReporterOptions.EXPORTER_ENDPOINT.key(),
                    getOtelContainer().getHttpEndpoint());
        }
        // For gRPC, not setting the endpoint here, it will be set by default by the test container
        config.setProperty(OpenTelemetryReporterOptions.EXPORTER_PROTOCOL.key(), protocol);
        return config;
    }

    private void setupAndReportMetric(MetricConfig config) {
        reporter.open(config);
        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, TEST_METRIC_NAME, metricGroup);
        reporter.report();
        reporter.waitForLastReportToComplete();
    }

    private void assertMetricReported() throws Exception {
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
