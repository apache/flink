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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpenTelemetryMetricReporter}. */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetryMetricReporterITCase extends OpenTelemetryTestBase {

    private static final long TIME_MS = 1234;

    private OpenTelemetryMetricReporter reporter;
    private final Histogram histogram = new TestHistogram();

    @BeforeEach
    public void setUpEach() {
        reporter =
                new OpenTelemetryMetricReporter(
                        Clock.fixed(Instant.ofEpochMilli(TIME_MS), Clock.systemUTC().getZone()));
    }

    @AfterEach
    public void tearDownEach() {
        reporter.close();
    }

    @Test
    public void testReport() throws Exception {
        MetricConfig metricConfig = createMetricConfig();
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        Gauge<Double> gauge = () -> 123.456d;
        reporter.notifyOfAddedMetric(gauge, "foo.gauge", group);

        reporter.report();

        MeterView meter = new MeterView(counter);
        reporter.notifyOfAddedMetric(meter, "foo.meter", group);

        reporter.notifyOfAddedMetric(histogram, "foo.histogram", group);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                (json) -> {
                    JsonNode scopeMetrics =
                            json.findPath("resourceMetrics").findPath("scopeMetrics");
                    assertThat(scopeMetrics.findPath("scope").findPath("name").asText())
                            .isEqualTo("io.confluent.flink.common.metrics");
                    JsonNode metrics = scopeMetrics.findPath("metrics");

                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames)
                            .contains(
                                    "flink.logical.scope.foo.counter",
                                    "flink.logical.scope.foo.gauge",
                                    "flink.logical.scope.foo.meter.count",
                                    "flink.logical.scope.foo.meter.rate",
                                    "flink.logical.scope.foo.histogram");

                    metrics.forEach(OpenTelemetryMetricReporterITCase::assertMetrics);
                });
    }

    private static void assertMetrics(JsonNode metric) {
        String name = metric.findPath("name").asText();
        if (name.equals("flink.logical.scope.foo.counter")) {
            assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt()).isEqualTo(0);
        } else if (name.equals("flink.logical.scope.foo.gauge")) {
            assertThat(metric.at("/gauge/dataPoints").findPath("asDouble").asDouble())
                    .isCloseTo(123.456, Percentage.withPercentage(1));
        } else if (name.equals("flink.logical.scope.foo.meter.count")) {
            assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt()).isEqualTo(0);
        } else if (name.equals("flink.logical.scope.foo.meter.rate")) {
            assertThat(metric.at("/gauge/dataPoints").findPath("asDouble").asDouble())
                    .isEqualTo(0.0);
        } else if (name.equals("flink.logical.scope.foo.histogram")) {
            assertThat(metric.at("/summary/dataPoints").findPath("sum").asInt()).isEqualTo(4);
        }
    }

    @Test
    public void testReportAfterUnregister() throws Exception {
        MetricConfig metricConfig = createMetricConfig();
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter1 = new SimpleCounter();
        SimpleCounter counter2 = new SimpleCounter();
        SimpleCounter counter3 = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter1, "foo.counter1", group);
        reporter.notifyOfAddedMetric(counter2, "foo.counter2", group);
        reporter.notifyOfAddedMetric(counter3, "foo.counter3", group);

        reporter.notifyOfRemovedMetric(counter2, "foo.counter2", group);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames)
                            .contains(
                                    "flink.logical.scope.foo.counter1",
                                    "flink.logical.scope.foo.counter3");
                });
    }

    @Test
    public void testCounterDelta() throws Exception {
        MetricConfig metricConfig = createMetricConfig();
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        counter.inc(1234);
        assertThat(counter.getCount()).isEqualTo(1234L);
        reporter.report();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames).contains("flink.logical.scope.foo.counter");

                    JsonNode metrics =
                            json.findPath("resourceMetrics")
                                    .findPath("scopeMetrics")
                                    .findPath("metrics");

                    metrics.forEach(
                            metric -> {
                                assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt())
                                        .isEqualTo(1234);
                            });
                });

        counter.inc(25);
        assertThat(counter.getCount()).isEqualTo(1259L);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames).contains("flink.logical.scope.foo.counter");

                    JsonNode metrics =
                            json.findPath("resourceMetrics")
                                    .findPath("scopeMetrics")
                                    .findPath("metrics");

                    metrics.forEach(
                            metric -> {
                                assertThat(metric.at("/sum/dataPoints").findPath("asInt").asInt())
                                        .isEqualTo(1234);
                            });
                });
    }

    @Test
    public void testOtelAttributes() throws Exception {
        MetricConfig metricConfig = createMetricConfig();
        String serviceName = "flink-bar";
        String serviceVersion = "v42";
        metricConfig.setProperty(OpenTelemetryReporterOptions.SERVICE_NAME.key(), serviceName);
        metricConfig.setProperty(
                OpenTelemetryReporterOptions.SERVICE_VERSION.key(), serviceVersion);
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        reporter.report();
        reporter.close();

        eventuallyConsumeJson(
                json -> {
                    List<String> metricNames = extractMetricNames(json);
                    assertThat(metricNames).contains("flink.logical.scope.foo.counter");

                    JsonNode attributes =
                            json.findPath("resourceMetrics")
                                    .findPath("resource")
                                    .findPath("attributes");

                    List<String> attributeKeys =
                            attributes.findValues("key").stream()
                                    .map(JsonNode::asText)
                                    .collect(Collectors.toList());

                    assertThat(attributeKeys).contains("service.name", "service.version");

                    attributes.forEach(
                            attribute -> {
                                if (attribute.get("key").asText().equals("service.name")) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(serviceName);
                                } else if (attribute
                                        .get("key")
                                        .asText()
                                        .equals("service.version")) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(serviceVersion);
                                }
                            });
                });
    }

    static class TestMetricGroup extends UnregisteredMetricsGroup implements LogicalScopeProvider {

        @Override
        public String getLogicalScope(CharacterFilter characterFilter) {
            return "logical.scope";
        }

        @Override
        public String getLogicalScope(CharacterFilter characterFilter, char c) {
            return "logical.scope";
        }

        @Override
        public MetricGroup getWrappedMetricGroup() {
            return this;
        }
    }
}
