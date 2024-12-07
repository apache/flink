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

package org.apache.flink.traces.otel;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.otel.OpenTelemetryTestBase;
import org.apache.flink.metrics.otel.VariableNameUtil;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link OpenTelemetryTraceReporter}. */
@ExtendWith({TestLoggerExtension.class})
public class OpenTelemetryTraceReporterITCase extends OpenTelemetryTestBase {

    private OpenTelemetryTraceReporter reporter;

    @BeforeEach
    public void setUp() {
        reporter = new OpenTelemetryTraceReporter();
    }

    @AfterEach
    public void tearDownEach() {
        reporter.close();
    }

    @Test
    public void testReportSpan() throws Exception {
        MetricConfig metricConfig = createMetricConfig();

        String scope = this.getClass().getCanonicalName();

        String attribute1KeyRoot = "foo";
        String attribute1ValueRoot = "bar";
        String attribute2KeyRoot = "<variable>";
        String attribute2ValueRoot = "value";
        String name = "root";
        Duration startTs = Duration.ofMillis(42);
        Duration endTs = Duration.ofMillis(64);

        reporter.open(metricConfig);
        try {
            SpanBuilder span =
                    Span.builder(this.getClass(), name)
                            .setAttribute(attribute1KeyRoot, attribute1ValueRoot)
                            .setAttribute(attribute2KeyRoot, attribute2ValueRoot)
                            .setStartTsMillis(startTs.toMillis())
                            .setEndTsMillis(endTs.toMillis());

            reporter.notifyOfAddedSpan(span.build());
        } finally {
            reporter.close();
        }

        eventuallyConsumeJson(
                (json) -> {
                    JsonNode scopeSpans = json.findPath("resourceSpans").findPath("scopeSpans");
                    assertThat(scopeSpans.findPath("scope").findPath("name").asText())
                            .isEqualTo(scope);
                    JsonNode spans = scopeSpans.findPath("spans");

                    assertThat(spans.findPath("name").asText()).isEqualTo(name);
                    assertThat(spans.findPath("startTimeUnixNano").asText())
                            .isEqualTo(Long.toString(startTs.toNanos()));
                    assertThat(spans.findPath("endTimeUnixNano").asText())
                            .isEqualTo(Long.toString(endTs.toNanos()));
                    assertThat(spans.findPath("name").asText()).isEqualTo(name);

                    JsonNode attributes = spans.findPath("attributes");

                    List<String> attributeKeys =
                            attributes.findValues("key").stream()
                                    .map(JsonNode::asText)
                                    .collect(Collectors.toList());

                    assertThat(attributeKeys)
                            .contains(
                                    attribute1KeyRoot,
                                    VariableNameUtil.getVariableName(attribute2KeyRoot));

                    attributes.forEach(
                            attribute -> {
                                if (attribute.get("key").asText().equals(attribute1KeyRoot)) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(attribute1ValueRoot);
                                } else if (attribute
                                        .get("key")
                                        .asText()
                                        .equals(
                                                VariableNameUtil.getVariableName(
                                                        attribute2KeyRoot))) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(attribute2ValueRoot);
                                }
                            });
                });
    }
}
