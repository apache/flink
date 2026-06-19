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

package org.apache.flink.events.otel;

import org.apache.flink.events.Event;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.otel.OpenTelemetryTestBase;
import org.apache.flink.metrics.otel.VariableNameUtil;
import org.apache.flink.traces.otel.OpenTelemetryTraceReporter;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.events.otel.OpenTelemetryEventReporter.NAME_ATTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link OpenTelemetryTraceReporter}. */
@ExtendWith({TestLoggerExtension.class})
public class OpenTelemetryEventReporterITCase extends OpenTelemetryTestBase {

    private OpenTelemetryEventReporter reporter;

    @BeforeEach
    public void setUp() {
        reporter = new OpenTelemetryEventReporter();
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
        Duration observedTs = Duration.ofMillis(42);

        reporter.open(metricConfig);
        try {
            EventBuilder event =
                    Event.builder(this.getClass(), name)
                            .setAttribute(attribute1KeyRoot, attribute1ValueRoot)
                            .setAttribute(attribute2KeyRoot, attribute2ValueRoot)
                            .setObservedTsMillis(observedTs.toMillis());

            reporter.notifyOfAddedEvent(event.build());
        } finally {
            reporter.close();
        }

        eventuallyConsumeJson(
                (json) -> {
                    JsonNode scopeLogs = json.findPath("resourceLogs").findPath("scopeLogs");
                    assertThat(scopeLogs.findPath("scope").findPath("name").asText())
                            .isEqualTo(scope);
                    JsonNode logRecords = scopeLogs.findPath("logRecords");

                    assertThat(logRecords.findPath("observedTimeUnixNano").asText())
                            .isEqualTo(Long.toString(observedTs.toNanos()));

                    JsonNode attributes = logRecords.findPath("attributes");

                    List<String> attributeKeys =
                            attributes.findValues("key").stream()
                                    .map(JsonNode::asText)
                                    .collect(Collectors.toList());

                    assertThat(attributeKeys)
                            .contains(
                                    attribute1KeyRoot,
                                    VariableNameUtil.getVariableName(attribute2KeyRoot),
                                    NAME_ATTRIBUTE);

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
                                } else if (attribute.get("key").asText().equals(NAME_ATTRIBUTE)) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(name);
                                } else {
                                    throw new IllegalStateException(
                                            "Unexpected attribute: "
                                                    + attribute.get("key").asText());
                                }
                            });
                });
    }
}
