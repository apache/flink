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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Test
    public void testReportNestedSpan() throws Exception {
        String scope = this.getClass().getCanonicalName();

        String attribute1KeyRoot = "foo";
        String attribute1ValueRoot = "bar";
        String attribute2KeyRoot = "<variable>";
        String attribute2ValueRoot = "value";
        String spanRoot = "root";

        String spanL1N1 = "1_1";
        String attribute1KeyL1N1 = "foo_" + spanL1N1;
        String attribute1ValueL1N1 = "bar_" + spanL1N1;

        String spanL1N2 = "1_2";
        String attribute1KeyL1N2 = "foo_" + spanL1N2;
        String attribute1ValueL1N2 = "bar_" + spanL1N2;

        String spanL2N1 = "2_1";
        String attribute1KeyL2N1 = "foo_" + spanL2N1;
        String attribute1ValueL2N1 = "bar_" + spanL2N1;

        reporter.open(createMetricConfig());
        try {
            SpanBuilder childLeveL2N1 =
                    Span.builder(this.getClass(), spanL2N1)
                            .setAttribute(attribute1KeyL2N1, attribute1ValueL2N1)
                            .setStartTsMillis(44)
                            .setEndTsMillis(46);

            SpanBuilder childL1N1 =
                    Span.builder(this.getClass(), spanL1N1)
                            .setAttribute(attribute1KeyL1N1, attribute1ValueL1N1)
                            .setStartTsMillis(43)
                            .setEndTsMillis(48)
                            .addChild(childLeveL2N1);

            SpanBuilder childL1N2 =
                    Span.builder(this.getClass(), spanL1N2)
                            .setAttribute(attribute1KeyL1N2, attribute1ValueL1N2)
                            .setStartTsMillis(44)
                            .setEndTsMillis(46);

            SpanBuilder rootSpan =
                    Span.builder(this.getClass(), spanRoot)
                            .setAttribute(attribute1KeyRoot, attribute1ValueRoot)
                            .setAttribute(attribute2KeyRoot, attribute2ValueRoot)
                            .setStartTsMillis(42)
                            .setEndTsMillis(64)
                            .addChildren(Arrays.asList(childL1N1, childL1N2));

            reporter.notifyOfAddedSpan(rootSpan.build());
        } finally {
            reporter.close();
        }

        eventuallyConsumeJson(
                (json) -> {
                    JsonNode scopeSpans = json.findPath("resourceSpans").findPath("scopeSpans");
                    assertThat(scopeSpans.findPath("scope").findPath("name").asText())
                            .isEqualTo(scope);
                    JsonNode spans = scopeSpans.findPath("spans");

                    Map<String, ActualSpan> actualSpanSummaries = convertToSummaries(spans);

                    assertThat(actualSpanSummaries.keySet())
                            .containsExactlyInAnyOrder(spanRoot, spanL1N1, spanL1N2, spanL2N1);

                    ActualSpan root = actualSpanSummaries.get(spanRoot);
                    ActualSpan l1n1 = actualSpanSummaries.get(spanL1N1);
                    ActualSpan l1n2 = actualSpanSummaries.get(spanL1N2);
                    ActualSpan l2n1 = actualSpanSummaries.get(spanL2N1);

                    assertThat(root.parentSpanId).isEmpty();
                    assertThat(root.attributes)
                            .containsEntry(attribute1KeyRoot, attribute1ValueRoot);
                    assertThat(root.attributes)
                            .containsEntry(
                                    VariableNameUtil.getVariableName(attribute2KeyRoot),
                                    attribute2ValueRoot);
                    assertThat(l1n1.attributes)
                            .containsEntry(attribute1KeyL1N1, attribute1ValueL1N1);
                    assertThat(l1n2.attributes)
                            .containsEntry(attribute1KeyL1N2, attribute1ValueL1N2);
                    assertThat(l2n1.attributes)
                            .containsEntry(attribute1KeyL2N1, attribute1ValueL2N1);

                    assertThat(root.traceId).isEqualTo(l1n1.traceId);
                    assertThat(root.traceId).isEqualTo(l1n2.traceId);
                    assertThat(root.traceId).isEqualTo(l2n1.traceId);
                    assertThat(root.spanId).isNotEmpty();
                    assertThat(root.spanId).isEqualTo(l1n1.parentSpanId);
                    assertThat(root.spanId).isEqualTo(l1n2.parentSpanId);

                    assertThat(root.children).containsExactlyInAnyOrder(l1n1, l1n2);
                    assertThat(l1n1.children).containsExactlyInAnyOrder(l2n1);
                    assertThat(l1n2.children).isEmpty();
                    assertThat(l2n1.children).isEmpty();
                });
    }

    private Map<String, ActualSpan> convertToSummaries(JsonNode spans) {
        Map<String, ActualSpan> spanIdToSpan = new HashMap<>();
        for (int i = 0; spans.get(i) != null; i++) {
            ActualSpan actualSpan = convertToActualSpan(spans.get(i));
            spanIdToSpan.put(actualSpan.spanId, actualSpan);
        }

        Map<String, ActualSpan> nameToSpan = new HashMap<>();

        spanIdToSpan.forEach(
                (spanId, actualSpan) -> {
                    if (!actualSpan.parentSpanId.isEmpty()) {
                        ActualSpan parentSpan = spanIdToSpan.get(actualSpan.parentSpanId);
                        parentSpan.addChild(actualSpan);
                    }
                    nameToSpan.put(actualSpan.name, actualSpan);
                });

        return nameToSpan;
    }

    private ActualSpan convertToActualSpan(JsonNode span) {
        String name = span.findPath("name").asText();
        String traceId = span.findPath("traceId").asText();
        String spanId = span.findPath("spanId").asText();
        String parentSpanId = span.findPath("parentSpanId").asText();

        Map<String, String> attributeMap = new HashMap<>();
        JsonNode attributes = span.findPath("attributes");

        for (int j = 0; attributes.get(j) != null; j++) {
            JsonNode attribute = attributes.get(j);
            String key = attribute.get("key").asText();
            String value = attribute.at("/value/stringValue").asText();
            attributeMap.put(key, value);
        }
        return new ActualSpan(traceId, spanId, name, parentSpanId, attributeMap);
    }

    private static class ActualSpan {
        final String traceId;
        final String spanId;
        final String name;
        final String parentSpanId;
        final Map<String, String> attributes;
        final List<ActualSpan> children = new ArrayList<>();

        public ActualSpan(
                String traceId,
                String spanId,
                String name,
                String parentSpanId,
                Map<String, String> attributes) {
            this.traceId = traceId;
            this.spanId = spanId;
            this.name = name;
            this.parentSpanId = parentSpanId;
            this.attributes = attributes;
        }

        public void addChild(ActualSpan child) {
            this.children.add(child);
        }
    }
}
