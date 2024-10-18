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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/** Tests for {@link OpenTelemetryMetricReporter}. */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetryTestBase {
    private static final Duration TIME_OUT = Duration.ofMinutes(2);

    @TempDir private static File tempDir;

    @RegisterExtension
    @Order(1)
    private static final AllCallbackWrapper<TestContainerExtension<OtelTestContainer>>
            OTEL_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(() -> new OtelTestContainer(tempDir)));

    public static OtelTestContainer getOtelContainer() {
        return OTEL_EXTENSION.getCustomExtension().getTestContainer();
    }

    public static MetricConfig createMetricConfig() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(
                OpenTelemetryReporterOptions.EXPORTER_ENDPOINT.key(),
                getOtelContainer().getGrpcEndpoint());
        return metricConfig;
    }

    public static void eventuallyConsumeJson(ThrowingConsumer<JsonNode, Exception> jsonConsumer)
            throws Exception {
        eventually(
                () -> {
                    // otel-collector dumps every report in a new line, so in order to re-use the
                    // same collector across multiple tests, let's read only the last line
                    BufferedReader input =
                            new BufferedReader(
                                    new FileReader(
                                            getOtelContainer().getOutputDataPath().toFile()));
                    String last = "";
                    String line;

                    while ((line = input.readLine()) != null) {
                        last = line;
                    }

                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode json = mapper.readValue(last, JsonNode.class);
                    jsonConsumer.accept(json);
                });
    }

    public static void eventually(ThrowingRunnable<Exception> runnable) throws Exception {
        eventually(Deadline.fromNow(TIME_OUT), runnable);
    }

    public static void eventually(Deadline deadline, ThrowingRunnable<Exception> runnable)
            throws Exception {
        Thread.sleep(10);
        while (true) {
            try {
                runnable.run();
                break;
            } catch (Throwable e) {
                if (deadline.isOverdue()) {
                    throw e;
                }
            }
            Thread.sleep(100);
        }
    }

    public static List<String> extractMetricNames(JsonNode json) {
        return json.findPath("resourceMetrics").findPath("scopeMetrics").findPath("metrics")
                .findValues("name").stream()
                .map(JsonNode::asText)
                .collect(Collectors.toList());
    }
}
