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

import com.esotericsoftware.minlog.Log;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Tests for {@link OpenTelemetryMetricReporter}. */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetryTestBase {
    public static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryTestBase.class);

    private static final Duration TIME_OUT = Duration.ofMinutes(2);

    @RegisterExtension
    @Order(1)
    private static final AllCallbackWrapper<TestContainerExtension<OtelTestContainer>>
            OTEL_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(() -> new OtelTestContainer()));

    @BeforeEach
    public void setup() {
        Slf4jLevelLogConsumer logConsumer = new Slf4jLevelLogConsumer(LOG);
        OTEL_EXTENSION.getCustomExtension().getTestContainer().followOutput(logConsumer);
    }

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
                    getOtelContainer()
                            .copyFileFromContainer(
                                    getOtelContainer().getOutputLogPath().toString(),
                                    inputStream -> {
                                        List<String> lines = new ArrayList<>();
                                        BufferedReader input =
                                                new BufferedReader(
                                                        new InputStreamReader(inputStream));
                                        String last = "";
                                        String line;

                                        while ((line = input.readLine()) != null) {
                                            lines.add(line);
                                            last = line;
                                        }

                                        ObjectMapper mapper = new ObjectMapper();
                                        JsonNode json = mapper.readValue(last, JsonNode.class);
                                        try {
                                            jsonConsumer.accept(json);
                                        } catch (Throwable t) {
                                            throw new ConsumeDataLogException(t, lines);
                                        }
                                        return null;
                                    });
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
                    if (e instanceof ConsumeDataLogException) {
                        LOG.error("Failure while the following data log:");
                        ((ConsumeDataLogException) e).getDataLog().forEach(Log::error);
                    }
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

    private static class ConsumeDataLogException extends Exception {
        private final List<String> dataLog;

        public ConsumeDataLogException(Throwable cause, List<String> dataLog) {
            super(cause);
            this.dataLog = dataLog;
        }

        public List<String> getDataLog() {
            return dataLog;
        }
    }

    /**
     * Similar to {@link Slf4jLogConsumer} but parses output lines and tries to log them with
     * appropriate levels.
     */
    private static class Slf4jLevelLogConsumer extends BaseConsumer<Slf4jLevelLogConsumer> {

        private final Logger logger;

        public Slf4jLevelLogConsumer(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void accept(OutputFrame outputFrame) {
            final OutputFrame.OutputType outputType = outputFrame.getType();
            final String utf8String = outputFrame.getUtf8StringWithoutLineEnding();

            String lowerCase = utf8String.toLowerCase();
            if (lowerCase.contains("error") || lowerCase.contains("exception")) {
                logger.error("{}: {}", outputType, utf8String);
            } else if (lowerCase.contains("warn") || lowerCase.contains("fail")) {
                logger.warn("{}: {}", outputType, utf8String);
            } else {
                logger.info("{}: {}", outputType, utf8String);
            }
        }
    }
}
