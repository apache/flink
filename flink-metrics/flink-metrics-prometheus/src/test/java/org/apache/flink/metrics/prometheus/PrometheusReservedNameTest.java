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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Records what happens when a user metric collides with a name reserved for a summary.
 *
 * <p>Which names a summary occupies is a property of the client. When that set grows, a user metric
 * stops being exported and we only log a warning. The suffixes are the format's reserved set, not
 * the set the client checks.
 */
class PrometheusReservedNameTest {

    private static final String LOGICAL_SCOPE = "logical_scope";
    private static final String BASE = "hazard";

    private static final List<String> RESERVED_SUFFIXES =
            Arrays.asList(
                    "_count", "_sum", "_created", "_bucket", "_total", "_info", "_gcount", "_gsum");

    /** Losing a metric is only acceptable because we say so in the log. */
    @RegisterExtension
    private final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(PrometheusReporter.class, Level.WARN);

    private PrometheusReporter reporter;
    private MetricGroup metricGroup;

    enum Disposition {
        BOTH_EXPORTED,
        SECOND_LOST
    }

    enum Order {
        GAUGE_FIRST,
        SUMMARY_FIRST
    }

    @BeforeEach
    void setUp() {
        reporter = TestUtils.reporterOnFreePort();
        metricGroup = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, Collections.emptyMap());
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
    }

    static Stream<Arguments> collisions() {
        final List<Arguments> rows = new ArrayList<>();
        for (String suffix : RESERVED_SUFFIXES) {
            for (Order order : Order.values()) {
                // Only the names a summary actually occupies can collide. On the client Flink
                // bundles today that is the count and the sum; a client that also reserves the
                // creation timestamp flips the _created rows.
                final Disposition expected =
                        suffix.equals("_count") || suffix.equals("_sum")
                                ? Disposition.SECOND_LOST
                                : Disposition.BOTH_EXPORTED;
                rows.add(Arguments.of(suffix, order, expected));
            }
        }
        return rows.stream();
    }

    @ParameterizedTest(name = "gauge named <name>{0} and summary, {1}")
    @MethodSource("collisions")
    void reservedNameCollisionDisposition(String suffix, Order order, Disposition expected)
            throws Exception {
        final String gaugeName = BASE + suffix;

        if (order == Order.GAUGE_FIRST) {
            register(counter(), gaugeName);
            register(new CoherentTestHistogram(), BASE);
        } else {
            register(new CoherentTestHistogram(), BASE);
            register(counter(), gaugeName);
        }

        final String body = TestUtils.scrapeBody(reporter);
        final boolean gaugeExported = hasFamilyOfType(body, scoped(gaugeName), "gauge");
        final boolean summaryExported = hasFamilyOfType(body, scoped(BASE), "summary");

        final Disposition actual =
                gaugeExported && summaryExported
                        ? Disposition.BOTH_EXPORTED
                        : Disposition.SECOND_LOST;
        assertThat(actual)
                .as(
                        "a gauge named <name>%s registered %s alongside a summary named <name>",
                        suffix, order)
                .isEqualTo(expected);

        if (expected == Disposition.SECOND_LOST) {
            // Whichever was registered second is the one that went missing.
            assertThat(order == Order.GAUGE_FIRST ? summaryExported : gaugeExported).isFalse();
            assertThat(order == Order.GAUGE_FIRST ? gaugeExported : summaryExported).isTrue();
            assertThat(loggerExtension.getMessages())
                    .anyMatch(
                            message -> message.contains("There was a problem registering metric"));
        } else {
            assertThat(loggerExtension.getMessages()).isEmpty();
        }
    }

    private void register(Metric metric, String metricName) {
        // The registry swallows whatever a reporter throws, so a user sees a log line and a missing
        // metric either way. The test has to treat both paths as the same outcome.
        TestUtils.registerLikeRegistry(reporter, metric, metricName, metricGroup);
    }

    private static boolean hasFamilyOfType(String body, String familyName, String type) {
        return body.contains("# TYPE " + familyName + " " + type + "\n");
    }

    private static Counter counter() {
        final Counter counter = new SimpleCounter();
        counter.inc(5);
        return counter;
    }

    private static String scoped(String metricName) {
        return "flink_" + LOGICAL_SCOPE + "_" + metricName;
    }
}
