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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.ConnectException;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Asserts the paths that serve metrics and what the reporter answers for a given {@code Accept}
 * header. Neither is covered today, and a real Prometheus server asks for OpenMetrics.
 */
class PrometheusHttpSurfaceTest {

    private static final String LOGICAL_SCOPE = "logical_scope";
    private static final String TEXT_PLAIN_0_0_4 = "text/plain; version=0.0.4; charset=utf-8";

    /**
     * Verbatim from a Prometheus 3.11.2 server scraping a logging endpoint. This is what production
     * sends, and it is the row that matters.
     */
    private static final String PROMETHEUS_3_ACCEPT =
            "application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.6,"
                    + "application/openmetrics-text;version=0.0.1;q=0.5,"
                    + "text/plain;version=1.0.0;escaping=allow-utf-8;q=0.4,"
                    + "text/plain;version=0.0.4;q=0.3,*/*;q=0.2";

    private PrometheusReporter reporter;

    @BeforeEach
    void setUp() {
        reporter = TestUtils.reporterOnFreePort();
        final MetricGroup group =
                TestUtils.createTestMetricGroup(LOGICAL_SCOPE, Collections.emptyMap());
        final Counter counter = new SimpleCounter();
        counter.inc(7);
        reporter.notifyOfAddedMetric(counter, "someCounter", group);
        reporter.notifyOfAddedMetric(new CoherentTestHistogram(), "someHistogram", group);
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
    }

    /**
     * Metrics are served on the root as well as on {@code /metrics}. A stock {@code prometheus.yml}
     * defaults to {@code /metrics}, so a client that stopped serving the root would go unnoticed.
     */
    @Test
    void metricsAreServedOnTheRootAndOnTheMetricsPath() throws Exception {
        for (String path : new String[] {"/", "/metrics"}) {
            assertThat(TestUtils.request(reporter.getPort(), path, null).statusCode())
                    .as("status of GET %s", path)
                    .isEqualTo(200);
            assertThat(TestUtils.request(reporter.getPort(), path, null).body())
                    .as("families served on GET %s", path)
                    .contains("# TYPE " + scoped("someCounter") + " gauge")
                    .contains("# TYPE " + scoped("someHistogram") + " summary");
        }
    }

    static Stream<Arguments> acceptHeaders() {
        return Stream.of(
                Arguments.of("no Accept header", null, TEXT_PLAIN_0_0_4),
                Arguments.of("*/*", "*/*", TEXT_PLAIN_0_0_4),
                Arguments.of("legacy text", "text/plain;version=0.0.4", TEXT_PLAIN_0_0_4),
                Arguments.of(
                        "OpenMetrics",
                        "application/openmetrics-text;version=1.0.0",
                        TEXT_PLAIN_0_0_4),
                Arguments.of("Prometheus 3.11.2", PROMETHEUS_3_ACCEPT, TEXT_PLAIN_0_0_4));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptHeaders")
    void contentTypeForAcceptHeader(String name, String accept, String expectedContentType)
            throws Exception {
        assertThat(
                        TestUtils.scrape(reporter.getPort(), accept)
                                .headers()
                                .firstValue("content-type"))
                .hasValue(expectedContentType);
    }

    /** There is no dedicated health endpoint: an unmatched path answers with the metrics body. */
    @Test
    void healthPathServesTheMetricsBody() throws Exception {
        assertThat(TestUtils.request(reporter.getPort(), "/-/healthy", null).body())
                .contains(scoped("someCounter"));
    }

    @Test
    void sampleNameFilterIsHonoured() throws Exception {
        final String filtered =
                TestUtils.request(
                                reporter.getPort(),
                                "/metrics?name%5B%5D=" + scoped("someCounter"),
                                null)
                        .body();

        assertThat(filtered)
                .contains(scoped("someCounter"))
                .doesNotContain(scoped("someHistogram"));
    }

    @Test
    void closingTheReporterReleasesThePort() throws Exception {
        final int port = reporter.getPort();
        reporter.close();
        reporter = null;

        assertThatThrownBy(() -> TestUtils.scrape(port, null)).isInstanceOf(ConnectException.class);
    }

    private static String scoped(String metricName) {
        return "flink_" + LOGICAL_SCOPE + "_" + metricName;
    }
}
