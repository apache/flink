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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.util.PortRange;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.ALLOW_LIST;
import static org.apache.flink.metrics.prometheus.PrometheusReporterFactory.ARG_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic test for {@link PrometheusReporter}. */
class PrometheusReporterTest {

    private static final String[] LABEL_NAMES = {"label1", "label2"};
    private static final String[] LABEL_VALUES = new String[] {"value1", "value2"};
    private static final String LOGICAL_SCOPE = "logical_scope";

    private static final String DIMENSIONS =
            String.format(
                    "%s=\"%s\",%s=\"%s\"",
                    LABEL_NAMES[0], LABEL_VALUES[0], LABEL_NAMES[1], LABEL_VALUES[1]);
    private static final String DEFAULT_LABELS = "{" + DIMENSIONS + ",}";
    private static final String SCOPE_PREFIX =
            PrometheusReporter.SCOPE_PREFIX + LOGICAL_SCOPE + PrometheusReporter.SCOPE_SEPARATOR;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    private static final PortRangeProvider portRangeProvider = new PortRangeProvider();

    private MetricGroup metricGroup;
    private PrometheusReporter reporter;

    @BeforeEach
    void setupReporter() {
        PortRange portRange = new PortRange(portRangeProvider.nextRange());
        reporter = new PrometheusReporter(portRange);

        metricGroup =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, LABEL_VALUES));
    }

    @AfterEach
    void teardown() throws Exception {
        if (reporter != null) {
            reporter.close();
        }
    }

    /**
     * {@link io.prometheus.client.Counter} may not decrease, so report {@link Counter} as {@link
     * io.prometheus.client.Gauge}.
     *
     * @throws IOException Might be thrown on I/O problems.
     * @throws InterruptedException Might be thrown if the thread is interrupted.
     */
    @Test
    void counterIsReportedAsPrometheusGauge() throws IOException, InterruptedException {
        Counter testCounter = new SimpleCounter();
        testCounter.inc(7);

        assertThatGaugeIsExported(testCounter, "testCounter", "7.0");
    }

    @Test
    void gaugeIsReportedAsPrometheusGauge() throws IOException, InterruptedException {
        Gauge<Integer> testGauge = () -> 1;

        assertThatGaugeIsExported(testGauge, "testGauge", "1.0");
    }

    @Test
    void nullGaugeDoesNotBreakReporter() throws IOException, InterruptedException {
        Gauge<Integer> testGauge = () -> null;

        assertThatGaugeIsExported(testGauge, "testGauge", "0.0");
    }

    @Test
    void meterRateIsReportedAsPrometheusGauge() throws IOException, InterruptedException {
        Meter testMeter = new TestMeter();

        assertThatGaugeIsExported(testMeter, "testMeter", "5.0");
    }

    private void assertThatGaugeIsExported(Metric metric, String name, String expectedValue)
            throws IOException, InterruptedException {
        assertThat(addMetricAndPollResponse(metric, name))
                .contains(createExpectedPollResponse(name, "", "gauge", expectedValue));
    }

    @Test
    void histogramIsReportedAsPrometheusSummary() throws IOException, InterruptedException {
        Histogram testHistogram = new TestHistogram();

        String histogramName = "testHistogram";
        String summaryName = SCOPE_PREFIX + histogramName;

        String response = addMetricAndPollResponse(testHistogram, histogramName);
        assertThat(response)
                .contains(createExpectedPollResponse(histogramName, "_count", "summary", "1.0"));
        for (String quantile : Arrays.asList("0.5", "0.75", "0.95", "0.98", "0.99", "0.999")) {
            assertThat(response)
                    .contains(
                            summaryName
                                    + "{"
                                    + DIMENSIONS
                                    + ",quantile=\""
                                    + quantile
                                    + "\",} "
                                    + quantile
                                    + "\n");
        }
        assertThat(response).contains(summaryName + "{" + DIMENSIONS + ",quantile=\"0.0\",} 7.0\n");
        assertThat(response).contains(summaryName + "{" + DIMENSIONS + ",quantile=\"1.0\",} 6.0\n");
    }

    /**
     * Metrics with the same name are stored by the reporter in a shared data-structure. This test
     * ensures that a metric is unregistered from Prometheus even if other metrics with the same
     * name still exist.
     */
    @Test
    void metricIsRemovedWhileOtherMetricsWithSameNameExist()
            throws IOException, InterruptedException {
        String metricName = "metric";

        Counter metric1 = new SimpleCounter();
        Counter metric2 = new SimpleCounter();

        // The variables are copied in iteration order: a metric reusing another's collector has to
        // carry the same label names in the same order, since the values are bound by position.
        final Map<String, String> variables2 = new LinkedHashMap<>(metricGroup.getAllVariables());
        final Map.Entry<String, String> entryToModify = variables2.entrySet().iterator().next();
        final String labelValueThatShouldBeRemoved = entryToModify.getValue();
        variables2.put(entryToModify.getKey(), "some_value");
        final MetricGroup metricGroup2 = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, variables2);

        reporter.notifyOfAddedMetric(metric1, metricName, metricGroup);
        reporter.notifyOfAddedMetric(metric2, metricName, metricGroup2);
        reporter.notifyOfRemovedMetric(metric1, metricName, metricGroup);

        String response = pollMetrics(reporter.getPort()).body();

        assertThat(response).contains("some_value").doesNotContain(labelValueThatShouldBeRemoved);
    }

    /**
     * Two variables that differ only in characters the filter replaces become one label name,
     * twice. Prometheus rejects such a body and abandons the whole scrape.
     */
    @Test
    void metricIsNotReportedWhenTwoVariablesSanitiseToTheSameLabelName()
            throws IOException, InterruptedException {
        final Map<String, String> colliding = new LinkedHashMap<>();
        colliding.put("<a.b>", "v1");
        colliding.put("<a-b>", "v2");

        reporter.notifyOfAddedMetric(
                new SimpleCounter(),
                "colliding",
                TestUtils.createTestMetricGroup(LOGICAL_SCOPE, colliding));

        final String response = pollMetrics(reporter.getPort()).body();

        assertThat(response).doesNotContain("a_b=\"v1\",a_b=\"v2\"");
        assertThat(response).doesNotContain(SCOPE_PREFIX + "colliding");
    }

    @Test
    void removingARefusedMetricDoesNotThrow() {
        final Map<String, String> colliding = new LinkedHashMap<>();
        colliding.put("<a.b>", "v1");
        colliding.put("<a-b>", "v2");
        final MetricGroup group = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, colliding);
        final Counter counter = new SimpleCounter();

        reporter.notifyOfAddedMetric(counter, "colliding", group);

        // The registry removes every metric it added, including the ones we refused.
        assertThatCode(() -> reporter.notifyOfRemovedMetric(counter, "colliding", group))
                .doesNotThrowAnyException();
    }

    /** One unreportable metric must not cost the rest of the process its metrics. */
    @Test
    void otherMetricsAreStillReportedAlongsideAnUnreportableOne()
            throws IOException, InterruptedException {
        final Map<String, String> colliding = new LinkedHashMap<>();
        colliding.put("<a.b>", "v1");
        colliding.put("<a-b>", "v2");

        reporter.notifyOfAddedMetric(
                new SimpleCounter(),
                "colliding",
                TestUtils.createTestMetricGroup(LOGICAL_SCOPE, colliding));
        final Counter healthy = new SimpleCounter();
        healthy.inc(3);
        reporter.notifyOfAddedMetric(healthy, "healthy", metricGroup);

        final String response = pollMetrics(reporter.getPort()).body();

        assertThat(response).contains(SCOPE_PREFIX + "healthy");
    }

    /**
     * Reporting a metric whose variables differ from the first one of that name would publish its
     * values under that one's label names.
     */
    @Test
    void metricIsNotReportedWhenItsVariablesDifferFromAnEarlierMetricOfTheSameName()
            throws IOException, InterruptedException {
        final Counter first = new SimpleCounter();
        first.inc(1);
        final Counter second = new SimpleCounter();
        second.inc(2);

        reporter.notifyOfAddedMetric(first, "m", groupWith("x", "1", "y", "2"));
        reporter.notifyOfAddedMetric(second, "m", groupWith("x", "1", "z", "9"));

        final String response = pollMetrics(reporter.getPort()).body();

        assertThat(response).contains("y=\"2\"").doesNotContain("y=\"9\"");
        assertThat(response).doesNotContain("z=");
    }

    /** The same name with the same variables is the case the shared collector exists for. */
    @Test
    void metricIsReportedWhenItsVariablesMatchAnEarlierMetricOfTheSameName()
            throws IOException, InterruptedException {
        final Counter first = new SimpleCounter();
        first.inc(1);
        final Counter second = new SimpleCounter();
        second.inc(2);

        reporter.notifyOfAddedMetric(first, "m", groupWith("x", "1", "y", "2"));
        reporter.notifyOfAddedMetric(second, "m", groupWith("x", "1", "y", "other"));

        final String response = pollMetrics(reporter.getPort()).body();

        assertThat(response).contains("y=\"2\"").contains("y=\"other\"");
    }

    private static MetricGroup groupWith(String... keysAndValues) {
        final Map<String, String> variables = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            variables.put("<" + keysAndValues[i] + ">", keysAndValues[i + 1]);
        }
        return TestUtils.createTestMetricGroup(LOGICAL_SCOPE, variables);
    }

    @Test
    void invalidCharactersAreReplacedWithUnderscore() {
        assertThat(PrometheusReporter.replaceInvalidChars("")).isEqualTo("");
        assertThat(PrometheusReporter.replaceInvalidChars("abc")).isEqualTo("abc");
        assertThat(PrometheusReporter.replaceInvalidChars("abc\"")).isEqualTo("abc_");
        assertThat(PrometheusReporter.replaceInvalidChars("\"abc")).isEqualTo("_abc");
        assertThat(PrometheusReporter.replaceInvalidChars("\"abc\"")).isEqualTo("_abc_");
        assertThat(PrometheusReporter.replaceInvalidChars("\"a\"b\"c\"")).isEqualTo("_a_b_c_");
        assertThat(PrometheusReporter.replaceInvalidChars("\"\"\"\"")).isEqualTo("____");
        assertThat(PrometheusReporter.replaceInvalidChars("    ")).isEqualTo("____");
        assertThat(PrometheusReporter.replaceInvalidChars("\"ab ;(c)'")).isEqualTo("_ab___c__");
        assertThat(PrometheusReporter.replaceInvalidChars("a b c")).isEqualTo("a_b_c");
        assertThat(PrometheusReporter.replaceInvalidChars("a b c ")).isEqualTo("a_b_c_");
        assertThat(PrometheusReporter.replaceInvalidChars("a;b'c*")).isEqualTo("a_b_c_");
        assertThat(PrometheusReporter.replaceInvalidChars("a,=;:?'b,=;:?'c"))
                .isEqualTo("a___:__b___:__c");
    }

    @Test
    void doubleGaugeIsConvertedCorrectly() {
        assertThat(reporter.gaugeFrom(() -> 3.14).get()).isEqualTo(3.14);
    }

    @Test
    void shortGaugeIsConvertedCorrectly() {
        assertThat(reporter.gaugeFrom(() -> 13).get()).isEqualTo(13.);
    }

    @Test
    void booleanGaugeIsConvertedCorrectly() {
        assertThat(reporter.gaugeFrom(() -> true).get()).isEqualTo(1.);
    }

    /** Prometheus only supports numbers, so report non-numeric gauges as 0. */
    @Test
    void stringGaugeCannotBeConverted() {
        assertThat(reporter.gaugeFrom(() -> "I am not a number").get()).isEqualTo(0.);
    }

    @Test
    void registeringSameMetricTwiceDoesNotThrowException() {
        Counter counter = new SimpleCounter();
        counter.inc();
        String counterName = "testCounter";

        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);
        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);
    }

    @Test
    void cannotStartTwoReportersOnSamePort() {
        PortRange portRange = new PortRange(reporter.getPort());
        assertThatThrownBy(() -> new PrometheusReporter(portRange))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching(
                        "^Could not start PrometheusReporter HTTP server on any configured port. Ports: \\d+(, \\d+)*");
    }

    @Test
    void canStartTwoReportersWhenUsingPortRange() {
        String ports = reporter.getPort() + ", " + portRangeProvider.nextRange();
        PortRange portRange = new PortRange(ports);
        new PrometheusReporter(portRange).close();
    }

    @Test
    public void testMetricFilter() throws Exception {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ALLOW_LIST.key(), LOGICAL_SCOPE + ".metricName1");
        metricConfig.setProperty(ARG_PORT, portRangeProvider.nextRange());

        PrometheusReporterFactory prometheusReporterFactory = new PrometheusReporterFactory();
        PrometheusReporter metricReporter =
                prometheusReporterFactory.createMetricReporter(metricConfig);
        metricReporter.open(metricConfig);

        final Map<String, String> variables = new HashMap<>(metricGroup.getAllVariables());
        final MetricGroup metricGroup2 = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, variables);

        Counter metric1 = new SimpleCounter();
        metric1.inc(7);
        Counter metric2 = new SimpleCounter();
        metric2.inc(2);
        metricReporter.notifyOfAddedMetric(metric1, "metricName1", metricGroup2);
        metricReporter.notifyOfAddedMetric(metric2, "metricName2", metricGroup2);

        String response = pollMetrics(metricReporter.getPort()).body();

        assertThat(response).contains("metricName1");
        assertThat(response).doesNotContain("metricName2");
    }

    private String addMetricAndPollResponse(Metric metric, String metricName)
            throws IOException, InterruptedException {
        reporter.notifyOfAddedMetric(metric, metricName, metricGroup);
        return pollMetrics(reporter.getPort()).body();
    }

    static HttpResponse<String> pollMetrics(int port) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/metrics"))
                        .GET()
                        .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String createExpectedPollResponse(
            String name, String nameSuffix, String type, String value) {
        final String scopedName = SCOPE_PREFIX + name;
        return ""
                + String.format("# HELP %s %s (scope: %s)\n", scopedName, name, LOGICAL_SCOPE)
                + String.format("# TYPE %s %s\n", scopedName, type)
                + String.format("%s%s%s %s\n", scopedName, nameSuffix, DEFAULT_LABELS, value);
    }

    /** Utility class providing distinct port ranges. */
    private static class PortRangeProvider {

        private int base = 9000;

        /**
         * Returns the next port range containing exactly 100 ports as string.
         *
         * @return next port range
         */
        public String nextRange() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int lowEnd = base;
            int highEnd = base + 99;
            base += 100;
            return lowEnd + "-" + highEnd;
        }

        private boolean hasNext() {
            return base < 14000; // arbitrary limit that should be sufficient for test purposes
        }
    }
}
