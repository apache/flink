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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.curator5.com.google.common.collect.Iterators;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
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

    private static final PortRangeProvider portRangeProvider = new PortRangeProvider();

    private MetricGroup metricGroup;
    private PrometheusReporter reporter;

    @BeforeEach
    void setupReporter() {
        reporter = new PrometheusReporter(portRangeProvider.next());

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
     * @throws UnirestException Might be thrown on HTTP problems.
     */
    @Test
    void counterIsReportedAsPrometheusGauge() throws UnirestException {
        Counter testCounter = new SimpleCounter();
        testCounter.inc(7);

        assertThatGaugeIsExported(testCounter, "testCounter", "7.0");
    }

    @Test
    void gaugeIsReportedAsPrometheusGauge() throws UnirestException {
        Gauge<Integer> testGauge = () -> 1;

        assertThatGaugeIsExported(testGauge, "testGauge", "1.0");
    }

    @Test
    void nullGaugeDoesNotBreakReporter() throws UnirestException {
        Gauge<Integer> testGauge = () -> null;

        assertThatGaugeIsExported(testGauge, "testGauge", "0.0");
    }

    @Test
    void meterRateIsReportedAsPrometheusGauge() throws UnirestException {
        Meter testMeter = new TestMeter();

        assertThatGaugeIsExported(testMeter, "testMeter", "5.0");
    }

    private void assertThatGaugeIsExported(Metric metric, String name, String expectedValue)
            throws UnirestException {
        assertThat(addMetricAndPollResponse(metric, name))
                .contains(createExpectedPollResponse(name, "", "gauge", expectedValue));
    }

    @Test
    void histogramIsReportedAsPrometheusSummary() throws UnirestException {
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
    }

    /**
     * Metrics with the same name are stored by the reporter in a shared data-structure. This test
     * ensures that a metric is unregistered from Prometheus even if other metrics with the same
     * name still exist.
     */
    @Test
    void metricIsRemovedWhileOtherMetricsWithSameNameExist() throws UnirestException {
        String metricName = "metric";

        Counter metric1 = new SimpleCounter();
        Counter metric2 = new SimpleCounter();

        final Map<String, String> variables2 = new HashMap<>(metricGroup.getAllVariables());
        final Map.Entry<String, String> entryToModify = variables2.entrySet().iterator().next();
        final String labelValueThatShouldBeRemoved = entryToModify.getValue();
        variables2.put(entryToModify.getKey(), "some_value");
        final MetricGroup metricGroup2 = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, variables2);

        reporter.notifyOfAddedMetric(metric1, metricName, metricGroup);
        reporter.notifyOfAddedMetric(metric2, metricName, metricGroup2);
        reporter.notifyOfRemovedMetric(metric1, metricName, metricGroup);

        String response = pollMetrics(reporter.getPort()).getBody();

        assertThat(response).contains("some_value").doesNotContain(labelValueThatShouldBeRemoved);
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
        assertThatThrownBy(
                        () ->
                                new PrometheusReporter(
                                        Collections.singleton(reporter.getPort()).iterator()))
                .isInstanceOf(Exception.class);
    }

    @Test
    void canStartTwoReportersWhenUsingPortRange() {
        final Iterator<Integer> portRange =
                Iterators.concat(
                        Iterators.singletonIterator(reporter.getPort()), portRangeProvider.next());
        new PrometheusReporter(portRange).close();
    }

    private String addMetricAndPollResponse(Metric metric, String metricName)
            throws UnirestException {
        reporter.notifyOfAddedMetric(metric, metricName, metricGroup);
        return pollMetrics(reporter.getPort()).getBody();
    }

    static HttpResponse<String> pollMetrics(int port) throws UnirestException {
        return Unirest.get("http://localhost:" + port + "/metrics").asString();
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
    private static class PortRangeProvider implements Iterator<Iterator<Integer>> {

        private int base = 9000;

        @Override
        public boolean hasNext() {
            return base < 14000; // arbitrary limit that should be sufficient for test purposes
        }

        /**
         * Returns the next port range containing exactly 100 ports.
         *
         * @return next port range
         */
        public Iterator<Integer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int lowEnd = base;
            int highEnd = base + 99;
            base += 100;
            return NetUtils.getPortRangeFromString(lowEnd + "-" + highEnd);
        }
    }
}
