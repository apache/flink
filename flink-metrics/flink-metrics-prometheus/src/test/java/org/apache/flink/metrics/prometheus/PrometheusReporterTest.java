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

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.ReporterScopedSettings;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.flink.metrics.prometheus.PrometheusReporterFactory.ARG_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic test for {@link PrometheusReporter}. */
class PrometheusReporterTest {

    private static final String HOST_NAME = "hostname";
    private static final String TASK_MANAGER = "tm";

    private static final String HELP_PREFIX = "# HELP ";
    private static final String TYPE_PREFIX = "# TYPE ";
    private static final String DIMENSIONS =
            "host=\"" + HOST_NAME + "\",tm_id=\"" + TASK_MANAGER + "\"";
    private static final String DEFAULT_LABELS = "{" + DIMENSIONS + ",}";
    private static final String SCOPE_PREFIX = "flink_taskmanager_";

    private static final PrometheusReporterFactory prometheusReporterFactory =
            new PrometheusReporterFactory();
    private static final PortRangeProvider portRangeProvider = new PortRangeProvider();

    private MetricRegistryImpl registry;
    private FrontMetricGroup<TaskManagerMetricGroup> metricGroup;
    private PrometheusReporter reporter;

    @BeforeEach
    void setupReporter() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                createReporterSetup("test1", portRangeProvider.next())));
        metricGroup =
                new FrontMetricGroup<>(
                        createReporterScopedSettings(),
                        TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                registry, HOST_NAME, new ResourceID(TASK_MANAGER)));
        reporter = (PrometheusReporter) registry.getReporters().get(0);
    }

    @AfterEach
    void shutdownRegistry() throws Exception {
        if (registry != null) {
            registry.shutdown().get();
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
        final String prometheusName = SCOPE_PREFIX + name;
        assertThat(addMetricAndPollResponse(metric, name))
                .contains(
                        HELP_PREFIX
                                + prometheusName
                                + " "
                                + name
                                + " (scope: taskmanager)\n"
                                + TYPE_PREFIX
                                + prometheusName
                                + " gauge"
                                + "\n"
                                + prometheusName
                                + DEFAULT_LABELS
                                + " "
                                + expectedValue
                                + "\n");
    }

    @Test
    void histogramIsReportedAsPrometheusSummary() throws UnirestException {
        Histogram testHistogram = new TestHistogram();

        String histogramName = "testHistogram";
        String summaryName = SCOPE_PREFIX + histogramName;

        String response = addMetricAndPollResponse(testHistogram, histogramName);
        assertThat(response)
                .contains(
                        HELP_PREFIX
                                + summaryName
                                + " "
                                + histogramName
                                + " (scope: taskmanager)\n"
                                + TYPE_PREFIX
                                + summaryName
                                + " summary"
                                + "\n"
                                + summaryName
                                + "_count"
                                + DEFAULT_LABELS
                                + " 1.0"
                                + "\n");
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

    @Test
    void metricIsRemovedWhenCollectorIsNotUnregisteredYet() throws UnirestException {
        JobManagerMetricGroup jmMetricGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, HOST_NAME);

        String metricName = "metric";

        Counter metric1 = new SimpleCounter();
        FrontMetricGroup<JobManagerJobMetricGroup> metricGroup1 =
                new FrontMetricGroup<>(
                        createReporterScopedSettings(),
                        jmMetricGroup.addJob(JobID.generate(), "job_1"));
        reporter.notifyOfAddedMetric(metric1, metricName, metricGroup1);

        Counter metric2 = new SimpleCounter();
        FrontMetricGroup<JobManagerJobMetricGroup> metricGroup2 =
                new FrontMetricGroup<>(
                        createReporterScopedSettings(),
                        jmMetricGroup.addJob(JobID.generate(), "job_2"));
        reporter.notifyOfAddedMetric(metric2, metricName, metricGroup2);

        reporter.notifyOfRemovedMetric(metric1, metricName, metricGroup1);

        String response = pollMetrics(reporter.getPort()).getBody();

        assertThat(response).doesNotContain("job_1");
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
    void addingUnknownMetricTypeDoesNotThrowException() {
        class SomeMetricType implements Metric {}

        reporter.notifyOfAddedMetric(new SomeMetricType(), "name", metricGroup);
    }

    @Test
    void cannotStartTwoReportersOnSamePort() throws Exception {
        ReporterSetup setup1 = createReporterSetup("test1", portRangeProvider.next());

        int usedPort = ((PrometheusReporter) setup1.getReporter()).getPort();

        try {
            assertThatThrownBy(() -> createReporterSetup("test2", String.valueOf(usedPort)))
                    .isInstanceOf(Exception.class);
        } finally {
            setup1.getReporter().close();
        }
    }

    @Test
    void canStartTwoReportersWhenUsingPortRange() throws Exception {
        String portRange = portRangeProvider.next();

        ReporterSetup setup1 = createReporterSetup("test1", portRange);
        ReporterSetup setup2 = createReporterSetup("test2", portRange);

        setup1.getReporter().close();
        setup2.getReporter().close();
    }

    private String addMetricAndPollResponse(Metric metric, String metricName)
            throws UnirestException {
        reporter.notifyOfAddedMetric(metric, metricName, metricGroup);
        return pollMetrics(reporter.getPort()).getBody();
    }

    static HttpResponse<String> pollMetrics(int port) throws UnirestException {
        return Unirest.get("http://localhost:" + port + "/metrics").asString();
    }

    static ReporterSetup createReporterSetup(String reporterName, String portString) {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_PORT, portString);

        PrometheusReporter metricReporter =
                prometheusReporterFactory.createMetricReporter(metricConfig);

        return ReporterSetup.forReporter(reporterName, metricConfig, metricReporter);
    }

    /** Utility class providing distinct port ranges. */
    private static class PortRangeProvider implements Iterator<String> {

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
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int lowEnd = base;
            int highEnd = base + 99;
            base += 100;
            return String.valueOf(lowEnd) + "-" + String.valueOf(highEnd);
        }
    }

    private static ReporterScopedSettings createReporterScopedSettings() {
        return new ReporterScopedSettings(0, ',', Collections.emptySet(), Collections.emptyMap());
    }
}
