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
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.ReporterScopedSettings;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.util.TestLogger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.flink.metrics.prometheus.PrometheusReporterFactory.ARG_PORT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/** Basic test for {@link PrometheusReporter}. */
public class PrometheusReporterTest extends TestLogger {

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

    @Rule public ExpectedException thrown = ExpectedException.none();

    private MetricRegistryImpl registry;
    private FrontMetricGroup<TaskManagerMetricGroup> metricGroup;
    private PrometheusReporter reporter;

    @Before
    public void setupReporter() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryConfiguration.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                createReporterSetup("test1", portRangeProvider.next())));
        metricGroup =
                new FrontMetricGroup<>(
                        createReporterScopedSettings(),
                        new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER));
        reporter = (PrometheusReporter) registry.getReporters().get(0);
    }

    @After
    public void shutdownRegistry() throws Exception {
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
    public void counterIsReportedAsPrometheusGauge() throws UnirestException {
        Counter testCounter = new SimpleCounter();
        testCounter.inc(7);

        assertThatGaugeIsExported(testCounter, "testCounter", "7.0");
    }

    @Test
    public void gaugeIsReportedAsPrometheusGauge() throws UnirestException {
        Gauge<Integer> testGauge =
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return 1;
                    }
                };

        assertThatGaugeIsExported(testGauge, "testGauge", "1.0");
    }

    @Test
    public void nullGaugeDoesNotBreakReporter() throws UnirestException {
        Gauge<Integer> testGauge =
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return null;
                    }
                };

        assertThatGaugeIsExported(testGauge, "testGauge", "0.0");
    }

    @Test
    public void meterRateIsReportedAsPrometheusGauge() throws UnirestException {
        Meter testMeter = new TestMeter();

        assertThatGaugeIsExported(testMeter, "testMeter", "5.0");
    }

    private void assertThatGaugeIsExported(Metric metric, String name, String expectedValue)
            throws UnirestException {
        final String prometheusName = SCOPE_PREFIX + name;
        assertThat(
                addMetricAndPollResponse(metric, name),
                containsString(
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
                                + "\n"));
    }

    @Test
    public void histogramIsReportedAsPrometheusSummary() throws UnirestException {
        Histogram testHistogram = new TestHistogram();

        String histogramName = "testHistogram";
        String summaryName = SCOPE_PREFIX + histogramName;

        String response = addMetricAndPollResponse(testHistogram, histogramName);
        assertThat(
                response,
                containsString(
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
                                + "\n"));
        for (String quantile : Arrays.asList("0.5", "0.75", "0.95", "0.98", "0.99", "0.999")) {
            assertThat(
                    response,
                    containsString(
                            summaryName
                                    + "{"
                                    + DIMENSIONS
                                    + ",quantile=\""
                                    + quantile
                                    + "\",} "
                                    + quantile
                                    + "\n"));
        }
    }

    @Test
    public void metricIsRemovedWhenCollectorIsNotUnregisteredYet() throws UnirestException {
        TaskManagerMetricGroup tmMetricGroup =
                new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER);

        String metricName = "metric";

        Counter metric1 = new SimpleCounter();
        FrontMetricGroup<TaskManagerJobMetricGroup> metricGroup1 =
                new FrontMetricGroup<>(
                        createReporterScopedSettings(),
                        new TaskManagerJobMetricGroup(
                                registry, tmMetricGroup, JobID.generate(), "job_1"));
        reporter.notifyOfAddedMetric(metric1, metricName, metricGroup1);

        Counter metric2 = new SimpleCounter();
        FrontMetricGroup<TaskManagerJobMetricGroup> metricGroup2 =
                new FrontMetricGroup<>(
                        createReporterScopedSettings(),
                        new TaskManagerJobMetricGroup(
                                registry, tmMetricGroup, JobID.generate(), "job_2"));
        reporter.notifyOfAddedMetric(metric2, metricName, metricGroup2);

        reporter.notifyOfRemovedMetric(metric1, metricName, metricGroup1);

        String response = pollMetrics(reporter.getPort()).getBody();

        assertThat(response, not(containsString("job_1")));
    }

    @Test
    public void invalidCharactersAreReplacedWithUnderscore() {
        assertThat(PrometheusReporter.replaceInvalidChars(""), equalTo(""));
        assertThat(PrometheusReporter.replaceInvalidChars("abc"), equalTo("abc"));
        assertThat(PrometheusReporter.replaceInvalidChars("abc\""), equalTo("abc_"));
        assertThat(PrometheusReporter.replaceInvalidChars("\"abc"), equalTo("_abc"));
        assertThat(PrometheusReporter.replaceInvalidChars("\"abc\""), equalTo("_abc_"));
        assertThat(PrometheusReporter.replaceInvalidChars("\"a\"b\"c\""), equalTo("_a_b_c_"));
        assertThat(PrometheusReporter.replaceInvalidChars("\"\"\"\""), equalTo("____"));
        assertThat(PrometheusReporter.replaceInvalidChars("    "), equalTo("____"));
        assertThat(PrometheusReporter.replaceInvalidChars("\"ab ;(c)'"), equalTo("_ab___c__"));
        assertThat(PrometheusReporter.replaceInvalidChars("a b c"), equalTo("a_b_c"));
        assertThat(PrometheusReporter.replaceInvalidChars("a b c "), equalTo("a_b_c_"));
        assertThat(PrometheusReporter.replaceInvalidChars("a;b'c*"), equalTo("a_b_c_"));
        assertThat(
                PrometheusReporter.replaceInvalidChars("a,=;:?'b,=;:?'c"),
                equalTo("a___:__b___:__c"));
    }

    @Test
    public void doubleGaugeIsConvertedCorrectly() {
        assertThat(
                reporter.gaugeFrom(
                                new Gauge<Double>() {
                                    @Override
                                    public Double getValue() {
                                        return 3.14;
                                    }
                                })
                        .get(),
                equalTo(3.14));
    }

    @Test
    public void shortGaugeIsConvertedCorrectly() {
        assertThat(
                reporter.gaugeFrom(
                                new Gauge<Short>() {
                                    @Override
                                    public Short getValue() {
                                        return 13;
                                    }
                                })
                        .get(),
                equalTo(13.));
    }

    @Test
    public void booleanGaugeIsConvertedCorrectly() {
        assertThat(
                reporter.gaugeFrom(
                                new Gauge<Boolean>() {
                                    @Override
                                    public Boolean getValue() {
                                        return true;
                                    }
                                })
                        .get(),
                equalTo(1.));
    }

    /** Prometheus only supports numbers, so report non-numeric gauges as 0. */
    @Test
    public void stringGaugeCannotBeConverted() {
        assertThat(
                reporter.gaugeFrom(
                                new Gauge<String>() {
                                    @Override
                                    public String getValue() {
                                        return "I am not a number";
                                    }
                                })
                        .get(),
                equalTo(0.));
    }

    @Test
    public void registeringSameMetricTwiceDoesNotThrowException() {
        Counter counter = new SimpleCounter();
        counter.inc();
        String counterName = "testCounter";

        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);
        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);
    }

    @Test
    public void addingUnknownMetricTypeDoesNotThrowException() {
        class SomeMetricType implements Metric {}

        reporter.notifyOfAddedMetric(new SomeMetricType(), "name", metricGroup);
    }

    @Test
    public void cannotStartTwoReportersOnSamePort() throws Exception {
        ReporterSetup setup1 = createReporterSetup("test1", portRangeProvider.next());

        int usedPort = ((PrometheusReporter) setup1.getReporter()).getPort();

        try {
            createReporterSetup("test2", String.valueOf(usedPort));
            Assert.fail("Should've failed since port is unavailable.");
        } catch (Exception e) {
            // expected
        } finally {
            setup1.getReporter().close();
        }
    }

    @Test
    public void canStartTwoReportersWhenUsingPortRange() throws Exception {
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

    @After
    public void closeReporterAndShutdownRegistry() throws Exception {
        registry.shutdown().get();
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
        return new ReporterScopedSettings(0, ',', Collections.emptySet());
    }
}
