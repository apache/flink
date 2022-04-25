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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.util.NetUtils;

import com.mashape.unirest.http.exceptions.UnirestException;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.flink.metrics.prometheus.PrometheusReporterTest.pollMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PrometheusReporter} that registers several instances of the same metric for
 * different subtasks.
 */
class PrometheusReporterTaskScopeTest {
    private static final String[] LABEL_NAMES = {"label1", "label2"};
    private static final String[][] LABEL_VALUES =
            new String[][] {{"value1_1", "value1_2"}, {"value2_1", "value2_2"}};
    private static final String LOGICAL_SCOPE = "logical_scope";
    private static final String METRIC_NAME = "myMetric";

    private final MetricGroup metricGroup1 =
            TestUtils.createTestMetricGroup(
                    LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, LABEL_VALUES[0]));
    private final MetricGroup metricGroup2 =
            TestUtils.createTestMetricGroup(
                    LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, LABEL_VALUES[1]));

    private PrometheusReporter reporter;

    @BeforeEach
    void setupReporter() {
        reporter = new PrometheusReporter(NetUtils.getPortRangeFromString("9400-9500"));
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
    }

    @Test
    void countersCanBeAddedSeveralTimesIfTheyDifferInLabels() {
        Counter counter1 = new SimpleCounter();
        counter1.inc(1);
        Counter counter2 = new SimpleCounter();
        counter2.inc(2);

        reporter.notifyOfAddedMetric(counter1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(counter2, METRIC_NAME, metricGroup2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[0]))
                .isEqualTo(1.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[1]))
                .isEqualTo(2.);
    }

    @Test
    void gaugesCanBeAddedSeveralTimesIfTheyDifferInLabels() {
        Gauge<Integer> gauge1 = () -> 3;
        Gauge<Integer> gauge2 = () -> 4;

        reporter.notifyOfAddedMetric(gauge1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(gauge2, METRIC_NAME, metricGroup2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[0]))
                .isEqualTo(3.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[1]))
                .isEqualTo(4.);
    }

    @Test
    void metersCanBeAddedSeveralTimesIfTheyDifferInLabels() {
        Meter meter = new TestMeter();

        reporter.notifyOfAddedMetric(meter, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(meter, METRIC_NAME, metricGroup2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[0]))
                .isEqualTo(5.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[1]))
                .isEqualTo(5.);
    }

    @Test
    void histogramsCanBeAddedSeveralTimesIfTheyDifferInLabels() throws UnirestException {
        Histogram histogram = new TestHistogram();

        reporter.notifyOfAddedMetric(histogram, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(histogram, METRIC_NAME, metricGroup2);

        final String exportedMetrics = pollMetrics(reporter.getPort()).getBody();
        assertThat(exportedMetrics)
                .contains("label2=\"value1_2\",quantile=\"0.5\",} 0.5"); // histogram
        assertThat(exportedMetrics)
                .contains("label2=\"value2_2\",quantile=\"0.5\",} 0.5"); // histogram

        final String[] labelNamesWithQuantile = addToArray(LABEL_NAMES, "quantile");
        for (Double quantile : PrometheusReporter.HistogramSummaryProxy.QUANTILES) {
            assertThat(
                            CollectorRegistry.defaultRegistry.getSampleValue(
                                    getLogicalScope(METRIC_NAME),
                                    labelNamesWithQuantile,
                                    addToArray(LABEL_VALUES[0], "" + quantile)))
                    .isEqualTo(quantile);
            assertThat(
                            CollectorRegistry.defaultRegistry.getSampleValue(
                                    getLogicalScope(METRIC_NAME),
                                    labelNamesWithQuantile,
                                    addToArray(LABEL_VALUES[1], "" + quantile)))
                    .isEqualTo(quantile);
        }
    }

    @Test
    void removingSingleInstanceOfMetricDoesNotBreakOtherInstances() {
        Counter counter1 = new SimpleCounter();
        counter1.inc(1);
        Counter counter2 = new SimpleCounter();
        counter2.inc(2);

        reporter.notifyOfAddedMetric(counter1, METRIC_NAME, metricGroup1);
        reporter.notifyOfAddedMetric(counter2, METRIC_NAME, metricGroup2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[0]))
                .isEqualTo(1.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[1]))
                .isEqualTo(2.);

        reporter.notifyOfRemovedMetric(counter2, METRIC_NAME, metricGroup2);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[0]))
                .isEqualTo(1.);

        reporter.notifyOfRemovedMetric(counter1, METRIC_NAME, metricGroup1);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                getLogicalScope(METRIC_NAME), LABEL_NAMES, LABEL_VALUES[0]))
                .isNull();
    }

    private static String getLogicalScope(String metricName) {
        return PrometheusReporter.SCOPE_PREFIX
                + LOGICAL_SCOPE
                + PrometheusReporter.SCOPE_SEPARATOR
                + metricName;
    }

    private String[] addToArray(String[] array, String element) {
        final String[] labelNames = Arrays.copyOf(array, LABEL_NAMES.length + 1);
        labelNames[LABEL_NAMES.length] = element;
        return labelNames;
    }
}
