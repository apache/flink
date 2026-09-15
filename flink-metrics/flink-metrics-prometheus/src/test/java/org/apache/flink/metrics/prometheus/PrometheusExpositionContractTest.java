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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the data the reporter exposes: family names and types, labels and values. A failure here
 * means users broke, not that the client changed its syntax.
 */
class PrometheusExpositionContractTest {

    private static final String LOGICAL_SCOPE = "logical_scope";
    private static final String[] LABEL_NAMES = {"label1", "label2"};
    private static final String[] LABEL_VALUES = {"value1", "value2"};
    private static final String LABELS = "{label1=\"value1\",label2=\"value2\",}";

    /** An unexpected warning means a metric silently went missing. */
    @RegisterExtension
    private final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(PrometheusReporter.class, Level.WARN);

    private PrometheusReporter reporter;
    private MetricGroup metricGroup;

    @BeforeEach
    void setUp() {
        reporter = TestUtils.reporterOnFreePort();
        metricGroup =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, LABEL_VALUES));
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
        assertThat(loggerExtension.getMessages())
                .as("the reporter logged a warning, so a metric was dropped")
                .isEmpty();
    }

    /**
     * A Flink Counter and Meter export as a gauge, since a Prometheus counter may not decrease. So
     * simpleclient 0.10.0's {@code _total} rule, which applies only to a counter family, reaches
     * nothing we emit, including a metric already named {@code _total}.
     */
    @Test
    void counterGaugeAndMeterAreGaugesAndKeepTheirNameExactly() throws Exception {
        reporter.notifyOfAddedMetric(counterWith(7), "numRecordsIn", metricGroup);
        reporter.notifyOfAddedMetric(counterWith(11), "records-consumed-total", metricGroup);
        reporter.notifyOfAddedMetric((Gauge<Integer>) () -> 3, "someGauge", metricGroup);
        reporter.notifyOfAddedMetric(new TestMeter(), "someMeter", metricGroup);

        final String body = TestUtils.scrapeBody(reporter);

        for (String name :
                Arrays.asList("numRecordsIn", "records_consumed_total", "someGauge", "someMeter")) {
            assertThat(body).contains("# TYPE " + scoped(name) + " gauge\n");
        }
        assertThat(body).contains(scoped("numRecordsIn") + LABELS + " 7.0\n");
        assertThat(body).contains(scoped("records_consumed_total") + LABELS + " 11.0\n");
        assertThat(body).contains(scoped("someGauge") + LABELS + " 3.0\n");
        // A Meter exports its rate and nothing else; there is no companion count.
        assertThat(body).contains(scoped("someMeter") + LABELS + " 5.0\n");
        assertThat(TestUtils.samplesNamed(body, scoped("someMeter") + "_count")).isZero();
    }

    /**
     * A Flink metric is a gauge or a summary, so none of the family shapes a newer client may emit
     * may appear, whichever client is bundled.
     */
    @Test
    void noReservedFamilyShapeIsEverEmitted() throws Exception {
        reporter.notifyOfAddedMetric(counterWith(7), "someCounter", metricGroup);
        reporter.notifyOfAddedMetric(new TestMeter(), "someMeter", metricGroup);
        reporter.notifyOfAddedMetric(new CoherentTestHistogram(), "someHistogram", metricGroup);

        final String body = TestUtils.scrapeBody(reporter);

        assertThat(
                        Arrays.stream(body.split("\n"))
                                .filter(line -> line.startsWith("# TYPE "))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "# TYPE " + scoped("someCounter") + " gauge",
                        "# TYPE " + scoped("someMeter") + " gauge",
                        "# TYPE " + scoped("someHistogram") + " summary");
        for (String suffix : Arrays.asList("_created", "_info", "_bucket", "_gcount", "_gsum")) {
            assertThat(body).doesNotContain(suffix + "{").doesNotContain(suffix + " ");
        }
    }

    /**
     * A Flink Histogram becomes a summary with a count and eight quantiles, the minimum and maximum
     * being the 0 and 1 quantiles. There is no {@code _sum}: the statistics expose only a sample of
     * recent values (FLINK-29037).
     */
    @Test
    void histogramIsACountAndEightQuantilesWithNoSum() throws Exception {
        reporter.notifyOfAddedMetric(new CoherentTestHistogram(), "someHistogram", metricGroup);

        final String body = TestUtils.scrapeBody(reporter);
        final String family = scoped("someHistogram");

        assertThat(body).contains("# TYPE " + family + " summary\n");
        assertThat(TestUtils.samplesNamed(body, family + "_count")).isOne();
        assertThat(body)
                .contains(
                        family
                                + "_count"
                                + LABELS
                                + " "
                                + CoherentTestHistogram.expectedCount()
                                + ".0\n");
        assertThat(TestUtils.samplesNamed(body, family + "_sum")).isZero();

        assertThat(TestUtils.samplesNamed(body, family)).isEqualTo(8);
        assertThat(quantile(body, family, "0.0")).isEqualTo(CoherentTestHistogram.expectedMin());
        assertThat(quantile(body, family, "1.0")).isEqualTo(CoherentTestHistogram.expectedMax());
        for (String q : Arrays.asList("0.5", "0.75", "0.95", "0.98", "0.99", "0.999")) {
            assertThat(quantile(body, family, q))
                    .isEqualTo(CoherentTestHistogram.expectedQuantile(Double.parseDouble(q)));
        }
    }

    /** The family name is the prefixed logical scope and metric name, sanitised. */
    @Test
    void unsupportedCharactersAreReplacedInTheFamilyName() throws Exception {
        final MetricGroup group =
                TestUtils.createTestMetricGroup(
                        "my.scope-here", TestUtils.toMap(LABEL_NAMES, LABEL_VALUES));
        reporter.notifyOfAddedMetric(counterWith(1), "my:metric-name", group);

        // A colon is legal in a metric name and is not replaced.
        assertThat(TestUtils.scrapeBody(reporter))
                .contains("# TYPE flink_my_scope_here_my:metric_name gauge\n");
    }

    /**
     * Label values are sanitised by default and passed through when {@code
     * filterLabelValueCharacters} is off. Neither branch is covered today.
     */
    @Test
    void labelValuesAreFilteredUnlessTheOptionIsDisabled() throws Exception {
        final Map<String, String> variables = new LinkedHashMap<>();
        variables.put("<job_name>", "a job, with \"quotes\" and\na newline");
        final MetricGroup group = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, variables);

        reporter.open(new MetricConfig());
        reporter.notifyOfAddedMetric(counterWith(1), "filtered", group);

        assertThat(TestUtils.scrapeBody(reporter))
                .contains("job_name=\"a_job__with__quotes__and_a_newline\"");

        final PrometheusReporter unfiltered = TestUtils.reporterOnFreePort();
        try {
            final MetricConfig config = new MetricConfig();
            config.setProperty(
                    PrometheusPushGatewayReporterOptions.FILTER_LABEL_VALUE_CHARACTER.key(),
                    "false");
            unfiltered.open(config);
            unfiltered.notifyOfAddedMetric(counterWith(1), "unfiltered", group);

            // The client escapes what the filter would otherwise have removed.
            assertThat(TestUtils.scrapeBody(unfiltered))
                    .contains("job_name=\"a job, with \\\"quotes\\\" and\\na newline\"");
        } finally {
            unfiltered.close();
        }
    }

    /**
     * A user Gauge may return any double, and a histogram with no observations reports {@code NaN}.
     */
    @Test
    void nonFiniteGaugeValuesReachTheWire() throws Exception {
        reporter.notifyOfAddedMetric((Gauge<Double>) () -> Double.NaN, "nan", metricGroup);
        reporter.notifyOfAddedMetric(
                (Gauge<Double>) () -> Double.POSITIVE_INFINITY, "posInf", metricGroup);
        reporter.notifyOfAddedMetric(
                (Gauge<Double>) () -> Double.NEGATIVE_INFINITY, "negInf", metricGroup);

        final String body = TestUtils.scrapeBody(reporter);

        assertThat(body).contains(scoped("nan") + LABELS + " NaN\n");
        assertThat(body).contains(scoped("posInf") + LABELS + " +Inf\n");
        assertThat(body).contains(scoped("negInf") + LABELS + " -Inf\n");
    }

    private static double quantile(String body, String family, String q) {
        final String needle = "quantile=\"" + q + "\",} ";
        for (String line : body.split("\n")) {
            if (line.startsWith(family + "{") && line.contains(needle)) {
                return Double.parseDouble(line.substring(line.indexOf(needle) + needle.length()));
            }
        }
        throw new AssertionError("no sample for quantile " + q);
    }

    private static Counter counterWith(long count) {
        final Counter counter = new SimpleCounter();
        counter.inc(count);
        return counter;
    }

    private static String scoped(String metricName) {
        return "flink_" + LOGICAL_SCOPE + "_" + metricName;
    }
}
