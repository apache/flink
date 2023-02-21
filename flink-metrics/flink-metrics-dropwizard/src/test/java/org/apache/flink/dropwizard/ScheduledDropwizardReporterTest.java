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

package org.apache.flink.dropwizard;

import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.metrics.util.TestMetricGroup;

import com.codahale.metrics.ScheduledReporter;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the ScheduledDropwizardReporter. */
class ScheduledDropwizardReporterTest {

    @Test
    void testInvalidCharacterReplacement() {
        ScheduledDropwizardReporter reporter =
                new ScheduledDropwizardReporter() {
                    @Override
                    public ScheduledReporter getReporter(MetricConfig config) {
                        return null;
                    }
                };

        assertThat(reporter.filterCharacters("abc")).isEqualTo("abc");
        assertThat(reporter.filterCharacters("a..b.c.")).isEqualTo("a--b-c-");
        assertThat(reporter.filterCharacters("a\"b.c")).isEqualTo("ab-c");
    }

    /** Tests that the registered metrics' names don't contain invalid characters. */
    @Test
    void testAddingMetrics() {
        final String scope = "scope";
        final char delimiter = '_';
        final String counterName = "testCounter";

        final MetricGroup metricGroup =
                TestMetricGroup.newBuilder()
                        .setMetricIdentifierFunction(
                                (metricName, characterFilter) ->
                                        characterFilter
                                                .orElse(s -> s)
                                                .filterCharacters(scope + delimiter + metricName))
                        .build();

        final TestingScheduledDropwizardReporter reporter =
                new TestingScheduledDropwizardReporter();

        SimpleCounter myCounter = new SimpleCounter();
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        DropwizardMeterWrapper meterWrapper = new DropwizardMeterWrapper(dropwizardMeter);

        reporter.notifyOfAddedMetric(myCounter, counterName, metricGroup);
        reporter.notifyOfAddedMetric(meterWrapper, "meter", metricGroup);

        Map<Counter, String> counters = reporter.getCounters();
        assertThat(counters).containsKey(myCounter);

        Map<Meter, String> meters = reporter.getMeters();
        assertThat(meters).containsKey(meterWrapper);

        String expectedCounterName =
                reporter.filterCharacters(scope)
                        + delimiter
                        + reporter.filterCharacters(counterName);

        assertThat(counters).containsEntry(myCounter, expectedCounterName);
    }

    /**
     * This test verifies that metrics are properly added and removed to/from the
     * ScheduledDropwizardReporter and the underlying Dropwizard MetricRegistry.
     */
    @Test
    void testMetricCleanup() {
        TestingScheduledDropwizardReporter rep = new TestingScheduledDropwizardReporter();

        MetricGroup mp = new UnregisteredMetricsGroup();

        Counter c = new SimpleCounter();
        Meter m = new TestMeter();
        Histogram h = new TestHistogram();
        Gauge<?> g = () -> null;

        rep.notifyOfAddedMetric(c, "counter", mp);
        assertThat(rep.getCounters()).hasSize(1);
        assertThat(rep.registry.getCounters()).hasSize(1);

        rep.notifyOfAddedMetric(m, "meter", mp);
        assertThat(rep.getMeters()).hasSize(1);
        assertThat(rep.registry.getMeters()).hasSize(1);

        rep.notifyOfAddedMetric(h, "histogram", mp);
        assertThat(rep.getHistograms()).hasSize(1);
        assertThat(rep.registry.getHistograms()).hasSize(1);

        rep.notifyOfAddedMetric(g, "gauge", mp);
        assertThat(rep.getGauges()).hasSize(1);
        assertThat(rep.registry.getGauges()).hasSize(1);

        rep.notifyOfRemovedMetric(c, "counter", mp);
        assertThat(rep.getCounters()).hasSize(0);
        assertThat(rep.registry.getCounters()).hasSize(0);

        rep.notifyOfRemovedMetric(m, "meter", mp);
        assertThat(rep.getMeters()).hasSize(0);
        assertThat(rep.registry.getMeters()).hasSize(0);

        rep.notifyOfRemovedMetric(h, "histogram", mp);
        assertThat(rep.getHistograms()).hasSize(0);
        assertThat(rep.registry.getHistograms()).hasSize(0);

        rep.notifyOfRemovedMetric(g, "gauge", mp);
        assertThat(rep.getGauges()).hasSize(0);
        assertThat(rep.registry.getGauges()).hasSize(0);
    }

    /** Dummy test reporter. */
    public static class TestingScheduledDropwizardReporter extends ScheduledDropwizardReporter {

        @Override
        public ScheduledReporter getReporter(MetricConfig config) {
            return null;
        }

        @Override
        public void close() {
            // don't do anything
        }
    }
}
