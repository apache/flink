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
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the ScheduledDropwizardReporter. */
public class ScheduledDropwizardReporterTest {

    @Test
    public void testInvalidCharacterReplacement() {
        ScheduledDropwizardReporter reporter =
                new ScheduledDropwizardReporter() {
                    @Override
                    public ScheduledReporter getReporter(MetricConfig config) {
                        return null;
                    }
                };

        assertEquals("abc", reporter.filterCharacters("abc"));
        assertEquals("a--b-c-", reporter.filterCharacters("a..b.c."));
        assertEquals("ab-c", reporter.filterCharacters("a\"b.c"));
    }

    /** Tests that the registered metrics' names don't contain invalid characters. */
    @Test
    public void testAddingMetrics() {
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
        assertTrue(counters.containsKey(myCounter));

        Map<Meter, String> meters = reporter.getMeters();
        assertTrue(meters.containsKey(meterWrapper));

        String expectedCounterName =
                reporter.filterCharacters(scope)
                        + delimiter
                        + reporter.filterCharacters(counterName);

        assertEquals(expectedCounterName, counters.get(myCounter));
    }

    /**
     * This test verifies that metrics are properly added and removed to/from the
     * ScheduledDropwizardReporter and the underlying Dropwizard MetricRegistry.
     */
    @Test
    public void testMetricCleanup() {
        TestingScheduledDropwizardReporter rep = new TestingScheduledDropwizardReporter();

        MetricGroup mp = new UnregisteredMetricsGroup();

        Counter c = new SimpleCounter();
        Meter m = new TestMeter();
        Histogram h = new TestHistogram();
        Gauge<?> g = () -> null;

        rep.notifyOfAddedMetric(c, "counter", mp);
        assertEquals(1, rep.getCounters().size());
        assertEquals(1, rep.registry.getCounters().size());

        rep.notifyOfAddedMetric(m, "meter", mp);
        assertEquals(1, rep.getMeters().size());
        assertEquals(1, rep.registry.getMeters().size());

        rep.notifyOfAddedMetric(h, "histogram", mp);
        assertEquals(1, rep.getHistograms().size());
        assertEquals(1, rep.registry.getHistograms().size());

        rep.notifyOfAddedMetric(g, "gauge", mp);
        assertEquals(1, rep.getGauges().size());
        assertEquals(1, rep.registry.getGauges().size());

        rep.notifyOfRemovedMetric(c, "counter", mp);
        assertEquals(0, rep.getCounters().size());
        assertEquals(0, rep.registry.getCounters().size());

        rep.notifyOfRemovedMetric(m, "meter", mp);
        assertEquals(0, rep.getMeters().size());
        assertEquals(0, rep.registry.getMeters().size());

        rep.notifyOfRemovedMetric(h, "histogram", mp);
        assertEquals(0, rep.getHistograms().size());
        assertEquals(0, rep.registry.getHistograms().size());

        rep.notifyOfRemovedMetric(g, "gauge", mp);
        assertEquals(0, rep.getGauges().size());
        assertEquals(0, rep.registry.getGauges().size());
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
