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

package org.apache.flink.dropwizard.metrics;

import org.apache.flink.dropwizard.ScheduledDropwizardReporter;
import org.apache.flink.metrics.AbstractHistogramTest;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.util.TestMetricGroup;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/** Tests for the DropwizardFlinkHistogramWrapper. */
public class DropwizardFlinkHistogramWrapperTest extends AbstractHistogramTest {

    /** Tests the histogram functionality of the DropwizardHistogramWrapper. */
    @Test
    public void testDropwizardHistogramWrapper() {
        int size = 10;
        DropwizardHistogramWrapper histogramWrapper =
                new DropwizardHistogramWrapper(
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(size)));
        testHistogram(size, histogramWrapper);
    }

    /**
     * Tests that the DropwizardHistogramWrapper reports correct dropwizard snapshots to the
     * ScheduledReporter.
     */
    @Test
    public void testDropwizardHistogramWrapperReporting() throws Exception {
        int size = 10;
        String histogramMetricName = "histogram";

        final TestingReporter testingReporter = new TestingReporter();
        testingReporter.open(new MetricConfig());

        DropwizardHistogramWrapper histogramWrapper =
                new DropwizardHistogramWrapper(
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(size)));

        final MetricGroup metricGroup = TestMetricGroup.newBuilder().build();

        testingReporter.notifyOfAddedMetric(histogramWrapper, histogramMetricName, metricGroup);

        // check that the metric has been registered
        assertEquals(1, testingReporter.getMetrics().size());

        for (int i = 0; i < size; i++) {
            histogramWrapper.update(i);
        }

        testingReporter.report();
        String fullMetricName = metricGroup.getMetricIdentifier(histogramMetricName);
        Snapshot snapshot = testingReporter.getNextHistogramSnapshot(fullMetricName);

        assertEquals(0, snapshot.getMin());
        assertEquals((size - 1) / 2.0, snapshot.getMedian(), 0.001);
        assertEquals(size - 1, snapshot.getMax());
        assertEquals(size, snapshot.size());

        testingReporter.notifyOfRemovedMetric(histogramWrapper, histogramMetricName, metricGroup);

        // check that the metric has been de-registered
        assertEquals(0, testingReporter.getMetrics().size());
    }

    /** Test reporter. */
    public static class TestingReporter extends ScheduledDropwizardReporter {
        TestingScheduledReporter scheduledReporter = null;

        @Override
        public ScheduledReporter getReporter(MetricConfig config) {
            scheduledReporter =
                    new TestingScheduledReporter(
                            registry,
                            getClass().getName(),
                            null,
                            TimeUnit.MILLISECONDS,
                            TimeUnit.MILLISECONDS);

            return scheduledReporter;
        }

        public Map<String, com.codahale.metrics.Metric> getMetrics() {
            return registry.getMetrics();
        }

        Snapshot getNextHistogramSnapshot(String name) {
            return scheduledReporter.histogramSnapshots.get(name);
        }
    }

    static class TestingScheduledReporter extends ScheduledReporter {

        final Map<String, Snapshot> histogramSnapshots = new HashMap<>();

        protected TestingScheduledReporter(
                com.codahale.metrics.MetricRegistry registry,
                String name,
                MetricFilter filter,
                TimeUnit rateUnit,
                TimeUnit durationUnit) {
            super(registry, name, filter, rateUnit, durationUnit);
        }

        @Override
        public void report(
                SortedMap<String, Gauge> gauges,
                SortedMap<String, Counter> counters,
                SortedMap<String, com.codahale.metrics.Histogram> histograms,
                SortedMap<String, Meter> meters,
                SortedMap<String, Timer> timers) {
            for (Map.Entry<String, com.codahale.metrics.Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey(), entry.getValue());
            }
        }

        private void reportHistogram(String name, com.codahale.metrics.Histogram histogram) {
            histogramSnapshots.put(name, histogram.getSnapshot());
        }
    }
}
