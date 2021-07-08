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

package org.apache.flink.metric.testutils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@link MetricListener}. */
public class MetricListenerTest {
    public static final String COUNTER_NAME = "testCounter";
    public static final String GAUGE_NAME = "testGauge";
    public static final String METER_NAME = "testMeter";
    public static final String HISTOGRAM_NAME = "testHistogram";

    public static final String GROUP_A = "groupA";
    public static final String GROUP_B = "groupB";
    public static final String GROUP_A_1 = "groupA_1";
    public static final String GROUP_B_1 = "groupB_1";
    public static final String GROUP_B_2 = "groupB_2";

    @Test
    public void testRegisterMetrics() {
        MetricListener metricListener = new MetricListener();
        final MetricGroup metricGroup = metricListener.getMetricGroup();

        // Counter
        final Counter counter = metricGroup.counter(COUNTER_NAME);
        counter.inc(15213);
        final Counter registeredCounter = metricListener.getCounter(COUNTER_NAME);
        assertEquals(15213L, registeredCounter.getCount());

        // Gauge
        metricGroup.gauge(GAUGE_NAME, () -> 15213);
        final Gauge<Integer> registeredGauge = metricListener.getGauge(GAUGE_NAME);
        assertEquals(Integer.valueOf(15213), registeredGauge.getValue());

        // Meter
        metricGroup.meter(
                METER_NAME,
                new Meter() {
                    @Override
                    public void markEvent() {}

                    @Override
                    public void markEvent(long n) {}

                    @Override
                    public double getRate() {
                        return 15213.0;
                    }

                    @Override
                    public long getCount() {
                        return 18213L;
                    }
                });
        final Meter registeredMeter = metricListener.getMeter(METER_NAME);
        assertEquals(15213.0, registeredMeter.getRate(), 0.1);
        assertEquals(18213L, registeredMeter.getCount());

        // Histogram
        metricGroup.histogram(
                HISTOGRAM_NAME,
                new Histogram() {
                    @Override
                    public void update(long value) {}

                    @Override
                    public long getCount() {
                        return 15213L;
                    }

                    @Override
                    public HistogramStatistics getStatistics() {
                        return null;
                    }
                });
        final Histogram registeredHistogram = metricListener.getHistogram(HISTOGRAM_NAME);
        assertEquals(15213L, registeredHistogram.getCount());
    }

    @Test
    public void testRegisterMetricGroup() {
        MetricListener metricListener = new MetricListener();
        final MetricGroup rootGroup = metricListener.getMetricGroup();
        final MetricGroup groupA1 = rootGroup.addGroup(GROUP_A).addGroup(GROUP_A_1);
        final MetricGroup groupB = rootGroup.addGroup(GROUP_B);
        final MetricGroup groupB1 = groupB.addGroup(GROUP_B_1);
        final MetricGroup groupB2 = groupB.addGroup(GROUP_B_2);

        groupA1.counter(COUNTER_NAME).inc(18213L);
        groupB1.gauge(GAUGE_NAME, () -> 15213L);
        groupB2.counter(COUNTER_NAME).inc(15513L);

        // groupA.groupA_1.testCounter
        assertEquals(
                18213L, metricListener.getCounter(GROUP_A, GROUP_A_1, COUNTER_NAME).getCount());

        // groupB.groupB_1.testGauge
        assertEquals(15213L, metricListener.getGauge(GROUP_B, GROUP_B_1, GAUGE_NAME).getValue());

        // groupB.groupB_2.testCounter
        assertEquals(
                15513L, metricListener.getCounter(GROUP_B, GROUP_B_2, COUNTER_NAME).getCount());
    }
}
