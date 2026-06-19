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
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MetricListener}. */
class MetricListenerTest {
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
    void testRegisterMetrics() {
        MetricListener metricListener = new MetricListener();
        final MetricGroup metricGroup = metricListener.getMetricGroup();

        // Counter
        final Counter counter = metricGroup.counter(COUNTER_NAME);
        counter.inc(15213);
        assertThat(metricListener.getCounter(COUNTER_NAME))
                .hasValueSatisfying(
                        registeredCounter ->
                                assertThat(registeredCounter.getCount()).isEqualTo(15213L));

        // Gauge
        metricGroup.gauge(GAUGE_NAME, () -> 15213);
        assertThat(metricListener.getGauge(GAUGE_NAME))
                .hasValueSatisfying(
                        registeredGauge -> assertThat(registeredGauge.getValue()).isEqualTo(15213));

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
        assertThat(metricListener.getMeter(METER_NAME))
                .hasValueSatisfying(
                        registeredMeter -> {
                            assertThat(registeredMeter.getCount()).isEqualTo(18213L);
                            assertThat(registeredMeter.getRate())
                                    .isCloseTo(15213.0, Offset.offset(0.1));
                        });

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
        assertThat(metricListener.getHistogram(HISTOGRAM_NAME))
                .hasValueSatisfying(
                        registeredHistogram ->
                                assertThat(registeredHistogram.getCount()).isEqualTo(15213L));
    }

    @Test
    void testRegisterMetricGroup() {
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
        assertThat(metricListener.getCounter(GROUP_A, GROUP_A_1, COUNTER_NAME))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(18213L));

        // groupB.groupB_1.testGauge
        assertThat(metricListener.getGauge(GROUP_B, GROUP_B_1, GAUGE_NAME))
                .hasValueSatisfying(gauge -> assertThat(gauge.getValue()).isEqualTo(15213L));

        // groupB.groupB_2.testCounter
        assertThat(metricListener.getCounter(GROUP_B, GROUP_B_2, COUNTER_NAME))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(15513L));
    }
}
