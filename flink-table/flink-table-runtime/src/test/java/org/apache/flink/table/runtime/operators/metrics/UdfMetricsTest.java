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

package org.apache.flink.table.runtime.operators.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UdfMetrics}. */
class UdfMetricsTest {

    private MetricListener metricListener;

    @BeforeEach
    void setUp() {
        metricListener = new MetricListener();
    }

    @Test
    void sampleEveryNth() {
        UdfMetrics metrics = register("myUdf", 100);

        List<Integer> sampledCalls = new ArrayList<>();
        for (int call = 1; call <= 300; call++) {
            if (metrics.shouldSample()) {
                sampledCalls.add(call);
            }
        }

        // With interval 100 exactly one call in every 100 is sampled, on the 1st, 101st, 201st.
        assertThat(sampledCalls).containsExactly(1, 101, 201);
    }

    @Test
    void sampleEveryCallWhenIntervalOne() {
        UdfMetrics metrics = register("myUdf", 1);

        for (int call = 0; call < 10; call++) {
            assertThat(metrics.shouldSample()).isTrue();
        }
    }

    @Test
    void rejectsNonPositiveInterval() {
        assertThatThrownBy(() -> register("myUdf", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> register("myUdf", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registrationNamesAndTypes() {
        register("myUdf", 100);

        assertThat(metricListener.getHistogram("udf", "myUdf", "udfProcessingTime"))
                .get()
                .isInstanceOf(DescriptiveStatisticsHistogram.class);
        assertThat(metricListener.getCounter("udf", "myUdf", "udfExceptionCount"))
                .get()
                .isInstanceOf(ThreadSafeSimpleCounter.class);
    }

    @Test
    void updateFeedsHistogram() {
        UdfMetrics metrics = register("myUdf", 1);
        Histogram histogram =
                metricListener.getHistogram("udf", "myUdf", "udfProcessingTime").get();

        metrics.update(10L);
        metrics.update(20L);
        metrics.update(30L);

        assertThat(histogram.getCount()).isEqualTo(3L);
        assertThat(histogram.getStatistics().getMin()).isEqualTo(10L);
        assertThat(histogram.getStatistics().getMax()).isEqualTo(30L);
    }

    @Test
    void markExceptionCounts() {
        UdfMetrics metrics = register("myUdf", 1);
        Counter counter = metricListener.getCounter("udf", "myUdf", "udfExceptionCount").get();

        metrics.markException();
        metrics.markException();

        assertThat(counter.getCount()).isEqualTo(2L);
    }

    private UdfMetrics register(String udfName, int sampleInterval) {
        return UdfMetrics.register(metricListener.getMetricGroup(), udfName, sampleInterval);
    }
}
