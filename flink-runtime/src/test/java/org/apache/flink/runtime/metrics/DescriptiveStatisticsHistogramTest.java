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

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.AbstractHistogramTest;
import org.apache.flink.util.InstantiationUtil;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.ThrowingFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link DescriptiveStatisticsHistogram} and {@link
 * DescriptiveStatisticsHistogramStatistics}.
 */
class DescriptiveStatisticsHistogramTest extends AbstractHistogramTest {

    private static final double[] DATA = {1, 2, 3, 4, 5, 6, 7, 8, 9};

    /** Tests the histogram functionality of the DropwizardHistogramWrapper. */
    @Test
    void testDescriptiveHistogram() {
        int size = 10;
        testHistogram(size, new DescriptiveStatisticsHistogram(size));
    }

    /** Tests our workaround for https://issues.apache.org/jira/browse/MATH-1642. */
    @Test
    void testSerialization() throws Exception {
        testDuplication(
                original -> {
                    final byte[] bytes = InstantiationUtil.serializeObject(original);
                    return (DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot)
                            InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
                });
    }

    @Test
    void testCopy() throws Exception {
        testDuplication(DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot::copy);
    }

    private static void testDuplication(
            ThrowingFunction<
                            DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot,
                            DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot>
                    duplicator)
            throws Exception {

        DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot original =
                new DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot();
        original.evaluate(DATA);

        assertOperations(original);

        final DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot copy =
                duplicator.apply(original);

        assertOperations(copy);
    }

    private static void assertOperations(
            DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot statistics) {
        assertThat(statistics.getPercentile(0.5)).isOne();
        assertThat(statistics.getCount()).isEqualTo(9);
        assertThat(statistics.getMin()).isOne();
        assertThat(statistics.getMax()).isEqualTo(9);
        assertThat(statistics.getMean()).isEqualTo(5);
        assertThat(statistics.getStandardDeviation()).isCloseTo(2.7, Offset.offset(0.5));
        assertThat(statistics.getValues()).containsExactly(DATA);
    }
}
