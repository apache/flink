/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlidingWindowHistogramTest extends AbstractHistogramTest {

    @Test
    void retainsMostRecentValues() {
        final SlidingWindowHistogram histogram = new SlidingWindowHistogram(4);
        for (long value = 1; value <= 10; value++) {
            histogram.update(value);
        }

        assertThat(histogram.getCount()).isEqualTo(10L);
        assertThat(histogram.getStatistics().getValues()).containsExactly(7L, 8L, 9L, 10L);
    }

    @Test
    void computesStatistics() {
        final SlidingWindowHistogram histogram = new SlidingWindowHistogram(10);
        for (long value = 1; value <= 5; value++) {
            histogram.update(value);
        }

        final HistogramStatistics statistics = histogram.getStatistics();
        assertThat(statistics.getMean()).isEqualTo(3.0);
        assertThat(statistics.getStdDev()).isEqualTo(Math.sqrt(2.5));
        assertThat(statistics.getQuantile(0.5)).isEqualTo(3.0);
    }

    @Test
    void statisticsAreSnapshots() {
        final SlidingWindowHistogram histogram = new SlidingWindowHistogram(2);
        histogram.update(1L);
        final HistogramStatistics statistics = histogram.getStatistics();

        histogram.update(2L);

        assertThat(statistics.getValues()).containsExactly(1L);
    }

    @Test
    void satisfiesHistogramContract() {
        testHistogram(10, new SlidingWindowHistogram(10));
    }

    @Test
    void rejectsNonPositiveWindowSize() {
        assertThatThrownBy(() -> new SlidingWindowHistogram(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("windowSize must be positive");
    }
}
