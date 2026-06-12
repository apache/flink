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

package org.apache.flink.fs.s3native.metrics;

import org.apache.flink.metrics.HistogramStatistics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3MetricHistogram}. */
class S3MetricHistogramTest {

    @Test
    void recordsCountMinMaxAndMean() {
        S3MetricHistogram histogram = new S3MetricHistogram(1024);
        for (long value = 1; value <= 100; value++) {
            histogram.update(value);
        }

        assertThat(histogram.getCount()).isEqualTo(100L);

        HistogramStatistics stats = histogram.getStatistics();
        assertThat(stats.size()).isEqualTo(100);
        assertThat(stats.getMin()).isEqualTo(1L);
        assertThat(stats.getMax()).isEqualTo(100L);
        assertThat(stats.getMean()).isEqualTo(50.5);
        assertThat(stats.getQuantile(0.5)).isBetween(49.0, 52.0);
    }

    @Test
    void slidingWindowEvictsOldestBeyondCapacity() {
        S3MetricHistogram histogram = new S3MetricHistogram(4);
        for (long value = 1; value <= 10; value++) {
            histogram.update(value);
        }

        assertThat(histogram.getCount()).isEqualTo(10L); // total ever recorded

        HistogramStatistics stats = histogram.getStatistics();
        assertThat(stats.size()).isEqualTo(4); // only the most recent four retained
        assertThat(stats.getMin()).isEqualTo(7L);
        assertThat(stats.getMax()).isEqualTo(10L);
    }

    @Test
    void emptyHistogramReturnsZeroes() {
        S3MetricHistogram histogram = new S3MetricHistogram(8);

        HistogramStatistics stats = histogram.getStatistics();
        assertThat(stats.size()).isZero();
        assertThat(stats.getMin()).isZero();
        assertThat(stats.getMax()).isZero();
        assertThat(stats.getMean()).isZero();
        assertThat(stats.getQuantile(0.99)).isZero();
    }

    @Test
    void rejectsNonPositiveWindowSize() {
        assertThatThrownBy(() -> new S3MetricHistogram(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("windowSize must be positive");
    }
}
