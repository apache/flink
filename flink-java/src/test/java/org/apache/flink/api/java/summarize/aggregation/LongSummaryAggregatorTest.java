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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.api.java.summarize.NumericColumnSummary;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Tests for {@link LongSummaryAggregator}. */
class LongSummaryAggregatorTest {

    @Test
    void testIsNan() {
        LongSummaryAggregator ag = new LongSummaryAggregator();
        // always false for Long
        assertThat(ag.isNan(-1L)).isFalse();
        assertThat(ag.isNan(0L)).isFalse();
        assertThat(ag.isNan(23L)).isFalse();
        assertThat(ag.isNan(Long.MAX_VALUE)).isFalse();
        assertThat(ag.isNan(Long.MIN_VALUE)).isFalse();
        assertThat(ag.isNan(null)).isFalse();
    }

    @Test
    void testIsInfinite() {
        LongSummaryAggregator ag = new LongSummaryAggregator();
        // always false for Long
        assertThat(ag.isInfinite(-1L)).isFalse();
        assertThat(ag.isInfinite(0L)).isFalse();
        assertThat(ag.isInfinite(23L)).isFalse();
        assertThat(ag.isInfinite(Long.MAX_VALUE)).isFalse();
        assertThat(ag.isInfinite(Long.MIN_VALUE)).isFalse();
        assertThat(ag.isInfinite(null)).isFalse();
    }

    @Test
    void testMean() {
        assertThat(summarize(0L, 100L).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0L, 0L, 100L).getMean()).isCloseTo(33.333333, offset(0.00001));
        assertThat(summarize(0L, 0L, 100L, 100L).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0L, 100L, null).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize().getMean()).isNull();
    }

    @Test
    void testSum() throws Exception {
        assertThat(summarize(0L, 100L).getSum().longValue()).isEqualTo(100L);
        assertThat(summarize(1L, 2L, 3L, 4L, 5L).getSum().longValue()).isEqualTo(15L);
        assertThat(summarize(-100L, 0L, 100L, null).getSum().longValue()).isZero();
        assertThat(summarize(-10L, 100L, null).getSum().longValue()).isEqualTo(90L);
        assertThat(summarize().getSum()).isNull();
    }

    @Test
    void testMax() {
        assertThat(summarize(-1000L, 0L, 1L, 50L, 999L, 1001L).getMax().longValue())
                .isEqualTo(1001L);
        assertThat(
                        summarize(1L, 8L, 7L, 6L, 9L, 10L, 2L, 3L, 5L, 0L, 11L, -2L, 3L)
                                .getMax()
                                .longValue())
                .isEqualTo(11L);
        assertThat(
                        summarize(1L, 8L, 7L, 6L, 9L, null, 10L, 2L, 3L, 5L, null, 0L, 11L, -2L, 3L)
                                .getMax()
                                .longValue())
                .isEqualTo(11L);
        assertThat(summarize().getMax()).isNull();
    }

    @Test
    void testMin() {
        assertThat(summarize(-1000L, 0L, 1L, 50L, 999L, 1001L).getMin().longValue())
                .isEqualTo(-1000L);
        assertThat(
                        summarize(1L, 8L, 7L, 6L, 9L, 10L, 2L, 3L, 5L, 0L, 11L, -2L, 3L)
                                .getMin()
                                .longValue())
                .isEqualTo(-2L);
        assertThat(
                        summarize(1L, 8L, 7L, 6L, 9L, null, 10L, 2L, 3L, 5L, null, 0L, 11L, -2L, 3L)
                                .getMin()
                                .longValue())
                .isEqualTo(-2L);
        assertThat(summarize().getMin()).isNull();
    }

    /** Helper method for summarizing a list of values. */
    protected NumericColumnSummary<Long> summarize(Long... values) {
        return new AggregateCombineHarness<
                Long, NumericColumnSummary<Long>, LongSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Long> result1, NumericColumnSummary<Long> result2) {

                assertThat(result2.getTotalCount()).isEqualTo(result1.getTotalCount());
                assertThat(result2.getNullCount()).isEqualTo(result1.getNullCount());
                assertThat(result2.getMissingCount()).isEqualTo(result1.getMissingCount());
                assertThat(result2.getNonMissingCount()).isEqualTo(result1.getNonMissingCount());
                assertThat(result2.getInfinityCount()).isEqualTo(result1.getInfinityCount());
                assertThat(result2.getNanCount()).isEqualTo(result1.getNanCount());

                assertThat(result2.containsNull()).isEqualTo(result1.containsNull());
                assertThat(result2.containsNonNull()).isEqualTo(result1.containsNonNull());

                assertThat(result2.getMin().longValue()).isEqualTo(result1.getMin().longValue());
                assertThat(result2.getMax().longValue()).isEqualTo(result1.getMax().longValue());
                assertThat(result2.getSum().longValue()).isEqualTo(result1.getSum().longValue());
                assertThat(result2.getMean().doubleValue())
                        .isCloseTo(result1.getMean().doubleValue(), offset(1e-12d));
                assertThat(result2.getVariance().doubleValue())
                        .isCloseTo(result1.getVariance().doubleValue(), offset(1e-9d));
                assertThat(result2.getStandardDeviation().doubleValue())
                        .isCloseTo(result1.getStandardDeviation().doubleValue(), offset(1e-12d));
            }
        }.summarize(values);
    }
}
