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

/** Tests for {@link IntegerSummaryAggregator}. */
class IntegerSummaryAggregatorTest {

    @Test
    void testIsNan() {
        IntegerSummaryAggregator ag = new IntegerSummaryAggregator();
        // always false for Integer
        assertThat(ag.isNan(-1)).isFalse();
        assertThat(ag.isNan(0)).isFalse();
        assertThat(ag.isNan(23)).isFalse();
        assertThat(ag.isNan(Integer.MAX_VALUE)).isFalse();
        assertThat(ag.isNan(Integer.MIN_VALUE)).isFalse();
        assertThat(ag.isNan(null)).isFalse();
    }

    @Test
    void testIsInfinite() {
        IntegerSummaryAggregator ag = new IntegerSummaryAggregator();
        // always false for Integer
        assertThat(ag.isInfinite(-1)).isFalse();
        assertThat(ag.isInfinite(0)).isFalse();
        assertThat(ag.isInfinite(23)).isFalse();
        assertThat(ag.isInfinite(Integer.MAX_VALUE)).isFalse();
        assertThat(ag.isInfinite(Integer.MIN_VALUE)).isFalse();
        assertThat(ag.isInfinite(null)).isFalse();
    }

    @Test
    void testMean() {
        assertThat(summarize(0, 100).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0, 0, 100).getMean()).isCloseTo(33.333333, offset(0.00001));
        assertThat(summarize(0, 0, 100, 100).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0, 100, null).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize().getMean()).isNull();
    }

    @Test
    void testSum() throws Exception {
        assertThat(summarize(0, 100).getSum().intValue()).isEqualTo(100);
        assertThat(summarize(1, 2, 3, 4, 5).getSum().intValue()).isEqualTo(15);
        assertThat(summarize(-100, 0, 100, null).getSum().intValue()).isZero();
        assertThat(summarize(-10, 100, null).getSum().intValue()).isEqualTo(90);
        assertThat(summarize().getSum()).isNull();
    }

    @Test
    void testMax() {
        assertThat(summarize(-1000, 0, 1, 50, 999, 1001).getMax().intValue()).isEqualTo(1001);
        assertThat(summarize(Integer.MIN_VALUE, -1000, 0).getMax().intValue()).isZero();
        assertThat(summarize(1, 8, 7, 6, 9, 10, 2, 3, 5, 0, 11, -2, 3).getMax().intValue())
                .isEqualTo(11);
        assertThat(
                        summarize(1, 8, 7, 6, 9, null, 10, 2, 3, 5, null, 0, 11, -2, 3)
                                .getMax()
                                .intValue())
                .isEqualTo(11);
        assertThat(summarize().getMax()).isNull();
    }

    @Test
    void testMin() {
        assertThat(summarize(-1000, 0, 1, 50, 999, 1001).getMin().intValue()).isEqualTo(-1000);
        assertThat(summarize(Integer.MIN_VALUE, -1000, 0).getMin().intValue())
                .isEqualTo(Integer.MIN_VALUE);
        assertThat(summarize(1, 8, 7, 6, 9, 10, 2, 3, 5, 0, 11, -2, 3).getMin().intValue())
                .isEqualTo(-2);
        assertThat(
                        summarize(1, 8, 7, 6, 9, null, 10, 2, 3, 5, null, 0, 11, -2, 3)
                                .getMin()
                                .intValue())
                .isEqualTo(-2);
        assertThat(summarize().getMin()).isNull();
    }

    /** Helper method for summarizing a list of values. */
    protected NumericColumnSummary<Integer> summarize(Integer... values) {

        return new AggregateCombineHarness<
                Integer, NumericColumnSummary<Integer>, IntegerSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Integer> result1, NumericColumnSummary<Integer> result2) {

                assertThat(result2.getTotalCount()).isEqualTo(result1.getTotalCount());
                assertThat(result2.getNullCount()).isEqualTo(result1.getNullCount());
                assertThat(result2.getMissingCount()).isEqualTo(result1.getMissingCount());
                assertThat(result2.getNonMissingCount()).isEqualTo(result1.getNonMissingCount());
                assertThat(result2.getInfinityCount()).isEqualTo(result1.getInfinityCount());
                assertThat(result2.getNanCount()).isEqualTo(result1.getNanCount());

                assertThat(result2.containsNull()).isEqualTo(result1.containsNull());
                assertThat(result2.containsNonNull()).isEqualTo(result1.containsNonNull());

                assertThat(result2.getMin().intValue()).isEqualTo(result1.getMin().intValue());
                assertThat(result2.getMax().intValue()).isEqualTo(result1.getMax().intValue());
                assertThat(result2.getSum().intValue()).isEqualTo(result1.getSum().intValue());
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
