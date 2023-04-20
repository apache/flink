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

/** Tests for {@link DoubleSummaryAggregator}. */
class DoubleSummaryAggregatorTest {

    /**
     * Use some values from Anscombe's Quartet for testing.
     *
     * <p>There was no particular reason to use these except they have known means and variance.
     *
     * <p>https://en.wikipedia.org/wiki/Anscombe%27s_quartet
     */
    @Test
    void testAnscomesQuartetXValues() {

        final Double[] q1x = {10.0, 8.0, 13.0, 9.0, 11.0, 14.0, 6.0, 4.0, 12.0, 7.0, 5.0};
        final Double[] q4x = {8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 19.0, 8.0, 8.0, 8.0};

        NumericColumnSummary<Double> q1 = summarize(q1x);
        NumericColumnSummary<Double> q4 = summarize(q4x);

        assertThat(q1.getMean().doubleValue()).isCloseTo(9.0, offset(0.0));
        assertThat(q4.getMean().doubleValue()).isCloseTo(9.0, offset(0.0));

        assertThat(q1.getVariance().doubleValue()).isCloseTo(11.0, offset(1e-10d));
        assertThat(q4.getVariance().doubleValue()).isCloseTo(11.0, offset(1e-10d));

        double stddev = Math.sqrt(11.0);
        assertThat(q1.getStandardDeviation().doubleValue()).isCloseTo(stddev, offset(1e-10d));
        assertThat(q4.getStandardDeviation().doubleValue()).isCloseTo(stddev, offset(1e-10d));
    }

    /**
     * Use some values from Anscombe's Quartet for testing.
     *
     * <p>There was no particular reason to use these except they have known means and variance.
     *
     * <p>https://en.wikipedia.org/wiki/Anscombe%27s_quartet
     */
    @Test
    void testAnscomesQuartetYValues() {
        final Double[] q1y = {8.04, 6.95, 7.58, 8.81, 8.33, 9.96, 7.24, 4.26, 10.84, 4.82, 5.68};
        final Double[] q2y = {9.14, 8.14, 8.74, 8.77, 9.26, 8.1, 6.13, 3.1, 9.13, 7.26, 4.74};
        final Double[] q3y = {7.46, 6.77, 12.74, 7.11, 7.81, 8.84, 6.08, 5.39, 8.15, 6.42, 5.73};
        final Double[] q4y = {6.58, 5.76, 7.71, 8.84, 8.47, 7.04, 5.25, 12.5, 5.56, 7.91, 6.89};

        NumericColumnSummary<Double> q1 = summarize(q1y);
        NumericColumnSummary<Double> q2 = summarize(q2y);
        NumericColumnSummary<Double> q3 = summarize(q3y);
        NumericColumnSummary<Double> q4 = summarize(q4y);

        // the y values are have less precisely matching means and variances

        assertThat(q1.getMean().doubleValue()).isCloseTo(7.5, offset(0.001));
        assertThat(q2.getMean().doubleValue()).isCloseTo(7.5, offset(0.001));
        assertThat(q3.getMean().doubleValue()).isCloseTo(7.5, offset(0.001));
        assertThat(q4.getMean().doubleValue()).isCloseTo(7.5, offset(0.001));

        assertThat(q1.getVariance().doubleValue()).isCloseTo(4.12, offset(0.01));
        assertThat(q2.getVariance().doubleValue()).isCloseTo(4.12, offset(0.01));
        assertThat(q3.getVariance().doubleValue()).isCloseTo(4.12, offset(0.01));
        assertThat(q4.getVariance().doubleValue()).isCloseTo(4.12, offset(0.01));
    }

    @Test
    void testIsNan() {
        DoubleSummaryAggregator ag = new DoubleSummaryAggregator();
        assertThat(ag.isNan(-1.0)).isFalse();
        assertThat(ag.isNan(0.0)).isFalse();
        assertThat(ag.isNan(23.0)).isFalse();
        assertThat(ag.isNan(Double.MAX_VALUE)).isFalse();
        assertThat(ag.isNan(Double.MIN_VALUE)).isFalse();
        assertThat(ag.isNan(Double.NaN)).isTrue();
    }

    @Test
    void testIsInfinite() {
        DoubleSummaryAggregator ag = new DoubleSummaryAggregator();
        assertThat(ag.isInfinite(-1.0)).isFalse();
        assertThat(ag.isInfinite(0.0)).isFalse();
        assertThat(ag.isInfinite(23.0)).isFalse();
        assertThat(ag.isInfinite(Double.MAX_VALUE)).isFalse();
        assertThat(ag.isInfinite(Double.MIN_VALUE)).isFalse();
        assertThat(ag.isInfinite(Double.POSITIVE_INFINITY)).isTrue();
        assertThat(ag.isInfinite(Double.NEGATIVE_INFINITY)).isTrue();
    }

    @Test
    void testMean() {
        assertThat(summarize(0.0, 100.0).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0.0, 0.0, 100.0).getMean()).isCloseTo(33.333333, offset(0.00001));
        assertThat(summarize(0.0, 0.0, 100.0, 100.0).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0.0, 100.0, null).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize().getMean()).isNull();
    }

    @Test
    void testSum() throws Exception {
        assertThat(summarize(0.0, 100.0).getSum().doubleValue()).isCloseTo(100.0, offset(0.0));
        assertThat(summarize(1.0, 2.0, 3.0, 4.0, 5.0).getSum().doubleValue())
                .isCloseTo(15, offset(0.0));
        assertThat(summarize(-100.0, 0.0, 100.0, null).getSum().doubleValue())
                .isCloseTo(0, offset(0.0));
        assertThat(summarize(-10.0, 100.0, null).getSum().doubleValue()).isCloseTo(90, offset(0.0));
        assertThat(summarize().getSum()).isNull();
    }

    @Test
    void testMax() {
        assertThat(summarize(-1000.0, 0.0, 1.0, 50.0, 999.0, 1001.0).getMax().doubleValue())
                .isCloseTo(1001.0, offset(0.0));
        assertThat(
                        summarize(
                                        1.0, 8.0, 7.0, 6.0, 9.0, 10.0, 2.0, 3.0, 5.0, 0.0, 11.0,
                                        -2.0, 3.0)
                                .getMax()
                                .doubleValue())
                .isCloseTo(11.0, offset(0.0));
        assertThat(
                        summarize(
                                        1.0, 8.0, 7.0, 6.0, 9.0, null, 10.0, 2.0, 3.0, 5.0, null,
                                        0.0, 11.0, -2.0, 3.0)
                                .getMax()
                                .doubleValue())
                .isCloseTo(11.0, offset(0.0));
        assertThat(summarize(-1.0, -8.0, -7.0, null, -11.0).getMax().doubleValue())
                .isCloseTo(-1.0, offset(0.0));
        assertThat(summarize().getMax()).isNull();
    }

    @Test
    void testMin() {
        assertThat(summarize(-1000.0, 0.0, 1.0, 50.0, 999.0, 1001.0).getMin().doubleValue())
                .isCloseTo(-1000, offset(0.0));
        assertThat(
                        summarize(
                                        1.0, 8.0, 7.0, 6.0, 9.0, 10.0, 2.0, 3.0, 5.0, 0.0, 11.0,
                                        -2.0, 3.0)
                                .getMin()
                                .doubleValue())
                .isCloseTo(-2.0, offset(0.0));
        assertThat(
                        summarize(
                                        1.0, 8.0, 7.0, 6.0, 9.0, null, 10.0, 2.0, 3.0, 5.0, null,
                                        0.0, 11.0, -2.0, 3.0)
                                .getMin()
                                .doubleValue())
                .isCloseTo(-2.0, offset(0.0));
        assertThat(summarize().getMin()).isNull();
    }

    @Test
    void testCounts() {
        NumericColumnSummary<Double> summary =
                summarize(
                        Double.NaN,
                        1.0,
                        null,
                        123.0,
                        -44.00001,
                        Double.POSITIVE_INFINITY,
                        55.0,
                        Double.NEGATIVE_INFINITY,
                        Double.NEGATIVE_INFINITY,
                        null,
                        Double.NaN);
        assertThat(summary.getTotalCount()).isEqualTo(11);
        assertThat(summary.getNullCount()).isEqualTo(2);
        assertThat(summary.getNonNullCount()).isEqualTo(9);
        assertThat(summary.getMissingCount()).isEqualTo(7);
        assertThat(summary.getNonMissingCount()).isEqualTo(4);
        assertThat(summary.getNanCount()).isEqualTo(2);
        assertThat(summary.getInfinityCount()).isEqualTo(3);
    }

    /**
     * Helper method for summarizing a list of values.
     *
     * <p>This method breaks the rule of "testing only one thing" by aggregating and combining a
     * bunch of different ways.
     */
    protected NumericColumnSummary<Double> summarize(Double... values) {
        return new AggregateCombineHarness<
                Double, NumericColumnSummary<Double>, DoubleSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Double> result1, NumericColumnSummary<Double> result2) {
                assertThat(result2.getMin()).isCloseTo(result1.getMin(), offset(0.0));
                assertThat(result2.getMax()).isCloseTo(result1.getMax(), offset(0.0));
                assertThat(result2.getMean()).isCloseTo(result1.getMean(), offset(1e-12d));
                assertThat(result2.getVariance()).isCloseTo(result1.getVariance(), offset(1e-9d));
                assertThat(result2.getStandardDeviation())
                        .isCloseTo(result1.getStandardDeviation(), offset(1e-12d));
            }
        }.summarize(values);
    }
}
