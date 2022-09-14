/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0f (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0f
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

/** Tests for {@link FloatSummaryAggregator}. */
class FloatSummaryAggregatorTest {

    /**
     * Use some values from Anscombe's Quartet for testing.
     *
     * <p>There was no particular reason to use these except they have known means and variance.
     *
     * <p>https://en.wikipedia.org/wiki/Anscombe%27s_quartet
     */
    @Test
    void testAnscomesQuartetXValues() {

        final Float[] q1x = {10.0f, 8.0f, 13.0f, 9.0f, 11.0f, 14.0f, 6.0f, 4.0f, 12.0f, 7.0f, 5.0f};
        final Float[] q4x = {8.0f, 8.0f, 8.0f, 8.0f, 8.0f, 8.0f, 8.0f, 19.0f, 8.0f, 8.0f, 8.0f};

        NumericColumnSummary<Float> q1 = summarize(q1x);
        NumericColumnSummary<Float> q4 = summarize(q4x);

        assertThat(q1.getMean().doubleValue()).isCloseTo(9.0, offset(0.0));
        assertThat(q4.getMean().doubleValue()).isCloseTo(9.0, offset(0.0));

        assertThat(q1.getVariance().doubleValue()).isCloseTo(11.0, offset(1e-10d));
        assertThat(q4.getVariance().doubleValue()).isCloseTo(11.0, offset(1e-10d));

        double stddev = Math.sqrt(11.0f);
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
        final Float[] q1y = {
            8.04f, 6.95f, 7.58f, 8.81f, 8.33f, 9.96f, 7.24f, 4.26f, 10.84f, 4.82f, 5.68f
        };
        final Float[] q2y = {
            9.14f, 8.14f, 8.74f, 8.77f, 9.26f, 8.1f, 6.13f, 3.1f, 9.13f, 7.26f, 4.74f
        };
        final Float[] q3y = {
            7.46f, 6.77f, 12.74f, 7.11f, 7.81f, 8.84f, 6.08f, 5.39f, 8.15f, 6.42f, 5.73f
        };
        final Float[] q4y = {
            6.58f, 5.76f, 7.71f, 8.84f, 8.47f, 7.04f, 5.25f, 12.5f, 5.56f, 7.91f, 6.89f
        };

        NumericColumnSummary<Float> q1 = summarize(q1y);
        NumericColumnSummary<Float> q2 = summarize(q2y);
        NumericColumnSummary<Float> q3 = summarize(q3y);
        NumericColumnSummary<Float> q4 = summarize(q4y);

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
        FloatSummaryAggregator ag = new FloatSummaryAggregator();
        assertThat(ag.isNan(-1.0f)).isFalse();
        assertThat(ag.isNan(0.0f)).isFalse();
        assertThat(ag.isNan(23.0f)).isFalse();
        assertThat(ag.isNan(Float.MAX_VALUE)).isFalse();
        assertThat(ag.isNan(Float.MIN_VALUE)).isFalse();
        assertThat(ag.isNan(Float.NaN)).isTrue();
    }

    @Test
    void testIsInfinite() {
        FloatSummaryAggregator ag = new FloatSummaryAggregator();
        assertThat(ag.isInfinite(-1.0f)).isFalse();
        assertThat(ag.isInfinite(0.0f)).isFalse();
        assertThat(ag.isInfinite(23.0f)).isFalse();
        assertThat(ag.isInfinite(Float.MAX_VALUE)).isFalse();
        assertThat(ag.isInfinite(Float.MIN_VALUE)).isFalse();
        assertThat(ag.isInfinite(Float.POSITIVE_INFINITY)).isTrue();
        assertThat(ag.isInfinite(Float.NEGATIVE_INFINITY)).isTrue();
    }

    @Test
    void testMean() {
        assertThat(summarize(0.0f, 100.0f).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0.0f, 0.0f, 100.0f).getMean()).isCloseTo(33.333333, offset(0.00001));
        assertThat(summarize(0.0f, 0.0f, 100.0f, 100.0f).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize(0.0f, 100.0f, null).getMean()).isCloseTo(50.0, offset(0.0));
        assertThat(summarize().getMean()).isNull();
    }

    @Test
    void testSum() throws Exception {
        assertThat(summarize(0.0f, 100.0f).getSum().floatValue()).isCloseTo(100.0f, offset(0.0f));
        assertThat(summarize(1.0f, 2.0f, 3.0f, 4.0f, 5.0f).getSum().floatValue())
                .isCloseTo(15, offset(0.0f));
        assertThat(summarize(-100.0f, 0.0f, 100.0f, null).getSum().floatValue())
                .isCloseTo(0, offset(0.0f));
        assertThat(summarize(-10.0f, 100.0f, null).getSum().floatValue())
                .isCloseTo(90, offset(0.0f));
        assertThat(summarize().getSum()).isNull();
    }

    @Test
    void testMax() {
        assertThat(summarize(-1000.0f, 0.0f, 1.0f, 50.0f, 999.0f, 1001.0f).getMax().floatValue())
                .isCloseTo(1001.0f, offset(0.0f));
        assertThat(
                        summarize(
                                        1.0f, 8.0f, 7.0f, 6.0f, 9.0f, 10.0f, 2.0f, 3.0f, 5.0f, 0.0f,
                                        11.0f, -2.0f, 3.0f)
                                .getMax()
                                .floatValue())
                .isCloseTo(11.0f, offset(0.0f));
        assertThat(
                        summarize(
                                        1.0f, 8.0f, 7.0f, 6.0f, 9.0f, null, 10.0f, 2.0f, 3.0f, 5.0f,
                                        null, 0.0f, 11.0f, -2.0f, 3.0f)
                                .getMax()
                                .floatValue())
                .isCloseTo(11.0f, offset(0.0f));
        assertThat(
                        summarize(-8.0f, -7.0f, -6.0f, -9.0f, null, -10.0f, null, -2.0f)
                                .getMax()
                                .floatValue())
                .isCloseTo(-2.0f, offset(0.0f));
        assertThat(summarize().getMax()).isNull();
    }

    @Test
    void testMin() {
        assertThat(summarize(-1000.0f, 0.0f, 1.0f, 50.0f, 999.0f, 1001.0f).getMin().floatValue())
                .isCloseTo(-1000, offset(0.0f));
        assertThat(
                        summarize(
                                        1.0f, 8.0f, 7.0f, 6.0f, 9.0f, 10.0f, 2.0f, 3.0f, 5.0f, 0.0f,
                                        11.0f, -2.0f, 3.0f)
                                .getMin()
                                .floatValue())
                .isCloseTo(-2.0f, offset(0.0f));
        assertThat(
                        summarize(
                                        1.0f, 8.0f, 7.0f, 6.0f, 9.0f, null, 10.0f, 2.0f, 3.0f, 5.0f,
                                        null, 0.0f, 11.0f, -2.0f, 3.0f)
                                .getMin()
                                .floatValue())
                .isCloseTo(-2.0f, offset(0.0f));
        assertThat(summarize().getMin()).isNull();
    }

    /**
     * Helper method for summarizing a list of values.
     *
     * <p>This method breaks the rule of "testing only one thing" by aggregating and combining a
     * bunch of different ways.
     */
    protected NumericColumnSummary<Float> summarize(Float... values) {

        return new AggregateCombineHarness<
                Float, NumericColumnSummary<Float>, FloatSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Float> result1, NumericColumnSummary<Float> result2) {
                assertThat(result2.getMin()).isCloseTo(result1.getMin(), offset(0.0f));
                assertThat(result2.getMax()).isCloseTo(result1.getMax(), offset(0.0f));
                assertThat(result2.getMean()).isCloseTo(result1.getMean(), offset(1e-12d));
                assertThat(result2.getVariance()).isCloseTo(result1.getVariance(), offset(1e-9d));
                assertThat(result2.getStandardDeviation())
                        .isCloseTo(result1.getStandardDeviation(), offset(1e-12d));
            }
        }.summarize(values);
    }
}
