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

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link DoubleSummaryAggregator}. */
public class DoubleSummaryAggregatorTest {

    /**
     * Use some values from Anscombe's Quartet for testing.
     *
     * <p>There was no particular reason to use these except they have known means and variance.
     *
     * <p>https://en.wikipedia.org/wiki/Anscombe%27s_quartet
     */
    @Test
    public void testAnscomesQuartetXValues() throws Exception {

        final Double[] q1x = {10.0, 8.0, 13.0, 9.0, 11.0, 14.0, 6.0, 4.0, 12.0, 7.0, 5.0};
        final Double[] q4x = {8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 19.0, 8.0, 8.0, 8.0};

        NumericColumnSummary<Double> q1 = summarize(q1x);
        NumericColumnSummary<Double> q4 = summarize(q4x);

        Assert.assertEquals(9.0, q1.getMean().doubleValue(), 0.0);
        Assert.assertEquals(9.0, q4.getMean().doubleValue(), 0.0);

        Assert.assertEquals(11.0, q1.getVariance().doubleValue(), 1e-10d);
        Assert.assertEquals(11.0, q4.getVariance().doubleValue(), 1e-10d);

        double stddev = Math.sqrt(11.0);
        Assert.assertEquals(stddev, q1.getStandardDeviation().doubleValue(), 1e-10d);
        Assert.assertEquals(stddev, q4.getStandardDeviation().doubleValue(), 1e-10d);
    }

    /**
     * Use some values from Anscombe's Quartet for testing.
     *
     * <p>There was no particular reason to use these except they have known means and variance.
     *
     * <p>https://en.wikipedia.org/wiki/Anscombe%27s_quartet
     */
    @Test
    public void testAnscomesQuartetYValues() throws Exception {
        final Double[] q1y = {8.04, 6.95, 7.58, 8.81, 8.33, 9.96, 7.24, 4.26, 10.84, 4.82, 5.68};
        final Double[] q2y = {9.14, 8.14, 8.74, 8.77, 9.26, 8.1, 6.13, 3.1, 9.13, 7.26, 4.74};
        final Double[] q3y = {7.46, 6.77, 12.74, 7.11, 7.81, 8.84, 6.08, 5.39, 8.15, 6.42, 5.73};
        final Double[] q4y = {6.58, 5.76, 7.71, 8.84, 8.47, 7.04, 5.25, 12.5, 5.56, 7.91, 6.89};

        NumericColumnSummary<Double> q1 = summarize(q1y);
        NumericColumnSummary<Double> q2 = summarize(q2y);
        NumericColumnSummary<Double> q3 = summarize(q3y);
        NumericColumnSummary<Double> q4 = summarize(q4y);

        // the y values are have less precisely matching means and variances

        Assert.assertEquals(7.5, q1.getMean().doubleValue(), 0.001);
        Assert.assertEquals(7.5, q2.getMean().doubleValue(), 0.001);
        Assert.assertEquals(7.5, q3.getMean().doubleValue(), 0.001);
        Assert.assertEquals(7.5, q4.getMean().doubleValue(), 0.001);

        Assert.assertEquals(4.12, q1.getVariance().doubleValue(), 0.01);
        Assert.assertEquals(4.12, q2.getVariance().doubleValue(), 0.01);
        Assert.assertEquals(4.12, q3.getVariance().doubleValue(), 0.01);
        Assert.assertEquals(4.12, q4.getVariance().doubleValue(), 0.01);
    }

    @Test
    public void testIsNan() throws Exception {
        DoubleSummaryAggregator ag = new DoubleSummaryAggregator();
        Assert.assertFalse(ag.isNan(-1.0));
        Assert.assertFalse(ag.isNan(0.0));
        Assert.assertFalse(ag.isNan(23.0));
        Assert.assertFalse(ag.isNan(Double.MAX_VALUE));
        Assert.assertFalse(ag.isNan(Double.MIN_VALUE));
        Assert.assertTrue(ag.isNan(Double.NaN));
    }

    @Test
    public void testIsInfinite() throws Exception {
        DoubleSummaryAggregator ag = new DoubleSummaryAggregator();
        Assert.assertFalse(ag.isInfinite(-1.0));
        Assert.assertFalse(ag.isInfinite(0.0));
        Assert.assertFalse(ag.isInfinite(23.0));
        Assert.assertFalse(ag.isInfinite(Double.MAX_VALUE));
        Assert.assertFalse(ag.isInfinite(Double.MIN_VALUE));
        Assert.assertTrue(ag.isInfinite(Double.POSITIVE_INFINITY));
        Assert.assertTrue(ag.isInfinite(Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testMean() throws Exception {
        Assert.assertEquals(50.0, summarize(0.0, 100.0).getMean(), 0.0);
        Assert.assertEquals(33.333333, summarize(0.0, 0.0, 100.0).getMean(), 0.00001);
        Assert.assertEquals(50.0, summarize(0.0, 0.0, 100.0, 100.0).getMean(), 0.0);
        Assert.assertEquals(50.0, summarize(0.0, 100.0, null).getMean(), 0.0);
        Assert.assertNull(summarize().getMean());
    }

    @Test
    public void testSum() throws Exception {
        Assert.assertEquals(100.0, summarize(0.0, 100.0).getSum().doubleValue(), 0.0);
        Assert.assertEquals(15, summarize(1.0, 2.0, 3.0, 4.0, 5.0).getSum().doubleValue(), 0.0);
        Assert.assertEquals(0, summarize(-100.0, 0.0, 100.0, null).getSum().doubleValue(), 0.0);
        Assert.assertEquals(90, summarize(-10.0, 100.0, null).getSum().doubleValue(), 0.0);
        Assert.assertNull(summarize().getSum());
    }

    @Test
    public void testMax() throws Exception {
        Assert.assertEquals(
                1001.0,
                summarize(-1000.0, 0.0, 1.0, 50.0, 999.0, 1001.0).getMax().doubleValue(),
                0.0);
        Assert.assertEquals(
                11.0,
                summarize(1.0, 8.0, 7.0, 6.0, 9.0, 10.0, 2.0, 3.0, 5.0, 0.0, 11.0, -2.0, 3.0)
                        .getMax()
                        .doubleValue(),
                0.0);
        Assert.assertEquals(
                11.0,
                summarize(
                                1.0, 8.0, 7.0, 6.0, 9.0, null, 10.0, 2.0, 3.0, 5.0, null, 0.0, 11.0,
                                -2.0, 3.0)
                        .getMax()
                        .doubleValue(),
                0.0);
        Assert.assertEquals(
                -1.0, summarize(-1.0, -8.0, -7.0, null, -11.0).getMax().doubleValue(), 0.0);
        Assert.assertNull(summarize().getMax());
    }

    @Test
    public void testMin() throws Exception {
        Assert.assertEquals(
                -1000,
                summarize(-1000.0, 0.0, 1.0, 50.0, 999.0, 1001.0).getMin().doubleValue(),
                0.0);
        Assert.assertEquals(
                -2.0,
                summarize(1.0, 8.0, 7.0, 6.0, 9.0, 10.0, 2.0, 3.0, 5.0, 0.0, 11.0, -2.0, 3.0)
                        .getMin()
                        .doubleValue(),
                0.0);
        Assert.assertEquals(
                -2.0,
                summarize(
                                1.0, 8.0, 7.0, 6.0, 9.0, null, 10.0, 2.0, 3.0, 5.0, null, 0.0, 11.0,
                                -2.0, 3.0)
                        .getMin()
                        .doubleValue(),
                0.0);
        Assert.assertNull(summarize().getMin());
    }

    @Test
    public void testCounts() throws Exception {
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
        Assert.assertEquals(11, summary.getTotalCount());
        Assert.assertEquals(2, summary.getNullCount());
        Assert.assertEquals(9, summary.getNonNullCount());
        Assert.assertEquals(7, summary.getMissingCount());
        Assert.assertEquals(4, summary.getNonMissingCount());
        Assert.assertEquals(2, summary.getNanCount());
        Assert.assertEquals(3, summary.getInfinityCount());
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
                Assert.assertEquals(result1.getMin(), result2.getMin(), 0.0);
                Assert.assertEquals(result1.getMax(), result2.getMax(), 0.0);
                Assert.assertEquals(result1.getMean(), result2.getMean(), 1e-12d);
                Assert.assertEquals(result1.getVariance(), result2.getVariance(), 1e-9d);
                Assert.assertEquals(
                        result1.getStandardDeviation(), result2.getStandardDeviation(), 1e-12d);
            }
        }.summarize(values);
    }
}
