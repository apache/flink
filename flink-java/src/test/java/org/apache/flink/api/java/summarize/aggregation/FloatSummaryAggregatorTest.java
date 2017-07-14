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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link FloatSummaryAggregator}.
 */
public class FloatSummaryAggregatorTest {

	/**
	 * Use some values from Anscombe's Quartet for testing.
	 *
	 * <p>There was no particular reason to use these except they have known means and variance.
	 *
	 * <p>https://en.wikipedia.org/wiki/Anscombe%27s_quartet
	 */
	@Test
	public void testAnscomesQuartetXValues() throws Exception {

		final Float[] q1x = { 10.0f, 8.0f, 13.0f, 9.0f, 11.0f, 14.0f, 6.0f, 4.0f, 12.0f, 7.0f, 5.0f };
		final Float[] q4x = { 8.0f, 8.0f, 8.0f, 8.0f, 8.0f, 8.0f, 8.0f, 19.0f, 8.0f, 8.0f, 8.0f };

		NumericColumnSummary<Float> q1 = summarize(q1x);
		NumericColumnSummary<Float> q4 = summarize(q4x);

		Assert.assertEquals(9.0, q1.getMean().doubleValue(), 0.0f);
		Assert.assertEquals(9.0, q4.getMean().doubleValue(), 0.0f);

		Assert.assertEquals(11.0, q1.getVariance().doubleValue(), 1e-10d);
		Assert.assertEquals(11.0, q4.getVariance().doubleValue(), 1e-10d);

		double stddev = Math.sqrt(11.0f);
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
		final Float[] q1y = { 8.04f, 6.95f, 7.58f, 8.81f, 8.33f, 9.96f, 7.24f, 4.26f, 10.84f, 4.82f, 5.68f };
		final Float[] q2y = { 9.14f, 8.14f, 8.74f, 8.77f, 9.26f, 8.1f, 6.13f, 3.1f, 9.13f, 7.26f, 4.74f };
		final Float[] q3y = { 7.46f, 6.77f, 12.74f, 7.11f, 7.81f, 8.84f, 6.08f, 5.39f, 8.15f, 6.42f, 5.73f };
		final Float[] q4y = { 6.58f, 5.76f, 7.71f, 8.84f, 8.47f, 7.04f, 5.25f, 12.5f, 5.56f, 7.91f, 6.89f };

		NumericColumnSummary<Float> q1 = summarize(q1y);
		NumericColumnSummary<Float> q2 = summarize(q2y);
		NumericColumnSummary<Float> q3 = summarize(q3y);
		NumericColumnSummary<Float> q4 = summarize(q4y);

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
		FloatSummaryAggregator ag = new FloatSummaryAggregator();
		Assert.assertFalse(ag.isNan(-1.0f));
		Assert.assertFalse(ag.isNan(0.0f));
		Assert.assertFalse(ag.isNan(23.0f));
		Assert.assertFalse(ag.isNan(Float.MAX_VALUE));
		Assert.assertFalse(ag.isNan(Float.MIN_VALUE));
		Assert.assertTrue(ag.isNan(Float.NaN));
	}

	@Test
	public void testIsInfinite() throws Exception {
		FloatSummaryAggregator ag = new FloatSummaryAggregator();
		Assert.assertFalse(ag.isInfinite(-1.0f));
		Assert.assertFalse(ag.isInfinite(0.0f));
		Assert.assertFalse(ag.isInfinite(23.0f));
		Assert.assertFalse(ag.isInfinite(Float.MAX_VALUE));
		Assert.assertFalse(ag.isInfinite(Float.MIN_VALUE));
		Assert.assertTrue(ag.isInfinite(Float.POSITIVE_INFINITY));
		Assert.assertTrue(ag.isInfinite(Float.NEGATIVE_INFINITY));
	}

	@Test
	public void testMean() throws Exception {
		Assert.assertEquals(50.0, summarize(0.0f, 100.0f).getMean(), 0.0);
		Assert.assertEquals(33.333333, summarize(0.0f, 0.0f, 100.0f).getMean(), 0.00001);
		Assert.assertEquals(50.0, summarize(0.0f, 0.0f, 100.0f, 100.0f).getMean(), 0.0);
		Assert.assertEquals(50.0, summarize(0.0f, 100.0f, null).getMean(), 0.0);
		Assert.assertNull(summarize().getMean());
	}

	@Test
	public void testSum() throws Exception {
		Assert.assertEquals(100.0, summarize(0.0f, 100.0f).getSum().floatValue(), 0.0f);
		Assert.assertEquals(15, summarize(1.0f, 2.0f, 3.0f, 4.0f, 5.0f).getSum().floatValue(), 0.0f);
		Assert.assertEquals(0, summarize(-100.0f, 0.0f, 100.0f, null).getSum().floatValue(), 0.0f);
		Assert.assertEquals(90, summarize(-10.0f, 100.0f, null).getSum().floatValue(), 0.0f);
		Assert.assertNull(summarize().getSum());
	}

	@Test
	public void testMax() throws Exception {
		Assert.assertEquals(1001.0f, summarize(-1000.0f, 0.0f, 1.0f, 50.0f, 999.0f, 1001.0f).getMax().floatValue(), 0.0f);
		Assert.assertEquals(11.0f, summarize(1.0f, 8.0f, 7.0f, 6.0f, 9.0f, 10.0f, 2.0f, 3.0f, 5.0f, 0.0f, 11.0f, -2.0f, 3.0f).getMax().floatValue(), 0.0f);
		Assert.assertEquals(11.0f, summarize(1.0f, 8.0f, 7.0f, 6.0f, 9.0f, null, 10.0f, 2.0f, 3.0f, 5.0f, null, 0.0f, 11.0f, -2.0f, 3.0f).getMax().floatValue(), 0.0f);
		Assert.assertEquals(-2.0f, summarize(-8.0f, -7.0f, -6.0f, -9.0f, null, -10.0f, null, -2.0f).getMax().floatValue(), 0.0f);
		Assert.assertNull(summarize().getMax());
	}

	@Test
	public void testMin() throws Exception {
		Assert.assertEquals(-1000, summarize(-1000.0f, 0.0f, 1.0f, 50.0f, 999.0f, 1001.0f).getMin().floatValue(), 0.0f);
		Assert.assertEquals(-2.0f, summarize(1.0f, 8.0f, 7.0f, 6.0f, 9.0f, 10.0f, 2.0f, 3.0f, 5.0f, 0.0f, 11.0f, -2.0f, 3.0f).getMin().floatValue(), 0.0f);
		Assert.assertEquals(-2.0f, summarize(1.0f, 8.0f, 7.0f, 6.0f, 9.0f, null, 10.0f, 2.0f, 3.0f, 5.0f, null, 0.0f, 11.0f, -2.0f, 3.0f).getMin().floatValue(), 0.0f);
		Assert.assertNull(summarize().getMin());
	}

	/**
	 * Helper method for summarizing a list of values.
	 *
	 * <p>This method breaks the rule of "testing only one thing" by aggregating
	 * and combining a bunch of different ways.
	 */
	protected NumericColumnSummary<Float> summarize(Float... values) {

		return new AggregateCombineHarness<Float, NumericColumnSummary<Float>, FloatSummaryAggregator>() {

			@Override
			protected void compareResults(NumericColumnSummary<Float> result1, NumericColumnSummary<Float> result2) {
				Assert.assertEquals(result1.getMin(), result2.getMin(), 0.0f);
				Assert.assertEquals(result1.getMax(), result2.getMax(), 0.0f);
				Assert.assertEquals(result1.getMean(), result2.getMean(), 1e-12d);
				Assert.assertEquals(result1.getVariance(), result2.getVariance(), 1e-9d);
				Assert.assertEquals(result1.getStandardDeviation(), result2.getStandardDeviation(), 1e-12d);
			}

		}.summarize(values);
	}

}
