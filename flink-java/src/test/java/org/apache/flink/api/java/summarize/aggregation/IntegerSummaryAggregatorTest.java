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

/**
 * Tests for {@link IntegerSummaryAggregator}.
 */
public class IntegerSummaryAggregatorTest {

	@Test
	public void testIsNan() throws Exception {
		IntegerSummaryAggregator ag = new IntegerSummaryAggregator();
		// always false for Integer
		Assert.assertFalse(ag.isNan(-1));
		Assert.assertFalse(ag.isNan(0));
		Assert.assertFalse(ag.isNan(23));
		Assert.assertFalse(ag.isNan(Integer.MAX_VALUE));
		Assert.assertFalse(ag.isNan(Integer.MIN_VALUE));
		Assert.assertFalse(ag.isNan(null));
	}

	@Test
	public void testIsInfinite() throws Exception {
		IntegerSummaryAggregator ag = new IntegerSummaryAggregator();
		// always false for Integer
		Assert.assertFalse(ag.isInfinite(-1));
		Assert.assertFalse(ag.isInfinite(0));
		Assert.assertFalse(ag.isInfinite(23));
		Assert.assertFalse(ag.isInfinite(Integer.MAX_VALUE));
		Assert.assertFalse(ag.isInfinite(Integer.MIN_VALUE));
		Assert.assertFalse(ag.isInfinite(null));
	}

	@Test
	public void testMean() throws Exception {
		Assert.assertEquals(50.0, summarize(0, 100).getMean(), 0.0);
		Assert.assertEquals(33.333333, summarize(0, 0, 100).getMean(), 0.00001);
		Assert.assertEquals(50.0, summarize(0, 0, 100, 100).getMean(), 0.0);
		Assert.assertEquals(50.0, summarize(0, 100, null).getMean(), 0.0);
		Assert.assertNull(summarize().getMean());
	}

	@Test
	public void testSum() throws Exception {
		Assert.assertEquals(100, summarize(0, 100).getSum().intValue());
		Assert.assertEquals(15, summarize(1, 2, 3, 4, 5).getSum().intValue());
		Assert.assertEquals(0, summarize(-100, 0, 100, null).getSum().intValue());
		Assert.assertEquals(90, summarize(-10, 100, null).getSum().intValue());
		Assert.assertNull(summarize().getSum());
	}

	@Test
	public void testMax() throws Exception {
		Assert.assertEquals(1001, summarize(-1000, 0, 1, 50, 999, 1001).getMax().intValue());
		Assert.assertEquals(0, summarize(Integer.MIN_VALUE, -1000, 0).getMax().intValue());
		Assert.assertEquals(11, summarize(1, 8, 7, 6, 9, 10, 2, 3, 5, 0, 11, -2, 3).getMax().intValue());
		Assert.assertEquals(11, summarize(1, 8, 7, 6, 9, null, 10, 2, 3, 5, null, 0, 11, -2, 3).getMax().intValue());
		Assert.assertNull(summarize().getMax());
	}

	@Test
	public void testMin() throws Exception {
		Assert.assertEquals(-1000, summarize(-1000, 0, 1, 50, 999, 1001).getMin().intValue());
		Assert.assertEquals(Integer.MIN_VALUE, summarize(Integer.MIN_VALUE, -1000, 0).getMin().intValue());
		Assert.assertEquals(-2, summarize(1, 8, 7, 6, 9, 10, 2, 3, 5, 0, 11, -2, 3).getMin().intValue());
		Assert.assertEquals(-2, summarize(1, 8, 7, 6, 9, null, 10, 2, 3, 5, null, 0, 11, -2, 3).getMin().intValue());
		Assert.assertNull(summarize().getMin());
	}

	/**
	 * Helper method for summarizing a list of values.
	 */
	protected NumericColumnSummary<Integer> summarize(Integer... values) {

		return new AggregateCombineHarness<Integer, NumericColumnSummary<Integer>, IntegerSummaryAggregator>() {

			@Override
			protected void compareResults(NumericColumnSummary<Integer> result1, NumericColumnSummary<Integer> result2) {

				Assert.assertEquals(result1.getTotalCount(), result2.getTotalCount());
				Assert.assertEquals(result1.getNullCount(), result2.getNullCount());
				Assert.assertEquals(result1.getMissingCount(), result2.getMissingCount());
				Assert.assertEquals(result1.getNonMissingCount(), result2.getNonMissingCount());
				Assert.assertEquals(result1.getInfinityCount(), result2.getInfinityCount());
				Assert.assertEquals(result1.getNanCount(), result2.getNanCount());

				Assert.assertEquals(result1.containsNull(), result2.containsNull());
				Assert.assertEquals(result1.containsNonNull(), result2.containsNonNull());

				Assert.assertEquals(result1.getMin().intValue(), result2.getMin().intValue());
				Assert.assertEquals(result1.getMax().intValue(), result2.getMax().intValue());
				Assert.assertEquals(result1.getSum().intValue(), result2.getSum().intValue());
				Assert.assertEquals(result1.getMean().doubleValue(), result2.getMean().doubleValue(), 1e-12d);
				Assert.assertEquals(result1.getVariance().doubleValue(), result2.getVariance().doubleValue(), 1e-9d);
				Assert.assertEquals(result1.getStandardDeviation().doubleValue(), result2.getStandardDeviation().doubleValue(), 1e-12d);
			}
		}.summarize(values);
	}

}
