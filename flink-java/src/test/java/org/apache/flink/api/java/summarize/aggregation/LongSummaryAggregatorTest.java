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
 * Tests for {@link LongSummaryAggregator}.
 */
public class LongSummaryAggregatorTest {

	@Test
	public void testIsNan() throws Exception {
		LongSummaryAggregator ag = new LongSummaryAggregator();
		// always false for Long
		Assert.assertFalse(ag.isNan(-1L));
		Assert.assertFalse(ag.isNan(0L));
		Assert.assertFalse(ag.isNan(23L));
		Assert.assertFalse(ag.isNan(Long.MAX_VALUE));
		Assert.assertFalse(ag.isNan(Long.MIN_VALUE));
		Assert.assertFalse(ag.isNan(null));
	}

	@Test
	public void testIsInfinite() throws Exception {
		LongSummaryAggregator ag = new LongSummaryAggregator();
		// always false for Long
		Assert.assertFalse(ag.isInfinite(-1L));
		Assert.assertFalse(ag.isInfinite(0L));
		Assert.assertFalse(ag.isInfinite(23L));
		Assert.assertFalse(ag.isInfinite(Long.MAX_VALUE));
		Assert.assertFalse(ag.isInfinite(Long.MIN_VALUE));
		Assert.assertFalse(ag.isInfinite(null));
	}

	@Test
	public void testMean() throws Exception {
		Assert.assertEquals(50.0, summarize(0L, 100L).getMean(), 0.0);
		Assert.assertEquals(33.333333, summarize(0L, 0L, 100L).getMean(), 0.00001);
		Assert.assertEquals(50.0, summarize(0L, 0L, 100L, 100L).getMean(), 0.0);
		Assert.assertEquals(50.0, summarize(0L, 100L, null).getMean(), 0.0);
		Assert.assertNull(summarize().getMean());
	}

	@Test
	public void testSum() throws Exception {
		Assert.assertEquals(100L, summarize(0L, 100L).getSum().longValue());
		Assert.assertEquals(15L, summarize(1L, 2L, 3L, 4L, 5L).getSum().longValue());
		Assert.assertEquals(0L, summarize(-100L, 0L, 100L, null).getSum().longValue());
		Assert.assertEquals(90L, summarize(-10L, 100L, null).getSum().longValue());
		Assert.assertNull(summarize().getSum());
	}

	@Test
	public void testMax() throws Exception {
		Assert.assertEquals(1001L, summarize(-1000L, 0L, 1L, 50L, 999L, 1001L).getMax().longValue());
		Assert.assertEquals(11L, summarize(1L, 8L, 7L, 6L, 9L, 10L, 2L, 3L, 5L, 0L, 11L, -2L, 3L).getMax().longValue());
		Assert.assertEquals(11L, summarize(1L, 8L, 7L, 6L, 9L, null, 10L, 2L, 3L, 5L, null, 0L, 11L, -2L, 3L).getMax().longValue());
		Assert.assertNull(summarize().getMax());
	}

	@Test
	public void testMin() throws Exception {
		Assert.assertEquals(-1000L, summarize(-1000L, 0L, 1L, 50L, 999L, 1001L).getMin().longValue());
		Assert.assertEquals(-2L, summarize(1L, 8L, 7L, 6L, 9L, 10L, 2L, 3L, 5L, 0L, 11L, -2L, 3L).getMin().longValue());
		Assert.assertEquals(-2L, summarize(1L, 8L, 7L, 6L, 9L, null, 10L, 2L, 3L, 5L, null, 0L, 11L, -2L, 3L).getMin().longValue());
		Assert.assertNull(summarize().getMin());
	}

	/**
	 * Helper method for summarizing a list of values.
	 */
	protected NumericColumnSummary<Long> summarize(Long... values) {
		return new AggregateCombineHarness<Long, NumericColumnSummary<Long>, LongSummaryAggregator>() {

			@Override
			protected void compareResults(NumericColumnSummary<Long> result1, NumericColumnSummary<Long> result2) {

				Assert.assertEquals(result1.getTotalCount(), result2.getTotalCount());
				Assert.assertEquals(result1.getNullCount(), result2.getNullCount());
				Assert.assertEquals(result1.getMissingCount(), result2.getMissingCount());
				Assert.assertEquals(result1.getNonMissingCount(), result2.getNonMissingCount());
				Assert.assertEquals(result1.getInfinityCount(), result2.getInfinityCount());
				Assert.assertEquals(result1.getNanCount(), result2.getNanCount());

				Assert.assertEquals(result1.containsNull(), result2.containsNull());
				Assert.assertEquals(result1.containsNonNull(), result2.containsNonNull());

				Assert.assertEquals(result1.getMin().longValue(), result2.getMin().longValue());
				Assert.assertEquals(result1.getMax().longValue(), result2.getMax().longValue());
				Assert.assertEquals(result1.getSum().longValue(), result2.getSum().longValue());
				Assert.assertEquals(result1.getMean().doubleValue(), result2.getMean().doubleValue(), 1e-12d);
				Assert.assertEquals(result1.getVariance().doubleValue(), result2.getVariance().doubleValue(), 1e-9d);
				Assert.assertEquals(result1.getStandardDeviation().doubleValue(), result2.getStandardDeviation().doubleValue(), 1e-12d);
			}
		}.summarize(values);
	}

}
