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
import org.apache.flink.types.LongValue;

import org.junit.Assert;

/**
 * Tests for {@link ValueSummaryAggregator.LongValueSummaryAggregator}.
 */
public class LongValueSummaryAggregatorTest extends LongSummaryAggregatorTest {

	/**
	 * Helper method for summarizing a list of values.
	 */
	@Override
	protected NumericColumnSummary<Long> summarize(Long... values) {

		LongValue[] longValues = new LongValue[values.length];
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				longValues[i] = new LongValue(values[i]);
			}
		}

		return new AggregateCombineHarness<LongValue, NumericColumnSummary<Long>, ValueSummaryAggregator.LongValueSummaryAggregator>() {

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
		}.summarize(longValues);
	}

}
