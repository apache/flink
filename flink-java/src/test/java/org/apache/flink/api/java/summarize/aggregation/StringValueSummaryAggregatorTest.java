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

import org.apache.flink.api.java.summarize.StringColumnSummary;
import org.apache.flink.types.StringValue;

import org.junit.Assert;

/**
 * Tests for {@link ValueSummaryAggregator.StringValueSummaryAggregator}.
 */
public class StringValueSummaryAggregatorTest extends StringSummaryAggregatorTest {

	/**
	 * Helper method for summarizing a list of values.
	 *
	 * <p>This method breaks the rule of "testing only one thing" by aggregating and combining
	 * a bunch of different ways.
	 */
	@Override
	protected StringColumnSummary summarize(String... values) {

		StringValue[] stringValues = new StringValue[values.length];
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				stringValues[i] = new StringValue(values[i]);
			}
		}

		return new AggregateCombineHarness<StringValue, StringColumnSummary, ValueSummaryAggregator.StringValueSummaryAggregator>(){

			@Override
			protected void compareResults(StringColumnSummary result1, StringColumnSummary result2) {
				Assert.assertEquals(result1.getEmptyCount(), result2.getEmptyCount());
				Assert.assertEquals(result1.getMaxLength(), result2.getMaxLength());
				Assert.assertEquals(result1.getMinLength(), result2.getMinLength());
				if (result1.getMeanLength() == null) {
					Assert.assertEquals(result1.getMeanLength(), result2.getMeanLength());
				}
				else {
					Assert.assertEquals(result1.getMeanLength().doubleValue(), result2.getMeanLength().doubleValue(), 1e-5d);
				}

				Assert.assertEquals(result1.getNullCount(), result2.getNullCount());
				Assert.assertEquals(result1.getNonNullCount(), result2.getNonNullCount());
			}

		}.summarize(stringValues);
	}

}
