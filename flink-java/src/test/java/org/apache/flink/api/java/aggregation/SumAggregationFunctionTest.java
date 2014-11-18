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

package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.apache.flink.util.TestHelper.uniqueLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.junit.Test;

public class SumAggregationFunctionTest {

	@Test
	public void shouldReset() {
		shouldResetHelper(BasicTypeInfo.LONG_TYPE_INFO, uniqueLong(), 0L);
		shouldResetHelper(BasicTypeInfo.INT_TYPE_INFO, uniqueInt(), 0);
		shouldResetHelper(BasicTypeInfo.BYTE_TYPE_INFO, (byte) 1, (byte) 0);
		shouldResetHelper(BasicTypeInfo.SHORT_TYPE_INFO, (short) 1, (short) 0);
		shouldResetHelper(BasicTypeInfo.DOUBLE_TYPE_INFO, RandomUtils.nextDouble(0, 10), 0.0);
		shouldResetHelper(BasicTypeInfo.FLOAT_TYPE_INFO, RandomUtils.nextFloat(0, 10), 0.0f);
	}
	
	@Test
	public void shouldReturnSum() {
		shouldReturnSumHelper(BasicTypeInfo.LONG_TYPE_INFO, new Long[] {1L, 2L, 3L}, 6L);
		shouldReturnSumHelper(BasicTypeInfo.INT_TYPE_INFO, new Integer[] {1, 2, 3}, 6);
		shouldReturnSumHelper(BasicTypeInfo.BYTE_TYPE_INFO, new Byte[] {1, 2, 3}, (byte) 6);
		shouldReturnSumHelper(BasicTypeInfo.SHORT_TYPE_INFO, new Short[] {1, 2, 3}, (short) 6);
		shouldReturnSumHelper(BasicTypeInfo.DOUBLE_TYPE_INFO, new Double[] {1.1, 2.2, 3.3}, 6.6);
		shouldReturnSumHelper(BasicTypeInfo.FLOAT_TYPE_INFO, new Float[] {1.1f, 2.2f, 3.3f}, 6.6f);
	}

	private <T extends Number> void shouldResetHelper(BasicTypeInfo<T> type, T value, T expected) {
		// given
		SumAggregationFunction<T> function = new SumAggregationFunction<T>();

		// when
		function.setInputType(type);
		function.initialize();
		function.aggregate(value);
		function.initialize();
		T actual = function.getAggregate();

		// then
		assertThat(actual, is(expected));
	}

	private <T extends Number> void shouldReturnSumHelper(BasicTypeInfo<T> type, T[] values, T expected) {
		// given
		SumAggregationFunction<T> function = new SumAggregationFunction<T>();

		// when
		function.setInputType(type);
		function.initialize();
		for (T value : values) {
			function.aggregate(value);
		}
		T actual = function.getAggregate();

		// then
		assertThat(actual.doubleValue(), closeTo(expected.doubleValue(), 0.001));
	}
	
}
