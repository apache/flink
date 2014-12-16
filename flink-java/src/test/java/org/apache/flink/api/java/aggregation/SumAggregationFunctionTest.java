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
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.junit.Test;

public class SumAggregationFunctionTest {

	@Test
	public void shouldReturnSum() {
		shouldReturnSumHelper(BasicTypeInfo.LONG_TYPE_INFO, 1L, 2L, 3L);
		shouldReturnSumHelper(BasicTypeInfo.INT_TYPE_INFO, 1, 2, 3);
		shouldReturnSumHelper(BasicTypeInfo.BYTE_TYPE_INFO, (byte) 1, (byte) 2, (byte) 3);
		shouldReturnSumHelper(BasicTypeInfo.SHORT_TYPE_INFO, (short) 1, (short) 2, (short) 3);
		shouldReturnSumHelper(BasicTypeInfo.DOUBLE_TYPE_INFO, 1.1, 2.2, 3.3);
		shouldReturnSumHelper(BasicTypeInfo.FLOAT_TYPE_INFO, 1.1f, 2.2f, 3.3f);
	}

	private <T extends Number> void shouldReturnSumHelper(BasicTypeInfo<T> type, T value1, T value2, T expected) {
		// given
		int field = uniqueInt();
		SumAggregationFunction<T> function = new SumAggregationFunction<T>(field);

		// when
		function.setInputType(type);
		T actual = function.reduce(value1, value2);

		// then
		assertThat(actual.doubleValue(), closeTo(expected.doubleValue(), 0.001));
	}
	
}
