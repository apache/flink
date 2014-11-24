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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Test;

public class AverageAggregationFunctionTest {

	private AverageAggregationFunction<Integer> function = new AverageAggregationFunction<Integer>(-1);
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldComputeAverageAsComposite() {
		// given
		// setup sum delegate
		int sumIntermediatePos = uniqueInt();
		SumAggregationFunction<Integer> sumDelegate = mock(SumAggregationFunction.class);
		given(sumDelegate.getIntermediatePosition()).willReturn(sumIntermediatePos);

		// setup count delegate
		int countIntermediatePos = uniqueInt();
		CountAggregationFunction countDelegate = mock(CountAggregationFunction.class);
		given(countDelegate.getIntermediatePosition()).willReturn(countIntermediatePos);

		// setup reduced intermediate tuple with sum and count
		Long count = uniqueLong(1, 100);
		Long sum = uniqueLong(count, 200);
		Tuple tuple = mock(Tuple.class);
		given(tuple.getField(countIntermediatePos)).willReturn(count);
		given(tuple.getField(sumIntermediatePos)).willReturn(sum);
		
		// setup composite average aggregation function
		int inputFieldPosition = uniqueInt();
		function = new AverageAggregationFunction<Integer>(inputFieldPosition, sumDelegate, countDelegate);
		
		// when
		Double actual = function.computeComposite(tuple);

		// then
		assertThat(actual, closeTo(sum.doubleValue() / count.doubleValue(), 0));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldUseSumAndCountIntermediatesAndSetInputFieldPosition() {
		// given
		int field = uniqueInt();
		AverageAggregationFunction<Integer> function = new AverageAggregationFunction<Integer>(field); 

		// when
		List<AggregationFunction<?, ?>> intermediates = function.getIntermediateAggregationFunctions(); 

		// then
		assertThat(intermediates.size(), is(2));
		assertThat(intermediates.get(0), instanceOf(SumAggregationFunction.class));
		assertThat(intermediates.get(1), instanceOf(CountAggregationFunction.class));
		AggregationFunction<?, ?> sumIntermediate = (SumAggregationFunction<Integer>) intermediates.get(0);
		assertThat(sumIntermediate.getInputPosition(), is(field));
		// count sets its initial value, no need to copy input position
	}
	
	@Test
	public void shouldReturnAverage() {
		// given
		int[] values = {1, 2, 3, 4};

		// when
		function.setInputType(BasicTypeInfo.INT_TYPE_INFO);
		function.initialize();
		for (int value : values) {
			function.aggregate(value);
		}
		double actual = function.getAggregate();

		// then
		assertThat(actual, is(2.5));
	}
	
	@Test
	public void shouldReset() {
		// given
		int value1 = uniqueInt();

		// when
		function.setInputType(BasicTypeInfo.INT_TYPE_INFO);
		function.initialize();
		function.aggregate(value1);
		function.initialize();
		double actual = function.getAggregate();

		// then
		assertThat(actual, is(Double.NaN));
	}
	
}
