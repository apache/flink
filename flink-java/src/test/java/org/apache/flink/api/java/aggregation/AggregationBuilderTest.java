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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AggregationBuilderTest {

	private final static String[] DATASET_FUNCTIONS =  { "count", "min", "max", "sum", "average" };
	private final static String[] GROUPING_FUNCTIONS = { "min", "max", "sum", "count", "average", "key" };
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void shouldCallAggregateForDataSet() throws Exception {
		// given
		DataSet input = mock(DataSet.class);
		AggregationBuilder builder = new AggregationBuilder(input);
		AggregationOperatorFactory factory = mock(AggregationOperatorFactory.class);
		builder.setAggregationOperatorFactory(factory);
		AggregationOperator aggregationOperator = mock(AggregationOperator.class);
		given(factory.aggregate(eq(input), any(AggregationFunction[].class))).willReturn(aggregationOperator);
		String name1 = selectFunction(DATASET_FUNCTIONS);
		String name2 = selectFunction(DATASET_FUNCTIONS);
		
		// when
		int pos1 = callFunction(builder, name1);
		int pos2 = callFunction(builder, name2);
		AggregationOperator actual = builder.aggregate();
		
		// then
		assertThat(actual, is(aggregationOperator));
		ArgumentCaptor<AggregationFunction[]> functionCaptor = ArgumentCaptor.forClass(AggregationFunction[].class);
		verify(factory).aggregate(eq(input), functionCaptor.capture());
		AggregationFunction[] functions = functionCaptor.getValue();
		assertThat(functions.length, is(2));
		verifyAggregationFunctionCall(functions[0], name1, pos1);
		verifyAggregationFunctionCall(functions[1], name2, pos2);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void shouldCallAggregateForUnsortedGrouping() throws Exception {
		// given
		UnsortedGrouping grouping = mock(UnsortedGrouping.class);
		AggregationBuilder builder = new AggregationBuilder(grouping);
		AggregationOperatorFactory factory = mock(AggregationOperatorFactory.class);
		builder.setAggregationOperatorFactory(factory);
		AggregationOperator aggregationOperator = mock(AggregationOperator.class);
		given(factory.aggregate(eq(grouping), any(AggregationFunction[].class))).willReturn(aggregationOperator);
		String name1 = selectFunction(GROUPING_FUNCTIONS);
		String name2 = selectFunction(GROUPING_FUNCTIONS);
		
		// when
		int pos1 = callFunction(builder, name1);
		int pos2 = callFunction(builder, name2);
		AggregationOperator actual = builder.aggregate();
		
		// then
		assertThat(actual, is(aggregationOperator));
		ArgumentCaptor<AggregationFunction[]> functionCaptor = ArgumentCaptor.forClass(AggregationFunction[].class);
		verify(factory).aggregate(eq(grouping), functionCaptor.capture());
		AggregationFunction[] functions = functionCaptor.getValue();
		assertThat(functions.length, is(2));
		verifyAggregationFunctionCall(functions[0], name1, pos1);
		verifyAggregationFunctionCall(functions[1], name2, pos2);
	}

	private String selectFunction(String[] functions) {
		int i = uniqueInt(0, functions.length);
		String function = functions[i];
		return function;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private int callFunction(AggregationBuilder builder, String function) throws Exception {
		int field = uniqueInt();
		Class<?>[] parameterTypes = { int.class };
		Object[] parameters = { field };
		if ("count".equals(function) ) {
			parameterTypes = new Class<?>[0];
			parameters = new Object[0];
			field = 0;
		}
		Class<AggregationBuilder> clazz = (Class<AggregationBuilder>) builder.getClass();
		Method method = clazz.getMethod(function, parameterTypes);
		method.invoke(builder, parameters);
		return field;
	}

	@SuppressWarnings("rawtypes")
	private void verifyAggregationFunctionCall(AggregationFunction function, String name, int field) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append("(");
		sb.append(field);
		sb.append(")");
		String expected = sb.toString();
		assertThat(function.toString(), is(expected));
	}

}
