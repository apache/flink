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

import static org.apache.flink.api.java.aggregation.AggregationMapIntermediateUdfTest.setupDummyFunctions;
import static org.apache.flink.api.java.aggregation.AggregationMapIntermediateUdfTest.setupOutputArity;
import static org.apache.flink.api.java.aggregation.AggregationMapIntermediateUdfTest.setupOutputPosition;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Test;

public class AggregationReduceUdfTest {
		
	@SuppressWarnings("rawtypes")
	@Test
	public void shouldReduceIntermediateTuple() throws Exception {
		// given
		// setup 2 input tuples; randomize position of values
		// arity must be at least 3 because of dummy functions;
		int arity = setupOutputArity(3);
		int pos1 = setupOutputPosition(arity);
		int pos2 = setupOutputPosition(arity, pos1);
		Object v1 = mock(Object.class);
		Object v2 = mock(Object.class);
		Object v3 = mock(Object.class);
		Object v4 = mock(Object.class);
		Tuple t1 = createInputTupleWithValueAtField(pos1, v1, pos2, v2);
		Tuple t2 = createInputTupleWithValueAtField(pos1, v3, pos2, v4);

		// setup 2 aggregation functions reducing the input values
		Object o1 = mock(Object.class);
		Object o2 = mock(Object.class);
		AggregationFunction[] functions = setupDummyFunctions(arity);
		functions[pos1] = createAggregationFunctionWithReduceOutputValue(pos1, v1, v3, o1); 
		functions[pos2] = createAggregationFunctionWithReduceOutputValue(pos2, v2, v4, o2);

		// create reducer udf
		AggregationReduceUdf<Tuple> udf = new AggregationReduceUdf<Tuple>(functions);
		
		// when
		Tuple actual = udf.reduce(t1, t2);
		
		// then
		assertThat(actual.getField(pos1), is(o1));
		assertThat(actual.getField(pos2), is(o2));
	}

	private Tuple createInputTupleWithValueAtField(int pos1, Object v1, int pos2, Object v2) {
		Tuple tuple = mock(Tuple.class);
		given(tuple.getField(pos1)).willReturn(v1);
		given(tuple.getField(pos2)).willReturn(v2);
		return tuple;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AggregationFunction createAggregationFunctionWithReduceOutputValue(
			int pos, Object v1, Object v2, Object output) {
		AggregationFunction function = mock(AggregationFunction.class);
		given(function.getIntermediatePosition()).willReturn(pos);
		given(function.reduce(v1, v2)).willReturn(output);
		return function;
	}

}
