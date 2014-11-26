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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Test;

public class AggregationMapIntermediateUdfTest {
	
	private AggregationMapIntermediateUdf<Tuple, Tuple> udf;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void shouldInitializeIntermediateTuple() throws Exception {
		// given
		// setup an input tuple with a fixed value at a random field position
		Object inputValue = mock(Object.class);
		Tuple inputTuple = mock(Tuple.class);
		int inputPos = uniqueInt(0, Tuple.MAX_ARITY - 1);
		given(inputTuple.getField(inputPos)).willReturn(inputValue);

		// setup an aggregation functions with a returning an initial value for the intermediate tuple
		Object outputValue1 = mock(Object.class);
		int intermediatePos = uniqueInt(0, Tuple.MAX_ARITY - 1, new int[] { inputPos } );
		AggregationFunction function = mock(AggregationFunction.class);
		given(function.getInputPosition()).willReturn(inputPos);
		given(function.getIntermediatePosition()).willReturn(intermediatePos);
		given(function.initializeIntermediate(inputValue)).willReturn(outputValue1);
		AggregationFunction[] functions = { function };

		// setup creation of output tuple
		Tuple intermediateTuple = mock(Tuple.class);
		udf = spy(new AggregationMapIntermediateUdf(functions));
		given(udf.createResultTuple()).willReturn(intermediateTuple);
		
		// when
		Tuple actual = udf.map(inputTuple);

		// then
		assertThat(actual, is(intermediateTuple));
		verify(intermediateTuple).setField(outputValue1, intermediatePos);
	}
	
}
