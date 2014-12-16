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

import java.util.Arrays;

import org.apache.commons.lang3.Validate;
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
		int arity = setupOutputArity();
		int intermediatePos = setupOutputPosition(arity, inputPos);
		Object outputValue1 = mock(Object.class);
		AggregationFunction function = mock(AggregationFunction.class);
		given(function.getInputPosition()).willReturn(inputPos);
		given(function.getIntermediatePosition()).willReturn(intermediatePos);
		given(function.initializeIntermediate(inputValue)).willReturn(outputValue1);
		AggregationFunction[] functions = setupDummyFunctions(arity);
		functions[intermediatePos] = function;

		// setup creation of output tuple
		udf = new AggregationMapIntermediateUdf(functions);
		
		// when
		Tuple actual = udf.map(inputTuple);

		// then
		assertThat(actual.getField(intermediatePos), is(outputValue1));
	}
	
	public static int setupOutputArity() {
		return setupOutputArity(2);
	}
	
	// arity must be at least 2 because output position 0 is overwritten by dummy function
	public static int setupOutputArity(int min) {
		Validate.inclusiveBetween(2, Tuple.MAX_ARITY - 1, min, "Arity must be between 2 and Tuple.MAX_ARITY - 1");
		int arity = uniqueInt(min, Tuple.MAX_ARITY - 1);
		return arity;
	}

	// setup an array of dummy functions in order to randomize the output position
	@SuppressWarnings("rawtypes")
	public static AggregationFunction[] setupDummyFunctions(int arity) {
		Validate.inclusiveBetween(2, Tuple.MAX_ARITY - 1, arity, "Arity must be between 2 and Tuple.MAX_ARITY - 1");
		AggregationFunction dummy = mock(AggregationFunction.class);
		AggregationFunction[] functions = new AggregationFunction[arity];
		Arrays.fill(functions, dummy);
		return functions;
	}
	

	// skip output position 0 because this is set by the dummy functions
	public static int setupOutputPosition(int arity, int... exclude) {
		int outputPos = uniqueInt(1, arity - 1, exclude);
		return outputPos;
	}

}
