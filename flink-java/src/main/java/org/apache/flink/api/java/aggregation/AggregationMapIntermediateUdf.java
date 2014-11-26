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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Map UDF that maps input tuples to an intermediate tuple on which the
 * aggregation is performed.
 * 
 * <p>
 * For each aggregation function {@code initializeIntermediate}
 * is called with the value of the input tuple at the input
 * position of the aggregation function.
 * 
 * @param <IN> Type of the intermediate tuple.
 * @param <OUT> Aggregation output type.
 */
public class AggregationMapIntermediateUdf<IN extends Tuple, OUT extends Tuple> extends AggregationUdfBase<OUT> implements MapFunction<IN, OUT>{
	private static final long serialVersionUID = 3326127349510588787L;

	private AggregationFunction<?, ?>[] functions;
	
	public AggregationMapIntermediateUdf(AggregationFunction<?, ?>[] functions) {
		super(functions.length);
		this.functions = functions;
	}
	
	@Override
	public OUT map(IN value) throws Exception {
		OUT result = createResultTuple();
		for (AggregationFunction<?, ?> function : functions) {
			processAggregationFunction(function, value, result);
		}
		return result;
	}
	
	// helper function to capture the generic types of AggregationFunction
	private <T, R> void processAggregationFunction(
			AggregationFunction<T, R> function, IN value, OUT result) {
		int inputPosition = function.getInputPosition();
		int intermediatePosition = function.getIntermediatePosition();
		T inputValue = value.getField(inputPosition);
		R outputValue = function.initializeIntermediate(inputValue);
		result.setField(outputValue, intermediatePosition);
	}

}
