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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * UDF computing a list of {@link AggregationFunction}'s inside a
 * Reducer.
 * 
 * @param <T> Input type of {@code reduce}.
 */
public class AggregationReduceUdf<IN extends Tuple> extends AggregationUdfBase<IN> implements ReduceFunction<IN> {
	private static final long serialVersionUID = 5563658873455921533L;

	private AggregationFunction<?, ?>[] functions;
	
	public AggregationReduceUdf(AggregationFunction<?, ?>... functions) {
		super(functions.length);
		this.functions = functions;
	}
	
	@Override
	public IN reduce(IN tuple1, IN tuple2) throws Exception {
		IN result = createResultTuple();
		for (AggregationFunction<?, ?> function : functions) {
			processAggregationFunction(function, tuple1, tuple2, result);
		}
		return result;
	}

	// helper function to capture the generic types of AggregationFunction
	private <T, R> void processAggregationFunction(
			AggregationFunction<T, R> function, IN tuple1, IN tuple2, IN result) {
		int intermediatePosition = function.getIntermediatePosition();
		R value1 = tuple1.getField(intermediatePosition);
		R value2 = tuple2.getField(intermediatePosition);
		R reduced = function.reduce(value1, value2);
		result.setField(reduced, intermediatePosition);
	}
}