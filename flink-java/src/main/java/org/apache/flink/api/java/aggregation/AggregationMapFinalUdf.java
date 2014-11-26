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
 * Map UDF that maps the intermediate result of an aggregation to the
 * final result.
 * 
 * <p>For simple aggregation functions the value in the respective tuple
 * field is copied. For composite aggregations, the final aggregation value
 * is computed. Fields holding group keys that are not requested by the
 * user are dropped.
 * 
 * @param <IN> Type of the intermediate tuple.
 * @param <OUT> Aggregation output type.
 */
public class AggregationMapFinalUdf<IN extends Tuple, OUT extends Tuple> extends AggregationUdfBase<OUT> implements MapFunction<IN, OUT>{
	private static final long serialVersionUID = 6792518274017437410L;

	private AggregationFunction<?, ?>[] functions;

	public AggregationMapFinalUdf(AggregationFunction<?, ?>[] functions) {
		super(functions.length);
		this.functions = functions;
	}

	@Override
	public OUT map(IN input) throws Exception {
		OUT result = createResultTuple();
		for (AggregationFunction<?, ?> function : functions) {
			int outputPosition = function.getOutputPosition();
			Object value = null;

			// compute composite value 
			if (function instanceof CompositeAggregationFunction) {
				CompositeAggregationFunction<?, ?> composite = (CompositeAggregationFunction<?, ?>) function;
				value = composite.computeComposite(input);

			// copy value for simple aggregations
			} else {
				int intermediatePosition = function.getIntermediatePosition();
				value = input.getField(intermediatePosition);
			}
			result.setField(value, outputPosition);
		}
		return result;
	}

}
