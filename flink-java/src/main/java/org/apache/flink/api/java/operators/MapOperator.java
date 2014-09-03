/**
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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.flink.api.java.DataSet;

/**
 * This operator represents the application of a "map" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 * 
 * @see org.apache.flink.api.common.functions.MapFunction
 */
public class MapOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, MapOperator<IN, OUT>> {
	
	protected final MapFunction<IN, OUT> function;
	
	
	public MapOperator(DataSet<IN> input, MapFunction<IN, OUT> function) {

		super(input, TypeExtractor.getMapReturnTypes(function, input.getType()));
		
		this.function = function;
		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	@Override
	protected org.apache.flink.api.common.operators.base.MapOperatorBase<IN, OUT, MapFunction<IN, OUT>> translateToDataFlow(Operator<IN> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		MapOperatorBase<IN, OUT, MapFunction<IN, OUT>> po = new MapOperatorBase<IN, OUT, MapFunction<IN, OUT>>(function, new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType()), name);
		// set input
		po.setInput(input);
		// set dop
		if(this.getParallelism() > 0) {
			// use specified dop
			po.setDegreeOfParallelism(this.getParallelism());
		} else {
			// if no dop has been specified, use dop of input operator to enable chaining
			po.setDegreeOfParallelism(input.getDegreeOfParallelism());
		}
		
		return po;
	}
	
}
