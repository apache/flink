/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.common.functions.GenericFlatMap;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.FlatMapOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.typeutils.TypeExtractor;

/**
 * This operator represents the application of a "flatMap" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class FlatMapOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, FlatMapOperator<IN, OUT>> {
	
	protected final FlatMapFunction<IN, OUT> function;
	
	
	public FlatMapOperator(DataSet<IN> input, FlatMapFunction<IN, OUT> function) {
		super(input, TypeExtractor.getFlatMapReturnTypes(function, input.getType()));
		
		this.function = function;
		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	@Override
	protected eu.stratosphere.api.common.operators.base.FlatMapOperatorBase<IN, OUT, GenericFlatMap<IN,OUT>> translateToDataFlow(Operator<IN> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		FlatMapOperatorBase<IN, OUT, GenericFlatMap<IN, OUT>> po = new FlatMapOperatorBase<IN, OUT, GenericFlatMap<IN, OUT>>(function, new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType()), name);
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
