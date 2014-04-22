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

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.operators.translation.PlanFlatMapOperator;
import eu.stratosphere.api.java.typeutils.TypeExtractor;

/**
 *
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class FlatMapOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, FlatMapOperator<IN, OUT>> {
	
	protected final FlatMapFunction<IN, OUT> function;
	
	
	public FlatMapOperator(DataSet<IN> input, FlatMapFunction<IN, OUT> function) {
		super(input, TypeExtractor.getFlatMapReturnTypes(function, input.getType()));
		
		if (function == null)
			throw new NullPointerException("FlatMap function must not be null.");
		
		this.function = function;
	}

	@Override
	protected Operator translateToDataFlow(Operator input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		PlanFlatMapOperator<IN, OUT> pfmo = new PlanFlatMapOperator<IN, OUT>(function, name, getInputType(), getResultType());
		// set input
		pfmo.setInput(input);
		
		return pfmo;
	}
}
