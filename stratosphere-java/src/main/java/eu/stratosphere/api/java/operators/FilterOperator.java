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
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.operators.translation.PlanFilterOperator;

/**
 *
 * @param <IN> The type of the data set filtered by the operator.
 */
public class FilterOperator<IN> extends SingleInputUdfOperator<IN, IN, FilterOperator<IN>> {
	
	protected final FilterFunction<IN> function;
	
	
	public FilterOperator(DataSet<IN> input, FilterFunction<IN> function) {
		super(input, input.getType());
		
		if (function == null) {
			throw new NullPointerException("Filter function must not be null.");
		}
		
		this.function = function;
	}

	@Override
	protected Operator translateToDataFlow(Operator input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		PlanFilterOperator<IN> po = new PlanFilterOperator<IN>(function, name, getInputType());
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
