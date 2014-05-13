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
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.operators.translation.PlanFilterOperator;

/**
 * This operator represents the application of a "filter" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <T> The type of the data set filtered by the operator.
 */
public class FilterOperator<T> extends SingleInputUdfOperator<T, T, FilterOperator<T>> {
	
	protected final FilterFunction<T> function;
	
	
	public FilterOperator(DataSet<T> input, FilterFunction<T> function) {
		super(input, input.getType());
		
		if (function == null) {
			throw new NullPointerException("Filter function must not be null.");
		}
		
		this.function = function;
		extractSemanticAnnotationsFromUdf(function.getClass());
	}
	
	@Override
	protected eu.stratosphere.api.common.operators.base.FilterOperatorBase<T, GenericFlatMap<T,T>> translateToDataFlow(Operator<T> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		PlanFilterOperator<T> po = new PlanFilterOperator<T>(function, name, getInputType());
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
