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

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator;
import eu.stratosphere.api.java.operators.translation.UnaryNodeTranslation;
import eu.stratosphere.api.java.typeutils.TypeExtractor;

/**
 *
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class MapOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, MapOperator<IN, OUT>> {
	
	protected final MapFunction<IN, OUT> function;
	
	
	public MapOperator(DataSet<IN> input, MapFunction<IN, OUT> function) {
		super(input, TypeExtractor.getMapReturnTypes(function));
		
		if (function == null)
			throw new NullPointerException("Map function must not be null.");
		
		this.function = function;
	}


	@Override
	protected UnaryNodeTranslation translateToDataFlow() {
		String name = getName() != null ? getName() : function.getClass().getName();
		return new UnaryNodeTranslation(new PlanMapOperator<IN, OUT>(function, name, getInputType(), getResultType()));
	}
}
