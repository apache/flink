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
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.operators.translation.PlanCrossOperator;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 */
public class CrossOperator<I1, I2, OUT> 
	extends TwoInputUdfOperator<I1, I2, OUT, CrossOperator<I1, I2, OUT>> {
	
	private final CrossFunction<I1, I2, OUT> function;

	protected CrossOperator(DataSet<I1> input1, DataSet<I2> input2,
							CrossFunction<I1, I2, OUT> function,
							TypeInformation<OUT> returnType)
	{
		super(input1, input2, returnType);

		this.function = function;
	}
	
	@Override
	protected Operator translateToDataFlow(Operator input1, Operator input2) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		PlanCrossOperator<I1, I2, OUT> po = new PlanCrossOperator<I1, I2, OUT>(function, name, getInput1Type(), getInput2Type(), getResultType());
		// set inputs
		po.setFirstInput(input1);
		po.setSecondInput(input2);
		// set dop
		po.setDegreeOfParallelism(this.getParallelism());
		
		return po;
	}
	

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	public static final class CrossOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		public CrossOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			if (input1 == null || input2 == null)
				throw new NullPointerException();
			
			this.input1 = input1;
			this.input2 = input2;
		}

		public <R> CrossOperator<I1, I2, R> with(CrossFunction<I1, I2, R> function) {
			TypeInformation<R> returnType = TypeExtractor.getCrossReturnTypes(function, input1.getType(), input2.getType());
			return new CrossOperator<I1, I2, R>(input1, input2, function, returnType);
		}
	}
}
