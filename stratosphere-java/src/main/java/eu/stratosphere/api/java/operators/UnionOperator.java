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
import eu.stratosphere.api.common.operators.Union;
import eu.stratosphere.api.java.DataSet;

/**
 * Operator for union of two data sets
 * 
 * @param <T> The type of the two input data sets and the Output dataSet 
 */
public class UnionOperator<T> extends TwoInputOperator<T, T, T, UnionOperator<T>> {

	/**
	 * Constructor just calls super constructor.
	 * @param input1
	 * @param input2
	 */
	public UnionOperator(DataSet<T> input1, DataSet<T> input2) {
		super(input1, input2, input1.getType());
	}
	 
	/**
	 * Returns the BinaryNodeTranslation of the Union.
	 * Just return a BinaryNodeTranslation holding a new common.operator Union.
	 * The inputs of the union are automatically set by the {@link OperatorTranslation}.
	 */
	@Override
	protected Operator translateToDataFlow(Operator input1, Operator input2) {
		
		// create operator
		Union u = new Union();
		// set inputs
		u.setFirstInput(input1);
		u.setSecondInput(input2);
		
		return u;
	}
	
}
