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
 * Java API operator for union of two data sets
 * 
 * @param <T> The type of the two input data sets and the result data set 
 */
public class UnionOperator<T> extends TwoInputOperator<T, T, T, UnionOperator<T>> {

	/**
	 * Create an operator that produces the union of the two given data sets.
	 * 
	 * @param input1 The first data set to be unioned.
	 * @param input2 The second data set to be unioned.
	 */
	public UnionOperator(DataSet<T> input1, DataSet<T> input2) {
		super(input1, input2, input1.getType());
	}
	 
	/**
	 * Returns the BinaryNodeTranslation of the Union.
	 * 
	 * @param input1 The first input of the union, as a common API operator.
	 * @param input2 The second input of the union, as a common API operator.
	 * @return The common API union operator.
	 */
	@Override
	protected Union translateToDataFlow(Operator input1, Operator input2) {
		return new Union(input1, input2);
	}
}
