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

/**
 * @param <IN> The type of the data set made distinct by the operator.
 */
public class DistinctOperator<IN> extends SingleInputOperator<IN, IN, DistinctOperator<IN>> {
	
	@SuppressWarnings("unused")
	private final Keys<IN> keys;
	
	public DistinctOperator(DataSet<IN> input, Keys<IN> keys) {
		super(input, input.getType());
		
		if (keys == null) {
			throw new NullPointerException();
		}
		
		this.keys = keys;
	}

	@Override
	protected eu.stratosphere.api.common.operators.SingleInputOperator<?> translateToDataFlow(Operator input) {
		throw new UnsupportedOperationException("NOT IMPLEMENTED");
	}
}
