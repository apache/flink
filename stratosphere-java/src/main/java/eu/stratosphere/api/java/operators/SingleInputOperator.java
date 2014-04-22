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
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 * @param <IN> The data type of the input data set.
 * @param <OUT> The data type of the returned data set.
 */
public abstract class SingleInputOperator<IN, OUT, O extends SingleInputOperator<IN, OUT, O>> extends Operator<OUT, O> {
	
	private final DataSet<IN> input;
	
	
	protected SingleInputOperator(DataSet<IN> input, TypeInformation<OUT> resultType) {
		super(input.getExecutionEnvironment(), resultType);
		this.input = input;
	}
	
	public DataSet<IN> getInput() {
		return this.input;
	}
	
	public TypeInformation<IN> getInputType() {
		return this.input.getType();
	}
	
	protected abstract eu.stratosphere.api.common.operators.Operator translateToDataFlow(
			eu.stratosphere.api.common.operators.Operator input);
	
}
