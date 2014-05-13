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
import eu.stratosphere.types.TypeInformation;

/**
 * Base class for operations that operates on a single input data set.
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
	
	/**
	 * Gets the data set that this operation uses as its input.
	 * 
	 * @return The data set that this operation uses as its input.
	 */
	public DataSet<IN> getInput() {
		return this.input;
	}
	
	/**
	 * Gets the type information of the data type of the input data set.
	 * This method returns equivalent information as {@code getInput().getType()}.
	 * 
	 * @return The input data type.
	 */
	public TypeInformation<IN> getInputType() {
		return this.input.getType();
	}
	
	/**
	 * Translates this operation to a data flow operator of the common data flow API.
	 * 
	 * @param input The data flow operator that produces this operation's input data.
	 * @return The translated data flow operator.
	 */
	protected abstract eu.stratosphere.api.common.operators.SingleInputOperator<?, OUT, ?> translateToDataFlow(
			eu.stratosphere.api.common.operators.Operator<IN> input);
	
}
