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
 * Base class for operations that operates on two input data sets.
 * 
 * @param <IN1> The data type of the first input data set.
 * @param <IN2> The data type of the second input data set.
 * @param <OUT> The data type of the returned data set.
 */
public abstract class TwoInputOperator<IN1, IN2, OUT, O extends TwoInputOperator<IN1, IN2, OUT, O>> extends Operator<OUT, O> {
	
	private final DataSet<IN1> input1;
	private final DataSet<IN2> input2;
	
	
	protected TwoInputOperator(DataSet<IN1> input1, DataSet<IN2> input2, TypeInformation<OUT> resultType) {
		super(input1.getExecutionEnvironment(), resultType);
		
		DataSet.checkSameExecutionContext(input1, input2);
		this.input1 = input1;
		this.input2 = input2;
	}
	
	/**
	 * Gets the data set that this operation uses as its first input.
	 * 
	 * @return The data set that this operation uses as its first input.
	 */
	public DataSet<IN1> getInput1() {
		return this.input1;
	}
	
	/**
	 * Gets the data set that this operation uses as its second input.
	 * 
	 * @return The data set that this operation uses as its second input.
	 */
	public DataSet<IN2> getInput2() {
		return this.input2;
	}
	
	/**
	 * Gets the type information of the data type of the first input data set.
	 * This method returns equivalent information as {@code getInput1().getType()}.
	 * 
	 * @return The first input data type.
	 */
	public TypeInformation<IN1> getInput1Type() {
		return this.input1.getType();
	}
	
	/**
	 * Gets the type information of the data type of the second input data set.
	 * This method returns equivalent information as {@code getInput2().getType()}.
	 * 
	 * @return The second input data type.
	 */
	public TypeInformation<IN2> getInput2Type() {
		return this.input2.getType();
	}
	
	/**
	 * Translates this java API operator into a common API operator with two inputs.
	 * 
	 * @param input1 The first input of the operation, as a common API operator.
	 * @param input2 The second input of the operation, as a common API operator.
	 * @return The created common API operator.
	 */
	protected abstract eu.stratosphere.api.common.operators.DualInputOperator<?, ?, OUT, ?> translateToDataFlow(
			eu.stratosphere.api.common.operators.Operator<IN1> input1, eu.stratosphere.api.common.operators.Operator<IN2> input2);
}
