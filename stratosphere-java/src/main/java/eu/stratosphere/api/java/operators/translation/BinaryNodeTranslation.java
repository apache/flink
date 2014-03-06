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
package eu.stratosphere.api.java.operators.translation;

import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SingleInputOperator;



public class BinaryNodeTranslation {
	
	private final SingleInputOperator<?> input1Operator;
	
	private final SingleInputOperator<?> input2Operator;
	
	private final DualInputOperator<?> binaryInputOperator;
	
	private final DualInputOperator<?> outputOperator;

	
	public BinaryNodeTranslation(DualInputOperator<?> operator) {
		this.input1Operator = null;
		this.input2Operator = null;
		this.binaryInputOperator = operator;
		this.outputOperator = operator;
	}
	
	public BinaryNodeTranslation(SingleInputOperator<?> input1, SingleInputOperator<?> input2, DualInputOperator<?>  output) {
		this.input1Operator = input1;
		this.input2Operator = input2;
		this.binaryInputOperator = null;
		this.outputOperator = output;
	}

	
	public void setInput1(Operator op) {
		if (input1Operator == null) {
			binaryInputOperator.setFirstInput(op);
		} else {
			input1Operator.setInput(op);
		}
	}
	
	public void setInput2(Operator op) {
		if (input2Operator == null) {
			binaryInputOperator.setSecondInput(op);
		} else {
			input2Operator.setInput(op);
		}
	}
	
	public DualInputOperator<?> getOutputOperator() {
		return outputOperator;
	}
}
