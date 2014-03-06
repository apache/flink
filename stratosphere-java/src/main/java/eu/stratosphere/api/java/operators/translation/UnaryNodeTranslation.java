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

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SingleInputOperator;



public class UnaryNodeTranslation {
	
	private final SingleInputOperator<?> inputOperator;
	
	private final SingleInputOperator<?> outputOperator;

	
	public UnaryNodeTranslation(SingleInputOperator<?> operator) {
		this(operator, operator);
	}
	
	public UnaryNodeTranslation(SingleInputOperator<?> inputOperator, SingleInputOperator<?> outputOperator) {
		this.inputOperator = inputOperator;
		this.outputOperator = outputOperator;
	}

	public void setInput(Operator op) {
		inputOperator.setInput(op);
	}
	
	public SingleInputOperator<?> getOutputOperator() {
		return outputOperator;
	}
}
