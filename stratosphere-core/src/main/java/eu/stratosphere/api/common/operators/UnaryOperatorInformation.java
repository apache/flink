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

package eu.stratosphere.api.common.operators;

import eu.stratosphere.types.TypeInformation;

/**
 *  A class for holding information about a single input operator, such as input/output TypeInformation.
 *
 * @param <IN> Output type of the input operator
 * @param <OUT> Output type of the records output by the operator described by this information
 */
public class UnaryOperatorInformation<IN, OUT> extends OperatorInformation<OUT> {

	/**
	 * Input Type of the operator
	 */
	protected final TypeInformation<IN> inputType;

	/**
	 * @param inputType Input type of first input
	 * @param outputType The output type of the operator
	 */
	public UnaryOperatorInformation(TypeInformation<IN> inputType, TypeInformation<OUT> outputType) {
		super(outputType);
		this.inputType = inputType;
	}

	public TypeInformation<IN> getInputType() {
		return inputType;
	}
}
