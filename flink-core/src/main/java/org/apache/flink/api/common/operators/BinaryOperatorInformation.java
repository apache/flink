/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 *  A class for holding information about a single input operator, such as input/output TypeInformation.
 *
 * @param <IN1> Output type of first input operator
 * @param <IN2> Output type of second input operator
 * @param <OUT> Output type of the records output by the operator described by this information
 */
@Internal
public class BinaryOperatorInformation<IN1, IN2, OUT> extends OperatorInformation<OUT> {

	/**
	 * Input type of the first input
	 */
	protected final TypeInformation<IN1> inputType1;

	/**
	 * Input type of the second input
	 */
	protected final TypeInformation<IN2> inputType2;

	/**
	 * @param inputType1 Input type of first input
	 * @param inputType2 Input type of second input
	 * @param outputType The output type of the operator
	 */
	public BinaryOperatorInformation(TypeInformation<IN1> inputType1, TypeInformation<IN2> inputType2, TypeInformation<OUT> outputType) {
		super(outputType);
		this.inputType1 = inputType1;
		this.inputType2 = inputType2;
	}

	public TypeInformation<IN1> getFirstInputType() {
		return inputType1;
	}

	public TypeInformation<IN2> getSecondInputType() {
		return inputType2;
	}

}
