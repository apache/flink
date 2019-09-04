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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;

/**
 * Strategies for inferring unknown types of the inputs of a function definition.
 *
 * @see InputTypeStrategy
 */
@Internal
public class InputTypeStrategies {

	/**
	 * Creates a input type strategy that set all input types to an explicitly given type.
	 */
	public static InputTypeStrategy explicit(DataType type) {
		return (callContext, outputType, inputTypes) -> Arrays.fill(inputTypes, type);
	}

	/**
	 * Input type-inference strategy where an unknown operand type is derived from the call's
	 * output type.
	 */
	public static final InputTypeStrategy OUTPUT_TYPE = (callContext, outputType, inputTypes) ->
		Arrays.fill(inputTypes, outputType);
}
