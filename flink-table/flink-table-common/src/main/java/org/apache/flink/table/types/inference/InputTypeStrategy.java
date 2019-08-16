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

import org.apache.flink.table.types.DataType;

/**
 * Strategy to infer unknown types of the inputs of a function definition.
 */
public interface InputTypeStrategy {

	/**
	 * Infers any unknown operand types.
	 *
	 * @param callContext description of the call being analyzed
	 * @param outputType  the type known or inferred for the result of the call
	 * @param inputTypes  receives the inferred types for all operands
	 */
	void inferTypes(CallContext callContext, DataType outputType, DataType[] inputTypes);
}
