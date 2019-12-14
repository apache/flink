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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;

/**
 * Validator that checks a single input argument type of a function call.
 */
@PublicEvolving
public interface ArgumentTypeValidator {

	/**
	 * Main logic for validating a single input type. Returns {@code true} if the argument is valid for the
	 * given call, {@code false} otherwise.
	 *
	 * @param callContext provides details about the function call
	 * @param argumentPos argument index in the {@link CallContext}
	 * @param throwOnFailure whether this function is allowed to throw an {@link ValidationException}
	 *                       with a meaningful exception in case the validation is not successful or
	 *                       if this function should simply return {@code false}.
	 */
	boolean validateArgument(CallContext callContext, int argumentPos, boolean throwOnFailure);

	/**
	 * Returns a summary of the function's expected argument at {@code argumentPos}.
	 *
	 * @param functionDefinition the function definition that defines the function currently being called.
	 * @param argumentPos the position within the function call for which the signature should be retrieved
	 */
	Signature.Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos);
}
