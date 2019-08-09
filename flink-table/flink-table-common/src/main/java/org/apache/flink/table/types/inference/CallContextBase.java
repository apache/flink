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
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.Optional;

/**
 * Provides details about the function call for which type inference is performed.
 */
@PublicEvolving
public interface CallContextBase {

	/**
	 * Returns the function definition that defines the function currently being called.
	 */
	FunctionDefinition getFunctionDefinition();

	/**
	 * Returns whether the argument at the given position is a value literal.
	 */
	boolean isArgumentLiteral(int pos);

	/**
	 * Returns {@code true} if the argument at the given position is a literal and {@code null},
	 * {@code false} otherwise.
	 *
	 * <p>Use {@link #isArgumentLiteral(int)} before to check if the argument is actually a literal.
	 */
	boolean isArgumentNull(int pos);

	/**
	 * Returns the literal value of the argument at the given position, given that the argument is a
	 * literal, is not null, and can be expressed as an instance of the provided class.
	 *
	 * <p>Use {@link #isArgumentLiteral(int)} before to check if the argument is actually a literal.
	 */
	<T> Optional<T> getArgumentValue(int pos, Class<T> clazz);

	/**
	 * Returns the function's name usually referencing the function in a catalog.
	 *
	 * <p>Note: The name is meant for debugging purposes only.
	 */
	String getName();
}
