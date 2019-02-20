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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.expressions.FunctionType.OTHER;

/**
 * The function definition for user-defined scalar function.
 */
@PublicEvolving
public final class ScalarFunctionDefinition extends FunctionDefinition {

	private final ScalarFunction scalarFunction;

	public ScalarFunctionDefinition(ScalarFunction scalarFunction) {
		super("Scalar Function", OTHER);
		this.scalarFunction = scalarFunction;
	}

	public ScalarFunction getScalarFunction() {
		return scalarFunction;
	}
}
