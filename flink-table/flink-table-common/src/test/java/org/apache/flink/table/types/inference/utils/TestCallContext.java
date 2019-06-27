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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A helper test implementation of {@link CallContext}.
 */
public class TestCallContext implements CallContext {
	private static final String DEFAULT_NAME = "function";
	private final String name;
	private final List<DataType> argumentTypes;
	private final @Nullable FunctionDefinition functionDefinition;

	private TestCallContext(
		String name,
		List<DataType> argumentTypes,
		@Nullable FunctionDefinition functionDefinition) {
		this.argumentTypes = argumentTypes;
		this.name = name;
		this.functionDefinition = functionDefinition;
	}

	public static CallContext forCall(String name, DataType... parameters) {
		return new TestCallContext(name, Arrays.asList(parameters), null);
	}

	public static CallContext forCall(DataType... parameters) {
		return new TestCallContext(DEFAULT_NAME, Arrays.asList(parameters), null);
	}

	public static CallContext forCall(BuiltInFunctionDefinition definition, DataType... parameters) {
		return new TestCallContext(definition.getName(), Arrays.asList(parameters), definition);
	}

	@Override
	public List<DataType> getArgumentDataTypes() {
		return argumentTypes;
	}

	@Override
	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	@Override
	public boolean isArgumentLiteral(int pos) {
		throw new UnsupportedOperationException("Unsupported in tests yet");
	}

	@Override
	public boolean isArgumentNull(int pos) {
		throw new UnsupportedOperationException("Unsupported in tests yet");
	}

	@Override
	public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
		throw new UnsupportedOperationException("Unsupported in tests yet");
	}

	@Override
	public String getName() {
		return name;
	}

}
