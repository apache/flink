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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link CallContext} with unknown data types.
 */
@Internal
public final class UnknownCallContext implements CallContext {

	private static final DataType NULL = DataTypes.NULL();

	private final DataTypeLookup lookup;

	private final String name;

	private final FunctionDefinition functionDefinition;

	private final List<DataType> argumentDataTypes;

	public UnknownCallContext(
			DataTypeLookup lookup,
			String name,
			FunctionDefinition functionDefinition,
			int argumentCount) {
		this.lookup = lookup;
		this.name = name;
		this.functionDefinition = functionDefinition;
		this.argumentDataTypes = new AbstractList<DataType>() {
			@Override
			public DataType get(int index) {
				return NULL;
			}

			@Override
			public int size() {
				return argumentCount;
			}
		};
	}

	@Override
	public DataTypeLookup getDataTypeLookup() {
		return lookup;
	}

	@Override
	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	@Override
	public boolean isArgumentLiteral(int pos) {
		return false;
	}

	@Override
	public boolean isArgumentNull(int pos) {
		return false;
	}

	@Override
	public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
		return Optional.empty();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public List<DataType> getArgumentDataTypes() {
		return argumentDataTypes;
	}

	@Override
	public Optional<DataType> getOutputDataType() {
		return Optional.empty();
	}
}
