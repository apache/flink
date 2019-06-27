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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.TestCallContext;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for validation of input arguments of {@link BuiltInFunctionDefinition}s.
 */
@RunWith(Parameterized.class)
public class BuiltinFunctionsValidationTest {

	private static final DataType BOOLEAN_NOT_NULL = DataTypes.BOOLEAN().notNull();

	@Parameters(name = "{index}: {0}=[parameters: {1}]")
	public static List<Object[]> functionCalls() {
		return Arrays.asList(
			new Object[][]{
				{BuiltInFunctionDefinitions.AND, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},
				{BuiltInFunctionDefinitions.AND, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL, BOOLEAN_NOT_NULL)},

				{BuiltInFunctionDefinitions.OR, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},
				{BuiltInFunctionDefinitions.OR, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL, BOOLEAN_NOT_NULL)},

				{BuiltInFunctionDefinitions.NOT, Collections.singletonList(DataTypes.INT())},
				{BuiltInFunctionDefinitions.NOT, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},

				{BuiltInFunctionDefinitions.IS_TRUE, Collections.singletonList(DataTypes.INT())},
				{BuiltInFunctionDefinitions.IS_TRUE, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},

				{BuiltInFunctionDefinitions.IS_FALSE, Collections.singletonList(DataTypes.INT())},
				{BuiltInFunctionDefinitions.IS_FALSE, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},

				{BuiltInFunctionDefinitions.IS_NOT_TRUE, Collections.singletonList(DataTypes.INT())},
				{BuiltInFunctionDefinitions.IS_NOT_TRUE, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},

				{BuiltInFunctionDefinitions.IS_NOT_FALSE, Collections.singletonList(DataTypes.INT())},
				{BuiltInFunctionDefinitions.IS_NOT_FALSE, Arrays.asList(DataTypes.INT(), BOOLEAN_NOT_NULL)},
			});
	}

	@Parameter
	public BuiltInFunctionDefinition functionDefinition;

	@Parameter(1)
	public List<DataType> dataTypes;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInvalidArguments() {
		thrown.expect(ValidationException.class);

		TypeInference typeInference = functionDefinition.getTypeInference();

		TypeInferenceUtil.runTypeInference(
			typeInference,
			TestCallContext.forCall(functionDefinition, dataTypes.toArray(new DataType[0])));
	}
}
