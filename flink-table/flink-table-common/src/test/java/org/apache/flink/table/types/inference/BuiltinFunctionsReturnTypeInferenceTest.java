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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.TestCallContext;

import org.hamcrest.CoreMatchers;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThat;

/**
 * Tests for return type inference of {@link BuiltInFunctionDefinition}s.
 */
@RunWith(Parameterized.class)
public class BuiltinFunctionsReturnTypeInferenceTest {

	private static final DataType BOOLEAN_NOT_NULL = DataTypes.BOOLEAN().notNull();
	private static final DataType BOOLEAN_NULLABLE = DataTypes.BOOLEAN().notNull();

	@Parameters(name = "{index}: {0}=[parameters: {1}, return type: {2}]")
	public static List<Object[]> functionCalls() {
		return Arrays.asList(
			new Object[][]{
				{BuiltInFunctionDefinitions.AND, Arrays.asList(BOOLEAN_NOT_NULL, BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.AND, Arrays.asList(BOOLEAN_NULLABLE, BOOLEAN_NOT_NULL), BOOLEAN_NULLABLE},
				{BuiltInFunctionDefinitions.AND, Arrays.asList(BOOLEAN_NULLABLE, BOOLEAN_NULLABLE), BOOLEAN_NULLABLE},

				{BuiltInFunctionDefinitions.OR, Arrays.asList(BOOLEAN_NOT_NULL, BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.OR, Arrays.asList(BOOLEAN_NULLABLE, BOOLEAN_NOT_NULL), BOOLEAN_NULLABLE},
				{BuiltInFunctionDefinitions.OR, Arrays.asList(BOOLEAN_NULLABLE, BOOLEAN_NULLABLE), BOOLEAN_NULLABLE},

				{BuiltInFunctionDefinitions.NOT, Collections.singletonList(BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.NOT, Collections.singletonList(BOOLEAN_NULLABLE), BOOLEAN_NULLABLE},

				{BuiltInFunctionDefinitions.IS_TRUE, Collections.singletonList(BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.IS_TRUE, Collections.singletonList(BOOLEAN_NULLABLE), BOOLEAN_NOT_NULL},

				{BuiltInFunctionDefinitions.IS_FALSE, Collections.singletonList(BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.IS_FALSE, Collections.singletonList(BOOLEAN_NULLABLE), BOOLEAN_NOT_NULL},

				{BuiltInFunctionDefinitions.IS_NOT_TRUE, Collections.singletonList(BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.IS_NOT_TRUE, Collections.singletonList(BOOLEAN_NULLABLE), BOOLEAN_NOT_NULL},

				{BuiltInFunctionDefinitions.IS_NOT_FALSE, Collections.singletonList(BOOLEAN_NOT_NULL), BOOLEAN_NOT_NULL},
				{BuiltInFunctionDefinitions.IS_NOT_FALSE, Collections.singletonList(BOOLEAN_NULLABLE), BOOLEAN_NOT_NULL}
			});
	}

	@Parameter
	public BuiltInFunctionDefinition functionDefinition;

	@Parameter(1)
	public List<DataType> dataTypes;

	@Parameter(2)
	public DataType returnType;

	@Test
	public void testReturnType() {
		TypeInference typeInference = functionDefinition.getTypeInference();

		TypeInferenceUtil.Result result = TypeInferenceUtil.runTypeInference(
			typeInference,
			TestCallContext.forCall(functionDefinition, dataTypes.toArray(new DataType[0])));

		assertThat(result, returns(returnType));
	}

	private Matcher<TypeInferenceUtil.Result> returns(DataType resultType) {
		return new FeatureMatcher<TypeInferenceUtil.Result, DataType>(
			CoreMatchers.equalTo(resultType),
			"output type of type inference result",
			"output type") {
			@Override
			protected DataType featureValueOf(TypeInferenceUtil.Result actual) {
				return actual.getOutputDataType();
			}
		};
	}
}
