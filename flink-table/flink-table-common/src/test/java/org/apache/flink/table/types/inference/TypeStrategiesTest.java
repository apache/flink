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
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.FunctionDefinitionMock;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.inference.TypeStrategies.MISSING;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for built-in {@link TypeStrategies}.
 */
@RunWith(Parameterized.class)
public class TypeStrategiesTest {

	@Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			// missing strategy with arbitrary argument
			TestSpec
				.forStrategy(MISSING)
				.inputTypes(DataTypes.INT())
				.expectErrorMessage("Could not infer an output type for the given arguments."),

			// valid explicit
			TestSpec
				.forStrategy(explicit(DataTypes.BIGINT()))
				.inputTypes()
				.expectDataType(DataTypes.BIGINT()),

			// infer from input
			TestSpec
				.forStrategy(argument(0))
				.inputTypes(DataTypes.INT(), DataTypes.STRING())
				.expectDataType(DataTypes.INT()),

			// infer from not existing input
			TestSpec
				.forStrategy(argument(0))
				.inputTypes()
				.expectErrorMessage("Could not infer an output type for the given arguments."),

			// (INT, BOOLEAN) -> STRING
			TestSpec
				.forStrategy(createMappingTypeStrategy())
				.inputTypes(DataTypes.INT(), DataTypes.BOOLEAN())
				.expectDataType(DataTypes.STRING()),

			// (INT, STRING) -> BOOLEAN
			TestSpec
				.forStrategy(createMappingTypeStrategy())
				.inputTypes(DataTypes.INT(), DataTypes.STRING())
				.expectDataType(DataTypes.BOOLEAN().bridgedTo(boolean.class)),

			// (INT, CHAR(10)) -> BOOLEAN
			// but avoiding casts (mapping actually expects STRING)
			TestSpec
				.forStrategy(createMappingTypeStrategy())
				.inputTypes(DataTypes.INT(), DataTypes.CHAR(10))
				.expectDataType(DataTypes.BOOLEAN().bridgedTo(boolean.class)),

			// invalid mapping strategy
			TestSpec
				.forStrategy(createMappingTypeStrategy())
				.inputTypes(DataTypes.INT(), DataTypes.INT())
				.expectErrorMessage("Could not infer an output type for the given arguments."),

			// invalid return type
			TestSpec
				.forStrategy(TypeStrategies.explicit(DataTypes.NULL()))
				.inputTypes()
				.expectErrorMessage("Could not infer an output type for the given arguments. Untyped NULL received."),

			TestSpec.forStrategy(
				"Infer a row type",
				TypeStrategies.ROW)
				.inputTypes(DataTypes.BIGINT(), DataTypes.STRING())
				.expectDataType(DataTypes.ROW(
					DataTypes.FIELD("f0", DataTypes.BIGINT()),
					DataTypes.FIELD("f1", DataTypes.STRING())).notNull()
				),

			TestSpec.forStrategy(
				"Infer an array type",
				TypeStrategies.ARRAY)
				.inputTypes(DataTypes.BIGINT(), DataTypes.BIGINT())
				.expectDataType(DataTypes.ARRAY(DataTypes.BIGINT()).notNull()),

			TestSpec.forStrategy(
				"Infer a map type",
				TypeStrategies.MAP)
				.inputTypes(DataTypes.BIGINT(), DataTypes.STRING().notNull())
				.expectDataType(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING().notNull()).notNull())
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testTypeStrategy() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectCause(containsCause(new ValidationException(testSpec.expectedErrorMessage)));
		}
		TypeInferenceUtil.Result result = runTypeInference();
		if (testSpec.expectedDataType != null) {
			Assert.assertThat(result.getOutputDataType(), equalTo(testSpec.expectedDataType));
		}
	}

	// --------------------------------------------------------------------------------------------

	private TypeInferenceUtil.Result runTypeInference() {
		final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
		functionDefinitionMock.functionKind = FunctionKind.SCALAR;
		final CallContextMock callContextMock = new CallContextMock();
		callContextMock.functionDefinition = functionDefinitionMock;
		callContextMock.argumentDataTypes = testSpec.inputTypes;
		callContextMock.name = "f";
		callContextMock.outputDataType = Optional.empty();

		final TypeInference typeInference = TypeInference.newBuilder()
			.inputTypeStrategy(InputTypeStrategies.WILDCARD)
			.outputTypeStrategy(testSpec.strategy)
			.build();
		return TypeInferenceUtil.runTypeInference(typeInference, callContextMock, null);
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		private @Nullable final String description;

		private final TypeStrategy strategy;

		private List<DataType> inputTypes;

		private @Nullable DataType expectedDataType;

		private @Nullable String expectedErrorMessage;

		private TestSpec(@Nullable String description, TypeStrategy strategy) {
			this.description = description;
			this.strategy = strategy;
		}

		static TestSpec forStrategy(TypeStrategy strategy) {
			return new TestSpec(null, strategy);
		}

		static TestSpec forStrategy(String description, TypeStrategy strategy) {
			return new TestSpec(description, strategy);
		}

		TestSpec inputTypes(DataType... dataTypes) {
			this.inputTypes = Arrays.asList(dataTypes);
			return this;
		}

		TestSpec expectDataType(DataType expectedDataType) {
			this.expectedDataType = expectedDataType;
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}

		@Override
		public String toString() {
			return description != null ? description : "";
		}
	}

	private static TypeStrategy createMappingTypeStrategy() {
		final Map<InputTypeStrategy, TypeStrategy> mappings = new HashMap<>();
		mappings.put(
			InputTypeStrategies.sequence(
				InputTypeStrategies.explicit(DataTypes.INT()),
				InputTypeStrategies.explicit(DataTypes.STRING())),
			TypeStrategies.explicit(DataTypes.BOOLEAN().bridgedTo(boolean.class)));
		mappings.put(
			InputTypeStrategies.sequence(
				InputTypeStrategies.explicit(DataTypes.INT()),
				InputTypeStrategies.explicit(DataTypes.BOOLEAN())),
			TypeStrategies.explicit(DataTypes.STRING()));
		return TypeStrategies.mapping(mappings);
	}
}
