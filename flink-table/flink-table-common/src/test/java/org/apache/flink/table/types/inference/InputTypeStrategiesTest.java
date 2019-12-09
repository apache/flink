/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.types.inference.strategies.BridgingInputTypeStrategy.BridgingSignature;
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
import java.util.List;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for built-in {@link InputTypeStrategies}.
 */
@RunWith(Parameterized.class)
public class InputTypeStrategiesTest {

	@Parameters
	public static List<TestSpec> testData() {
		return Arrays.asList(
			// no inference
			TestSpec
				.forInputStrategy(InputTypeStrategies.NOP)
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING()),

			// incomplete inference
			TestSpec
				.forInputStrategy(InputTypeStrategies.NOP)
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectErrorMessage("Invalid use of untyped NULL in arguments."),

			// typed arguments help inferring a type
			TestSpec
				.forInputStrategy(InputTypeStrategies.NOP)
				.typedArguments(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN())
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN()),

			// surrounding function helps inferring a type
			TestSpec
				.forInputStrategy(InputTypeStrategies.OUTPUT_TYPE)
				.surroundingStrategy(InputTypeStrategies.explicit(DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectArgumentTypes(DataTypes.BOOLEAN(), DataTypes.STRING(), DataTypes.BOOLEAN()),

			// enrich data type with conversion class of INT
			TestSpec
				.forInputStrategy(createBridgingInputTypeStrategy())
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.INT())
				.expectArgumentTypes(DataTypes.STRING(), DataTypes.INT().bridgedTo(int.class)),

			// enrich data type with conversion class of varying INT
			TestSpec
				.forInputStrategy(createBridgingInputTypeStrategy())
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT(), DataTypes.INT())
				.expectArgumentTypes(
					DataTypes.INT().bridgedTo(int.class),
					DataTypes.INT().bridgedTo(int.class),
					DataTypes.INT().bridgedTo(int.class)),

			// check function without arguments
			TestSpec
				.forInputStrategy(createBridgingInputTypeStrategy())
				.calledWithArgumentTypes()
				.expectArgumentTypes()
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInputTypeStrategy() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectCause(containsCause(new ValidationException(testSpec.expectedErrorMessage)));
		}
		TypeInferenceUtil.Result result = runTypeInference();
		if (testSpec.expectedArgumentTypes != null) {
			Assert.assertThat(result.getExpectedArgumentTypes(), equalTo(testSpec.expectedArgumentTypes));
		}
	}

	// --------------------------------------------------------------------------------------------

	private TypeInferenceUtil.Result runTypeInference() {
		final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
		functionDefinitionMock.functionKind = FunctionKind.SCALAR;
		final CallContextMock callContextMock = new CallContextMock();
		callContextMock.functionDefinition = functionDefinitionMock;
		callContextMock.argumentDataTypes = testSpec.actualArgumentTypes;
		callContextMock.name = "f";

		final TypeInference.Builder builder = TypeInference.newBuilder()
			.inputTypeStrategy(testSpec.strategy)
			.inputTypeValidator(InputTypeValidators.PASSING)
			.outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN()));

		if (testSpec.typedArguments != null) {
			builder.typedArguments(testSpec.typedArguments);
		}

		final TypeInferenceUtil.SurroundingInfo surroundingInfo;
		if (testSpec.surroundingStrategy != null) {
			final TypeInference outerTypeInference = TypeInference.newBuilder()
				.inputTypeStrategy(testSpec.surroundingStrategy)
				.outputTypeStrategy(TypeStrategies.MISSING)
				.build();
			surroundingInfo = new TypeInferenceUtil.SurroundingInfo(
				"f_outer",
				functionDefinitionMock,
				outerTypeInference,
				1,
				0);
		} else {
			surroundingInfo = null;
		}
		return TypeInferenceUtil.runTypeInference(builder.build(), callContextMock, surroundingInfo);
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		private final InputTypeStrategy strategy;

		// types explicitly expected by the type inference
		private @Nullable List<DataType> typedArguments;

		private @Nullable InputTypeStrategy surroundingStrategy;

		private @Nullable List<DataType> actualArgumentTypes;

		private @Nullable List<DataType> expectedArgumentTypes;

		private @Nullable String expectedErrorMessage;

		private TestSpec(InputTypeStrategy strategy) {
			this.strategy = strategy;
		}

		static TestSpec forInputStrategy(InputTypeStrategy strategy) {
			return new TestSpec(strategy);
		}

		TestSpec typedArguments(DataType... dataTypes) {
			this.typedArguments = Arrays.asList(dataTypes);
			return this;
		}

		TestSpec surroundingStrategy(InputTypeStrategy surroundingStrategy) {
			this.surroundingStrategy = surroundingStrategy;
			return this;
		}

		TestSpec calledWithArgumentTypes(DataType... dataTypes) {
			this.actualArgumentTypes = Arrays.asList(dataTypes);
			return this;
		}

		TestSpec expectArgumentTypes(DataType... dataTypes) {
			this.expectedArgumentTypes = Arrays.asList(dataTypes);
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}
	}

	private static InputTypeStrategy createBridgingInputTypeStrategy() {
		return InputTypeStrategies.bridging(
			Arrays.asList(
				new BridgingSignature(
					new DataType[]{DataTypes.STRING(), DataTypes.INT().bridgedTo(int.class)}, false
				),
				new BridgingSignature(
					new DataType[]{DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class), DataTypes.STRING()}, false
				),
				new BridgingSignature(
					new DataType[]{}, false
				),
				new BridgingSignature(
					new DataType[]{DataTypes.INT().bridgedTo(int.class)}, true
				)
			)
		);
	}
}
