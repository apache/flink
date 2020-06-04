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
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Base class for testing {@link InputTypeStrategy}.
 */
@RunWith(Parameterized.class)
public abstract class InputTypeStrategiesTestBase {

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testStrategy() {
		if (testSpec.expectedSignature != null) {
			assertThat(
				generateSignature(),
				equalTo(testSpec.expectedSignature));
		}
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectCause(containsCause(new ValidationException(testSpec.expectedErrorMessage)));
		}
		for (List<DataType> actualArgumentTypes : testSpec.actualArgumentTypes) {
			TypeInferenceUtil.Result result = runTypeInference(actualArgumentTypes);
			if (testSpec.expectedArgumentTypes != null) {
				assertThat(result.getExpectedArgumentTypes(), equalTo(testSpec.expectedArgumentTypes));
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private String generateSignature() {
		final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
		functionDefinitionMock.functionKind = FunctionKind.SCALAR;
		return TypeInferenceUtil.generateSignature(createTypeInference(), "f", functionDefinitionMock);
	}

	private TypeInferenceUtil.Result runTypeInference(List<DataType> actualArgumentTypes) {
		final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
		functionDefinitionMock.functionKind = FunctionKind.SCALAR;

		final CallContextMock callContextMock = new CallContextMock();
		callContextMock.typeFactory = new DataTypeFactoryMock();
		callContextMock.functionDefinition = functionDefinitionMock;
		callContextMock.argumentDataTypes = actualArgumentTypes;
		callContextMock.argumentLiterals = IntStream.range(0, actualArgumentTypes.size())
			.mapToObj(i -> testSpec.literalPos != null && i == testSpec.literalPos)
			.collect(Collectors.toList());
		callContextMock.argumentValues = IntStream.range(0, actualArgumentTypes.size())
			.mapToObj(i -> (testSpec.literalPos != null && i == testSpec.literalPos) ?
				Optional.ofNullable(testSpec.literalValue) : Optional.empty())
			.collect(Collectors.toList());
		callContextMock.argumentNulls = IntStream.range(0, actualArgumentTypes.size())
			.mapToObj(i -> false)
			.collect(Collectors.toList());
		callContextMock.name = "f";
		callContextMock.outputDataType = Optional.empty();

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
		return TypeInferenceUtil.runTypeInference(
			createTypeInference(),
			callContextMock,
			surroundingInfo);
	}

	private TypeInference createTypeInference() {
		final TypeInference.Builder builder = TypeInference.newBuilder()
			.inputTypeStrategy(testSpec.strategy)
			.outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN()));

		if (testSpec.namedArguments != null) {
			builder.namedArguments(testSpec.namedArguments);
		}
		if (testSpec.typedArguments != null) {
			builder.typedArguments(testSpec.typedArguments);
		}
		return builder.build();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * A specification for tests to execute.
	 */
	protected static class TestSpec {

		private final @Nullable
		String description;

		private final InputTypeStrategy strategy;

		private @Nullable List<String> namedArguments;

		private @Nullable List<DataType> typedArguments;

		private List<List<DataType>> actualArgumentTypes = new ArrayList<>();

		private @Nullable Integer literalPos;

		private @Nullable Object literalValue;

		private @Nullable InputTypeStrategy surroundingStrategy;

		private @Nullable String expectedSignature;

		private @Nullable List<DataType> expectedArgumentTypes;

		private @Nullable String expectedErrorMessage;

		private TestSpec(@Nullable String description, InputTypeStrategy strategy) {
			this.description = description;
			this.strategy = strategy;
		}

		static TestSpec forStrategy(InputTypeStrategy strategy) {
			return new TestSpec(null, strategy);
		}

		static TestSpec forStrategy(String description, InputTypeStrategy strategy) {
			return new TestSpec(description, strategy);
		}

		TestSpec namedArguments(String... names) {
			this.namedArguments = Arrays.asList(names);
			return this;
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
			this.actualArgumentTypes.add(Arrays.asList(dataTypes));
			return this;
		}

		TestSpec calledWithLiteralAt(int pos) {
			this.literalPos = pos;
			return this;
		}

		TestSpec calledWithLiteralAt(int pos, Object value) {
			this.literalPos = pos;
			this.literalValue = value;
			return this;
		}

		TestSpec expectSignature(String signature) {
			this.expectedSignature = signature;
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

		@Override
		public String toString() {
			return description != null ? description : strategy.getClass().getSimpleName();
		}
	}
}
