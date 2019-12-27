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
import org.apache.flink.table.types.inference.utils.DataTypeLookupMock;
import org.apache.flink.table.types.inference.utils.FunctionDefinitionMock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL_OR_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.OUTPUT_IF_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.WILDCARD;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicitSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;
import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for built-in {@link InputTypeStrategies}.
 */
@RunWith(Parameterized.class)
public class InputTypeStrategiesTest {

	@Parameters
	public static List<TestSpec> testData() {
		return asList(
			// wildcard with 2 arguments
			TestSpec
				.forStrategy(WILDCARD)
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT())
				.expectSignature("f(*)")
				.expectArgumentTypes(DataTypes.INT(), DataTypes.INT()),

			// wildcard with 0 arguments
			TestSpec
				.forStrategy(WILDCARD)
				.calledWithArgumentTypes()
				.expectSignature("f(*)")
				.expectArgumentTypes(),

			// explicit sequence
			TestSpec
				.forStrategy(explicitSequence(DataTypes.INT().bridgedTo(int.class), DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.BOOLEAN())
				.expectSignature("f(INT, BOOLEAN)")
				.expectArgumentTypes(DataTypes.INT().bridgedTo(int.class), DataTypes.BOOLEAN()),

			// invalid named sequence
			TestSpec
				.forStrategy(
					explicitSequence(
						new String[]{"i", "s"},
						new DataType[]{DataTypes.INT(), DataTypes.STRING()}))
				.calledWithArgumentTypes(DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING)"),

			// incompatible nullability
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT().notNull()))
				.calledWithArgumentTypes(DataTypes.BIGINT())
				.expectErrorMessage("Unsupported argument type. Expected type 'BIGINT NOT NULL' but actual type was 'BIGINT'."),

			// implicit cast
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT()))
				.calledWithArgumentTypes(DataTypes.INT())
				.expectArgumentTypes(DataTypes.BIGINT()),

			// incompatible types
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT()))
				.calledWithArgumentTypes(DataTypes.STRING())
				.expectErrorMessage("Unsupported argument type. Expected type 'BIGINT' but actual type was 'STRING'."),

			// incompatible number of arguments
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT(), DataTypes.BIGINT()))
				.calledWithArgumentTypes(DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. At least 2 arguments expected but 1 passed."),

			// any type
			TestSpec
				.forStrategy(sequence(ANY))
				.calledWithArgumentTypes(DataTypes.BIGINT())
				.expectSignature("f(<ANY>)")
				.expectArgumentTypes(DataTypes.BIGINT()),

			// incompatible number of arguments
			TestSpec
				.forStrategy(sequence(ANY))
				.calledWithArgumentTypes(DataTypes.BIGINT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. At most 1 arguments expected but 2 passed."),

			// OR with bridging class
			TestSpec
				.forStrategy(
					or(
						explicitSequence(DataTypes.STRING()),
						explicitSequence(DataTypes.INT().bridgedTo(int.class)),
						explicitSequence(DataTypes.BOOLEAN())))
				.calledWithArgumentTypes(DataTypes.INT())
				.calledWithArgumentTypes(DataTypes.TINYINT())
				.expectSignature("f(STRING)\nf(INT)\nf(BOOLEAN)")
				.expectArgumentTypes(DataTypes.INT().bridgedTo(int.class)),

			// OR with implicit casting
			TestSpec
				.forStrategy(
					or(
						explicitSequence(DataTypes.TINYINT()),
						explicitSequence(DataTypes.INT()),
						explicitSequence(DataTypes.BIGINT())))
				.calledWithArgumentTypes(DataTypes.SMALLINT())
				.expectArgumentTypes(DataTypes.INT()),

			// invalid type in OR
			TestSpec
				.forStrategy(or(explicitSequence(DataTypes.INT()), explicitSequence(DataTypes.STRING())))
				.calledWithArgumentTypes(DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT)\nf(STRING)"),

			// invalid typed sequence
			TestSpec
				.forStrategy(explicitSequence(DataTypes.INT(), DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT, BOOLEAN)"),

			// sequence with wildcard
			TestSpec
				.forStrategy(sequence(ANY, explicit(DataTypes.INT())))
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.INT())
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.TINYINT())
				.expectArgumentTypes(DataTypes.BOOLEAN(), DataTypes.INT()),

			// invalid named sequence
			TestSpec
				.forStrategy(sequence(
					new String[]{"any", "int"},
					new ArgumentTypeStrategy[]{ANY, explicit(DataTypes.INT())}))
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(any <ANY>, int INT)"),

			// sequence with OR and implicit casting
			TestSpec
				.forStrategy(
					sequence(
						explicit(DataTypes.INT()),
						or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))))
				.expectSignature("f(INT, [BOOLEAN | INT])")
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT())
				.calledWithArgumentTypes(DataTypes.TINYINT(), DataTypes.TINYINT())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.INT()),

			// sequence with OR
			TestSpec
				.forStrategy(
					sequence(
						explicit(DataTypes.INT()),
						or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.STRING()))))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT, [BOOLEAN | STRING])"),

			// sequence with literal
			TestSpec
				.forStrategy(sequence(LITERAL))
				.calledWithLiteralAt(0)
				.calledWithArgumentTypes(DataTypes.INT())
				.expectArgumentTypes(DataTypes.INT()),

			// sequence with literal
			TestSpec
				.forStrategy(sequence(and(LITERAL, explicit(DataTypes.STRING())), explicit(DataTypes.INT())))
				.calledWithLiteralAt(0)
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.INT())
				.expectSignature("f([<LITERAL NOT NULL> & STRING], INT)")
				.expectArgumentTypes(DataTypes.STRING(), DataTypes.INT()),

			// sequence with missing literal
			TestSpec
				.forStrategy(sequence(and(explicit(DataTypes.STRING()), LITERAL_OR_NULL), explicit(DataTypes.INT())))
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf([STRING & <LITERAL>], INT)"),

			// vararg sequence
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(
					DataTypes.INT(),
					DataTypes.STRING(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN())
				.expectArgumentTypes(
					DataTypes.INT(),
					DataTypes.STRING(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN()),

			// vararg sequence with conversion class
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"var"},
						new ArgumentTypeStrategy[]{explicit(DataTypes.BOOLEAN().bridgedTo(boolean.class))}))
				.calledWithArgumentTypes(
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN())
				.expectSignature("f(var BOOLEAN...)")
				.expectArgumentTypes(
					DataTypes.BOOLEAN().bridgedTo(boolean.class),
					DataTypes.BOOLEAN().bridgedTo(boolean.class),
					DataTypes.BOOLEAN().bridgedTo(boolean.class)),

			// vararg sequence
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING()),

			// invalid vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var BOOLEAN...)"),

			// invalid non-vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT(), DataTypes.BOOLEAN())
				.expectErrorMessage("Unsupported argument type. Expected type 'STRING' but actual type was 'INT'."),

			// OR in vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BOOLEAN())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BOOLEAN()),

			// invalid OR in vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var [BOOLEAN | INT]...)"),

			// incomplete inference
			TestSpec
				.forStrategy(WILDCARD)
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectErrorMessage("Invalid use of untyped NULL in arguments."),

			// typed arguments help inferring a type
			TestSpec
				.forStrategy(WILDCARD)
				.typedArguments(DataTypes.INT().bridgedTo(int.class), DataTypes.STRING(), DataTypes.BOOLEAN())
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectArgumentTypes(DataTypes.INT().bridgedTo(int.class), DataTypes.STRING(), DataTypes.BOOLEAN()),

			// surrounding function helps inferring a type
			TestSpec
				.forStrategy(sequence(OUTPUT_IF_NULL, OUTPUT_IF_NULL, OUTPUT_IF_NULL))
				.surroundingStrategy(explicitSequence(DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectSignature("f(<OUTPUT>, <OUTPUT>, <OUTPUT>)")
				.expectArgumentTypes(DataTypes.BOOLEAN(), DataTypes.STRING(), DataTypes.BOOLEAN()),

			// surrounding function helps inferring a type
			TestSpec
				.forStrategy(sequence(or(OUTPUT_IF_NULL, explicit(DataTypes.INT()))))
				.surroundingStrategy(explicitSequence(DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectSignature("f([<OUTPUT> | INT])")
				.expectArgumentTypes(DataTypes.BOOLEAN()),

			// surrounding function does not help inferring a type
			TestSpec
				.forStrategy(sequence(or(OUTPUT_IF_NULL, explicit(DataTypes.INT()))))
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectSignature("f([<OUTPUT> | INT])")
				.expectArgumentTypes(DataTypes.INT()),

			// typed arguments only with casting
			TestSpec
				.forStrategy(WILDCARD)
				.typedArguments(DataTypes.INT(), DataTypes.STRING())
				.calledWithArgumentTypes(DataTypes.TINYINT(), DataTypes.STRING())
				.expectSignature("f(INT, STRING)")
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING()),

			// invalid typed arguments
			TestSpec
				.forStrategy(WILDCARD)
				.typedArguments(DataTypes.INT(), DataTypes.STRING())
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid argument type at position 0. Data type INT expected but STRING passed."),

			// named arguments
			TestSpec
				.forStrategy(WILDCARD)
				.namedArguments("i", "s")
				.typedArguments(DataTypes.INT(), DataTypes.STRING())
				.expectSignature("f(i => INT, s => STRING)")
		);
	}

	@Parameter
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
		return TypeInferenceUtil.generateSignature("f", functionDefinitionMock, createTypeInference());
	}

	private TypeInferenceUtil.Result runTypeInference(List<DataType> actualArgumentTypes) {
		final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
		functionDefinitionMock.functionKind = FunctionKind.SCALAR;

		final DataTypeLookupMock dataTypeLookupMock = new DataTypeLookupMock();

		final CallContextMock callContextMock = new CallContextMock();
		callContextMock.functionDefinition = functionDefinitionMock;
		callContextMock.argumentDataTypes = actualArgumentTypes;
		callContextMock.argumentLiterals = IntStream.range(0, actualArgumentTypes.size())
			.mapToObj(i -> testSpec.literalPos != null && i == testSpec.literalPos)
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

	private static class TestSpec {

		private final InputTypeStrategy strategy;

		private @Nullable List<String> namedArguments;

		private @Nullable List<DataType> typedArguments;

		private List<List<DataType>> actualArgumentTypes = new ArrayList<>();

		private @Nullable Integer literalPos;

		private @Nullable InputTypeStrategy surroundingStrategy;

		private @Nullable String expectedSignature;

		private @Nullable List<DataType> expectedArgumentTypes;

		private @Nullable String expectedErrorMessage;

		private TestSpec(InputTypeStrategy strategy) {
			this.strategy = strategy;
		}

		static TestSpec forStrategy(InputTypeStrategy strategy) {
			return new TestSpec(strategy);
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
	}
}
