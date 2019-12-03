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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.flink.table.types.inference.InputTypeValidators.ANY;
import static org.apache.flink.table.types.inference.InputTypeValidators.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeValidators.LITERAL_OR_NULL;
import static org.apache.flink.table.types.inference.InputTypeValidators.PASSING;
import static org.apache.flink.table.types.inference.InputTypeValidators.and;
import static org.apache.flink.table.types.inference.InputTypeValidators.explicit;
import static org.apache.flink.table.types.inference.InputTypeValidators.explicitSequence;
import static org.apache.flink.table.types.inference.InputTypeValidators.or;
import static org.apache.flink.table.types.inference.InputTypeValidators.sequence;
import static org.apache.flink.table.types.inference.InputTypeValidators.varyingSequence;
import static org.apache.flink.util.CoreMatchers.containsCause;

/**
 * Tests for built-in {@link InputTypeValidators}.
 */
@RunWith(Parameterized.class)
public class InputTypeValidatorsTest {

	@Parameters
	public static List<TestSpec> testData() {
		return asList(
			// 2 arguments
			TestSpec
				.forValidator(PASSING)
				.inputTypes(DataTypes.INT(), DataTypes.INT())
				.expectSuccess(),

			// 0 arguments
			TestSpec
				.forValidator(PASSING)
				.inputTypes()
				.expectSuccess(),

			// full equivalence
			TestSpec
				.forValidator(explicitSequence(DataTypes.INT()))
				.inputTypes(DataTypes.INT())
				.expectSuccess(),

			// invalid named sequence
			TestSpec
				.forValidator(
					explicitSequence(
						new String[]{"i", "s"},
						new DataType[]{DataTypes.INT(), DataTypes.STRING()}))
				.inputTypes(DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING)"),

			// incompatible nullability
			TestSpec
				.forValidator(explicitSequence(DataTypes.BIGINT().notNull()))
				.inputTypes(DataTypes.BIGINT())
				.expectErrorMessage("Unsupported argument type. Expected type 'BIGINT NOT NULL' but actual type was 'BIGINT'."),

			// implicit cast
			TestSpec
				.forValidator(explicitSequence(DataTypes.BIGINT()))
				.inputTypes(DataTypes.INT())
				.expectSuccess(),

			// incompatible types
			TestSpec
				.forValidator(explicitSequence(DataTypes.BIGINT()))
				.inputTypes(DataTypes.STRING())
				.expectErrorMessage("Unsupported argument type. Expected type 'BIGINT' but actual type was 'STRING'."),

			// incompatible number of arguments
			TestSpec
				.forValidator(explicitSequence(DataTypes.BIGINT(), DataTypes.BIGINT()))
				.inputTypes(DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. At least 2 arguments expected but 1 passed."),

			// any type
			TestSpec
				.forValidator(ANY)
				.inputTypes(DataTypes.BIGINT())
				.expectSuccess(),

			// incompatible number of arguments
			TestSpec
				.forValidator(ANY)
				.inputTypes(DataTypes.BIGINT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. At most 1 arguments expected but 2 passed."),

			// left of OR
			TestSpec
				.forValidator(or(explicitSequence(DataTypes.INT()), explicitSequence(DataTypes.NULL())))
				.inputTypes(DataTypes.INT())
				.expectSuccess(),

			// right of OR
			TestSpec
				.forValidator(or(explicitSequence(DataTypes.INT()), explicitSequence(DataTypes.NULL())))
				.inputTypes(DataTypes.NULL())
				.expectSuccess(),

			// invalid type in OR
			TestSpec
				.forValidator(or(explicitSequence(DataTypes.INT()), explicitSequence(DataTypes.NULL())))
				.inputTypes(DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT)\nf(NULL)"),

			// explicit sequence
			TestSpec
				.forValidator(sequence(explicit(DataTypes.INT()), explicit(DataTypes.BOOLEAN())))
				.inputTypes(DataTypes.INT(), DataTypes.BOOLEAN())
				.expectSuccess(),

			// invalid sequence
			TestSpec
				.forValidator(sequence(explicit(DataTypes.INT()), explicit(DataTypes.BOOLEAN())))
				.inputTypes(DataTypes.BOOLEAN(), DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT, BOOLEAN)"),

			// correct sequence with wildcard
			TestSpec
				.forValidator(sequence(ANY, explicit(DataTypes.INT())))
				.inputTypes(DataTypes.BOOLEAN(), DataTypes.INT())
				.expectSuccess(),

			// invalid named sequence
			TestSpec
				.forValidator(sequence(
					new String[]{"any", "int"},
					new ArgumentTypeValidator[]{ANY, explicit(DataTypes.INT())}))
				.inputTypes(DataTypes.STRING(), DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(any <ANY>, int INT)"),

			// sequence with OR
			TestSpec
				.forValidator(sequence(explicit(DataTypes.INT()), or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.STRING()))))
				.inputTypes(DataTypes.INT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT, [BOOLEAN | STRING])"),

			// literal
			TestSpec
				.forValidator(LITERAL)
				.inputTypes(DataTypes.INT())
				.literalAt(0)
				.expectSuccess(),

			// sequence with literal
			TestSpec
				.forValidator(sequence(and(LITERAL, explicit(DataTypes.STRING())), explicit(DataTypes.INT())))
				.inputTypes(DataTypes.STRING(), DataTypes.INT())
				.literalAt(0)
				.expectSuccess(),

			// sequence with missing literal
			TestSpec
				.forValidator(sequence(and(LITERAL_OR_NULL, explicit(DataTypes.STRING())), explicit(DataTypes.INT())))
				.inputTypes(DataTypes.STRING(), DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf([<LITERAL> & STRING], INT)"),

			// vararg sequence
			TestSpec
				.forValidator(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeValidator[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.inputTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.BOOLEAN(), DataTypes.BOOLEAN())
				.expectSuccess(),

			// vararg sequence
			TestSpec
				.forValidator(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeValidator[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.inputTypes(DataTypes.INT(), DataTypes.STRING())
				.expectSuccess(),

			// invalid vararg type
			TestSpec
				.forValidator(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeValidator[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.inputTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var BOOLEAN...)"),

			// invalid non-vararg type
			TestSpec
				.forValidator(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeValidator[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.inputTypes(DataTypes.INT(), DataTypes.INT(), DataTypes.BOOLEAN())
				.expectErrorMessage("Unsupported argument type. Expected type 'STRING' but actual type was 'INT'."),

			// OR in vararg type
			TestSpec
				.forValidator(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeValidator[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))}))
				.inputTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BOOLEAN())
				.expectSuccess(),

			// invalid OR in vararg type
			TestSpec
				.forValidator(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeValidator[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))}))
				.inputTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var [BOOLEAN | INT]...)")
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testValidator() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectCause(containsCause(new ValidationException(testSpec.expectedErrorMessage)));
		}
		runTypeInference();
	}

	// --------------------------------------------------------------------------------------------

	private void runTypeInference() {
		final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
		functionDefinitionMock.functionKind = FunctionKind.SCALAR;
		final CallContextMock callContextMock = new CallContextMock();
		callContextMock.functionDefinition = functionDefinitionMock;
		callContextMock.argumentDataTypes = testSpec.inputTypes;
		callContextMock.argumentLiterals = IntStream.range(0, testSpec.inputTypes.size())
			.mapToObj(i -> testSpec.literalPos != null && i == testSpec.literalPos)
			.collect(Collectors.toList());
		callContextMock.argumentNulls = IntStream.range(0, testSpec.inputTypes.size())
			.mapToObj(i -> false)
			.collect(Collectors.toList());
		callContextMock.name = "f";

		final TypeInference typeInference = TypeInference.newBuilder()
			.inputTypeValidator(testSpec.validator)
			.outputTypeStrategy(callContext -> Optional.of(DataTypes.NULL()))
			.build();
		TypeInferenceUtil.runTypeInference(typeInference, callContextMock);
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		private final InputTypeValidator validator;

		private List<DataType> inputTypes;

		private @Nullable Integer literalPos;

		private @Nullable String expectedErrorMessage;

		private TestSpec(InputTypeValidator validator) {
			this.validator = validator;
		}

		static TestSpec forValidator(InputTypeValidator validator) {
			return new TestSpec(validator);
		}

		TestSpec inputTypes(DataType... dataTypes) {
			this.inputTypes = asList(dataTypes);
			return this;
		}

		TestSpec literalAt(int pos) {
			this.literalPos = pos;
			return this;
		}

		TestSpec expectSuccess() {
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}
	}
}
