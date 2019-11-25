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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.validators.AndTypeArgumentValidator;
import org.apache.flink.table.types.inference.validators.AnyTypeValidator;
import org.apache.flink.table.types.inference.validators.ExplicitTypeValidator;
import org.apache.flink.table.types.inference.validators.LiteralTypeValidator;
import org.apache.flink.table.types.inference.validators.OrTypeArgumentValidator;
import org.apache.flink.table.types.inference.validators.OrTypeInputValidator;
import org.apache.flink.table.types.inference.validators.PassingTypeValidator;
import org.apache.flink.table.types.inference.validators.SequenceInputValidator;
import org.apache.flink.table.types.inference.validators.VaryingSequenceTypeValidator;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Validators for checking the input data types of a function call.
 *
 * @see InputTypeValidator
 * @see ArgumentTypeValidator
 */
@Internal
public final class InputTypeValidators {

	/**
	 * Validator that does not perform any validation and always passes.
	 */
	public static final PassingTypeValidator PASSING = new PassingTypeValidator();

	/**
	 * Validator that checks for a single argument that can be of any type.
	 */
	public static final AnyTypeValidator ANY = new AnyTypeValidator();

	/**
	 * Validator that checks if a single argument is a literal.
	 */
	public static final LiteralTypeValidator LITERAL = new LiteralTypeValidator(false);

	/**
	 * Validator that checks if a single argument is a literal or null.
	 */
	public static final LiteralTypeValidator LITERAL_OR_NULL = new LiteralTypeValidator(true);

	/**
	 * Validator that checks if a single argument corresponds to an explicitly defined logical type.
	 *
	 * <p>Note: The validation happens on {@link LogicalType} level only.
	 */
	public static ArgumentTypeValidator explicit(DataType expectedDataType) {
		return new ExplicitTypeValidator(expectedDataType.getLogicalType());
	}

	/**
	 * Conjunction of multiple {@link ArgumentTypeValidator}s into one like {@code f(NUMERIC && LITERAL)}.
	 */
	public static ArgumentTypeValidator and(ArgumentTypeValidator... validators) {
		return new AndTypeArgumentValidator(Arrays.asList(validators));
	}

	/**
	 * Conjunction of multiple {@link ArgumentTypeValidator}s into one like {@code f(NUMERIC || STRING)}.
	 */
	public static ArgumentTypeValidator or(ArgumentTypeValidator... validators) {
		return new OrTypeArgumentValidator(Arrays.asList(validators));
	}

	/**
	 * Conjunction of multiple {@link InputTypeValidator}s into one like {@code f(NUMERIC) || f(STRING)}.
	 */
	public static InputTypeValidator or(InputTypeValidator... validators) {
		return new OrTypeInputValidator(Arrays.asList(validators));
	}

	/**
	 * Validator that checks if each operand corresponds to an explicitly defined logical type
	 * like {@code f(STRING, INT)}.
	 *
	 * <p>Note: The validation happens on {@link LogicalType} level only.
	 */
	public static InputTypeValidator explicitSequence(DataType... expectedDataTypes) {
		final List<ArgumentTypeValidator> validators = Arrays.stream(expectedDataTypes)
			.map(InputTypeValidators::explicit)
			.collect(Collectors.toList());
		return new SequenceInputValidator(validators, null);
	}

	/**
	 * Validator that checks if each named operand corresponds to an explicitly defined logical type
	 * like {@code f(s STRING, i INT)}.
	 *
	 * <p>Note: The validation happens on {@link LogicalType} level only.
	 */
	public static InputTypeValidator explicitSequence(String[] argumentNames, DataType[] expectedDataTypes) {
		final List<ArgumentTypeValidator> validators = Arrays.stream(expectedDataTypes)
			.map(InputTypeValidators::explicit)
			.collect(Collectors.toList());
		return new SequenceInputValidator(validators, Arrays.asList(argumentNames));
	}

	/**
	 * A sequence of {@link ArgumentTypeValidator}s for validating an entire function signature
	 * like {@code f(STRING, NUMERIC)}.
	 */
	public static InputTypeValidator sequence(ArgumentTypeValidator... validators) {
		return new SequenceInputValidator(Arrays.asList(validators), null);
	}

	/**
	 * A sequence of {@link ArgumentTypeValidator}s for validating an entire named function signature
	 * like {@code f(s STRING, n NUMERIC)}.
	 */
	public static InputTypeValidator sequence(String[] argumentNames, ArgumentTypeValidator[] validators) {
		return new SequenceInputValidator(Arrays.asList(validators), Arrays.asList(argumentNames));
	}

	/**
	 * A varying sequence of {@link ArgumentTypeValidator}s for validating an entire function signature
	 * like {@code f(INT, STRING, NUMERIC...)}. The first n - 1 arguments must be constant and are validated
	 * according to {@link #sequence(ArgumentTypeValidator[])}. The n-th argument can occur 0, 1, or
	 * more times.
	 */
	public static InputTypeValidator varyingSequence(ArgumentTypeValidator... validators) {
		return new VaryingSequenceTypeValidator(Arrays.asList(validators), null);
	}

	/**
	 * A varying sequence of {@link ArgumentTypeValidator}s for validating an entire function signature
	 * like {@code f(i INT, str STRING, n NUMERIC...)}. The first n - 1 arguments must be constant and are validated
	 * according to {@link #sequence(String[], ArgumentTypeValidator[])}. The n-th argument can occur 0, 1,
	 * or more times.
	 */
	public static InputTypeValidator varyingSequence(String[] argumentNames, ArgumentTypeValidator[] validators) {
		return new VaryingSequenceTypeValidator(Arrays.asList(validators), Arrays.asList(argumentNames));
	}

	// --------------------------------------------------------------------------------------------

	private InputTypeValidators() {
		// no instantiation
	}
}
