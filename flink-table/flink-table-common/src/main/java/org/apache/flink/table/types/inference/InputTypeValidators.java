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
import org.apache.flink.table.types.inference.validators.AnyTypeValidator;
import org.apache.flink.table.types.inference.validators.CompositeTypeValidator;
import org.apache.flink.table.types.inference.validators.CompositeTypeValidator.Composition;
import org.apache.flink.table.types.inference.validators.ExplicitTypeValidator;
import org.apache.flink.table.types.inference.validators.LiteralTypeValidator;
import org.apache.flink.table.types.inference.validators.PassingTypeValidator;
import org.apache.flink.table.types.inference.validators.SingleInputTypeValidator;
import org.apache.flink.table.types.inference.validators.VaryingSequenceTypeValidator;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Validators for checking the input data types of a function call.
 *
 * @see InputTypeValidator
 * @see SingleInputTypeValidator
 */
@Internal
public final class InputTypeValidators {

	/**
	 * Validator that does not perform any validation and always passes.
	 */
	public static final InputTypeValidator PASSING = new PassingTypeValidator();

	/**
	 * Validator that checks for a single argument that can be of any type.
	 */
	public static final SingleInputTypeValidator ANY = new AnyTypeValidator();

	/**
	 * Validator that checks if a single argument is a literal.
	 */
	public static final SingleInputTypeValidator LITERAL = new LiteralTypeValidator(false);

	/**
	 * Validator that checks if a single argument is a literal or null.
	 */
	public static final SingleInputTypeValidator LITERAL_OR_NULL = new LiteralTypeValidator(true);

	/**
	 * Validator that checks if each operand corresponds to an explicitly defined logical type.
	 *
	 * <p>Note: The validation happens on {@link LogicalType} level only.
	 */
	public static SingleInputTypeValidator explicit(DataType... expectedDataTypes) {
		final List<LogicalType> expectedTypes = Arrays.stream(expectedDataTypes)
			.map(DataType::getLogicalType)
			.collect(Collectors.toList());
		return new ExplicitTypeValidator(expectedTypes);
	}

	/**
	 * Conjunction of multiple {@link SingleInputTypeValidator}s into one like {@code f(NUMERIC && LITERAL)}.
	 */
	public static SingleInputTypeValidator and(SingleInputTypeValidator... validators) {
		return new CompositeTypeValidator(Composition.AND, Arrays.asList(validators), null);
	}

	/**
	 * Conjunction of multiple {@link InputTypeValidator}s into one like {@code f(NUMERIC) && f(LITERAL)}.
	 */
	public static InputTypeValidator and(InputTypeValidator... validators) {
		return new CompositeTypeValidator(Composition.AND, Arrays.asList(validators), null);
	}

	/**
	 * Conjunction of multiple {@link SingleInputTypeValidator}s into one like {@code f(NUMERIC || STRING)}.
	 */
	public static SingleInputTypeValidator or(SingleInputTypeValidator... validators) {
		return new CompositeTypeValidator(Composition.OR, Arrays.asList(validators), null);
	}

	/**
	 * Conjunction of multiple {@link InputTypeValidator}s into one like {@code f(NUMERIC) || f(STRING)}.
	 */
	public static InputTypeValidator or(InputTypeValidator... validators) {
		return new CompositeTypeValidator(Composition.OR, Arrays.asList(validators), null);
	}

	/**
	 * A sequence of {@link SingleInputTypeValidator}s for validating an entire function signature
	 * like {@code f(STRING, NUMERIC)}.
	 */
	public static InputTypeValidator sequence(SingleInputTypeValidator... validators) {
		return new CompositeTypeValidator(Composition.SEQUENCE, Arrays.asList(validators), null);
	}

	/**
	 * A sequence of {@link SingleInputTypeValidator}s for validating an entire named function signature
	 * like {@code f(s STRING, n NUMERIC)}.
	 */
	public static InputTypeValidator sequence(String[] argumentNames, SingleInputTypeValidator[] validators) {
		return new CompositeTypeValidator(Composition.SEQUENCE, Arrays.asList(validators), Arrays.asList(argumentNames));
	}

	/**
	 * A varying sequence of {@link SingleInputTypeValidator}s for validating an entire function signature
	 * like {@code f(INT, STRING, NUMERIC...)}. The first n - 1 arguments must be constant and are validated
	 * according to {@link #sequence(String[], SingleInputTypeValidator[])}. The n-th argument can
	 * occur 0, 1, or more times.
	 */
	public static InputTypeValidator varyingSequence(SingleInputTypeValidator... validators) {
		return new VaryingSequenceTypeValidator(Arrays.asList(validators), null);
	}

	/**
	 * A varying sequence of {@link SingleInputTypeValidator}s for validating an entire function signature
	 * like {@code f(i INT, str STRING, n NUMERIC...)}. The first n - 1 arguments must be constant and are validated
	 * according to {@link #sequence(String[], SingleInputTypeValidator[])}. The n-th argument can
	 * occur 0, 1, or more times.
	 */
	public static InputTypeValidator varyingSequence(String[] argumentNames, SingleInputTypeValidator[] validators) {
		return new VaryingSequenceTypeValidator(Arrays.asList(validators), Arrays.asList(argumentNames));
	}

	// --------------------------------------------------------------------------------------------

	private InputTypeValidators() {
		// no instantiation
	}
}
