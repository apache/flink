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
import org.apache.flink.table.types.inference.strategies.AndArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.AnyArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ExplicitArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.LiteralArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OrArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OrInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OutputArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.SequenceInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.VaryingSequenceInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.WildcardInputTypeStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Strategies for inferring and validating input arguments in a function call.
 *
 * @see InputTypeStrategy
 * @see ArgumentTypeStrategy
 */
@Internal
public final class InputTypeStrategies {

	// --------------------------------------------------------------------------------------------
	// Input type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Strategy that does not perform any modification or validation of the input.
	 */
	public static final WildcardInputTypeStrategy WILDCARD = new WildcardInputTypeStrategy();

	/**
	 * Strategy for a function signature like {@code f(STRING, NUMERIC)} using a sequence of
	 * {@link ArgumentTypeStrategy}s.
	 */
	public static InputTypeStrategy sequence(ArgumentTypeStrategy... strategies) {
		return new SequenceInputTypeStrategy(Arrays.asList(strategies), null);
	}

	/**
	 * Strategy for a named function signature like {@code f(s STRING, n NUMERIC)} using a sequence
	 * of {@link ArgumentTypeStrategy}s.
	 */
	public static InputTypeStrategy sequence(String[] argumentNames, ArgumentTypeStrategy[] strategies) {
		return new SequenceInputTypeStrategy(Arrays.asList(strategies), Arrays.asList(argumentNames));
	}

	/**
	 * Strategy for a varying function signature like {@code f(INT, STRING, NUMERIC...)} using a
	 * sequence of {@link ArgumentTypeStrategy}s. The first n - 1 arguments must be constant. The
	 * n-th argument can occur 0, 1, or more times.
	 */
	public static InputTypeStrategy varyingSequence(ArgumentTypeStrategy... strategies) {
		return new VaryingSequenceInputTypeStrategy(Arrays.asList(strategies), null);
	}

	/**
	 * Strategy for a varying named function signature like {@code f(i INT, str STRING, num NUMERIC...)}
	 * using a sequence of {@link ArgumentTypeStrategy}s. The first n - 1 arguments must be constant. The
	 * n-th argument can occur 0, 1, or more times.
	 */
	public static InputTypeStrategy varyingSequence(String[] argumentNames, ArgumentTypeStrategy[] strategies) {
		return new VaryingSequenceInputTypeStrategy(Arrays.asList(strategies), Arrays.asList(argumentNames));
	}

	/**
	 * Strategy for a function signature of explicitly defined types like {@code f(STRING, INT)}.
	 * Implicit casts will be inserted if possible.
	 *
	 * <p>This is equivalent to using {@link #sequence(ArgumentTypeStrategy...)} and
	 * {@link #explicit(DataType)}.
	 */
	public static InputTypeStrategy explicitSequence(DataType... expectedDataTypes) {
		final List<ArgumentTypeStrategy> strategies = Arrays.stream(expectedDataTypes)
			.map(InputTypeStrategies::explicit)
			.collect(Collectors.toList());
		return new SequenceInputTypeStrategy(strategies, null);
	}

	/**
	 * Strategy for a named function signature of explicitly defined types like {@code f(s STRING, i INT)}.
	 * Implicit casts will be inserted if possible.
	 *
	 * <p>This is equivalent to using {@link #sequence(String[], ArgumentTypeStrategy[])} and
	 * {@link #explicit(DataType)}.
	 */
	public static InputTypeStrategy explicitSequence(String[] argumentNames, DataType[] expectedDataTypes) {
		final List<ArgumentTypeStrategy> strategies = Arrays.stream(expectedDataTypes)
			.map(InputTypeStrategies::explicit)
			.collect(Collectors.toList());
		return new SequenceInputTypeStrategy(strategies, Arrays.asList(argumentNames));
	}

	/**
	 * Strategy for a disjunction of multiple {@link InputTypeStrategy}s into one like
	 * {@code f(NUMERIC) || f(STRING)}.
	 */
	public static InputTypeStrategy or(InputTypeStrategy... strategies) {
		return new OrInputTypeStrategy(Arrays.asList(strategies));
	}

	// --------------------------------------------------------------------------------------------
	// Argument type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Strategy for inferring an unknown argument type from the function's output {@link DataType}
	 * if available.
	 */
	public static final OutputArgumentTypeStrategy OUTPUT_IF_NULL = new OutputArgumentTypeStrategy();

	/**
	 * Strategy for an argument that can be of any type.
	 */
	public static final AnyArgumentTypeStrategy ANY = new AnyArgumentTypeStrategy();

	/**
	 * Strategy that checks if an argument is a literal.
	 */
	public static final LiteralArgumentTypeStrategy LITERAL = new LiteralArgumentTypeStrategy(false);

	/**
	 * Strategy that checks if an argument is a literal or NULL.
	 */
	public static final LiteralArgumentTypeStrategy LITERAL_OR_NULL = new LiteralArgumentTypeStrategy(true);

	/**
	 * Strategy for an argument that corresponds to an explicitly defined type casting.
	 * Implicit casts will be inserted if possible.
	 */
	public static ExplicitArgumentTypeStrategy explicit(DataType expectedDataType) {
		return new ExplicitArgumentTypeStrategy(expectedDataType);
	}

	/**
	 * Strategy for a conjunction of multiple {@link ArgumentTypeStrategy}s into one like
	 * {@code f(NUMERIC && LITERAL)}.
	 *
	 * <p>Some {@link ArgumentTypeStrategy}s cannot contribute an inferred type that is different from
	 * the input type (e.g. {@link #LITERAL}). Therefore, the order {@code f(X && Y)} or {@code f(Y && X)}
	 * matters as it defines the precedence in case the result must be casted to a more specific type.
	 *
	 * <p>This strategy aims to infer the first more specific, casted type or (if this is not possible)
	 * a type that has been inferred from all {@link ArgumentTypeStrategy}s.
	 */
	public static AndArgumentTypeStrategy and(ArgumentTypeStrategy... strategies) {
		return new AndArgumentTypeStrategy(Arrays.asList(strategies));
	}

	/**
	 * Strategy for a disjunction of multiple {@link ArgumentTypeStrategy}s into one like
	 * {@code f(NUMERIC || STRING)}.
	 *
	 * <p>Some {@link ArgumentTypeStrategy}s cannot contribute an inferred type that is different from
	 * the input type (e.g. {@link #LITERAL}). Therefore, the order {@code f(X || Y)} or {@code f(Y || X)}
	 * matters as it defines the precedence in case the result must be casted to a more specific type.
	 *
	 * <p>This strategy aims to infer a type that is equal to the input type (to prevent unnecessary casting)
	 * or (if this is not possible) the first more specific, casted type.
	 */
	public static OrArgumentTypeStrategy or(ArgumentTypeStrategy... strategies) {
		return new OrArgumentTypeStrategy(Arrays.asList(strategies));
	}

	// --------------------------------------------------------------------------------------------

	private InputTypeStrategies() {
		// no instantiation
	}
}
