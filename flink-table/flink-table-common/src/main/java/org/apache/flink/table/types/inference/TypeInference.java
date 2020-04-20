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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Provides logic for the type inference of function calls. It includes:
 * <ul>
 *     <li>explicit input specification for (possibly named and/or typed) arguments</li>
 *     <li>inference of missing or incomplete input types</li>
 *     <li>validation of input types</li>
 *     <li>inference of an intermediate accumulation type</li>
 *     <li>inference of the final output type</li>
 * </ul>
 *
 * <p>See {@link TypeInferenceUtil} for more information about the type inference process.
 */
@PublicEvolving
public final class TypeInference {

	private final @Nullable List<String> namedArguments;

	private final @Nullable List<DataType> typedArguments;

	private final InputTypeStrategy inputTypeStrategy;

	private final @Nullable TypeStrategy accumulatorTypeStrategy;

	private final TypeStrategy outputTypeStrategy;

	private TypeInference(
			@Nullable List<String> namedArguments,
			@Nullable List<DataType> typedArguments,
			InputTypeStrategy inputTypeStrategy,
			@Nullable TypeStrategy accumulatorTypeStrategy,
			TypeStrategy outputTypeStrategy) {
		this.namedArguments = namedArguments;
		this.typedArguments = typedArguments;
		this.inputTypeStrategy = inputTypeStrategy;
		this.accumulatorTypeStrategy = accumulatorTypeStrategy;
		this.outputTypeStrategy = outputTypeStrategy;
		if (namedArguments != null && typedArguments != null && namedArguments.size() != typedArguments.size()) {
			throw new IllegalArgumentException(
				String.format(
					"Mismatch between typed arguments %d and named argument %d.",
					namedArguments.size(),
					typedArguments.size()));
		}
	}

	/**
	 * Builder for configuring and creating instances of {@link TypeInference}.
	 */
	public static TypeInference.Builder newBuilder() {
		return new TypeInference.Builder();
	}

	public Optional<List<String>> getNamedArguments() {
		return Optional.ofNullable(namedArguments);
	}

	public Optional<List<DataType>> getTypedArguments() {
		return Optional.ofNullable(typedArguments);
	}

	public InputTypeStrategy getInputTypeStrategy() {
		return inputTypeStrategy;
	}

	public Optional<TypeStrategy> getAccumulatorTypeStrategy() {
		return Optional.ofNullable(accumulatorTypeStrategy);
	}

	public TypeStrategy getOutputTypeStrategy() {
		return outputTypeStrategy;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for configuring and creating instances of {@link TypeInference}.
	 */
	public static class Builder {

		private @Nullable List<String> namedArguments;

		private @Nullable List<DataType> typedArguments;

		private InputTypeStrategy inputTypeStrategy = InputTypeStrategies.WILDCARD;

		private @Nullable TypeStrategy accumulatorTypeStrategy;

		private @Nullable TypeStrategy outputTypeStrategy;

		public Builder() {
			// default constructor to allow a fluent definition
		}

		/**
		 * Sets the list of argument names for specifying a fixed, not overloaded, not vararg input
		 * signature explicitly.
		 *
		 * <p>This information is useful for SQL's concept of named arguments using the assignment
		 * operator (e.g. {@code FUNC(max => 42)}). The names are used for reordering the call's
		 * arguments to the formal argument order of the function.
		 */
		public Builder namedArguments(List<String> argumentNames) {
			this.namedArguments =
				Preconditions.checkNotNull(argumentNames, "List of argument names must not be null.");
			return this;
		}

		/**
		 * @see #namedArguments(List)
		 */
		public Builder namedArguments(String... argumentNames) {
			return namedArguments(Arrays.asList(argumentNames));
		}

		/**
		 * Sets the list of argument types for specifying a fixed, not overloaded, not vararg input
		 * signature explicitly.
		 *
		 * <p>This information is useful for optional arguments with default value. In particular, the
		 * number of arguments that need to be filled with a default value and their types is important.
		 */
		public Builder typedArguments(List<DataType> argumentTypes) {
			this.typedArguments =
				Preconditions.checkNotNull(argumentTypes, "List of argument types must not be null.");
			return this;
		}

		/**
		 * @see #typedArguments(List)
		 */
		public Builder typedArguments(DataType... argumentTypes) {
			return typedArguments(Arrays.asList(argumentTypes));
		}

		/**
		 * Sets the strategy for inferring and validating input arguments in a function call.
		 *
		 * <p>A {@link InputTypeStrategies#WILDCARD} strategy function is assumed by default.
		 */
		public Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
			this.inputTypeStrategy =
				Preconditions.checkNotNull(inputTypeStrategy, "Input type strategy must not be null.");
			return this;
		}

		/**
		 * Sets the strategy for inferring the intermediate accumulator data type of a function call.
		 */
		public Builder accumulatorTypeStrategy(TypeStrategy accumulatorTypeStrategy) {
			this.accumulatorTypeStrategy =
				Preconditions.checkNotNull(accumulatorTypeStrategy, "Accumulator type strategy must not be null.");
			return this;
		}

		/**
		 * Sets the strategy for inferring the final output data type of a function call.
		 *
		 * <p>Required.
		 */
		public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
			this.outputTypeStrategy =
				Preconditions.checkNotNull(outputTypeStrategy, "Output type strategy must not be null.");
			return this;
		}

		public TypeInference build() {
			return new TypeInference(
				namedArguments,
				typedArguments,
				inputTypeStrategy,
				accumulatorTypeStrategy,
				Preconditions.checkNotNull(outputTypeStrategy, "Output type strategy must not be null."));
		}
	}
}
