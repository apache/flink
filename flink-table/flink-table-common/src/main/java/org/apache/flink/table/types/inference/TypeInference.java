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

import java.util.List;
import java.util.Optional;

/**
 * Provides logic for the type inference of function calls. It includes:
 * <ul>
 *     <li>explicit input specification for (possibly named) arguments</li>
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

	private final @Nullable List<String> argumentNames;

	private final @Nullable List<DataType> argumentTypes;

	private final @Nullable InputTypeStrategy inputTypeStrategy;

	private final InputTypeValidator inputTypeValidator;

	private final @Nullable TypeStrategy accumulatorTypeStrategy;

	private final TypeStrategy outputTypeStrategy;

	private TypeInference(
			@Nullable List<String> argumentNames,
			@Nullable List<DataType> argumentTypes,
			@Nullable InputTypeStrategy inputTypeStrategy,
			InputTypeValidator inputTypeValidator,
			@Nullable TypeStrategy accumulatorTypeStrategy,
			TypeStrategy outputTypeStrategy) {
		this.argumentNames = argumentNames;
		this.argumentTypes = argumentTypes;
		this.inputTypeStrategy = inputTypeStrategy;
		this.inputTypeValidator = inputTypeValidator;
		this.accumulatorTypeStrategy = accumulatorTypeStrategy;
		this.outputTypeStrategy = outputTypeStrategy;
		if (argumentNames != null && argumentTypes != null && argumentNames.size() != argumentTypes.size()) {
			throw new IllegalArgumentException(
				String.format(
					"Mismatch between argument types %d and argument names %d.",
					argumentNames.size(),
					argumentTypes.size()));
		}
	}

	/**
	 * Builder for configuring and creating instances of {@link TypeInference}.
	 */
	public static TypeInference.Builder newBuilder() {
		return new TypeInference.Builder();
	}

	public Optional<List<String>> getArgumentNames() {
		return Optional.ofNullable(argumentNames);
	}

	public Optional<List<DataType>> getArgumentTypes() {
		return Optional.ofNullable(argumentTypes);
	}

	public Optional<InputTypeStrategy> getInputTypeStrategy() {
		return Optional.ofNullable(inputTypeStrategy);
	}

	public InputTypeValidator getInputTypeValidator() {
		return inputTypeValidator;
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

		private @Nullable List<String> argumentNames;

		private @Nullable List<DataType> argumentTypes;

		private @Nullable InputTypeStrategy inputTypeStrategy;

		private InputTypeValidator inputTypeValidator = InputTypeValidators.PASSING;

		private @Nullable TypeStrategy accumulatorTypeStrategy;

		private @Nullable TypeStrategy outputTypeStrategy;

		public Builder() {
			// default constructor to allow a fluent definition
		}

		/**
		 * Sets the list of argument names for specifying static input explicitly.
		 *
		 * <p>This information is useful for SQL's concept of named arguments using the assignment
		 * operator (e.g. {@code FUNC(max => 42)}).
		 */
		public Builder namedArguments(List<String> argumentNames) {
			this.argumentNames =
				Preconditions.checkNotNull(argumentNames, "List of argument names must not be null.");
			return this;
		}

		/**
		 * Sets the list of argument types for specifying static input explicitly.
		 *
		 * <p>This information is useful for implicit and safe casting.
		 */
		public Builder typedArguments(List<DataType> argumentTypes) {
			this.argumentTypes =
				Preconditions.checkNotNull(argumentTypes, "List of argument types must not be null.");
			return this;
		}

		/**
		 * Sets the strategy for inferring missing or incomplete input argument data types.
		 */
		public Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
			this.inputTypeStrategy =
				Preconditions.checkNotNull(inputTypeStrategy, "Input type strategy must not be null.");
			return this;
		}

		/**
		 * Sets the validator for checking the input data types of a function call.
		 *
		 * <p>A always passing function is assumed by default (see {@link InputTypeValidators#PASSING}).
		 */
		public Builder inputTypeValidator(InputTypeValidator inputTypeValidator) {
			this.inputTypeValidator =
				Preconditions.checkNotNull(inputTypeValidator, "Input type validator must not be null.");
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
				argumentNames,
				argumentTypes,
				inputTypeStrategy,
				inputTypeValidator,
				accumulatorTypeStrategy,
				Preconditions.checkNotNull(outputTypeStrategy, "Output type strategy must not be null."));
		}
	}
}
