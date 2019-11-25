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

package org.apache.flink.table.types.inference.validators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Validator that checks for set of {@link InputTypeValidator}s. It is able to combine function signature validators
 * or single argument validators.
 *
 * <p>This validator offers different semantics depending on the passed {@link Composition}.
 *
 * <p>AND: Conjunction of multiple {@link InputTypeValidator}s into one like {@code f(NUMERIC) && f(LITERAL)}. Only
 * the first validator is used for signature generation.
 *
 * <p>OR: Disjunction of multiple {@link InputTypeValidator}s into one like {@code f(NUMERIC) || f(STRING)}.
 *
 * <p>SEQUENCE: A sequence of {@link SingleInputTypeValidator}s for validating an entire function signature
 * like {@code f(STRING, NUMERIC)}.
 */
@Internal
public final class CompositeTypeValidator implements SingleInputTypeValidator {

	private final Composition composition;

	private final List<? extends InputTypeValidator> validators;

	private final @Nullable List<String> argumentNames;

	/**
	 * Kind of composition for combining {@link InputTypeValidator}s.
	 */
	public enum Composition {
		AND,
		OR,
		SEQUENCE
	}

	public CompositeTypeValidator(
			Composition composition,
			List<? extends InputTypeValidator> validators,
			@Nullable List<String> argumentNames) {
		Preconditions.checkArgument(validators.size() > 0);
		Preconditions.checkArgument(argumentNames == null || argumentNames.size() == validators.size());
		this.composition = composition;
		this.validators = validators;
		this.argumentNames = argumentNames;
	}

	@Override
	public boolean validateArgument(CallContext callContext, int argumentPos, int validatorPos, boolean throwOnFailure) {
		final List<SingleInputTypeValidator> singleValidators = validators.stream()
			.map(v -> (SingleInputTypeValidator) v)
			.collect(Collectors.toList());

		switch (composition) {
			case SEQUENCE:
				return singleValidators.get(validatorPos)
					.validateArgument(callContext, argumentPos, 0, throwOnFailure);
			case AND:
				for (SingleInputTypeValidator validator : singleValidators) {
					if (!validator.validateArgument(callContext, argumentPos, validatorPos, throwOnFailure)) {
						return false;
					}
				}
				return true;
			case OR:
				for (SingleInputTypeValidator validator : singleValidators) {
					if (validator.validateArgument(callContext, argumentPos, validatorPos, false)) {
						return true;
					}
				}
				// generate a helpful exception if possible
				if (throwOnFailure) {
					for (SingleInputTypeValidator validator : singleValidators) {
						validator.validateArgument(callContext, argumentPos, validatorPos, true);
					}
				}
				return false;
			default:
				throw new IllegalStateException("Unsupported composition.");
		}
	}

	@Override
	public ArgumentCount getArgumentCount() {
		switch (composition) {
			case SEQUENCE:
				return ConstantArgumentCount.of(validators.size());
			case AND:
			case OR:
			default:
				final List<ArgumentCount> counts = new AbstractList<ArgumentCount>() {
					public ArgumentCount get(int index) {
						return validators.get(index).getArgumentCount();
					}

					public int size() {
						return validators.size();
					}
				};
				final Integer min = commonMin(counts);
				final Integer max = commonMax(counts);
				final ArgumentCount compositeCount = new ArgumentCount() {
					@Override
					public boolean isValidCount(int count) {
						switch (composition) {
							case AND:
								for (ArgumentCount c : counts) {
									if (!c.isValidCount(count)) {
										return false;
									}
								}
								return true;
							case OR:
							default:
								for (ArgumentCount c : counts) {
									if (c.isValidCount(count)) {
										return true;
									}
								}
								return false;
						}
					}

					@Override
					public Optional<Integer> getMinCount() {
						return Optional.ofNullable(min);
					}

					@Override
					public Optional<Integer> getMaxCount() {
						return Optional.ofNullable(max);
					}
				};

				// use constant count if applicable
				if (min == null || max == null) {
					// no boundaries
					return compositeCount;
				}
				for (int i = min; i <= max; i++) {
					if (!compositeCount.isValidCount(i)) {
						// not the full range
						return compositeCount;
					}
				}
				if (min.equals(max)) {
					return ConstantArgumentCount.of(min);
				}
				return ConstantArgumentCount.between(min, max);
		}
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		switch (composition) {
			case SEQUENCE:
				final List<DataType> dataTypes = callContext.getArgumentDataTypes();
				if (dataTypes.size() != validators.size()) {
					return false;
				}
				for (int i = 0; i < validators.size(); i++) {
					final InputTypeValidator validator = validators.get(i);
					if (!((SingleInputTypeValidator) validator).validateArgument(callContext, i, 0, false)) {
						return false;
					}
				}
				return true;
			case AND:
				for (final InputTypeValidator validator : validators) {
					if (!validator.validate(callContext, false)) {
						return false;
					}
				}
				return true;
			case OR:
				for (final InputTypeValidator validator : validators) {
					if (validator.validate(callContext, false)) {
						return true;
					}
				}
				return false;
			default:
				throw new IllegalStateException("Unsupported composition.");
		}
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		switch (composition) {
			case SEQUENCE:
				final List<Signature.Argument> arguments = new ArrayList<>();
				// according to precondition we can assume single type validators
				// thus we can pass constant 0s
				for (int i = 0; i < validators.size(); i++) {
					final List<Signature> signatures = validators.get(i).getExpectedSignatures(definition);
					final String type;
					if (signatures.size() > 1) {
						type = signatures.stream()
							.map(s -> s.getArguments().get(0).getType())
							.collect(Collectors.joining(" | ", "[", "]"));
					} else {
						type = signatures.get(0).getArguments().get(0).getType();
					}
					if (argumentNames != null) {
						arguments.add(Signature.Argument.of(argumentNames.get(i), type));
					} else {
						arguments.add(Signature.Argument.of(type));
					}
				}
				return Collections.singletonList(Signature.of(arguments));
			case AND:
				final List<Signature> signatures = validators.stream()
					.flatMap(v -> v.getExpectedSignatures(definition).stream())
					.collect(Collectors.toList());
				// handle only the case for simple single type conjunction like "<LITERAL> and STRING"
				final boolean isSimpleAnd = signatures.size() == validators.size() &&
					signatures.stream().allMatch(s -> s.getArguments().size() == 1);
				if (isSimpleAnd) {
					final String type = signatures.stream()
						.map(s -> s.getArguments().get(0).getType())
						.collect(Collectors.joining(" & ", "[", "]"));
					return Collections.singletonList(Signature.of(Signature.Argument.of(type)));
				}
				// use the signatures of the first validator for simplification
				return validators.get(0).getExpectedSignatures(definition);
			case OR:
				return validators.stream()
					.flatMap(v -> v.getExpectedSignatures(definition).stream())
					.collect(Collectors.toList());
			default:
				throw new IllegalStateException("Unsupported composition.");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CompositeTypeValidator that = (CompositeTypeValidator) o;
		return composition == that.composition &&
			validators.equals(that.validators) &&
			Objects.equals(argumentNames, that.argumentNames);
	}

	@Override
	public int hashCode() {
		return Objects.hash(composition, validators, argumentNames);
	}

// --------------------------------------------------------------------------------------------

	/**
	 * Returns the common minimum argument count or null if undefined.
	 */
	private @Nullable Integer commonMin(List<ArgumentCount> counts) {
		// min=5, min=3, min=0           -> min=0
		// min=5, min=3, min=0, min=null -> min=null
		int commonMin = Integer.MAX_VALUE;
		for (ArgumentCount count : counts) {
			final Optional<Integer> min = count.getMinCount();
			if (!min.isPresent()) {
				return null;
			}
			commonMin = Math.min(commonMin, min.get());
		}
		if (commonMin == Integer.MAX_VALUE) {
			return null;
		}
		return commonMin;
	}

	/**
	 * Returns the common maximum argument count or null if undefined.
	 */
	private @Nullable Integer commonMax(List<ArgumentCount> counts) {
		// max=5, max=3, max=0           -> max=5
		// max=5, max=3, max=0, max=null -> max=null
		int commonMax = Integer.MIN_VALUE;
		for (ArgumentCount count : counts) {
			final Optional<Integer> max = count.getMaxCount();
			if (!max.isPresent()) {
				return null;
			}
			commonMax = Math.max(commonMax, max.get());
		}
		if (commonMax == Integer.MIN_VALUE) {
			return null;
		}
		return commonMax;
	}
}
