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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility for performing type inference.
 */
@Internal
public final class TypeInferenceUtil {

	public static Result runTypeInference(TypeInference typeInference, CallContext callContext) {
		try {
			return runTypeInferenceInternal(typeInference, callContext);
		} catch (ValidationException e) {
			throw new ValidationException(
				String.format(
					"Invalid call to function '%s'. Given arguments: %s",
					callContext.getName(),
					callContext.getArgumentDataTypes().stream()
						.map(DataType::toString)
						.collect(Collectors.joining(", "))),
				e);
		} catch (Throwable t) {
			throw new TableException(
				String.format(
					"Unexpected error in type inference logic of function '%s'. This is a bug.",
					callContext.getName()),
				t);
		}
	}

	/**
	 * The result of a type inference run. It contains information about how arguments need to be
	 * adapted in order to comply with the function's signature.
	 *
	 * <p>This includes casts that need to be inserted, reordering of arguments (*), or insertion of default
	 * values (*) where (*) is future work.
	 */
	public static final class Result {

		private final List<DataType> expectedArgumentTypes;

		private final @Nullable DataType accumulatorDataType;

		private final DataType outputDataType;

		public Result(
				List<DataType> expectedArgumentTypes,
				@Nullable DataType accumulatorDataType,
				DataType outputDataType) {
			this.expectedArgumentTypes = expectedArgumentTypes;
			this.accumulatorDataType = accumulatorDataType;
			this.outputDataType = outputDataType;
		}

		public List<DataType> getExpectedArgumentTypes() {
			return expectedArgumentTypes;
		}

		public Optional<DataType> getAccumulatorDataType() {
			return Optional.ofNullable(accumulatorDataType);
		}

		public DataType getOutputDataType() {
			return outputDataType;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static Result runTypeInferenceInternal(TypeInference typeInference, CallContext callContext) {
		final List<DataType> argumentTypes = callContext.getArgumentDataTypes();

		try {
			validateArgumentCount(
				typeInference.getInputTypeValidator().getArgumentCount(),
				argumentTypes.size());
		} catch (ValidationException e) {
			throw getInvalidInputException(typeInference.getInputTypeValidator(), callContext);
		}

		final List<DataType> expectedTypes = typeInference.getArgumentTypes()
			.orElse(argumentTypes);

		final AdaptedCallContext adaptedCallContext = adaptArguments(
			callContext,
			expectedTypes);

		try {
			validateInputTypes(
				typeInference.getInputTypeValidator(),
				adaptedCallContext);
		} catch (ValidationException e) {
			throw getInvalidInputException(typeInference.getInputTypeValidator(), adaptedCallContext);
		}

		return inferTypes(
			adaptedCallContext,
			typeInference.getAccumulatorTypeStrategy().orElse(null),
			typeInference.getOutputTypeStrategy());
	}

	private static ValidationException getInvalidInputException(
			InputTypeValidator validator,
			CallContext callContext) {

		final String expectedSignatures = validator.getExpectedSignatures(callContext.getFunctionDefinition())
			.stream()
			.map(s -> formatSignature(callContext.getName(), s))
			.collect(Collectors.joining("\n"));
		return new ValidationException(
			String.format(
				"Invalid input arguments. Expected signatures are:\n%s",
				expectedSignatures));
	}

	private static String formatSignature(String name, Signature s) {
		final String arguments = s.getArguments()
			.stream()
			.map(TypeInferenceUtil::formatArgument)
			.collect(Collectors.joining(", "));
		return String.format("%s(%s)", name, arguments);
	}

	private static String formatArgument(Signature.Argument arg) {
		final StringBuilder stringBuilder = new StringBuilder();
		arg.getName().ifPresent(n -> stringBuilder.append(n).append(" => "));
		stringBuilder.append(arg.getType());
		return stringBuilder.toString();
	}

	private static void validateArgumentCount(ArgumentCount argumentCount, int actualCount) {
		argumentCount.getMinCount().ifPresent((min) -> {
			if (actualCount < min) {
				throw new ValidationException(
					String.format(
						"Invalid number of arguments. At least %d arguments expected but %d passed.",
						min,
						actualCount));
			}
		});

		argumentCount.getMaxCount().ifPresent((max) -> {
			if (actualCount > max) {
				throw new ValidationException(
					String.format(
						"Invalid number of arguments. At most %d arguments expected but %d passed.",
						max,
						actualCount));
			}
		});

		if (!argumentCount.isValidCount(actualCount)) {
			throw new ValidationException(
				String.format(
					"Invalid number of arguments. %d arguments passed.",
					actualCount));
		}
	}

	private static void validateInputTypes(InputTypeValidator inputTypeValidator, CallContext callContext) {
		if (!inputTypeValidator.validate(callContext, true)) {
			throw new ValidationException("Invalid input arguments.");
		}
	}

	/**
	 * Adapts the call's argument if necessary.
	 *
	 * <p>This includes casts that need to be inserted, reordering of arguments (*), or insertion of default
	 * values (*) where (*) is future work.
	 */
	private static AdaptedCallContext adaptArguments(
			CallContext callContext,
			List<DataType> expectedTypes) {

		final List<DataType> actualTypes = callContext.getArgumentDataTypes();
		for (int pos = 0; pos < actualTypes.size(); pos++) {
			final DataType expectedType = expectedTypes.get(pos);
			final DataType actualType = actualTypes.get(pos);

			if (!actualType.equals(expectedType) && !canCast(actualType, expectedType)) {
				throw new ValidationException(
					String.format(
						"Invalid argument type at position %d. Data type %s expected but %s passed.",
						pos,
						expectedType,
						actualType));
			}
		}

		return new AdaptedCallContext(callContext, expectedTypes);
	}

	private static boolean canCast(DataType sourceDataType, DataType targetDataType) {
		return LogicalTypeCasts.supportsImplicitCast(
			sourceDataType.getLogicalType(),
			targetDataType.getLogicalType());
	}

	private static Result inferTypes(
			AdaptedCallContext adaptedCallContext,
			@Nullable TypeStrategy accumulatorTypeStrategy,
			TypeStrategy outputTypeStrategy) {

		// infer output type first for better error message
		// (logically an accumulator type should be inferred first)
		final Optional<DataType> potentialOutputType = outputTypeStrategy.inferType(adaptedCallContext);
		if (!potentialOutputType.isPresent()) {
			throw new ValidationException("Could not infer an output type for the given arguments.");
		}
		final DataType outputType = potentialOutputType.get();

		if (adaptedCallContext.getFunctionDefinition().getKind() == FunctionKind.TABLE_AGGREGATE ||
				adaptedCallContext.getFunctionDefinition().getKind() == FunctionKind.AGGREGATE) {
			// an accumulator might be an internal feature of the planner, therefore it is not
			// mandatory here; we assume the output type to be the accumulator type in this case
			if (accumulatorTypeStrategy == null) {
				return new Result(adaptedCallContext.expectedArguments, outputType, outputType);
			}
			final Optional<DataType> potentialAccumulatorType = accumulatorTypeStrategy.inferType(adaptedCallContext);
			if (!potentialAccumulatorType.isPresent()) {
				throw new ValidationException("Could not infer an accumulator type for the given arguments.");
			}
			return new Result(adaptedCallContext.expectedArguments, potentialAccumulatorType.get(), outputType);

		} else {
			return new Result(adaptedCallContext.expectedArguments, null, outputType);
		}
	}

	/**
	 * Helper context that deals with adapted arguments.
	 *
	 * <p>For example, if an argument needs to be casted to a target type, an expression that was a
	 * literal before is not a literal anymore in this call context.
	 */
	private static class AdaptedCallContext implements CallContext {

		private final CallContext originalContext;
		private final List<DataType> expectedArguments;

		public AdaptedCallContext(CallContext originalContext, List<DataType> castedArguments) {
			this.originalContext = originalContext;
			this.expectedArguments = castedArguments;
		}

		@Override
		public List<DataType> getArgumentDataTypes() {
			return expectedArguments;
		}

		@Override
		public FunctionDefinition getFunctionDefinition() {
			return originalContext.getFunctionDefinition();
		}

		@Override
		public boolean isArgumentLiteral(int pos) {
			if (isCasted(pos)) {
				return false;
			}
			return originalContext.isArgumentLiteral(pos);
		}

		@Override
		public boolean isArgumentNull(int pos) {
			// null remains null regardless of casting
			return originalContext.isArgumentNull(pos);
		}

		@Override
		public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
			if (isCasted(pos)) {
				return Optional.empty();
			}
			return originalContext.getArgumentValue(pos, clazz);
		}

		@Override
		public String getName() {
			return originalContext.getName();
		}

		private boolean isCasted(int pos) {
			return !originalContext.getArgumentDataTypes().get(pos).equals(expectedArguments.get(pos));
		}
	}

	private TypeInferenceUtil() {
		// no instantiation
	}
}
