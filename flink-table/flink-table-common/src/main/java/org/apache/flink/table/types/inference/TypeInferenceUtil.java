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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.AdaptedCallContext;
import org.apache.flink.table.types.inference.utils.UnknownCallContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utility for performing type inference.
 *
 * <p>The following steps summarize the envisioned type inference process. Not all features are
 * implemented or exposed through the API yet (*).
 *
 * <ul>
 *   <li>1. Validate number of arguments.
 *   <li>2. (*) Apply assignment operators on the call by permuting operands and adding default
 *       values. These are preparations for {@link CallContext}.
 *   <li>3. For resolving unknown (NULL) operands: Access the outer wrapping call and try to get its
 *       operand type for the return type of the actual call. E.g. for {@code
 *       takes_string(this_function(NULL))} infer operands from {@code takes_string(NULL)} and use
 *       the inferred string type as the return type of {@code this_function(NULL)}.
 *   <li>4. Try infer unknown operands, fail if not possible.
 *   <li>5. (*) Check the usage of DEFAULT operands are correct using validator.isOptional().
 *   <li>6. Perform input type validation.
 *   <li>7. (Optional) Infer accumulator type.
 *   <li>8. Infer return type.
 *   <li>9. (*) In the planner: Call the strategies again at any point in time to enrich a DataType
 *       that has been created from a logical type with a conversion class.
 *   <li>10. (*) In the planner: Check for an implementation evaluation method matching the
 *       operands. The matching happens class-based. Thus, for example, eval(Object) is valid for
 *       (INT). Or eval(Object...) is valid for (INT, STRING). We rely on the conversion classes
 *       specified by DataType.
 * </ul>
 */
@Internal
public final class TypeInferenceUtil {

    /**
     * Runs the entire type inference process.
     *
     * @param typeInference type inference of the current call
     * @param callContext call context of the current call
     * @param surroundingInfo information about the outer wrapping call of a current function call
     *     for performing input type inference
     */
    public static Result runTypeInference(
            TypeInference typeInference,
            CallContext callContext,
            @Nullable SurroundingInfo surroundingInfo) {
        try {
            return runTypeInferenceInternal(typeInference, callContext, surroundingInfo);
        } catch (ValidationException e) {
            throw createInvalidCallException(callContext, e);
        } catch (Throwable t) {
            throw createUnexpectedException(callContext, t);
        }
    }

    /**
     * Adapts the call's argument if necessary.
     *
     * <p>This includes casts that need to be inserted, reordering of arguments (*), or insertion of
     * default values (*) where (*) is future work.
     */
    public static CallContext adaptArguments(
            TypeInference typeInference, CallContext callContext, @Nullable DataType outputType) {
        return adaptArguments(typeInference, callContext, outputType, true);
    }

    private static CallContext adaptArguments(
            TypeInference typeInference,
            CallContext callContext,
            @Nullable DataType outputType,
            boolean throwOnInferInputFailure) {
        final List<DataType> actualTypes = callContext.getArgumentDataTypes();

        typeInference
                .getTypedArguments()
                .ifPresent(
                        (dataTypes) -> {
                            if (actualTypes.size() != dataTypes.size()) {
                                throw new ValidationException(
                                        String.format(
                                                "Invalid number of arguments. %d arguments expected after argument expansion but %d passed.",
                                                dataTypes.size(), actualTypes.size()));
                            }
                        });

        final AdaptedCallContext adaptedCallContext =
                inferInputTypes(typeInference, callContext, outputType, throwOnInferInputFailure);

        // final check if the call is valid after casting
        final List<DataType> expectedTypes = adaptedCallContext.getArgumentDataTypes();
        for (int pos = 0; pos < actualTypes.size(); pos++) {
            final DataType expectedType = expectedTypes.get(pos);
            final DataType actualType = actualTypes.get(pos);
            if (!supportsImplicitCast(actualType.getLogicalType(), expectedType.getLogicalType())) {
                if (!throwOnInferInputFailure) {
                    // abort the adaption, e.g. if a NULL is passed for a NOT NULL argument
                    return callContext;
                }
                throw new ValidationException(
                        String.format(
                                "Invalid argument type at position %d. Data type %s expected but %s passed.",
                                pos, expectedType, actualType));
            }
        }

        return adaptedCallContext;
    }

    /**
     * Infers an output type using the given {@link TypeStrategy}. It assumes that input arguments
     * have been adapted before if necessary.
     */
    public static DataType inferOutputType(
            CallContext callContext, TypeStrategy outputTypeStrategy) {
        final Optional<DataType> potentialOutputType = outputTypeStrategy.inferType(callContext);
        if (!potentialOutputType.isPresent()) {
            throw new ValidationException(
                    "Could not infer an output type for the given arguments.");
        }
        final DataType outputType = potentialOutputType.get();

        if (isUnknown(outputType)) {
            throw new ValidationException(
                    "Could not infer an output type for the given arguments. Untyped NULL received.");
        }
        return outputType;
    }

    /** Generates a signature of the given {@link FunctionDefinition}. */
    public static String generateSignature(
            TypeInference typeInference, String name, FunctionDefinition definition) {
        if (typeInference.getTypedArguments().isPresent()) {
            return formatNamedOrTypedArguments(name, typeInference);
        }
        return typeInference.getInputTypeStrategy().getExpectedSignatures(definition).stream()
                .map(s -> formatSignature(name, s))
                .collect(Collectors.joining("\n"));
    }

    /** Returns an exception for invalid input arguments. */
    public static ValidationException createInvalidInputException(
            TypeInference typeInference, CallContext callContext, ValidationException cause) {
        return new ValidationException(
                String.format(
                        "Invalid input arguments. Expected signatures are:\n%s",
                        generateSignature(
                                typeInference,
                                callContext.getName(),
                                callContext.getFunctionDefinition())),
                cause);
    }

    /** Returns an exception for an invalid call to a function. */
    public static ValidationException createInvalidCallException(
            CallContext callContext, ValidationException cause) {
        return new ValidationException(
                String.format(
                        "Invalid function call:\n%s(%s)",
                        callContext.getName(),
                        callContext.getArgumentDataTypes().stream()
                                .map(DataType::toString)
                                .collect(Collectors.joining(", "))),
                cause);
    }

    /** Returns an exception for an unexpected error during type inference. */
    public static TableException createUnexpectedException(
            CallContext callContext, Throwable cause) {
        return new TableException(
                String.format(
                        "Unexpected error in type inference logic of function '%s'. This is a bug.",
                        callContext.getName()),
                cause);
    }

    /**
     * Information what the outer world (i.e. an outer wrapping call) expects from the current
     * function call. This can be helpful for an {@link InputTypeStrategy}.
     *
     * @see CallContext#getOutputDataType()
     */
    public interface SurroundingInfo {

        static SurroundingInfo of(
                String name,
                FunctionDefinition functionDefinition,
                TypeInference typeInference,
                int argumentCount,
                int innerCallPosition,
                boolean isGroupedAggregation) {
            return typeFactory -> {
                final boolean isValidCount =
                        validateArgumentCount(
                                typeInference.getInputTypeStrategy().getArgumentCount(),
                                argumentCount,
                                false);
                if (!isValidCount) {
                    return Optional.empty();
                }
                // for "takes_string(this_function(NULL))" simulate "takes_string(NULL)"
                // for retrieving the output type of "this_function(NULL)"
                final CallContext callContext =
                        new UnknownCallContext(
                                typeFactory,
                                name,
                                functionDefinition,
                                argumentCount,
                                isGroupedAggregation);

                // We might not be able to infer the input types at this moment, if the surrounding
                // function does not provide an explicit input type strategy.
                final CallContext adaptedContext =
                        adaptArguments(typeInference, callContext, null, false);
                return typeInference
                        .getInputTypeStrategy()
                        .inferInputTypes(adaptedContext, false)
                        .map(dataTypes -> dataTypes.get(innerCallPosition));
            };
        }

        static SurroundingInfo of(DataType dataType) {
            return typeFactory -> Optional.of(dataType);
        }

        Optional<DataType> inferOutputType(DataTypeFactory typeFactory);
    }

    /**
     * The result of a type inference run. It contains information about how arguments need to be
     * adapted in order to comply with the function's signature.
     *
     * <p>This includes casts that need to be inserted, reordering of arguments (*), or insertion of
     * default values (*) where (*) is future work.
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

    private static Result runTypeInferenceInternal(
            TypeInference typeInference,
            CallContext callContext,
            @Nullable SurroundingInfo surroundingInfo) {
        try {
            validateArgumentCount(
                    typeInference.getInputTypeStrategy().getArgumentCount(),
                    callContext.getArgumentDataTypes().size(),
                    true);
        } catch (ValidationException e) {
            throw createInvalidInputException(typeInference, callContext, e);
        }

        final CallContext adaptedCallContext;
        try {
            // use information of surrounding call to determine output type of this call
            final DataType outputType;
            if (surroundingInfo != null) {
                outputType =
                        surroundingInfo
                                .inferOutputType(callContext.getDataTypeFactory())
                                .orElse(null);
            } else {
                outputType = null;
            }

            adaptedCallContext = adaptArguments(typeInference, callContext, outputType);
        } catch (ValidationException e) {
            throw createInvalidInputException(typeInference, callContext, e);
        }

        // infer output type first for better error message
        // (logically an accumulator type should be inferred first)
        final DataType outputType =
                inferOutputType(adaptedCallContext, typeInference.getOutputTypeStrategy());

        final DataType accumulatorType =
                inferAccumulatorType(
                        adaptedCallContext,
                        outputType,
                        typeInference.getAccumulatorTypeStrategy().orElse(null));

        return new Result(adaptedCallContext.getArgumentDataTypes(), accumulatorType, outputType);
    }

    private static String formatNamedOrTypedArguments(String name, TypeInference typeInference) {
        final Optional<List<String>> optionalNames = typeInference.getNamedArguments();
        final Optional<List<DataType>> optionalDataTypes = typeInference.getTypedArguments();
        final int count =
                Math.max(
                        optionalNames.map(List::size).orElse(0),
                        optionalDataTypes.map(List::size).orElse(0));
        final String arguments =
                IntStream.range(0, count)
                        .mapToObj(
                                pos -> {
                                    final StringBuilder builder = new StringBuilder();
                                    optionalNames.ifPresent(
                                            names -> builder.append(names.get(pos)).append(" => "));
                                    optionalDataTypes.ifPresent(
                                            dataTypes ->
                                                    builder.append(dataTypes.get(pos).toString()));
                                    return builder.toString();
                                })
                        .collect(Collectors.joining(", "));
        return String.format("%s(%s)", name, arguments);
    }

    private static String formatSignature(String name, Signature s) {
        final String arguments =
                s.getArguments().stream()
                        .map(TypeInferenceUtil::formatArgument)
                        .collect(Collectors.joining(", "));
        return String.format("%s(%s)", name, arguments);
    }

    private static String formatArgument(Signature.Argument arg) {
        final StringBuilder stringBuilder = new StringBuilder();
        arg.getName().ifPresent(n -> stringBuilder.append(n).append(" "));
        stringBuilder.append(arg.getType());
        return stringBuilder.toString();
    }

    private static boolean validateArgumentCount(
            ArgumentCount argumentCount, int actualCount, boolean throwOnFailure) {
        final int minCount = argumentCount.getMinCount().orElse(0);
        if (actualCount < minCount) {
            if (throwOnFailure) {
                throw new ValidationException(
                        String.format(
                                "Invalid number of arguments. At least %d arguments expected but %d passed.",
                                minCount, actualCount));
            }
            return false;
        }
        final int maxCount = argumentCount.getMaxCount().orElse(Integer.MAX_VALUE);
        if (actualCount > maxCount) {
            if (throwOnFailure) {
                throw new ValidationException(
                        String.format(
                                "Invalid number of arguments. At most %d arguments expected but %d passed.",
                                maxCount, actualCount));
            }
            return false;
        }
        if (!argumentCount.isValidCount(actualCount)) {
            if (throwOnFailure) {
                throw new ValidationException(
                        String.format(
                                "Invalid number of arguments. %d arguments passed.", actualCount));
            }
            return false;
        }
        return true;
    }

    private static AdaptedCallContext inferInputTypes(
            TypeInference typeInference,
            CallContext callContext,
            @Nullable DataType outputType,
            boolean throwOnFailure) {

        final AdaptedCallContext adaptedCallContext =
                new AdaptedCallContext(callContext, outputType);

        // typed arguments have highest priority
        typeInference.getTypedArguments().ifPresent(adaptedCallContext::setExpectedArguments);

        final List<DataType> inferredDataTypes =
                typeInference
                        .getInputTypeStrategy()
                        .inferInputTypes(adaptedCallContext, throwOnFailure)
                        .orElse(null);

        if (inferredDataTypes != null) {
            adaptedCallContext.setExpectedArguments(inferredDataTypes);
        } else if (throwOnFailure) {
            throw new ValidationException("Invalid input arguments.");
        }

        return adaptedCallContext;
    }

    private static @Nullable DataType inferAccumulatorType(
            CallContext callContext,
            DataType outputType,
            @Nullable TypeStrategy accumulatorTypeStrategy) {
        if (callContext.getFunctionDefinition().getKind() != FunctionKind.TABLE_AGGREGATE
                && callContext.getFunctionDefinition().getKind() != FunctionKind.AGGREGATE) {
            return null;
        }

        // an accumulator might be an internal feature of the planner, therefore it is not
        // mandatory here; we assume the output type to be the accumulator type in this case
        if (accumulatorTypeStrategy == null) {
            return outputType;
        }
        final Optional<DataType> potentialAccumulatorType =
                accumulatorTypeStrategy.inferType(callContext);
        if (!potentialAccumulatorType.isPresent()) {
            throw new ValidationException(
                    "Could not infer an accumulator type for the given arguments.");
        }
        final DataType accumulatorType = potentialAccumulatorType.get();

        if (isUnknown(accumulatorType)) {
            throw new ValidationException(
                    "Could not infer an accumulator type for the given arguments. Untyped NULL received.");
        }

        return accumulatorType;
    }

    private static boolean isUnknown(DataType dataType) {
        return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.NULL);
    }

    private TypeInferenceUtil() {
        // no instantiation
    }
}
