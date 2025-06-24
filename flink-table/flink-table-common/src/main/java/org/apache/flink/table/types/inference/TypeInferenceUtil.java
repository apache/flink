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
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CastCallContext;
import org.apache.flink.table.types.inference.utils.UnknownCallContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/**
 * Utility for performing type inference.
 *
 * <p>It assumes that argument assignments have been applied by argument reordering and inserting
 * values for optional arguments.
 *
 * <p>The following steps summarize the type inference process.
 *
 * <ul>
 *   <li>1. Validate number of arguments.
 *   <li>2. For resolving unknown (NULL) operands: Access the outer wrapping call and try to get its
 *       operand type for the return type of the actual call. E.g. for {@code
 *       takes_string(this_function(NULL))} infer operands from {@code takes_string(NULL)} and use
 *       the inferred string type as the return type of {@code this_function(NULL)}.
 *   <li>3. Try infer unknown operands, fail if not possible.
 *   <li>4. Perform input type validation.
 *   <li>5. Infer return type.
 *   <li>6. (Optional) Infer state types.
 *   <li>7. In the planner: Call the strategies again at any point in time to enrich a DataType that
 *       has been created from a logical type with a conversion class.
 *   <li>8. In the planner: Check for an implementation evaluation method matching the operands. The
 *       matching happens class-based. Thus, for example, {@code eval(Object)} is valid for (INT).
 *       Or {@code eval(Object...)} is valid for (INT, STRING). We rely on the conversion classes
 *       specified by {@link DataType}.
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

    /** Casts the call's argument if necessary. */
    public static CallContext castArguments(
            TypeInference typeInference, CallContext callContext, @Nullable DataType outputType) {
        return castArguments(typeInference, callContext, outputType, true);
    }

    private static CallContext castArguments(
            TypeInference typeInference,
            CallContext callContext,
            @Nullable DataType outputType,
            boolean throwOnInferInputFailure) {
        final List<DataType> actualTypes = callContext.getArgumentDataTypes();

        typeInference
                .getStaticArguments()
                .ifPresent(
                        staticArgs -> {
                            if (actualTypes.size() != staticArgs.size()) {
                                throw new ValidationException(
                                        String.format(
                                                "Invalid number of arguments. %d arguments expected after argument expansion but %d passed.",
                                                staticArgs.size(), actualTypes.size()));
                            }
                        });

        final CastCallContext castCallContext =
                inferInputTypes(typeInference, callContext, outputType, throwOnInferInputFailure);

        // final check if the call is valid after casting
        final List<DataType> expectedTypes = castCallContext.getArgumentDataTypes();
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

        return castCallContext;
    }

    /**
     * Infers an output type using the given {@link TypeStrategy}. It assumes that input arguments
     * have been adapted before if necessary.
     */
    public static DataType inferOutputType(
            CallContext callContext, TypeStrategy outputTypeStrategy) {
        final Optional<DataType> potentialOutputType = outputTypeStrategy.inferType(callContext);
        if (potentialOutputType.isEmpty()) {
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

    /**
     * Infers {@link StateInfo}s using the given {@link StateTypeStrategy}s. It assumes that input
     * arguments have been adapted before if necessary.
     */
    public static LinkedHashMap<String, StateInfo> inferStateInfos(
            CallContext callContext, LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies) {
        return stateTypeStrategies.entrySet().stream()
                .map(
                        e ->
                                Map.entry(
                                        e.getKey(),
                                        inferStateInfo(callContext, e.getKey(), e.getValue())))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (x, y) -> y,
                                LinkedHashMap::new));
    }

    /** Generates a signature of the given {@link FunctionDefinition}. */
    public static String generateSignature(
            TypeInference typeInference, String name, FunctionDefinition definition) {
        final List<StaticArgument> staticArguments =
                typeInference.getStaticArguments().orElse(null);
        if (staticArguments != null) {
            return formatStaticArguments(name, staticArguments);
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
     * Validates argument counts.
     *
     * @param argumentCount expected argument count
     * @param actualCount actual argument count
     * @param throwOnFailure if true, the function throws a {@link ValidationException} if the
     *     actual value does not meet the expected argument count
     * @return a boolean indicating if expected argument counts match the actual counts
     */
    public static boolean validateArgumentCount(
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

    /**
     * Information what the outer world (i.e. an outer wrapping call) expects from the current
     * function call. This can be helpful for an {@link InputTypeStrategy}.
     *
     * @see CallContext#getOutputDataType()
     */
    @Internal
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
                        castArguments(typeInference, callContext, null, false);
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
    @Internal
    public static final class Result {

        private final List<DataType> expectedArgumentTypes;

        private final LinkedHashMap<String, StateInfo> stateInfos;

        private final DataType outputDataType;

        public Result(
                List<DataType> expectedArgumentTypes,
                LinkedHashMap<String, StateInfo> stateInfos,
                DataType outputDataType) {
            this.expectedArgumentTypes = expectedArgumentTypes;
            this.stateInfos = stateInfos;
            this.outputDataType = outputDataType;
        }

        public List<DataType> getExpectedArgumentTypes() {
            return expectedArgumentTypes;
        }

        public LinkedHashMap<String, StateInfo> getStateInfos() {
            return stateInfos;
        }

        public DataType getOutputDataType() {
            return outputDataType;
        }
    }

    /** Result of running {@link StateTypeStrategy}. */
    @Internal
    public static final class StateInfo {

        private final DataType dataType;
        private final @Nullable Duration timeToLive;

        private StateInfo(DataType dataType, @Nullable Duration timeToLive) {
            this.dataType = dataType;
            this.timeToLive = timeToLive;
        }

        public DataType getDataType() {
            return dataType;
        }

        public Optional<Duration> getTimeToLive() {
            return Optional.ofNullable(timeToLive);
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

            adaptedCallContext = castArguments(typeInference, callContext, outputType);
        } catch (ValidationException e) {
            throw createInvalidInputException(typeInference, callContext, e);
        }

        // infer output type first for better error message
        // (logically state types should be inferred first)
        final DataType outputType =
                inferOutputType(adaptedCallContext, typeInference.getOutputTypeStrategy());

        final LinkedHashMap<String, StateInfo> stateInfos =
                inferStateInfos(adaptedCallContext, typeInference.getStateTypeStrategies());

        return new Result(adaptedCallContext.getArgumentDataTypes(), stateInfos, outputType);
    }

    private static String formatStaticArguments(String name, List<StaticArgument> staticArguments) {
        final String arguments =
                staticArguments.stream()
                        .map(StaticArgument::toString)
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

    private static CastCallContext inferInputTypes(
            TypeInference typeInference,
            CallContext callContext,
            @Nullable DataType outputType,
            boolean throwOnFailure) {

        final CastCallContext castCallContext = new CastCallContext(callContext, outputType);

        // Static arguments have the highest priority
        final List<StaticArgument> staticArgs = typeInference.getStaticArguments().orElse(null);
        if (staticArgs != null) {
            final List<DataType> fromStaticArgs =
                    IntStream.range(0, staticArgs.size())
                            .mapToObj(
                                    pos -> {
                                        final StaticArgument expectedArg = staticArgs.get(pos);
                                        if (expectedArg.is(StaticArgumentTrait.TABLE)) {
                                            final TableSemantics semantics =
                                                    callContext.getTableSemantics(pos).orElse(null);
                                            if (semantics == null) {
                                                if (throwOnFailure) {
                                                    throw new ValidationException(
                                                            String.format(
                                                                    "Invalid argument value. "
                                                                            + "Argument '%s' expects a table to be passed.",
                                                                    expectedArg.getName()));
                                                }
                                                return null;
                                            }
                                            return semantics.dataType();
                                        }
                                        return expectedArg.getDataType().orElse(null);
                                    })
                            .collect(Collectors.toList());
            if (fromStaticArgs.stream().allMatch(Objects::nonNull)) {
                castCallContext.setExpectedArguments(fromStaticArgs);
            } else if (throwOnFailure) {
                throw new ValidationException("Invalid input arguments.");
            }
        }

        // Even if static arguments are defined, the input strategy is always called
        // for validation purposes
        final List<DataType> inferredDataTypes =
                typeInference
                        .getInputTypeStrategy()
                        .inferInputTypes(castCallContext, throwOnFailure)
                        .orElse(null);
        if (inferredDataTypes != null) {
            castCallContext.setExpectedArguments(inferredDataTypes);
        } else if (throwOnFailure) {
            throw new ValidationException("Invalid input arguments.");
        }

        return castCallContext;
    }

    private static StateInfo inferStateInfo(
            CallContext callContext, String name, StateTypeStrategy stateTypeStrategy) {
        final DataType stateType = stateTypeStrategy.inferType(callContext).orElse(null);
        if (stateType == null || isUnknown(stateType)) {
            final String errorMessage;
            if (name.equals(UserDefinedFunctionHelper.DEFAULT_ACCUMULATOR_NAME)) {
                errorMessage = "Could not infer an accumulator type for the given arguments.";
            } else {
                errorMessage =
                        String.format("Could not infer a data type for state entry '%s'.", name);
            }
            throw new ValidationException(errorMessage);
        }

        final Duration ttl = stateTypeStrategy.getTimeToLive(callContext).orElse(null);

        return new StateInfo(stateType, ttl);
    }

    private static boolean isUnknown(DataType dataType) {
        return dataType.getLogicalType().is(LogicalTypeRoot.NULL);
    }

    private TypeInferenceUtil() {
        // no instantiation
    }
}
