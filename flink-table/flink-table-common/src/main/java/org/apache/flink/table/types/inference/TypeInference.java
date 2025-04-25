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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Provides logic for the type inference of function calls. It includes:
 *
 * <ul>
 *   <li>explicit input specification for static arguments
 *   <li>inference of missing or incomplete input types
 *   <li>validation of input types
 *   <li>inference of intermediate result types (i.e. state entries)
 *   <li>inference of the final output type
 * </ul>
 *
 * <p>See {@link TypeInferenceUtil} for more information about the type inference process.
 */
@PublicEvolving
public final class TypeInference {

    /** Format for both arguments and state entries. */
    static final Predicate<String> PARAMETER_NAME_FORMAT =
            Pattern.compile("^[a-zA-Z_$][a-zA-Z_$0-9]*$").asPredicate();

    private final @Nullable List<StaticArgument> staticArguments;
    private final InputTypeStrategy inputTypeStrategy;
    private final LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies;
    private final TypeStrategy outputTypeStrategy;

    private TypeInference(
            @Nullable List<StaticArgument> staticArguments,
            InputTypeStrategy inputTypeStrategy,
            LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies,
            TypeStrategy outputTypeStrategy) {
        this.staticArguments = staticArguments;
        this.inputTypeStrategy = inputTypeStrategy;
        this.stateTypeStrategies = stateTypeStrategies;
        this.outputTypeStrategy = outputTypeStrategy;
        checkStateEntries();
    }

    /** Builder for configuring and creating instances of {@link TypeInference}. */
    public static TypeInference.Builder newBuilder() {
        return new TypeInference.Builder();
    }

    public Optional<List<StaticArgument>> getStaticArguments() {
        return Optional.ofNullable(staticArguments);
    }

    public InputTypeStrategy getInputTypeStrategy() {
        return inputTypeStrategy;
    }

    public LinkedHashMap<String, StateTypeStrategy> getStateTypeStrategies() {
        return stateTypeStrategies;
    }

    public TypeStrategy getOutputTypeStrategy() {
        return outputTypeStrategy;
    }

    /**
     * @deprecated Use {@link #getStaticArguments()} instead.
     */
    @Deprecated
    public Optional<List<String>> getNamedArguments() {
        return Optional.ofNullable(staticArguments)
                .map(
                        args ->
                                args.stream()
                                        .map(StaticArgument::getName)
                                        .collect(Collectors.toList()));
    }

    /**
     * @deprecated Use {@link #getStaticArguments()} instead.
     */
    @Deprecated
    public Optional<List<DataType>> getTypedArguments() {
        return Optional.ofNullable(staticArguments)
                .map(
                        args ->
                                args.stream()
                                        .map(
                                                arg ->
                                                        arg.getDataType()
                                                                .orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Scalar argument with a data type expected.")))
                                        .collect(Collectors.toList()));
    }

    /**
     * @deprecated Use {@link #getStaticArguments()} instead.
     */
    @Deprecated
    public Optional<List<Boolean>> getOptionalArguments() {
        return Optional.ofNullable(staticArguments)
                .map(
                        args ->
                                args.stream()
                                        .map(StaticArgument::isOptional)
                                        .collect(Collectors.toList()));
    }

    /**
     * @deprecated Use {@link #getStateTypeStrategies()} instead.
     */
    @Deprecated
    public Optional<TypeStrategy> getAccumulatorTypeStrategy() {
        if (stateTypeStrategies.isEmpty()) {
            return Optional.empty();
        }
        if (stateTypeStrategies.size() != 1) {
            throw new IllegalArgumentException(
                    "An accumulator should contain exactly one state type strategy.");
        }
        return Optional.of(stateTypeStrategies.values().iterator().next());
    }

    private void checkStateEntries() {
        // Verify state
        final List<String> invalidStateEntries =
                stateTypeStrategies.keySet().stream()
                        .filter(n -> !PARAMETER_NAME_FORMAT.test(n))
                        .collect(Collectors.toList());
        if (!invalidStateEntries.isEmpty()) {
            throw new ValidationException(
                    "Invalid state names. A state entry must follow the pattern [a-zA-Z_$][a-zA-Z_$0-9]*. But found: "
                            + invalidStateEntries);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Builder
    // --------------------------------------------------------------------------------------------

    /** Builder for configuring and creating instances of {@link TypeInference}. */
    @PublicEvolving
    public static class Builder {

        private @Nullable List<StaticArgument> staticArguments;
        private InputTypeStrategy inputTypeStrategy = InputTypeStrategies.WILDCARD;
        private LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies =
                new LinkedHashMap<>();
        private @Nullable TypeStrategy outputTypeStrategy;

        // Legacy
        private @Nullable List<String> namedArguments;
        private @Nullable List<Boolean> optionalArguments;
        private @Nullable List<DataType> typedArguments;

        public Builder() {
            // default constructor to allow a fluent definition
        }

        /**
         * Sets a list of arguments in a static signature that is not overloaded and does not
         * support varargs.
         *
         * <p>Static arguments are a special case of an input type strategy and takes precedence. A
         * signature can take tables, models, or scalar values. It allows optional and/or named
         * argument like {@code f(myArg => 12)}.
         */
        public Builder staticArguments(StaticArgument... staticArguments) {
            this.staticArguments = Arrays.asList(staticArguments);
            return this;
        }

        /**
         * Sets a list of arguments in a static signature that is not overloaded and does not
         * support varargs.
         *
         * <p>Static arguments are a special case of an input type strategy and takes precedence. A
         * signature can take tables, models, or scalar values. It allows optional and/or named
         * argument like {@code f(myArg => 12)}.
         */
        public Builder staticArguments(List<StaticArgument> staticArgument) {
            this.staticArguments = staticArgument;
            return this;
        }

        /**
         * Sets the strategy for inferring and validating input arguments in a function call.
         *
         * <p>A {@link InputTypeStrategies#WILDCARD} strategy function is assumed by default.
         */
        public Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
            this.inputTypeStrategy =
                    Preconditions.checkNotNull(
                            inputTypeStrategy, "Input type strategy must not be null.");
            return this;
        }

        /**
         * Sets the strategy for inferring the intermediate accumulator data type of an aggregate
         * function call.
         */
        public Builder accumulatorTypeStrategy(TypeStrategy accumulatorTypeStrategy) {
            Preconditions.checkNotNull(
                    accumulatorTypeStrategy, "Accumulator type strategy must not be null.");
            this.stateTypeStrategies.put(
                    UserDefinedFunctionHelper.DEFAULT_ACCUMULATOR_NAME,
                    StateTypeStrategy.of(accumulatorTypeStrategy));
            return this;
        }

        /**
         * Sets a map of state names to {@link StateTypeStrategy}s for inferring a function call's
         * intermediate result data types (i.e. state entries). For aggregate functions, only one
         * entry is allowed which defines the accumulator's data type.
         */
        public Builder stateTypeStrategies(
                LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies) {
            this.stateTypeStrategies = stateTypeStrategies;
            return this;
        }

        /**
         * Sets the strategy for inferring the final output data type of a function call.
         *
         * <p>Required.
         */
        public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
            this.outputTypeStrategy =
                    Preconditions.checkNotNull(
                            outputTypeStrategy, "Output type strategy must not be null.");
            return this;
        }

        public TypeInference build() {
            return new TypeInference(
                    createStaticArguments(),
                    inputTypeStrategy,
                    stateTypeStrategies,
                    Preconditions.checkNotNull(
                            outputTypeStrategy, "Output type strategy must not be null."));
        }

        /**
         * Sets the list of argument names for specifying a fixed, not overloaded, not vararg input
         * signature explicitly.
         *
         * <p>This information is useful for SQL's concept of named arguments using the assignment
         * operator (e.g. {@code FUNC(max => 42)}). The names are used for reordering the call's
         * arguments to the formal argument order of the function.
         *
         * @deprecated Use {@link #staticArguments(List)} instead.
         */
        @Deprecated
        public Builder namedArguments(List<String> argumentNames) {
            this.namedArguments =
                    Preconditions.checkNotNull(
                            argumentNames, "List of argument names must not be null.");
            return this;
        }

        /**
         * @see #namedArguments(List)
         * @deprecated Use {@link #staticArguments(StaticArgument...)} instead.
         */
        @Deprecated
        public Builder namedArguments(String... argumentNames) {
            return namedArguments(Arrays.asList(argumentNames));
        }

        /**
         * Sets the list of argument optionals for specifying optional arguments in the input
         * signature explicitly.
         *
         * <p>This information is useful for SQL's concept of named arguments using the assignment
         * operator. The optionals are used to determine whether an argument is optional or required
         * in the function call.
         *
         * @deprecated Use {@link #staticArguments(List)} instead.
         */
        @Deprecated
        public Builder optionalArguments(List<Boolean> optionalArguments) {
            this.optionalArguments =
                    Preconditions.checkNotNull(
                            optionalArguments, "List of argument optionals must not be null.");
            return this;
        }

        /**
         * Sets the list of argument types for specifying a fixed, not overloaded, not vararg input
         * signature explicitly.
         *
         * <p>This information is useful for optional arguments with default value. In particular,
         * the number of arguments that need to be filled with a default value and their types is
         *
         * @deprecated Use {@link #staticArguments(List)} instead.
         */
        @Deprecated
        public Builder typedArguments(List<DataType> argumentTypes) {
            this.typedArguments =
                    Preconditions.checkNotNull(
                            argumentTypes, "List of argument types must not be null.");
            return this;
        }

        /**
         * @see #typedArguments(List)
         * @deprecated Use {@link #staticArguments(StaticArgument...)} instead.
         */
        @Deprecated
        public Builder typedArguments(DataType... argumentTypes) {
            return typedArguments(Arrays.asList(argumentTypes));
        }

        private @Nullable List<StaticArgument> createStaticArguments() {
            if (staticArguments != null) {
                return staticArguments;
            }
            // Legacy path
            if (typedArguments != null) {
                if (namedArguments != null && namedArguments.size() != typedArguments.size()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Mismatch between typed arguments %d and named arguments %d.",
                                    typedArguments.size(), namedArguments.size()));
                }
                if (optionalArguments != null
                        && optionalArguments.size() != typedArguments.size()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Mismatch between typed arguments %d and optional arguments %d.",
                                    typedArguments.size(), optionalArguments.size()));
                }
                return IntStream.range(0, typedArguments.size())
                        .mapToObj(
                                pos ->
                                        StaticArgument.scalar(
                                                Optional.ofNullable(namedArguments)
                                                        .map(args -> args.get(pos))
                                                        .orElse("arg" + pos),
                                                typedArguments.get(pos),
                                                Optional.ofNullable(optionalArguments)
                                                        .map(args -> args.get(pos))
                                                        .orElse(false)))
                        .collect(Collectors.toList());
            }
            return null;
        }
    }
}
