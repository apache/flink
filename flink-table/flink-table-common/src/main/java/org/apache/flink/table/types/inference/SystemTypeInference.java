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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Extends the {@link TypeInference} function-aware by additional system columns and validation.
 *
 * <p>During planning system columns are available and can be accessed in SQL, during runtime those
 * columns are not passed or returned by the eval() method. They are handled with custom code paths.
 *
 * <p>For example, for {@link ProcessTableFunction}, this utility class implicitly adds the optional
 * {@code uid} and {@code on_time} args and an additional {@code rowtime} column in the output.
 * Additionally, it adds a validation layer for complex {@link StaticArgument}s.
 */
@Internal
public class SystemTypeInference {

    private static final List<StaticArgument> PROCESS_TABLE_FUNCTION_SYSTEM_ARGS =
            List.of(StaticArgument.scalar("uid", DataTypes.STRING(), true));

    /** Format of unique identifiers for {@link ProcessTableFunction}. */
    private static final Predicate<String> UID_FORMAT =
            Pattern.compile("^[a-zA-Z_][a-zA-Z-_0-9]*$").asPredicate();

    public static TypeInference of(FunctionKind functionKind, TypeInference origin) {
        final TypeInference.Builder builder = TypeInference.newBuilder();

        final List<StaticArgument> systemArgs =
                deriveSystemArgs(functionKind, origin.getStaticArguments().orElse(null));
        if (systemArgs != null) {
            builder.staticArguments(systemArgs);
        }
        builder.inputTypeStrategy(
                deriveSystemInputStrategy(functionKind, systemArgs, origin.getInputTypeStrategy()));
        builder.stateTypeStrategies(origin.getStateTypeStrategies());
        builder.outputTypeStrategy(
                deriveSystemOutputStrategy(
                        functionKind, systemArgs, origin.getOutputTypeStrategy()));
        return builder.build();
    }

    // --------------------------------------------------------------------------------------------

    private static void checkScalarArgsOnly(List<StaticArgument> defaultArgs) {
        defaultArgs.forEach(
                arg -> {
                    if (!arg.is(StaticArgumentTrait.SCALAR)) {
                        throw new ValidationException(
                                String.format(
                                        "Only scalar arguments are supported at this location. "
                                                + "But argument '%s' declared the following traits: %s",
                                        arg.getName(), arg.getTraits()));
                    }
                });
    }

    private static @Nullable List<StaticArgument> deriveSystemArgs(
            FunctionKind functionKind, @Nullable List<StaticArgument> declaredArgs) {
        if (functionKind != FunctionKind.PROCESS_TABLE) {
            if (declaredArgs != null) {
                checkScalarArgsOnly(declaredArgs);
            }
            return declaredArgs;
        }
        if (declaredArgs == null) {
            throw new ValidationException(
                    "Function requires a static signature that is not overloaded and doesn't contain varargs.");
        }

        checkReservedArgs(declaredArgs);

        final List<StaticArgument> newStaticArgs = new ArrayList<>(declaredArgs);
        newStaticArgs.addAll(PROCESS_TABLE_FUNCTION_SYSTEM_ARGS);
        return newStaticArgs;
    }

    private static void checkReservedArgs(List<StaticArgument> staticArgs) {
        final Set<String> declaredArgs =
                staticArgs.stream().map(StaticArgument::getName).collect(Collectors.toSet());
        final Set<String> reservedArgs =
                PROCESS_TABLE_FUNCTION_SYSTEM_ARGS.stream()
                        .map(StaticArgument::getName)
                        .collect(Collectors.toSet());
        if (reservedArgs.stream().anyMatch(declaredArgs::contains)) {
            throw new ValidationException(
                    "Function signature must not declare system arguments. "
                            + "Reserved argument names are: "
                            + reservedArgs);
        }
    }

    private static InputTypeStrategy deriveSystemInputStrategy(
            FunctionKind functionKind,
            @Nullable List<StaticArgument> staticArgs,
            InputTypeStrategy inputStrategy) {
        if (functionKind != FunctionKind.PROCESS_TABLE) {
            return inputStrategy;
        }
        return new SystemInputStrategy(staticArgs, inputStrategy);
    }

    private static TypeStrategy deriveSystemOutputStrategy(
            FunctionKind functionKind,
            @Nullable List<StaticArgument> staticArgs,
            TypeStrategy outputStrategy) {
        if (functionKind != FunctionKind.TABLE && functionKind != FunctionKind.PROCESS_TABLE) {
            return outputStrategy;
        }
        return new SystemOutputStrategy(staticArgs, outputStrategy);
    }

    private static class SystemOutputStrategy implements TypeStrategy {

        private final List<StaticArgument> staticArgs;
        private final TypeStrategy origin;

        private SystemOutputStrategy(List<StaticArgument> staticArgs, TypeStrategy origin) {
            this.staticArgs = staticArgs;
            this.origin = origin;
        }

        @Override
        public Optional<DataType> inferType(CallContext callContext) {
            return origin.inferType(callContext)
                    .map(
                            functionDataType -> {
                                final List<Field> fields = new ArrayList<>();

                                // According to the SQL standard, pass-through columns should
                                // actually be added at the end of the output row type. However,
                                // looking at the overall landscape we deviate from the standard in
                                // this regard:
                                // - Calcite built-in window functions add them at the beginning
                                // - MATCH_RECOGNIZE adds PARTITION BY columns at the beginning
                                // - Flink SESSION windows add pass-through columns at the beginning
                                // - Oracle adds pass-through columns for all ROW semantics args, so
                                // this whole topic is kind of vendor specific already
                                fields.addAll(derivePassThroughFields(callContext));
                                fields.addAll(deriveFunctionOutputFields(functionDataType));

                                final List<Field> uniqueFields = makeFieldNamesUnique(fields);

                                return DataTypes.ROW(uniqueFields).notNull();
                            });
        }

        private List<Field> makeFieldNamesUnique(List<Field> fields) {
            final Map<String, Integer> fieldCount = new HashMap<>();
            return fields.stream()
                    .map(
                            item -> {
                                final int nextCount =
                                        fieldCount.compute(
                                                item.getName(),
                                                (fieldName, count) ->
                                                        count == null ? -1 : count + 1);
                                final String newFieldName =
                                        nextCount < 0 ? item.getName() : item.getName() + nextCount;
                                return DataTypes.FIELD(newFieldName, item.getDataType());
                            })
                    .collect(Collectors.toList());
        }

        private List<Field> derivePassThroughFields(CallContext callContext) {
            if (staticArgs == null) {
                return List.of();
            }
            final List<DataType> argDataTypes = callContext.getArgumentDataTypes();
            return IntStream.range(0, staticArgs.size())
                    .mapToObj(
                            pos -> {
                                final StaticArgument arg = staticArgs.get(pos);
                                if (arg.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH)) {
                                    return DataType.getFields(argDataTypes.get(pos)).stream();
                                }
                                if (!arg.is(StaticArgumentTrait.TABLE_AS_SET)) {
                                    return Stream.<Field>empty();
                                }
                                final TableSemantics semantics =
                                        callContext
                                                .getTableSemantics(pos)
                                                .orElseThrow(IllegalStateException::new);
                                final DataType projectedRow =
                                        Projection.of(semantics.partitionByColumns())
                                                .project(argDataTypes.get(pos));
                                return DataType.getFields(projectedRow).stream();
                            })
                    .flatMap(s -> s)
                    .collect(Collectors.toList());
        }

        private List<Field> deriveFunctionOutputFields(DataType functionDataType) {
            final List<DataType> fieldTypes = DataType.getFieldDataTypes(functionDataType);
            final List<String> fieldNames = DataType.getFieldNames(functionDataType);

            if (fieldTypes.isEmpty()) {
                // Before the system type inference was introduced, SQL and
                // Table API chose a different default field name.
                // EXPR$0 is chosen for best-effort backwards compatibility for
                // SQL users.
                return List.of(DataTypes.FIELD("EXPR$0", functionDataType));
            }
            return IntStream.range(0, fieldTypes.size())
                    .mapToObj(pos -> DataTypes.FIELD(fieldNames.get(pos), fieldTypes.get(pos)))
                    .collect(Collectors.toList());
        }
    }

    private static class SystemInputStrategy implements InputTypeStrategy {

        private final List<StaticArgument> staticArgs;
        private final InputTypeStrategy origin;

        private SystemInputStrategy(List<StaticArgument> staticArgs, InputTypeStrategy origin) {
            this.staticArgs = staticArgs;
            this.origin = origin;
        }

        @Override
        public ArgumentCount getArgumentCount() {
            // Static arguments take precedence. Thus, the input strategy only serves as a
            // validation layer. Since the count is already validated we don't need to validate it a
            // second time.
            return InputTypeStrategies.WILDCARD.getArgumentCount();
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            final List<DataType> args = callContext.getArgumentDataTypes();

            // Access the original input type strategy to give the function a chance to perform
            // validation
            final List<DataType> inferredDataTypes =
                    origin.inferInputTypes(callContext, throwOnFailure).orElse(null);

            // Check that the input type strategy doesn't influence the static arguments
            if (inferredDataTypes == null || !inferredDataTypes.equals(args)) {
                throw new ValidationException(
                        "Process table functions must declare a static signature "
                                + "that is not overloaded and doesn't contain varargs.");
            }

            checkUidColumn(callContext);
            checkMultipleTableArgs(callContext);
            checkTableArgTraits(staticArgs, callContext);

            return Optional.of(inferredDataTypes);
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            // Access the original input type strategy to give the function a chance to return a
            // valid signature
            return origin.getExpectedSignatures(definition);
        }

        private static void checkUidColumn(CallContext callContext) {
            final List<DataType> args = callContext.getArgumentDataTypes();

            // Verify the uid format if provided
            int uidPos = args.size() - 1;
            if (!callContext.isArgumentNull(uidPos)) {
                final String uid = callContext.getArgumentValue(uidPos, String.class).orElse("");
                if (!UID_FORMAT.test(uid)) {
                    throw new ValidationException(
                            "Invalid unique identifier for process table function. The 'uid' argument "
                                    + "must be a string literal that follows the pattern [a-zA-Z_][a-zA-Z-_0-9]*. "
                                    + "But found: "
                                    + uid);
                }
            }
        }

        private static void checkMultipleTableArgs(CallContext callContext) {
            final List<DataType> args = callContext.getArgumentDataTypes();

            final List<TableSemantics> tableSemantics =
                    IntStream.range(0, args.size())
                            .mapToObj(pos -> callContext.getTableSemantics(pos).orElse(null))
                            .collect(Collectors.toList());
            if (tableSemantics.stream().filter(Objects::nonNull).count() > 1) {
                throw new ValidationException(
                        "Currently, only signatures with at most one table argument are supported.");
            }
        }

        private static void checkTableArgTraits(
                List<StaticArgument> staticArgs, CallContext callContext) {
            IntStream.range(0, staticArgs.size())
                    .forEach(
                            pos -> {
                                final StaticArgument staticArg = staticArgs.get(pos);
                                if (!staticArg.is(StaticArgumentTrait.TABLE)) {
                                    return;
                                }
                                final TableSemantics semantics =
                                        callContext.getTableSemantics(pos).orElse(null);
                                if (semantics == null) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Table expected for argument '%s'.",
                                                    staticArg.getName()));
                                }
                                checkRowSemantics(staticArg, semantics);
                                checkSetSemantics(staticArg, semantics);
                            });
        }

        private static void checkRowSemantics(StaticArgument staticArg, TableSemantics semantics) {
            if (!staticArg.is(StaticArgumentTrait.TABLE_AS_ROW)) {
                return;
            }
            if (semantics.partitionByColumns().length > 0
                    || semantics.orderByColumns().length > 0) {
                throw new ValidationException(
                        "PARTITION BY or ORDER BY are not supported for table arguments with row semantics.");
            }
        }

        private static void checkSetSemantics(StaticArgument staticArg, TableSemantics semantics) {
            if (!staticArg.is(StaticArgumentTrait.TABLE_AS_SET)) {
                return;
            }
            if (semantics.partitionByColumns().length == 0
                    && !staticArg.is(StaticArgumentTrait.OPTIONAL_PARTITION_BY)) {
                throw new ValidationException(
                        String.format(
                                "Table argument '%s' requires a PARTITION BY clause for parallel processing.",
                                staticArg.getName()));
            }
        }
    }
}
