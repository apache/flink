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
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.ColumnList;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    public static final int PROCESS_TABLE_FUNCTION_ARG_UID_OFFSET = 0;
    public static final String PROCESS_TABLE_FUNCTION_ARG_UID = "uid";
    public static final int PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET = 1;
    public static final String PROCESS_TABLE_FUNCTION_ARG_ON_TIME = "on_time";

    public static final List<StaticArgument> PROCESS_TABLE_FUNCTION_SYSTEM_ARGS =
            List.of(
                    StaticArgument.scalar(
                            PROCESS_TABLE_FUNCTION_ARG_ON_TIME, DataTypes.DESCRIPTOR(), true),
                    StaticArgument.scalar(
                            PROCESS_TABLE_FUNCTION_ARG_UID, DataTypes.STRING(), true));

    public static final String PROCESS_TABLE_FUNCTION_RESULT_ROWTIME = "rowtime";

    /**
     * Format of unique identifiers for {@link ProcessTableFunction}.
     *
     * <p>Leading digits are not allowed. This also prevents that a custom PTF uid can interfere
     * with {@code ExecutionConfigOptions#TABLE_EXEC_UID_FORMAT}.
     */
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

    public static boolean isInvalidUidForProcessTableFunction(String uid) {
        return !UID_FORMAT.test(uid);
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
        checkMultipleTableArgs(declaredArgs);
        checkPassThroughColumns(declaredArgs);

        final List<StaticArgument> newStaticArgs = new ArrayList<>(declaredArgs);
        newStaticArgs.addAll(PROCESS_TABLE_FUNCTION_SYSTEM_ARGS);
        return newStaticArgs;
    }

    private static void checkReservedArgs(List<StaticArgument> staticArgs) {
        final Set<String> declaredArgs =
                staticArgs.stream().map(StaticArgument::getName).collect(Collectors.toSet());
        final List<String> reservedArgs =
                PROCESS_TABLE_FUNCTION_SYSTEM_ARGS.stream()
                        .map(StaticArgument::getName)
                        .collect(Collectors.toList());
        if (reservedArgs.stream().anyMatch(declaredArgs::contains)) {
            throw new ValidationException(
                    "Function signature must not declare system arguments. "
                            + "Reserved argument names are: "
                            + reservedArgs);
        }
    }

    private static void checkMultipleTableArgs(List<StaticArgument> staticArgs) {
        if (staticArgs.stream().filter(arg -> arg.is(StaticArgumentTrait.TABLE)).count() <= 1) {
            return;
        }
        if (staticArgs.stream().anyMatch(arg -> !arg.is(StaticArgumentTrait.TABLE_AS_SET))) {
            throw new ValidationException(
                    "All table arguments must use set semantics if multiple table arguments are declared.");
        }
    }

    private static void checkPassThroughColumns(List<StaticArgument> staticArgs) {
        final Set<StaticArgumentTrait> traits =
                staticArgs.stream()
                        .flatMap(arg -> arg.getTraits().stream())
                        .collect(Collectors.toSet());
        if (!traits.contains(StaticArgumentTrait.PASS_COLUMNS_THROUGH)) {
            return;
        }
        if (traits.contains(StaticArgumentTrait.SUPPORT_UPDATES)) {
            throw new ValidationException(
                    "Signatures with updating inputs must not pass columns through.");
        }
        if (staticArgs.stream().filter(arg -> arg.is(StaticArgumentTrait.TABLE)).count() > 1) {
            throw new ValidationException(
                    "Pass-through columns are not supported if multiple table arguments are declared.");
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
        return new SystemOutputStrategy(functionKind, staticArgs, outputStrategy);
    }

    private static class SystemOutputStrategy implements TypeStrategy {

        private final FunctionKind functionKind;
        private final List<StaticArgument> staticArgs;
        private final TypeStrategy origin;

        private SystemOutputStrategy(
                FunctionKind functionKind, List<StaticArgument> staticArgs, TypeStrategy origin) {
            this.functionKind = functionKind;
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
                                fields.addAll(deriveRowtimeField(callContext));

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
            if (functionKind != FunctionKind.PROCESS_TABLE) {
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
                                final DataType rowDataType =
                                        DataTypes.ROW(DataType.getFields(argDataTypes.get(pos)));
                                final DataType projectedRow =
                                        Projection.of(semantics.partitionByColumns())
                                                .project(rowDataType);
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

        private List<Field> deriveRowtimeField(CallContext callContext) {
            if (this.functionKind != FunctionKind.PROCESS_TABLE) {
                return List.of();
            }
            final List<DataType> args = callContext.getArgumentDataTypes();

            // Check if on_time is defined and non-empty
            final int onTimePos = args.size() - 1 - PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET;
            final Set<String> onTimeFields =
                    callContext
                            .getArgumentValue(onTimePos, ColumnList.class)
                            .map(ColumnList::getNames)
                            .map(Set::copyOf)
                            .orElse(Set.of());

            final Set<String> usedOnTimeFields = new HashSet<>();

            final List<LogicalType> onTimeColumns = new ArrayList<>();
            final List<String> missingOnTimeColumns = new ArrayList<>();
            IntStream.range(0, staticArgs.size())
                    .forEach(
                            pos -> {
                                final StaticArgument staticArg = staticArgs.get(pos);
                                if (!staticArg.is(StaticArgumentTrait.TABLE)) {
                                    return;
                                }
                                final RowType rowType =
                                        LogicalTypeUtils.toRowType(args.get(pos).getLogicalType());
                                final int onTimeColumn =
                                        findUniqueOnTimeColumn(
                                                staticArg.getName(), rowType, onTimeFields);
                                if (onTimeColumn >= 0) {
                                    usedOnTimeFields.add(rowType.getFieldNames().get(onTimeColumn));
                                    onTimeColumns.add(rowType.getTypeAt(onTimeColumn));
                                    return;
                                }
                                if (staticArg.is(StaticArgumentTrait.REQUIRE_ON_TIME)) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Table argument '%s' requires a time attribute. "
                                                            + "Please provide one using the implicit `on_time` argument. "
                                                            + "For example: myFunction(..., on_time => DESCRIPTOR(`my_timestamp`)",
                                                    staticArg.getName()));
                                } else {
                                    missingOnTimeColumns.add(staticArg.getName());
                                }
                            });

            if (!onTimeColumns.isEmpty() && !missingOnTimeColumns.isEmpty()) {
                throw new ValidationException(
                        "Invalid time attribute declaration. If multiple tables are declared, the `on_time` argument "
                                + "must reference a time column for each table argument or none. "
                                + "Missing time attributes for: "
                                + missingOnTimeColumns);
            }

            final Set<String> unusedOnTimeFields = new HashSet<>(onTimeFields);
            unusedOnTimeFields.removeAll(usedOnTimeFields);
            if (!unusedOnTimeFields.isEmpty()) {
                throw new ValidationException(
                        "Invalid time attribute declaration. "
                                + "Each column in the `on_time` argument must reference at least one "
                                + "column in one of the table arguments. Unknown references: "
                                + unusedOnTimeFields);
            }

            if (onTimeColumns.isEmpty()) {
                return List.of();
            }

            // Don't allow mixtures of time attribute roots
            final Set<LogicalTypeRoot> onTimeRoots =
                    onTimeColumns.stream()
                            .map(LogicalType::getTypeRoot)
                            .collect(Collectors.toSet());
            if (onTimeRoots.size() > 1) {
                throw new ValidationException(
                        "Invalid time attribute declaration. "
                                + "All columns in the `on_time` argument must reference the same data type kind. "
                                + "But found: "
                                + onTimeRoots);
            }

            final LogicalType commonOnTimeType =
                    LogicalTypeMerging.findCommonType(onTimeColumns)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Unable to derive data type for PTF result time attribute."));

            final LogicalType resultTimestamp =
                    forwardTimeAttribute(commonOnTimeType, onTimeColumns);

            return List.of(
                    DataTypes.FIELD(
                            PROCESS_TABLE_FUNCTION_RESULT_ROWTIME, DataTypes.of(resultTimestamp)));
        }

        private static int findUniqueOnTimeColumn(
                String tableArgName, RowType rowType, Set<String> onTimeFields) {
            final List<RowField> fields = rowType.getFields();
            int found = -1;
            for (int pos = 0; pos < fields.size(); pos++) {
                final RowField field = fields.get(pos);
                if (!onTimeFields.contains(field.getName())) {
                    continue;
                }
                if (found != -1) {
                    throw new ValidationException(
                            String.format(
                                    "Ambiguous time attribute found. "
                                            + "The `on_time` argument must reference at most one column in a table argument. "
                                            + "Currently, the columns in `on_time` point to both '%s' and '%s' in table argument '%s'.",
                                    fields.get(found).getName(), field.getName(), tableArgName));
                }
                found = pos;
                if (isUnsupportedOnTimeColumn(field.getType())) {
                    throw new ValidationException(
                            String.format(
                                    "Unsupported data type for time attribute. "
                                            + "The `on_time` argument must reference a TIMESTAMP or TIMESTAMP_LTZ column (up to precision 3). "
                                            + "However, column '%s' in table argument '%s' has data type '%s'.",
                                    field.getName(),
                                    tableArgName,
                                    field.getType().asSummaryString()));
                }
            }
            return found;
        }

        private static LogicalType forwardTimeAttribute(
                LogicalType timestampType, List<LogicalType> onTimeColumns) {
            if (onTimeColumns.stream().noneMatch(LogicalTypeChecks::isTimeAttribute)) {
                return timestampType.copy(false);
            }
            switch (timestampType.getTypeRoot()) {
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return new TimestampType(
                            false,
                            TimestampKind.ROWTIME,
                            LogicalTypeChecks.getPrecision(timestampType));
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return new LocalZonedTimestampType(
                            false,
                            TimestampKind.ROWTIME,
                            LogicalTypeChecks.getPrecision(timestampType));
                default:
                    throw new IllegalStateException(
                            "Timestamp type expected for PTF result time attribute.");
            }
        }

        private static boolean isUnsupportedOnTimeColumn(LogicalType type) {
            return !LogicalTypeChecks.canBeTimeAttributeType(type)
                    || LogicalTypeChecks.getPrecision(type) > 3;
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
                return callContext.fail(
                        throwOnFailure,
                        "Process table functions must declare a static signature "
                                + "that is not overloaded and doesn't contain varargs.");
            }

            try {
                checkTableArgs(staticArgs, callContext);
                checkUidArg(callContext);
            } catch (ValidationException e) {
                return callContext.fail(throwOnFailure, e.getMessage());
            }

            return Optional.of(inferredDataTypes);
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            // Access the original input type strategy to give the function a chance to return a
            // valid signature
            return origin.getExpectedSignatures(definition);
        }

        private static void checkUidArg(CallContext callContext) {
            final List<DataType> args = callContext.getArgumentDataTypes();

            // Verify the uid format if provided
            final int uidPos = args.size() - 1 - PROCESS_TABLE_FUNCTION_ARG_UID_OFFSET;
            if (!callContext.isArgumentNull(uidPos)) {
                final String uid = callContext.getArgumentValue(uidPos, String.class).orElse("");
                if (isInvalidUidForProcessTableFunction(uid)) {
                    throw new ValidationException(
                            "Invalid unique identifier for process table function. The `uid` argument "
                                    + "must be a string literal that follows the pattern [a-zA-Z_][a-zA-Z-_0-9]*. "
                                    + "But found: "
                                    + uid);
                }
            }
        }

        private static void checkTableArgs(
                List<StaticArgument> staticArgs, CallContext callContext) {
            final List<TableSemantics> tableSemantics = new ArrayList<>();
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
                                tableSemantics.add(semantics);
                            });
            checkCoPartitioning(tableSemantics);
        }

        private static void checkCoPartitioning(List<TableSemantics> tableSemantics) {
            if (tableSemantics.isEmpty()) {
                return;
            }
            final List<LogicalType> partitioningTypes =
                    tableSemantics.stream()
                            .map(
                                    semantics -> {
                                        final LogicalType tableType =
                                                semantics.dataType().getLogicalType();
                                        final List<LogicalType> fieldTypes =
                                                LogicalTypeChecks.getFieldTypes(tableType);
                                        final LogicalType[] partitionTypes =
                                                Arrays.stream(semantics.partitionByColumns())
                                                        .mapToObj(fieldTypes::get)
                                                        .toArray(LogicalType[]::new);
                                        return (LogicalType) RowType.of(partitionTypes);
                                    })
                            .collect(Collectors.toList());
            final LogicalType commonType =
                    LogicalTypeMerging.findCommonType(partitioningTypes).orElse(null);
            if (commonType == null
                    || partitioningTypes.stream()
                            .anyMatch(
                                    partitioningType ->
                                            !LogicalTypeCasts.supportsAvoidingCast(
                                                    partitioningType, commonType))) {
                throw new ValidationException(
                        "Invalid PARTITION BY columns. The number of columns and their data types must match "
                                + "across all involved table arguments. Given partition key sets: "
                                + partitioningTypes.stream()
                                        .map(LogicalType::getChildren)
                                        .map(Object::toString)
                                        .collect(Collectors.joining(", ")));
            }
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
