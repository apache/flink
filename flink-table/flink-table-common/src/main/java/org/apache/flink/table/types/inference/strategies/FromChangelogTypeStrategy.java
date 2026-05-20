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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction.ChangelogContext;
import org.apache.flink.table.functions.ChangelogModeStrategy;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/** Type strategies for the {@code FROM_CHANGELOG} process table function. */
@Internal
public final class FromChangelogTypeStrategy {

    // Positional argument indexes for FROM_CHANGELOG. Must match the order of StaticArguments
    // registered in BuiltInFunctionDefinitions#FROM_CHANGELOG; changing one without the other
    // silently breaks argument resolution.
    public static final int ARG_TABLE = 0;
    public static final int ARG_OP = 1;
    public static final int ARG_OP_MAPPING = 2;
    public static final int ARG_ERROR_HANDLING = 3;

    private static final Set<String> VALID_ROW_KIND_NAMES =
            Set.of("INSERT", "UPDATE_BEFORE", "UPDATE_AFTER", "DELETE");

    private static final String UPDATE_BEFORE = RowKind.UPDATE_BEFORE.name();

    private static final String UPDATE_AFTER = RowKind.UPDATE_AFTER.name();

    // --------------------------------------------------------------------------------------------
    // Input validation
    // --------------------------------------------------------------------------------------------

    public static final InputTypeStrategy INPUT_TYPE_STRATEGY =
            new ValidationOnlyInputTypeStrategy() {
                @Override
                public Optional<List<DataType>> inferInputTypes(
                        final CallContext callContext, final boolean throwOnFailure) {
                    return validateInputs(callContext, throwOnFailure);
                }
            };

    // --------------------------------------------------------------------------------------------
    // Output type inference
    // --------------------------------------------------------------------------------------------

    public static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext -> {
                final TableSemantics tableSemantics =
                        callContext
                                .getTableSemantics(ARG_TABLE)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "First argument must be a table for FROM_CHANGELOG."));

                final String opColumnName =
                        ChangelogTypeStrategyUtils.resolveOpColumnName(callContext);

                final List<Field> outputFields = buildOutputFields(tableSemantics, opColumnName);

                return Optional.of(DataTypes.ROW(outputFields).notNull());
            };

    // --------------------------------------------------------------------------------------------
    // Changelog mode inference
    // --------------------------------------------------------------------------------------------

    /**
     * Emits an upsert changelog when the input is partitioned (set semantics) and the resolved
     * {@code op_mapping} maps to {@code UPDATE_AFTER} without {@code UPDATE_BEFORE}. In all other
     * cases the output is a retract changelog. When upsert mode is selected, the partition key acts
     * as the upsert key.
     */
    public static final ChangelogModeStrategy CHANGELOG_MODE_STRATEGY =
            ctx -> isUpsertConfig(ctx) ? ChangelogMode.upsert(false) : ChangelogMode.all();

    /**
     * Returns {@code true} when the FROM_CHANGELOG call should emit an upsert changelog: the input
     * table is partitioned AND the resolved {@code op_mapping} contains {@code UPDATE_AFTER}
     * without {@code UPDATE_BEFORE}. Falls back to {@code false} when the mapping is absent or
     * cannot be resolved as a literal. The default mapping maps to a retract table.
     */
    private static boolean isUpsertConfig(final ChangelogContext ctx) {
        if (!isPartitioned(ctx::getTableSemantics)) {
            return false;
        }
        return ctx.getArgumentValue(ARG_OP_MAPPING, Map.class)
                .map(FromChangelogTypeStrategy::describesUpsert)
                .orElse(false);
    }

    /**
     * Returns {@code true} if the mapping maps {@code UPDATE_AFTER} without {@code UPDATE_BEFORE},
     * i.e. it describes an upsert changelog.
     */
    private static boolean describesUpsert(final Map<String, String> mapping) {
        return mappingContains(mapping, UPDATE_AFTER) && !mappingContains(mapping, UPDATE_BEFORE);
    }

    private static boolean mappingContains(
            final Map<String, String> mapping, final String rowKindName) {
        return mapping.values().stream()
                .filter(Objects::nonNull)
                .anyMatch(v -> rowKindName.equals(v.trim()));
    }

    private static boolean isPartitioned(
            final IntFunction<Optional<TableSemantics>> tableSemantics) {
        return tableSemantics
                .apply(ARG_TABLE)
                .map(ts -> ts.partitionByColumns().length > 0)
                .orElse(false);
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static Optional<List<DataType>> validateInputs(
            final CallContext callContext, final boolean throwOnFailure) {
        Optional<List<DataType>> error;

        error = validateTableArg(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        error = validateOpDescriptor(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        error = validateOpColumn(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        error = validateOpMapping(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        error = validateErrorHandling(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    private static Optional<List<DataType>> validateTableArg(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.getTableSemantics(ARG_TABLE).isEmpty()) {
            return callContext.fail(
                    throwOnFailure, "First argument must be a table for FROM_CHANGELOG.");
        }
        return Optional.empty();
    }

    private static Optional<List<DataType>> validateOpDescriptor(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<ColumnList> opDescriptor =
                callContext.getArgumentValue(ARG_OP, ColumnList.class);
        if (opDescriptor.isPresent() && opDescriptor.get().getNames().size() != 1) {
            return callContext.fail(
                    throwOnFailure,
                    "The descriptor for argument 'op' must contain exactly one column name.");
        }
        return Optional.empty();
    }

    /** Validates that the op column exists in the input schema and is of STRING type. */
    private static Optional<List<DataType>> validateOpColumn(
            final CallContext callContext, final boolean throwOnFailure) {

        final TableSemantics tableSemantics = callContext.getTableSemantics(ARG_TABLE).get();
        final String opColumnName = ChangelogTypeStrategyUtils.resolveOpColumnName(callContext);
        final OptionalInt opIndex =
                ChangelogTypeStrategyUtils.resolveOpColumnIndex(tableSemantics, opColumnName);
        if (opIndex.isEmpty()) {
            return callContext.fail(
                    throwOnFailure,
                    String.format(
                            "The op column '%s' does not exist in the input schema.",
                            opColumnName));
        }
        final LogicalType opFieldType =
                DataType.getFieldDataTypes(tableSemantics.dataType())
                        .get(opIndex.getAsInt())
                        .getLogicalType();
        if (!opFieldType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return callContext.fail(
                    throwOnFailure,
                    String.format(
                            "The op column '%s' must be of STRING type, but was '%s'.",
                            opColumnName, opFieldType));
        }
        return Optional.empty();
    }

    /** Validates op_mapping is a literal and its values are valid change operation names. */
    @SuppressWarnings("unchecked")
    private static Optional<List<DataType>> validateOpMapping(
            final CallContext callContext, final boolean throwOnFailure) {
        final boolean hasMappingArgProvided = !callContext.isArgumentNull(ARG_OP_MAPPING);
        final boolean isMappingArgLiteral = callContext.isArgumentLiteral(ARG_OP_MAPPING);
        if (hasMappingArgProvided && !isMappingArgLiteral) {
            return callContext.fail(
                    throwOnFailure, "The 'op_mapping' argument must be a constant MAP literal.");
        }

        final Optional<Map> opMapping = callContext.getArgumentValue(ARG_OP_MAPPING, Map.class);
        if (opMapping.isPresent()) {
            final Map<String, String> mapping = opMapping.get();
            final Optional<List<DataType>> validationError =
                    validateOpMappingValues(callContext, mapping, throwOnFailure);
            if (validationError.isPresent()) {
                return validationError;
            }
            // A mapping that produces UPDATE_AFTER without UPDATE_BEFORE describes an upsert
            // changelog. Upsert mode requires a key, so PARTITION BY must be present on the
            // table argument; otherwise the call would produce key-less updates.
            if (describesUpsert(mapping) && !isPartitioned(callContext::getTableSemantics)) {
                return callContext.fail(
                        throwOnFailure,
                        "An 'op_mapping' that produces UPDATE_AFTER without UPDATE_BEFORE "
                                + "describes an upsert changelog and requires a key. Add "
                                + "PARTITION BY to the input table to define the upsert key, "
                                + "or include UPDATE_BEFORE in the mapping for retract "
                                + "semantics.");
            }
        }
        return Optional.empty();
    }

    /**
     * Validates op_mapping values. Values must be valid Flink change operation names. Each name
     * must appear at most once across all entries.
     */
    private static Optional<List<DataType>> validateOpMappingValues(
            final CallContext callContext,
            final Map<String, String> opMapping,
            final boolean throwOnFailure) {
        final Set<String> allRowKindsSeen = new HashSet<>();

        for (final String value : opMapping.values()) {
            final String rowKindName = value.trim();
            if (!VALID_ROW_KIND_NAMES.contains(rowKindName)) {
                return callContext.fail(
                        throwOnFailure,
                        String.format(
                                "Invalid target mapping for argument 'op_mapping'. "
                                        + "Unknown change operation: '%s'. Valid values are: %s.",
                                rowKindName, VALID_ROW_KIND_NAMES));
            }
            final boolean isDuplicate = !allRowKindsSeen.add(rowKindName);
            if (isDuplicate) {
                return callContext.fail(
                        throwOnFailure,
                        String.format(
                                "Invalid target mapping for argument 'op_mapping'. "
                                        + "Duplicate change operation: '%s'. "
                                        + "Use comma-separated keys to map multiple codes to the same operation "
                                        + "(e.g., MAP['c, r', 'INSERT']).",
                                rowKindName));
            }
        }
        return Optional.empty();
    }

    private static Optional<List<DataType>> validateErrorHandling(
            final CallContext callContext, final boolean throwOnFailure) {
        final boolean hasErrorHandlingArgProvided = !callContext.isArgumentNull(ARG_ERROR_HANDLING);
        final boolean isErrorHandlingArgLiteral = callContext.isArgumentLiteral(ARG_ERROR_HANDLING);
        if (hasErrorHandlingArgProvided && !isErrorHandlingArgLiteral) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'error_handling' argument must be a constant STRING literal.");
        }

        final Optional<String> optionalErrorHandlingArg =
                callContext.getArgumentValue(ARG_ERROR_HANDLING, String.class);
        if (optionalErrorHandlingArg.isPresent()) {
            final String errorHandlingMode = optionalErrorHandlingArg.get();
            if (ErrorHandlingMode.fromName(errorHandlingMode).isEmpty()) {
                return callContext.fail(
                        throwOnFailure,
                        String.format(
                                "Invalid value for argument 'error_handling': '%s'. Valid values are: %s.",
                                errorHandlingMode, ErrorHandlingMode.validNames()));
            }
        }

        return Optional.empty();
    }

    private static List<Field> buildOutputFields(
            final TableSemantics tableSemantics, final String opColumnName) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        return Arrays.stream(
                        ChangelogTypeStrategyUtils.computeOutputIndices(
                                tableSemantics, opColumnName))
                .mapToObj(inputFields::get)
                .collect(Collectors.toList());
    }

    private FromChangelogTypeStrategy() {}
}
