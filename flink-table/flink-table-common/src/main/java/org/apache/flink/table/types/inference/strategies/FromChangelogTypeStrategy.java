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
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Type strategies for the {@code FROM_CHANGELOG} process table function. */
@Internal
public final class FromChangelogTypeStrategy {

    private static final String DEFAULT_OP_COLUMN_NAME = "op";

    private static final Set<String> VALID_ROW_KIND_NAMES =
            Set.of("INSERT", "UPDATE_BEFORE", "UPDATE_AFTER", "DELETE");

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
                                .getTableSemantics(0)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "First argument must be a table for FROM_CHANGELOG."));

                final String opColumnName = resolveOpColumnName(callContext);

                final List<Field> outputFields = buildOutputFields(tableSemantics, opColumnName);

                return Optional.of(DataTypes.ROW(outputFields).notNull());
            };
    private static final String UPDATE_BEFORE = RowKind.UPDATE_BEFORE.name();
    private static final String UPDATE_AFTER = RowKind.UPDATE_AFTER.name();

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("rawtypes")
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

        return Optional.of(callContext.getArgumentDataTypes());
    }

    private static Optional<List<DataType>> validateTableArg(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.getTableSemantics(0).isEmpty()) {
            return callContext.fail(
                    throwOnFailure, "First argument must be a table for FROM_CHANGELOG.");
        }
        return Optional.empty();
    }

    private static Optional<List<DataType>> validateOpDescriptor(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<ColumnList> opDescriptor = callContext.getArgumentValue(1, ColumnList.class);
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

        final TableSemantics tableSemantics = callContext.getTableSemantics(0).get();
        final String opColumnName = resolveOpColumnName(callContext);
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        final Optional<Field> opField =
                inputFields.stream().filter(f -> f.getName().equals(opColumnName)).findFirst();
        if (opField.isEmpty()) {
            return callContext.fail(
                    throwOnFailure,
                    String.format(
                            "The op column '%s' does not exist in the input schema.",
                            opColumnName));
        }
        if (!opField.get().getDataType().getLogicalType().is(LogicalTypeFamily.CHARACTER_STRING)) {
            return callContext.fail(
                    throwOnFailure,
                    String.format(
                            "The op column '%s' must be of STRING type, but was '%s'.",
                            opColumnName, opField.get().getDataType().getLogicalType()));
        }
        return Optional.empty();
    }

    /** Validates op_mapping is a literal and its values are valid change operation names. */
    @SuppressWarnings("unchecked")
    private static Optional<List<DataType>> validateOpMapping(
            final CallContext callContext, final boolean throwOnFailure) {
        final boolean hasMappingArgProvided = !callContext.isArgumentNull(2);
        final boolean isMappingArgLiteral = callContext.isArgumentLiteral(2);
        if (hasMappingArgProvided && !isMappingArgLiteral) {
            return callContext.fail(
                    throwOnFailure, "The 'op_mapping' argument must be a constant MAP literal.");
        }

        final Optional<Map> opMapping = callContext.getArgumentValue(2, Map.class);
        if (opMapping.isPresent()) {
            final Map<String, String> mapping = opMapping.get();
            final Optional<List<DataType>> validationError =
                    validateOpMappingValues(callContext, mapping, throwOnFailure);
            if (validationError.isPresent()) {
                return validationError;
            }

            final boolean hasUpdateBefore =
                    mapping.values().stream().anyMatch(v -> UPDATE_BEFORE.equals(v.trim()));
            final boolean hasUpdateAfter =
                    mapping.values().stream().anyMatch(v -> UPDATE_AFTER.equals(v.trim()));
            if (hasUpdateAfter && !hasUpdateBefore) {
                return callContext.fail(
                        throwOnFailure,
                        "The 'op_mapping' must include UPDATE_BEFORE for retract mode. "
                                + "Upsert mode (without UPDATE_BEFORE) is not supported "
                                + "in this version.");
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

    private static String resolveOpColumnName(final CallContext callContext) {
        return callContext
                .getArgumentValue(1, ColumnList.class)
                .filter(cl -> !cl.getNames().isEmpty())
                .map(cl -> cl.getNames().get(0))
                .orElse(DEFAULT_OP_COLUMN_NAME);
    }

    private static List<Field> buildOutputFields(
            final TableSemantics tableSemantics, final String opColumnName) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());

        // Exclude the op column (becomes RowKind), keep all other columns
        return inputFields.stream()
                .filter(f -> !f.getName().equals(opColumnName))
                .collect(Collectors.toList());
    }

    private FromChangelogTypeStrategy() {}
}
