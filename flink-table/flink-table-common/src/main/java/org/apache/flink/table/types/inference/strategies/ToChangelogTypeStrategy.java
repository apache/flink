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
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Type strategies for the {@code TO_CHANGELOG} process table function. */
@Internal
public final class ToChangelogTypeStrategy {

    // Positional argument indexes for TO_CHANGELOG. Must match the order of StaticArguments
    // registered in BuiltInFunctionDefinitions#TO_CHANGELOG; changing one without the other
    // silently breaks argument resolution.
    public static final int ARG_TABLE = 0;
    public static final int ARG_OP = 1;
    public static final int ARG_OP_MAPPING = 2;
    public static final int ARG_PRODUCES_FULL_DELETES = 3;

    private static final Set<String> VALID_ROW_KIND_NAMES =
            Set.of("INSERT", "UPDATE_BEFORE", "UPDATE_AFTER", "DELETE");

    private static final String DELETE = RowKind.DELETE.name();

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
                final TableSemantics semantics =
                        callContext
                                .getTableSemantics(ARG_TABLE)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "First argument must be a table for TO_CHANGELOG."));

                final String opColumnName =
                        ChangelogTypeStrategyUtils.resolveOpColumnName(callContext);

                final List<Field> outputFields = buildOutputFields(semantics, opColumnName);

                return Optional.of(DataTypes.ROW(outputFields).notNull());
            };

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

        error = validateOpMapping(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        error = validateProducesFullDeletes(callContext, throwOnFailure);
        if (error.isPresent()) {
            return error;
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    private static Optional<List<DataType>> validateTableArg(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.getTableSemantics(ARG_TABLE).isEmpty()) {
            return callContext.fail(
                    throwOnFailure, "First argument must be a table for TO_CHANGELOG.");
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

    /** Validates op_mapping is a constant literal and that its keys are well-formed. */
    @SuppressWarnings("rawtypes")
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
            return validateOpMappingKeys(
                    callContext, (Map<String, String>) opMapping.get(), throwOnFailure);
        }
        return Optional.empty();
    }

    /**
     * Validates op_mapping keys. Keys may be comma-separated (e.g., {@code "INSERT, UPDATE_AFTER"})
     * to map multiple change operations to the same output code. Whitespace around names is
     * trimmed. Names are case-sensitive and must match exactly (e.g., {@code INSERT}, not {@code
     * insert}). Each name must be valid and appear at most once across all entries.
     */
    private static Optional<List<DataType>> validateOpMappingKeys(
            final CallContext callContext,
            final Map<String, String> opMapping,
            final boolean throwOnFailure) {
        final Set<String> allRowKindsSeen = new HashSet<>();
        for (final String key : opMapping.keySet()) {
            final String[] rowKindNames = key.split(",");
            for (final String rawName : rowKindNames) {
                final String rowKindName = rawName.trim();
                if (!VALID_ROW_KIND_NAMES.contains(rowKindName)) {
                    return callContext.fail(
                            throwOnFailure,
                            String.format(
                                    "Invalid target mapping for argument 'op_mapping'. "
                                            + "Unknown change operation: '%s'. Operations are case-sensitive. Valid values are: %s.",
                                    rowKindName, VALID_ROW_KIND_NAMES));
                }
                final boolean isDuplicate = !allRowKindsSeen.add(rowKindName);
                if (isDuplicate) {
                    return callContext.fail(
                            throwOnFailure,
                            String.format(
                                    "Invalid target mapping for argument 'op_mapping'. "
                                            + "Duplicate change operation: '%s'.",
                                    rowKindName));
                }
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("rawtypes")
    private static Optional<List<DataType>> validateProducesFullDeletes(
            final CallContext callContext, final boolean throwOnFailure) {
        final boolean isExplicit = !callContext.isArgumentNull(ARG_PRODUCES_FULL_DELETES);
        if (!isExplicit) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(ARG_PRODUCES_FULL_DELETES)) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'produces_full_deletes' argument must be a constant BOOLEAN literal.");
        }
        final boolean producesFullDeletes =
                callContext.getArgumentValue(ARG_PRODUCES_FULL_DELETES, Boolean.class).orElse(true);
        if (!producesFullDeletes) {
            return Optional.empty();
        }
        // The check against the input changelog mode lives in the function constructor since
        // TableSemantics#changelogMode() returns empty here at type-inference time. The mapping
        // check below only needs the literal op_mapping argument, so it lives here. Only runs
        // when the user explicitly set produces_full_deletes=true; the default true is not
        // validated since it is a safe no-op for any input.
        final Optional<Map> opMapping = callContext.getArgumentValue(ARG_OP_MAPPING, Map.class);
        if (opMapping.isPresent() && !mapsDelete((Map<String, String>) opMapping.get())) {
            return callContext.fail(
                    throwOnFailure,
                    "Invalid 'produces_full_deletes' for TO_CHANGELOG: the active 'op_mapping' "
                            + "does not map DELETE rows, so no DELETE rows are emitted. Remove "
                            + "the 'produces_full_deletes' argument or add a DELETE entry to "
                            + "'op_mapping'.");
        }
        return Optional.empty();
    }

    /**
     * Returns {@code true} when at least one {@code op_mapping} key references {@code DELETE}. Keys
     * may be comma-separated (e.g., {@code "INSERT, DELETE"}) per the user-facing contract.
     */
    private static boolean mapsDelete(final Map<String, String> opMapping) {
        for (final String key : opMapping.keySet()) {
            for (final String rawName : key.split(",")) {
                if (DELETE.equals(rawName.trim())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<Field> buildOutputFields(
            final TableSemantics semantics, final String opColumnName) {
        final List<Field> inputFields = DataType.getFields(semantics.dataType());
        final int[] outputIndices = ChangelogTypeStrategyUtils.computeOutputIndices(semantics);
        final List<Field> outputFields = new ArrayList<>();
        outputFields.add(DataTypes.FIELD(opColumnName, DataTypes.STRING()));
        Arrays.stream(outputIndices).mapToObj(inputFields::get).forEach(outputFields::add);
        return outputFields;
    }

    private ToChangelogTypeStrategy() {}
}
