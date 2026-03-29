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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.types.ColumnList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Type strategies for the {@code TO_CHANGELOG} process table function. */
@Internal
public final class ToChangelogTypeStrategy {

    private static final String DEFAULT_OP_COLUMN_NAME = "op";

    private static final Set<String> VALID_ROW_KIND_NAMES =
            Set.of("INSERT", "UPDATE_BEFORE", "UPDATE_AFTER", "DELETE");

    // --------------------------------------------------------------------------------------------
    // Input validation
    // --------------------------------------------------------------------------------------------

    public static final InputTypeStrategy INPUT_TYPE_STRATEGY =
            new InputTypeStrategy() {
                @Override
                public ArgumentCount getArgumentCount() {
                    return ConstantArgumentCount.between(1, 3);
                }

                @Override
                public Optional<List<DataType>> inferInputTypes(
                        final CallContext callContext, final boolean throwOnFailure) {
                    return validateInputs(callContext, throwOnFailure);
                }

                @Override
                public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
                    return List.of(
                            Signature.of(Argument.of("input", "TABLE")),
                            Signature.of(
                                    Argument.of("input", "TABLE"), Argument.of("op", "DESCRIPTOR")),
                            Signature.of(
                                    Argument.of("input", "TABLE"),
                                    Argument.of("op", "DESCRIPTOR"),
                                    Argument.of("op_mapping", "MAP<STRING, STRING>")));
                }
            };

    // --------------------------------------------------------------------------------------------
    // Output type inference
    // --------------------------------------------------------------------------------------------

    public static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext -> {
                final TableSemantics semantics =
                        callContext
                                .getTableSemantics(0)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "First argument must be a table for TO_CHANGELOG."));

                final String opColumnName = resolveOpColumnName(callContext);
                final List<Field> inputFields = DataType.getFields(semantics.dataType());
                final Set<Integer> partitionKeys = intArrayToSet(semantics.partitionByColumns());

                final List<Field> outputFields = new ArrayList<>();
                outputFields.add(DataTypes.FIELD(opColumnName, DataTypes.STRING()));
                for (int i = 0; i < inputFields.size(); i++) {
                    if (!partitionKeys.contains(i)) {
                        outputFields.add(inputFields.get(i));
                    }
                }

                return Optional.of(DataTypes.ROW(outputFields).notNull());
            };

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static Optional<List<DataType>> validateInputs(
            final CallContext callContext, final boolean throwOnFailure) {
        final boolean isMissingTableArg = callContext.getTableSemantics(0).isEmpty();
        if (isMissingTableArg) {
            return callContext.fail(
                    throwOnFailure, "First argument must be a table for TO_CHANGELOG.");
        }

        final Optional<ColumnList> opDescriptor = callContext.getArgumentValue(1, ColumnList.class);
        final boolean hasInvalidOpDescriptor =
                opDescriptor.isPresent() && opDescriptor.get().getNames().size() != 1;
        if (hasInvalidOpDescriptor) {
            return callContext.fail(
                    throwOnFailure,
                    "The descriptor for argument 'op' must contain exactly one column name.");
        }

        final boolean hasMappingArgProvided = !callContext.isArgumentNull(2);
        final boolean isMappingArgLiteral = callContext.isArgumentLiteral(2);
        if (hasMappingArgProvided && !isMappingArgLiteral) {
            return callContext.fail(
                    throwOnFailure, "The 'op_mapping' argument must be a constant MAP literal.");
        }

        final Optional<Map> opMapping = callContext.getArgumentValue(2, Map.class);
        if (opMapping.isPresent()) {
            final Optional<List<DataType>> validationError =
                    validateOpMappingKeys(callContext, opMapping.get(), throwOnFailure);
            if (validationError.isPresent()) {
                return validationError;
            }
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    /**
     * Validates op_mapping keys. Keys may be comma-separated RowKind names (e.g., "INSERT,
     * UPDATE_AFTER") to map multiple RowKinds to the same output code. Each individual RowKind must
     * be valid and appear at most once across all entries.
     */
    @SuppressWarnings("rawtypes")
    private static Optional<List<DataType>> validateOpMappingKeys(
            final CallContext callContext, final Map opMapping, final boolean throwOnFailure) {
        final Set<String> allRowKindsSeen = new HashSet<>();
        for (final Object key : opMapping.keySet()) {
            if (!(key instanceof String)) {
                return callContext.fail(
                        throwOnFailure, "Invalid target mapping for argument 'op_mapping'.");
            }
            final String[] rowKindNames = ((String) key).split(",");
            for (final String rawName : rowKindNames) {
                final String rowKindName = rawName.trim();
                if (!VALID_ROW_KIND_NAMES.contains(rowKindName)) {
                    return callContext.fail(
                            throwOnFailure,
                            String.format(
                                    "Invalid target mapping for argument 'op_mapping'. "
                                            + "Unknown RowKind: '%s'. Valid values are: %s.",
                                    rowKindName, VALID_ROW_KIND_NAMES));
                }
                final boolean isDuplicate = !allRowKindsSeen.add(rowKindName);
                if (isDuplicate) {
                    return callContext.fail(
                            throwOnFailure,
                            String.format(
                                    "Invalid target mapping for argument 'op_mapping'. "
                                            + "Duplicate RowKind: '%s'.",
                                    rowKindName));
                }
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

    private static Set<Integer> intArrayToSet(final int[] array) {
        return IntStream.of(array).boxed().collect(Collectors.toSet());
    }

    private ToChangelogTypeStrategy() {}
}
