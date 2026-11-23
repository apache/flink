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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction.ChangelogContext;
import org.apache.flink.table.functions.ChangelogModeStrategy;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategyUtils.Keep;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Type strategies for the {@code DEDUPLICATE} process table function.
 *
 * <p>The output is updating and keyed by the PARTITION BY columns: the PARTITION BY columns
 * followed by the remaining input columns, without time attributes.
 */
@Internal
public final class DeduplicateTypeStrategy {

    public static final int ARG_INPUT = 0;
    public static final int ARG_KEEP = 1;
    public static final int ARG_SEQUENCE_BY = 2;
    public static final int ARG_STATE_TTL = 3;
    private static final String NAME = "DEDUPLICATE";

    public static final InputTypeStrategy INPUT_TYPE_STRATEGY =
            new ValidationOnlyInputTypeStrategy() {
                @Override
                public Optional<List<DataType>> inferInputTypes(
                        final CallContext callContext, final boolean throwOnFailure) {
                    final Optional<String> error = validate(callContext);
                    if (error.isPresent()) {
                        return callContext.fail(throwOnFailure, "%s", error.get());
                    }
                    return Optional.of(callContext.getArgumentDataTypes());
                }
            };

    public static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext ->
                    Optional.of(
                            DataTypes.ROW(
                                            DeduplicateTypeStrategyUtils.outputFields(
                                                    tableSemantics(callContext)))
                                    .notNull());

    public static final ChangelogModeStrategy CHANGELOG_MODE_STRATEGY =
            DeduplicateTypeStrategy::inferChangelogMode;

    /** Returns the {@code keep} argument, or {@link Keep#FIRST} when it is omitted. */
    public static Keep keep(final CallContext callContext) {
        return DeduplicateTypeStrategyUtils.keep(callContext, ARG_KEEP);
    }

    /**
     * Returns the positions of the {@code sequence_by} columns, in the listed order, within the
     * output row without the PARTITION BY columns.
     */
    public static int[] sequenceByOutputIndices(final CallContext callContext) {
        return DeduplicateTypeStrategyUtils.sequenceByOutputIndices(
                callContext, tableSemantics(callContext), ARG_SEQUENCE_BY);
    }

    private static Optional<String> validate(final CallContext callContext) {
        final TableSemantics tableSemantics = tableSemantics(callContext);
        return validateNoOrderBy(tableSemantics)
                .or(
                        () ->
                                DeduplicateTypeStrategyUtils.validatePartitionByNoTimeAttribute(
                                        tableSemantics, NAME))
                .or(() -> DeduplicateTypeStrategyUtils.validateKeep(callContext, ARG_KEEP, NAME))
                .or(
                        () ->
                                DeduplicateTypeStrategyUtils.validateSequenceBy(
                                        callContext, tableSemantics, ARG_SEQUENCE_BY, NAME))
                .or(() -> DeduplicateTypeStrategyUtils.validateStateTtl(callContext, ARG_STATE_TTL));
    }

    private static Optional<String> validateNoOrderBy(final TableSemantics tableSemantics) {
        if (tableSemantics.orderByColumns().length == 0) {
            return Optional.empty();
        }
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final String columns =
                Arrays.stream(tableSemantics.orderByColumns())
                        .mapToObj(fieldNames::get)
                        .collect(Collectors.joining(", "));
        return Optional.of(
                String.format(
                        "DEDUPLICATE does not support ORDER BY, but received ORDER BY %s. "
                                + "Use sequence_by => DESCRIPTOR(%s) instead.",
                        columns, columns));
    }

    private static ChangelogMode inferChangelogMode(final ChangelogContext changelogContext) {
        final ChangelogMode requiredMode = changelogContext.getRequiredChangelogMode();
        final ChangelogMode.Builder builder =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_AFTER);
        if (requiredMode != null && requiredMode.contains(RowKind.UPDATE_BEFORE)) {
            builder.addContainedKind(RowKind.UPDATE_BEFORE);
        }
        return builder.build();
    }

    private static TableSemantics tableSemantics(final CallContext callContext) {
        return callContext
                .getTableSemantics(ARG_INPUT)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "First argument must be a table for DEDUPLICATE."));
    }

    private DeduplicateTypeStrategy() {}
}
