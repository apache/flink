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

package org.apache.flink.table.functions.definitions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.ChangelogFunction.ChangelogContext;
import org.apache.flink.table.functions.ChangelogModeStrategy;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.strategies.ChangelogTypeStrategyUtils;
import org.apache.flink.table.types.inference.strategies.ValidationOnlyInputTypeStrategy;
import org.apache.flink.table.types.inference.utils.CallContextUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.FunctionKind.PROCESS_TABLE;

/**
 * Definition of the {@code DEDUPLICATE} built-in process table function.
 *
 * <p>The output is updating and keyed by the PARTITION BY columns: the PARTITION BY columns
 * followed by the remaining input columns.
 */
@Internal
public final class DeduplicateDefinition {

    public static final int ARG_INPUT = 0;
    public static final int ARG_KEEP = 1;
    public static final int ARG_SEQUENCE_BY = 2;
    public static final int ARG_STATE_TTL = 3;

    private static final String NAME = "DEDUPLICATE";

    /** Which record of a key is kept. */
    @Internal
    public enum Keep {
        FIRST,
        LAST
    }

    static final InputTypeStrategy INPUT_TYPE_STRATEGY =
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

    static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext ->
                    Optional.of(DataTypes.ROW(outputFields(tableSemantics(callContext))).notNull());

    static final ChangelogModeStrategy CHANGELOG_MODE_STRATEGY =
            DeduplicateDefinition::inferChangelogMode;

    public static final BuiltInFunctionDefinition INSTANCE =
            BuiltInFunctionDefinition.newBuilder()
                    .name(NAME)
                    .kind(PROCESS_TABLE)
                    .staticArguments(
                            StaticArgument.table(
                                    "input",
                                    Row.class,
                                    false,
                                    EnumSet.of(
                                            StaticArgumentTrait.TABLE,
                                            StaticArgumentTrait.SET_SEMANTIC_TABLE,
                                            StaticArgumentTrait.NO_ORDER_BY)),
                            StaticArgument.scalar("keep", DataTypes.STRING(), true),
                            StaticArgument.scalar("sequence_by", DataTypes.DESCRIPTOR(), true),
                            StaticArgument.scalar(
                                    "state_ttl", DataTypes.INTERVAL(DataTypes.SECOND()), true))
                    .changelogModeStrategy(CHANGELOG_MODE_STRATEGY)
                    .inputTypeStrategy(INPUT_TYPE_STRATEGY)
                    .outputTypeStrategy(OUTPUT_TYPE_STRATEGY)
                    .runtimeClass("org.apache.flink.table.runtime.functions.ptf.Deduplicate")
                    .build();

    /** Returns the {@code keep} argument, or {@link Keep#FIRST} when it is omitted. */
    public static Keep keep(final CallContext callContext) {
        return CallContextUtils.getEnumArgument(callContext, ARG_KEEP, Keep.class)
                .orElse(Keep.FIRST);
    }

    /**
     * Returns the positions of the {@code sequence_by} columns, in the listed order, within the
     * output row without the PARTITION BY columns.
     */
    public static int[] sequenceByOutputIndices(final CallContext callContext) {
        final TableSemantics tableSemantics = tableSemantics(callContext);
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final List<Integer> outputIndices =
                Arrays.stream(ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics))
                        .boxed()
                        .collect(Collectors.toList());
        return sequenceBy(callContext).stream()
                .mapToInt(column -> outputIndices.indexOf(fieldNames.indexOf(column)))
                .toArray();
    }

    private static Optional<String> validate(final CallContext callContext) {
        final TableSemantics tableSemantics = tableSemantics(callContext);
        return CallContextUtils.validateEnumArgument(
                        callContext, ARG_KEEP, "keep", NAME, Keep.class)
                .or(() -> validateSequenceBy(callContext, tableSemantics))
                .or(() -> CallContextUtils.validateStateTtl(callContext, ARG_STATE_TTL));
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
        return CallContextUtils.getTableSemanticsOrThrow(callContext, ARG_INPUT);
    }

    /** Returns the output columns after the PARTITION BY columns. */
    private static List<Field> outputFields(final TableSemantics tableSemantics) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        return Arrays.stream(ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics))
                .mapToObj(inputFields::get)
                .collect(Collectors.toList());
    }

    private static Optional<String> validateSequenceBy(
            final CallContext callContext, final TableSemantics tableSemantics) {
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final List<DataType> fieldTypes = DataType.getFieldDataTypes(tableSemantics.dataType());
        final Set<String> seen = new HashSet<>();
        for (final String column : sequenceBy(callContext)) {
            final int index = fieldNames.indexOf(column);
            if (index == -1) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' does not exist in the input of "
                                        + "%s. Available columns are: %s.",
                                column, NAME, String.join(", ", fieldNames)));
            }
            if (!seen.add(column)) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' is listed more than once. List each "
                                        + "ordering column once.",
                                column));
            }
            if (Arrays.stream(tableSemantics.partitionByColumns()).anyMatch(c -> c == index)) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' is a PARTITION BY column, which has "
                                        + "the same value for every record of a key. Use a "
                                        + "non-PARTITION BY column.",
                                column));
            }
            final LogicalType type = fieldTypes.get(index).getLogicalType();
            if (!isOrderable(type)) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' has type '%s', which cannot order "
                                        + "records. Use a numeric, string, boolean, or date-time "
                                        + "column.",
                                column, type.asSummaryString()));
            }
        }
        return Optional.empty();
    }

    private static List<String> sequenceBy(final CallContext callContext) {
        return callContext
                .getArgumentValue(ARG_SEQUENCE_BY, ColumnList.class)
                .map(ColumnList::getNames)
                .orElse(List.of());
    }

    private static boolean isOrderable(final LogicalType type) {
        return type.is(LogicalTypeFamily.NUMERIC)
                || type.is(LogicalTypeFamily.CHARACTER_STRING)
                || type.is(LogicalTypeFamily.BINARY_STRING)
                || type.is(LogicalTypeFamily.DATETIME)
                || type.is(LogicalTypeRoot.BOOLEAN);
    }

    private DeduplicateDefinition() {}
}
