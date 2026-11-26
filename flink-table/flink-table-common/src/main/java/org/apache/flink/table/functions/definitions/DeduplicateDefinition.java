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
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.strategies.ValidationOnlyInputTypeStrategy;
import org.apache.flink.table.types.inference.utils.CallContextUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.FunctionKind.PROCESS_TABLE;

/**
 * Definition of the {@code DEDUPLICATE} built-in process table function.
 *
 * <p>The output is updating and keyed by the PARTITION BY columns: the PARTITION BY columns
 * followed by the remaining input columns. Over an insert-only input with {@code keep => 'first'}
 * and no {@code sequence_by}, the output only contains inserts.
 */
@Internal
public final class DeduplicateDefinition {

    public static final BuiltInFunctionDefinition INSTANCE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("DEDUPLICATE")
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
                    .inputTypeStrategy(inputTypeStrategy())
                    .outputTypeStrategy(DeduplicateDefinition::inferOutputType)
                    .changelogModeStrategy(DeduplicateDefinition::inferChangelogMode)
                    .runtimeClass("org.apache.flink.table.runtime.functions.ptf.Deduplicate")
                    .build();

    public static final int ARG_INPUT = 0;
    public static final int ARG_KEEP = 1;
    public static final int ARG_SEQUENCE_BY = 2;
    public static final int ARG_STATE_TTL = 3;

    /** Which record of a key is kept. */
    @Internal
    public enum Keep {
        FIRST,
        LAST
    }

    // --------------------------------------------------------------------------------------------
    // Accessors for the runtime
    // --------------------------------------------------------------------------------------------

    /** Returns the {@code keep} argument, or {@link Keep#FIRST} when it is omitted. */
    public static Keep keep(final CallContext callContext) {
        return keep(callContext.getArgumentValue(ARG_KEEP, String.class));
    }

    /**
     * Returns the positions of the {@code sequence_by} columns, in the listed order, within the
     * output row without the PARTITION BY columns.
     */
    public static int[] sequenceByOutputIndices(final CallContext callContext) {
        final TableSemantics tableSemantics =
                CallContextUtils.getTableSemanticsOrThrow(callContext, ARG_INPUT);
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final List<Integer> outputIndices =
                Arrays.stream(CallContextUtils.computeOutputIndices(tableSemantics))
                        .boxed()
                        .collect(Collectors.toList());
        return sequenceBy(callContext).stream()
                .mapToInt(column -> outputIndices.indexOf(fieldNames.indexOf(column)))
                .toArray();
    }

    // --------------------------------------------------------------------------------------------
    // Strategies
    // --------------------------------------------------------------------------------------------

    static InputTypeStrategy inputTypeStrategy() {
        return new ValidationOnlyInputTypeStrategy() {
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
    }

    private static Optional<DataType> inferOutputType(final CallContext callContext) {
        final TableSemantics tableSemantics =
                CallContextUtils.getTableSemanticsOrThrow(callContext, ARG_INPUT);
        return Optional.of(DataTypes.ROW(outputFields(tableSemantics)).notNull());
    }

    private static ChangelogMode inferChangelogMode(final ChangelogContext changelogContext) {
        if (emitsOnlyInserts(changelogContext)) {
            return ChangelogMode.insertOnly();
        }
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

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static Optional<String> validate(final CallContext callContext) {
        final TableSemantics tableSemantics =
                CallContextUtils.getTableSemanticsOrThrow(callContext, ARG_INPUT);
        return CallContextUtils.validateEnumArgument(
                        callContext, ARG_KEEP, "keep", INSTANCE.getName(), Keep.class)
                .or(() -> validateSequenceBy(callContext, tableSemantics))
                .or(() -> CallContextUtils.validateStateTtl(callContext, ARG_STATE_TTL));
    }

    /**
     * Over insert-only input, the kept row of a key never changes when the first arrival is kept.
     * The function then only emits insertions, so its changelog mode can be insert-only.
     */
    private static boolean emitsOnlyInserts(final ChangelogContext context) {
        if (!context.getTableChangelogMode(ARG_INPUT).containsOnly(RowKind.INSERT)) {
            return false;
        }
        final boolean keepsFirst =
                keep(context.getArgumentValue(ARG_KEEP, String.class)) == Keep.FIRST;
        final boolean hasSequenceBy =
                context.getArgumentValue(ARG_SEQUENCE_BY, ColumnList.class).isPresent();

        return keepsFirst && !hasSequenceBy;
    }

    private static Keep keep(final Optional<String> value) {
        return CallContextUtils.toEnum(value, Keep.class).orElse(Keep.FIRST);
    }

    /** Returns the output columns after the PARTITION BY columns. */
    private static List<Field> outputFields(final TableSemantics tableSemantics) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        return Arrays.stream(CallContextUtils.computeOutputIndices(tableSemantics))
                .mapToObj(inputFields::get)
                .collect(Collectors.toList());
    }

    private static Optional<String> validateSequenceBy(
            final CallContext callContext, final TableSemantics tableSemantics) {
        if (callContext
                .getArgumentValue(ARG_SEQUENCE_BY, ColumnList.class)
                .filter(columns -> columns.getNames().isEmpty())
                .isPresent()) {
            return Optional.of("sequence_by requires at least one column.");
        }
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final List<DataType> fieldTypes = DataType.getFieldDataTypes(tableSemantics.dataType());
        for (final String column : sequenceBy(callContext)) {
            final int index = fieldNames.indexOf(column);
            if (index == -1) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' does not exist in the input of "
                                        + "%s. Available columns are: %s.",
                                column, INSTANCE.getName(), String.join(", ", fieldNames)));
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
            if (!LogicalTypeChecks.areComparable(type, type, StructuredComparison.FULL)) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' has type '%s', which cannot be "
                                        + "ordered. Use a column of a type that supports ORDER BY.",
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

    private DeduplicateDefinition() {}
}
