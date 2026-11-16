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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.inference.SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET;

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
    public static final int ARG_RESET_TTL_ON_DUPLICATE = 4;
    private static final int DECLARED_ARG_COUNT = ARG_RESET_TTL_ON_DUPLICATE + 1;

    /** Which record of a key {@code DEDUPLICATE} keeps. */
    @Internal
    public enum Keep {
        FIRST("first"),
        LAST("last");

        private final String value;

        Keep(final String value) {
            this.value = value;
        }

        /** Returns the mode for an argument value, or empty if the value names no mode. */
        public static Optional<Keep> fromValue(final String value) {
            return Arrays.stream(values()).filter(keep -> keep.value.equals(value)).findFirst();
        }
    }

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
                    Optional.of(DataTypes.ROW(outputFields(tableSemantics(callContext))).notNull());

    public static final ChangelogModeStrategy CHANGELOG_MODE_STRATEGY =
            DeduplicateTypeStrategy::inferChangelogMode;

    /** Returns the {@code keep} argument, or {@link Keep#FIRST} when it is omitted. */
    public static Keep keep(final CallContext callContext) {
        return callContext
                .getArgumentValue(ARG_KEEP, String.class)
                .map(
                        value ->
                                Keep.fromValue(value)
                                        .orElseThrow(
                                                () ->
                                                        new IllegalStateException(
                                                                "Validated keep argument expected.")))
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
        return validatePartitionBy(tableSemantics)
                .or(() -> validateNoOrderBy(tableSemantics))
                .or(() -> validatePartitionByNoTimeAttribute(tableSemantics))
                .or(() -> validateOnTime(callContext))
                .or(() -> validateKeep(callContext))
                .or(() -> validateSequenceBy(callContext, tableSemantics))
                .or(() -> validateStateTtl(callContext))
                .or(() -> validateResetTtlOnDuplicate(callContext));
    }

    private static Optional<String> validatePartitionBy(final TableSemantics tableSemantics) {
        if (tableSemantics.partitionByColumns().length > 0) {
            return Optional.empty();
        }
        return Optional.of(
                "DEDUPLICATE requires a key, but the input has no PARTITION BY. Add PARTITION BY "
                        + "with the columns to deduplicate on, for example TABLE t PARTITION BY "
                        + "id. To drop only rows that repeat in full, partition by every column.");
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
                        "DEDUPLICATE does not support ORDER BY on its input, but received "
                                + "ORDER BY %s. Use sequence_by => DESCRIPTOR(%s) to order the "
                                + "records of a key.",
                        columns, columns));
    }

    private static Optional<String> validatePartitionByNoTimeAttribute(
            final TableSemantics tableSemantics) {
        final List<Field> fields = DataType.getFields(tableSemantics.dataType());
        return Arrays.stream(tableSemantics.partitionByColumns())
                .mapToObj(fields::get)
                .filter(
                        field ->
                                LogicalTypeChecks.isTimeAttribute(
                                        field.getDataType().getLogicalType()))
                .findFirst()
                .map(
                        field ->
                                String.format(
                                        "DEDUPLICATE does not support a time attribute as a "
                                                + "PARTITION BY column, but '%s' is one. Partition "
                                                + "by a regular column, or cast the column in a "
                                                + "subquery first, for example CAST(%s AS %s) AS %s.",
                                        field.getName(),
                                        field.getName(),
                                        DataTypeUtils.removeTimeAttribute(field.getDataType())
                                                .getLogicalType()
                                                .asSummaryString(),
                                        field.getName()));
    }

    private static Optional<String> validateOnTime(final CallContext callContext) {
        final int argCount = callContext.getArgumentDataTypes().size();
        if (argCount == DECLARED_ARG_COUNT) {
            return Optional.empty();
        }
        final int onTimePos = argCount - 1 - PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET;
        return callContext
                .getArgumentValue(onTimePos, ColumnList.class)
                .map(ColumnList::getNames)
                .filter(names -> !names.isEmpty())
                .map(
                        names ->
                                String.format(
                                        "DEDUPLICATE does not support on_time, but received "
                                                + "on_time => DESCRIPTOR(%s). Use sequence_by => "
                                                + "DESCRIPTOR(%s) to order by timestamp, or "
                                                + "DEDUPLICATE_WINDOW for event-time windows."
                                                + "ps.this function is not currently implemented",
                                        names.get(0), names.get(0)));
    }

    private static Optional<String> validateKeep(final CallContext callContext) {
        if (callContext.isArgumentNull(ARG_KEEP)) {
            return Optional.empty();
        }
        final String received;
        if (!callContext.isArgumentLiteral(ARG_KEEP)) {
            received = "a non-literal expression";
        } else {
            final String value = callContext.getArgumentValue(ARG_KEEP, String.class).orElse(null);
            if (value == null || Keep.fromValue(value).isPresent()) {
                return Optional.empty();
            }
            received = String.format("keep => '%s'", value);
        }
        return Optional.of(
                String.format(
                        "DEDUPLICATE requires keep => 'first' or keep => 'last', but received %s. "
                                + "Use keep => 'first' to keep the first record per key, or "
                                + "keep => 'last' to keep the last one.",
                        received));
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
                                        + "DEDUPLICATE. Available columns are: %s.",
                                column, String.join(", ", fieldNames)));
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
                                        + "column that is not part of PARTITION BY, such as a "
                                        + "version or sequence number.",
                                column));
            }
            final LogicalType type = fieldTypes.get(index).getLogicalType();
            if (!isOrderable(type)) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' has type '%s', which cannot order "
                                        + "records. Use a numeric, character string, binary "
                                        + "string, boolean, or date-time column.",
                                column, type.asSummaryString()));
            }
        }
        return Optional.empty();
    }

    private static Optional<String> validateStateTtl(final CallContext callContext) {
        if (callContext.isArgumentNull(ARG_STATE_TTL)) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(ARG_STATE_TTL)) {
            return Optional.of(
                    "The 'state_ttl' argument must be a constant INTERVAL literal, but received a "
                            + "non-literal expression. Use a literal such as INTERVAL '1' HOUR, "
                            + "INTERVAL '0' to retain state indefinitely, or omit the argument "
                            + "to fall back to 'table.exec.state.ttl'.");
        }
        return callContext
                .getArgumentValue(ARG_STATE_TTL, Duration.class)
                .filter(Duration::isNegative)
                .map(
                        stateTtl ->
                                String.format(
                                        "The 'state_ttl' argument must not be negative, but was "
                                                + "'%s'. Use a positive INTERVAL to bound state "
                                                + "retention, INTERVAL '0' to retain state "
                                                + "indefinitely, or omit the argument to fall "
                                                + "back to 'table.exec.state.ttl'.",
                                        TimeUtils.formatWithHighestUnit(stateTtl)));
    }

    private static Optional<String> validateResetTtlOnDuplicate(final CallContext callContext) {
        if (callContext.isArgumentNull(ARG_RESET_TTL_ON_DUPLICATE)
                || callContext.isArgumentLiteral(ARG_RESET_TTL_ON_DUPLICATE)) {
            return Optional.empty();
        }
        return Optional.of(
                "The 'reset_ttl_on_duplicate' argument must be a constant BOOLEAN literal, but "
                        + "received a non-literal expression. Use TRUE to refresh the TTL on a "
                        + "dropped record, FALSE to keep it, or omit the argument for the default "
                        + "TRUE.");
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

    /** Returns the output columns after the PARTITION BY columns, without time attributes. */
    public static List<Field> outputFields(final TableSemantics tableSemantics) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        return Arrays.stream(ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics))
                .mapToObj(inputFields::get)
                .map(
                        field ->
                                DataTypes.FIELD(
                                        field.getName(),
                                        DataTypeUtils.removeTimeAttribute(field.getDataType())))
                .collect(Collectors.toList());
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
