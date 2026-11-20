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
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.ColumnList;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Argument validation and output type helpers shared by the deduplication PTFs. */
@Internal
public final class DeduplicateTypeStrategyUtils {

    /** Which record of a key is kept. */
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

    /** Returns the {@code keep} argument, or {@link Keep#FIRST} when it is omitted. */
    public static Keep keep(final CallContext callContext, final int argPos) {
        return callContext
                .getArgumentValue(argPos, String.class)
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
    public static int[] sequenceByOutputIndices(
            final CallContext callContext, final TableSemantics tableSemantics, final int argPos) {
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final List<Integer> outputIndices =
                Arrays.stream(ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics))
                        .boxed()
                        .collect(Collectors.toList());
        return sequenceBy(callContext, argPos).stream()
                .mapToInt(column -> outputIndices.indexOf(fieldNames.indexOf(column)))
                .toArray();
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

    public static Optional<String> validatePartitionByNoTimeAttribute(
            final TableSemantics tableSemantics, final String functionName) {
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
                                        "%s does not support a time attribute as a "
                                                + "PARTITION BY column, but '%s' is one. Use "
                                                + "CAST(%s AS %s) in a subquery first.",
                                        functionName,
                                        field.getName(),
                                        field.getName(),
                                        DataTypeUtils.removeTimeAttribute(field.getDataType())
                                                .getLogicalType()
                                                .asSummaryString()));
    }

    public static Optional<String> validateKeep(
            final CallContext callContext, final int argPos, final String functionName) {
        if (callContext.isArgumentNull(argPos)) {
            return Optional.empty();
        }
        final String received;
        if (!callContext.isArgumentLiteral(argPos)) {
            received = "a non-literal expression";
        } else {
            final String value = callContext.getArgumentValue(argPos, String.class).orElse(null);
            if (value == null || Keep.fromValue(value).isPresent()) {
                return Optional.empty();
            }
            received = String.format("keep => '%s'", value);
        }
        return Optional.of(
                String.format(
                        "%s requires keep => 'first' or keep => 'last', but received %s.",
                        functionName, received));
    }

    public static Optional<String> validateSequenceBy(
            final CallContext callContext,
            final TableSemantics tableSemantics,
            final int argPos,
            final String functionName) {
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final List<DataType> fieldTypes = DataType.getFieldDataTypes(tableSemantics.dataType());
        final Set<String> seen = new HashSet<>();
        for (final String column : sequenceBy(callContext, argPos)) {
            final int index = fieldNames.indexOf(column);
            if (index == -1) {
                return Optional.of(
                        String.format(
                                "The sequence_by column '%s' does not exist in the input of "
                                        + "%s. Available columns are: %s.",
                                column, functionName, String.join(", ", fieldNames)));
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

    public static Optional<String> validateStateTtl(
            final CallContext callContext, final int argPos) {
        if (callContext.isArgumentNull(argPos)) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(argPos)) {
            return Optional.of(
                    "The 'state_ttl' argument must be a constant INTERVAL literal, but received a "
                            + "non-literal expression. Use a literal such as INTERVAL '1' HOUR.");
        }
        return callContext
                .getArgumentValue(argPos, Duration.class)
                .filter(Duration::isNegative)
                .map(
                        stateTtl ->
                                String.format(
                                        "The 'state_ttl' argument must not be negative, but was "
                                                + "'%s'. Use INTERVAL '0' SECOND to retain state "
                                                + "indefinitely.",
                                        TimeUtils.formatWithHighestUnit(stateTtl)));
    }

    public static Optional<String> validateResetTtlOnDuplicate(
            final CallContext callContext, final int argPos) {
        if (callContext.isArgumentNull(argPos) || callContext.isArgumentLiteral(argPos)) {
            return Optional.empty();
        }
        return Optional.of(
                "The 'reset_ttl_on_duplicate' argument must be a constant BOOLEAN literal, but "
                        + "received a non-literal expression. Use TRUE or FALSE.");
    }

    private static List<String> sequenceBy(final CallContext callContext, final int argPos) {
        return callContext
                .getArgumentValue(argPos, ColumnList.class)
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

    private DeduplicateTypeStrategyUtils() {}
}
