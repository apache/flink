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
        FIRST,
        LAST
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

    /** Returns the output columns after the PARTITION BY columns. */
    public static List<Field> outputFields(final TableSemantics tableSemantics) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        return Arrays.stream(ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics))
                .mapToObj(inputFields::get)
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
