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
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.types.ColumnList;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.inference.strategies.FromChangelogTypeStrategy.ARG_OP;

/** Shared helpers for changelog-style PTFs ({@code TO_CHANGELOG}, {@code FROM_CHANGELOG}). */
@Internal
public final class ChangelogTypeStrategyUtils {
    private static final String DEFAULT_OP_COLUMN_NAME = "op";

    /**
     * Resolves the op column name from the {@code op} descriptor argument, falling back to {@link
     * #DEFAULT_OP_COLUMN_NAME} when the argument is omitted or empty.
     */
    public static String resolveOpColumnName(final CallContext callContext) {
        return callContext
                .getArgumentValue(ARG_OP, ColumnList.class)
                .filter(cl -> !cl.getNames().isEmpty())
                .map(cl -> cl.getNames().get(0))
                .orElse(DEFAULT_OP_COLUMN_NAME);
    }

    /**
     * Returns the index of the column matching {@code opColumnName} within the input schema, or
     * empty if no field matches.
     */
    public static OptionalInt resolveOpColumnIndex(
            final TableSemantics tableSemantics, final String opColumnName) {
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        return IntStream.range(0, fieldNames.size())
                .filter(i -> fieldNames.get(i).equals(opColumnName))
                .findFirst();
    }

    /**
     * Returns the input column indices that pass through to the function's output, excluding the
     * partition key columns (the PTF framework prepends them when the input has set semantics).
     */
    public static int[] computeOutputIndices(final TableSemantics tableSemantics) {
        return computeOutputIndices(tableSemantics, -1);
    }

    /**
     * Returns the input column indices that pass through to the function's output, excluding the
     * partition key columns and the operation column matching {@code opColumnName}.
     */
    public static int[] computeOutputIndices(
            final TableSemantics tableSemantics, final String opColumnName) {
        final int opIndex = DataType.getFieldNames(tableSemantics.dataType()).indexOf(opColumnName);
        return computeOutputIndices(tableSemantics, opIndex);
    }

    private static int[] computeOutputIndices(
            final TableSemantics tableSemantics, final int extraExcludedIndex) {
        final Set<Integer> excluded = collectPartitionKeyIndices(tableSemantics);
        if (extraExcludedIndex >= 0) {
            excluded.add(extraExcludedIndex);
        }
        final int inputFieldCount = DataType.getFieldCount(tableSemantics.dataType());
        return IntStream.range(0, inputFieldCount).filter(i -> !excluded.contains(i)).toArray();
    }

    private static Set<Integer> collectPartitionKeyIndices(final TableSemantics tableSemantics) {
        return Arrays.stream(tableSemantics.partitionByColumns())
                .boxed()
                .collect(Collectors.toSet());
    }

    private ChangelogTypeStrategyUtils() {}
}
