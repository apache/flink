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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/** Shared helpers for changelog-style PTFs ({@code TO_CHANGELOG}, {@code FROM_CHANGELOG}). */
@Internal
public final class ChangelogTypeStrategyUtils {

    /**
     * Returns the input column indices that pass through to the function's output, excluding the
     * partition key columns (the PTF framework prepends them when the input has set semantics).
     */
    public static int[] computeOutputIndices(final TableSemantics tableSemantics) {
        final int inputFieldCount = DataType.getFieldNames(tableSemantics.dataType()).size();
        final Set<Integer> excluded = collectPartitionKeyIndices(tableSemantics);
        return filterIndices(inputFieldCount, excluded);
    }

    /**
     * Returns the input column indices that pass through to the function's output, excluding the
     * partition key columns and the operation column matching {@code opColumnName}.
     */
    public static int[] computeOutputIndices(
            final TableSemantics tableSemantics, final String opColumnName) {
        final List<String> inputFieldNames = DataType.getFieldNames(tableSemantics.dataType());
        final Set<Integer> excluded = collectPartitionKeyIndices(tableSemantics);
        final int opIndex = inputFieldNames.indexOf(opColumnName);
        if (opIndex >= 0) {
            excluded.add(opIndex);
        }
        return filterIndices(inputFieldNames.size(), excluded);
    }

    private static Set<Integer> collectPartitionKeyIndices(final TableSemantics tableSemantics) {
        final Set<Integer> indices = new HashSet<>();
        for (final int idx : tableSemantics.partitionByColumns()) {
            indices.add(idx);
        }
        return indices;
    }

    private static int[] filterIndices(final int total, final Set<Integer> excluded) {
        return IntStream.range(0, total).filter(i -> !excluded.contains(i)).toArray();
    }

    private ChangelogTypeStrategyUtils() {}
}
