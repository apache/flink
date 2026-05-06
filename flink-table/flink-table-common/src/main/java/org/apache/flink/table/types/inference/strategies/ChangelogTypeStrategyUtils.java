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

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/** Shared helpers for changelog-style PTFs ({@code TO_CHANGELOG}, {@code FROM_CHANGELOG}). */
@Internal
public final class ChangelogTypeStrategyUtils {

    /**
     * Returns the input column indices that pass through to the function's output, excluding: -
     * Partition keys, if present (the PTF framework prepends them when the input has set semantics)
     * - The operation column {@code opColumnName} when non-null and present.
     */
    public static int[] computeOutputIndices(
            final TableSemantics tableSemantics, final @Nullable String opColumnName) {
        final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
        final Set<Integer> excluded = new HashSet<>();
        for (final int idx : tableSemantics.partitionByColumns()) {
            excluded.add(idx);
        }
        if (opColumnName != null) {
            for (int i = 0; i < inputFields.size(); i++) {
                if (inputFields.get(i).getName().equals(opColumnName)) {
                    excluded.add(i);
                    break;
                }
            }
        }
        return IntStream.range(0, inputFields.size()).filter(i -> !excluded.contains(i)).toArray();
    }

    private ChangelogTypeStrategyUtils() {}
}
