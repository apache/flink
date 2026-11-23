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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition.Keep;
import org.apache.flink.table.runtime.functions.BuiltInSpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runtime implementation of {@link BuiltInFunctionDefinitions#DEDUPLICATE}.
 *
 * <pre>
 * keep    sequence_by  implementation
 * first   absent       {@link DeduplicateEmitOnceFunction}, or CurrentRow if input has deletes
 * first   present      {@link DeduplicateCurrentRowFunction}
 * last    any          {@link DeduplicateCurrentRowFunction}
 * </pre>
 */
@Internal
public class Deduplicate extends BuiltInSpecializedFunction {

    public Deduplicate() {
        super(BuiltInFunctionDefinitions.DEDUPLICATE);
    }

    @Override
    public UserDefinedFunction specialize(final SpecializedContext context) {
        final CallContext callContext = context.getCallContext();
        final TableSemantics tableSemantics =
                callContext
                        .getTableSemantics(DeduplicateDefinition.ARG_INPUT)
                        .orElseThrow(IllegalStateException::new);
        final ChangelogMode inputMode =
                tableSemantics.changelogMode().orElse(ChangelogMode.insertOnly());
        if (!inputMode.containsOnly(RowKind.INSERT)) {
            validatePartitionByContainsUpsertKey(tableSemantics);
        }
        final boolean isKeepFirst = DeduplicateDefinition.keep(callContext) == Keep.FIRST;

        if (isKeepFirst) {
            final boolean hasSequenceBy =
                    DeduplicateDefinition.sequenceByOutputIndices(callContext).length > 0;
            final boolean isDeleteInput = inputMode.contains(RowKind.DELETE);
            if (!hasSequenceBy && !isDeleteInput) {
                return new DeduplicateEmitOnceFunction(context);
            } else {
                return new DeduplicateCurrentRowFunction(context);
            }
        } else {
            return new DeduplicateCurrentRowFunction(context);
        }
    }

    /**
     * A retraction does not fall back to an earlier row of the key, so updating input must have at
     * most one row per key at a time.
     */
    private static void validatePartitionByContainsUpsertKey(final TableSemantics tableSemantics) {
        final Set<Integer> partitionBy =
                Arrays.stream(tableSemantics.partitionByColumns())
                        .boxed()
                        .collect(Collectors.toSet());
        final List<int[]> upsertKeys = tableSemantics.upsertKeyColumns();
        final boolean isCovered =
                upsertKeys.stream()
                        .anyMatch(key -> Arrays.stream(key).allMatch(partitionBy::contains));
        if (isCovered) {
            return;
        }
        final List<String> fieldNames = DataType.getFieldNames(tableSemantics.dataType());
        throw new ValidationException(
                String.format(
                        "DEDUPLICATE on updating input requires the PARTITION BY columns to "
                                + "contain an upsert key of the input. PARTITION BY columns: %s. "
                                + "Upsert keys of the input: %s. Partition by the columns of an "
                                + "upsert key, or use ROW_NUMBER(), which falls back to the "
                                + "previous row of a key after a retraction.",
                        columnNames(tableSemantics.partitionByColumns(), fieldNames),
                        upsertKeys.isEmpty()
                                ? "none"
                                : upsertKeys.stream()
                                        .map(key -> columnNames(key, fieldNames))
                                        .collect(Collectors.joining(", "))));
    }

    private static String columnNames(final int[] columns, final List<String> fieldNames) {
        return Arrays.stream(columns)
                .mapToObj(fieldNames::get)
                .collect(Collectors.joining(", ", "(", ")"));
    }
}
