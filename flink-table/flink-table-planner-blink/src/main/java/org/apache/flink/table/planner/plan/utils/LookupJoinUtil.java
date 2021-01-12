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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

/** Utilities for lookup joins using {@link LookupTableSource}. */
@Internal
public final class LookupJoinUtil {

    /** A field used as an equal condition when querying content from a dimension table. */
    public static class LookupKey {
        private LookupKey() {
            // sealed class
        }
    }

    /** A {@link LookupKey} whose value is constant. */
    public static class ConstantLookupKey extends LookupKey {
        public final LogicalType sourceType;
        public final RexLiteral literal;

        public ConstantLookupKey(LogicalType sourceType, RexLiteral literal) {
            this.sourceType = sourceType;
            this.literal = literal;
        }
    }

    /** A {@link LookupKey} whose value comes from the left table field. */
    public static class FieldRefLookupKey extends LookupKey {
        public final int index;

        public FieldRefLookupKey(int index) {
            this.index = index;
        }
    }

    private LookupJoinUtil() {
        // no instantiation
    }

    /** Gets lookup keys sorted by index in ascending order. */
    public static int[] getOrderedLookupKeys(Collection<Integer> allLookupKeys) {
        List<Integer> lookupKeyIndicesInOrder = new ArrayList<>(allLookupKeys);
        lookupKeyIndicesInOrder.sort(Integer::compareTo);
        return lookupKeyIndicesInOrder.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Gets LookupFunction from temporal table according to the given lookup keys. */
    public static UserDefinedFunction getLookupFunction(
            RelOptTable temporalTable, Collection<Integer> lookupKeys) {

        int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);

        if (temporalTable instanceof TableSourceTable) {
            // TODO: support nested lookup keys in the future,
            //  currently we only support top-level lookup keys
            int[][] indices =
                    IntStream.of(lookupKeyIndicesInOrder)
                            .mapToObj(i -> new int[] {i})
                            .toArray(int[][]::new);
            LookupTableSource tableSource =
                    (LookupTableSource) ((TableSourceTable) temporalTable).tableSource();
            LookupRuntimeProviderContext providerContext =
                    new LookupRuntimeProviderContext(indices);
            LookupTableSource.LookupRuntimeProvider provider =
                    tableSource.getLookupRuntimeProvider(providerContext);
            if (provider instanceof TableFunctionProvider) {
                return ((TableFunctionProvider<?>) provider).createTableFunction();
            } else if (provider instanceof AsyncTableFunctionProvider) {
                return ((AsyncTableFunctionProvider<?>) provider).createAsyncTableFunction();
            }
        }

        if (temporalTable instanceof LegacyTableSourceTable) {
            String[] lookupFieldNamesInOrder =
                    IntStream.of(lookupKeyIndicesInOrder)
                            .mapToObj(temporalTable.getRowType().getFieldNames()::get)
                            .toArray(String[]::new);
            LegacyTableSourceTable<?> legacyTableSourceTable =
                    (LegacyTableSourceTable<?>) temporalTable;
            LookupableTableSource<?> tableSource =
                    (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
            if (tableSource.isAsyncEnabled()) {
                return tableSource.getAsyncLookupFunction(lookupFieldNamesInOrder);
            } else {
                return tableSource.getLookupFunction(lookupFieldNamesInOrder);
            }
        }
        throw new TableException(
                String.format(
                        "table %s is neither TableSourceTable not LegacyTableSourceTable",
                        temporalTable.getQualifiedName()));
    }
}
