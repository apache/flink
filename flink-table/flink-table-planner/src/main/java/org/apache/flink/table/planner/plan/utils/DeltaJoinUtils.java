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

import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.runtime.functions.table.lookup.CachingAsyncLookupFunction;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;

import java.util.Collection;
import java.util.Set;

/** Utils for delta join. */
public class DeltaJoinUtils {
    private static final String NON_ASYNC_LOOKUP_TABLE_ERROR_MSG_TEMPLATE =
            "Table [%s] does not support async lookup.";

    private static final String NON_ASYNC_LOOKUP_TABLE_SOLUTION_MSG =
            "If the table supports the option of async lookup joins, "
                    + "add it to the with parameters of the DDL.";

    private static final Set<JoinRelType> ALL_SUPPORTED_JOIN_TYPES =
            Sets.newHashSet(JoinRelType.INNER);

    /** Check if the join type is supported during delta join optimization. */
    public static void validateSupportedJoinType(JoinRelType joinType) {
        if (!ALL_SUPPORTED_JOIN_TYPES.contains(joinType)) {
            throw new IllegalStateException(
                    String.format(
                            "The current sql doesn't support to do delta join optimization.\n"
                                    + "Unsupported join type [%s] encountered while attempting to convert to delta join.",
                            joinType));
        }
    }

    public static RelOptTable getTable(RelNode node) {
        return getTableScan(node).getTable();
    }

    public static TableScan getTableScan(RelNode node) {
        // support to get table across more nodes if we support more nodes in
        // `ALL_ALLOWED_DELTA_JOIN_UPSTREAM_NODES`
        if (node instanceof StreamPhysicalExchange) {
            return getTableScan(((StreamPhysicalExchange) node).getInput());
        }
        if (node instanceof StreamPhysicalWatermarkAssigner) {
            return getTableScan(((StreamPhysicalWatermarkAssigner) node).getInput());
        }
        if (!(node instanceof TableScan)) {
            throw new IllegalStateException(
                    String.format(
                            "The current sql doesn't support to do delta join optimization.\n"
                                    + "Failed to get table from node [%s].",
                            node.getClass().getSimpleName()));
        }
        return (TableScan) node;
    }

    /**
     * Get the async lookup function to lookup join this temporal table. Furthermore, this method
     * also unwraps the cache and retryable lookup function to access the inner {@link
     * AsyncTableFunction}.
     */
    public static AsyncTableFunction<?> getUnwrappedAsyncLookupFunction(
            RelOptTable temporalTable, Collection<Integer> lookupKeys, ClassLoader classLoader) {
        UserDefinedFunction lookupFunction =
                LookupJoinUtil.getLookupFunction(
                        temporalTable,
                        lookupKeys,
                        classLoader,
                        true, // async
                        null, // retryStrategy
                        false); // applyCustomShuffle

        if (!(lookupFunction instanceof AsyncTableFunction)) {
            throw new IllegalStateException(
                    String.format(
                            NON_ASYNC_LOOKUP_TABLE_ERROR_MSG_TEMPLATE
                                    + NON_ASYNC_LOOKUP_TABLE_SOLUTION_MSG,
                            String.join(".", temporalTable.getQualifiedName())));
        }

        boolean changed = true;
        while (changed) {
            // unwrap cache delegator
            if (lookupFunction instanceof CachingAsyncLookupFunction) {
                lookupFunction = ((CachingAsyncLookupFunction) temporalTable).getDelegate();
                continue;
            }
            // unwrap retryable delegator
            if (lookupFunction instanceof RetryableAsyncLookupFunctionDelegator) {
                lookupFunction =
                        ((RetryableAsyncLookupFunctionDelegator) temporalTable)
                                .getUserLookupFunction();
                continue;
            }
            changed = false;
        }
        return (AsyncTableFunction<?>) lookupFunction;
    }
}
