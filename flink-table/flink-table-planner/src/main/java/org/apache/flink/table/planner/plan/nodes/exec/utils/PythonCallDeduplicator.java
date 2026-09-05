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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Utility for Python UDF Common Sub-expression Elimination (CSE) of nested calls.
 *
 * <p>Nested Python UDF call trees are flattened in post-order and deduplicated by structural
 * equivalence, so that a sub-expression shared between calls is evaluated only once by the Python
 * worker. Duplicates between whole projection entries are already removed in the planner by {@code
 * RemoteCalcProjectionCseRule}, so this only concerns sub-expressions.
 */
@Internal
public class PythonCallDeduplicator {

    /**
     * Flattens the given Python UDF call trees and deduplicates the resulting calls.
     *
     * <p>Flattening all trees into a single list enables cross-subtree reuse: e.g. in {@code SELECT
     * udf1(x), udf2(udf1(x))}, the inner {@code udf1(x)} is evaluated only once and {@code udf2}
     * receives its result by reference.
     *
     * <p>Flattening is only worth it when a sub-expression is actually shared. Otherwise the calls
     * are returned unchanged, so a plan such as {@code SELECT f(g(a))} keeps the original nested
     * evaluation instead of paying for sequential execution and a per-record result list without
     * any benefit.
     */
    public static PythonCallCseResult deduplicate(List<RexCall> pythonRexCalls) {
        // Flatten: collect all Python UDF calls from all projection trees in post-order, so a
        // nested sub-expression is always evaluated before the call referencing it. The root of
        // each tree is the last element of its own sub-list.
        List<RexCall> allCalls = new ArrayList<>();
        int[] rootPositions = new int[pythonRexCalls.size()];
        for (int i = 0; i < pythonRexCalls.size(); i++) {
            List<RexCall> subtreeCalls = collectAllPythonUdfCalls(pythonRexCalls.get(i));
            rootPositions[i] = allCalls.size() + subtreeCalls.size() - 1;
            allCalls.addAll(subtreeCalls);
        }

        // Deduplicate the flattened list by structural equivalence, preserving post-order.
        LinkedHashMap<RexCall, Integer> callToIndex = new LinkedHashMap<>();
        List<RexCall> deduplicatedCalls = new ArrayList<>();
        int[] allToDeduplicated = new int[allCalls.size()];
        for (int i = 0; i < allCalls.size(); i++) {
            RexCall call = allCalls.get(i);
            boolean canReuse = ShortcutUtils.isDeterministicThroughProgram(call, null);
            Integer existing = canReuse ? callToIndex.get(call) : null;
            if (existing != null) {
                allToDeduplicated[i] = existing;
                continue;
            }
            int newPos = deduplicatedCalls.size();
            if (canReuse) {
                callToIndex.put(call, newPos);
            }
            deduplicatedCalls.add(call);
            allToDeduplicated[i] = newPos;
        }

        // Nothing was shared when deduplication did not merge any entry. Flattening would then
        // only move the nested calls into a sequentially evaluated list without saving any
        // evaluation, so keep the original trees and let the worker nest them as before.
        if (deduplicatedCalls.size() == allCalls.size()) {
            return new PythonCallCseResult(pythonRexCalls, identity(pythonRexCalls.size()));
        }

        // Flattening adds entries for nested sub-expressions, and post-order means a top-level
        // result is not necessarily last, so record where each projection entry ended up. Only
        // those positions form the operator output.
        int[] outputIndices = new int[pythonRexCalls.size()];
        for (int i = 0; i < pythonRexCalls.size(); i++) {
            outputIndices[i] = allToDeduplicated[rootPositions[i]];
        }

        return new PythonCallCseResult(
                Collections.unmodifiableList(deduplicatedCalls), outputIndices);
    }

    private static int[] identity(int size) {
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = i;
        }
        return result;
    }

    /**
     * Recursively collects all deterministic Python UDF calls from a call tree in DFS post-order.
     *
     * <p>Post-order ensures child results are computed before parents that reference them via
     * refIndex. Non-deterministic children are NOT flattened to prevent incorrect sharing.
     */
    private static List<RexCall> collectAllPythonUdfCalls(RexCall root) {
        List<RexCall> result = new ArrayList<>();
        for (RexNode operand : root.getOperands()) {
            if (operand instanceof RexCall && PythonUtil.isPythonCall((RexCall) operand)) {
                RexCall childCall = (RexCall) operand;
                // Only flatten deterministic child calls for CSE.
                // Non-deterministic calls must remain nested to avoid incorrect sharing.
                if (ShortcutUtils.isDeterministicThroughProgram(childCall, null)) {
                    result.addAll(collectAllPythonUdfCalls(childCall));
                }
            }
        }
        result.add(root);
        return result;
    }

    private PythonCallDeduplicator() {
        // Utility class, no instantiation
    }
}
