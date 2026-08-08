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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Utility for Python UDF Common Sub-expression Elimination (CSE).
 *
 * <p>Deduplicates Python UDF calls to reduce cross-process (JVM ↔ Python Worker) overhead.
 * Workflow: flatten nested call trees → deduplicate by structural equivalence → build index
 * mappings and cross-reference maps.
 */
@Internal
public class PythonCallDeduplicator {

    /**
     * Recursively collects all deterministic Python UDF calls from a call tree in DFS post-order.
     *
     * <p>Post-order ensures child results are computed before parents that reference them via
     * refIndex. Non-deterministic children are NOT flattened to prevent incorrect sharing.
     */
    @VisibleForTesting
    static List<RexCall> collectAllPythonUdfCalls(RexCall root) {
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

    /**
     * Deduplicates Python UDF calls including nested sub-expressions (full-tree CSE).
     *
     * <p>All calls from projection trees are flattened into a single list, then deduplicated by
     * structural equivalence. This enables cross-subtree reuse: e.g., in {@code SELECT udf1(x),
     * udf2(udf1(x))}, the inner {@code udf1(x)} is computed only once.
     */
    public static PythonCallCseResult deduplicate(List<RexCall> pythonRexCalls) {
        // Step 1: Flatten — collect all Python UDF calls from all projection trees (post-order)
        int[] projectionRootPositions = new int[pythonRexCalls.size()];
        List<RexCall> allCalls = new ArrayList<>();
        for (int i = 0; i < pythonRexCalls.size(); i++) {
            List<RexCall> subtreeCalls = collectAllPythonUdfCalls(pythonRexCalls.get(i));
            // In post-order, the root call is always the last element in the subtree list
            projectionRootPositions[i] = allCalls.size() + subtreeCalls.size() - 1;
            allCalls.addAll(subtreeCalls);
        }

        // Step 2: Deduplicate the flattened list
        LinkedHashMap<RexCall, Integer> callToIndex = new LinkedHashMap<>();
        List<RexCall> uniqueCalls = new ArrayList<>();
        int[] allToUnique = new int[allCalls.size()];

        for (int i = 0; i < allCalls.size(); i++) {
            RexCall call = allCalls.get(i);
            boolean canReuse = ShortcutUtils.isDeterministicThroughProgram(call, null);
            Integer existingIndex = callToIndex.get(call);
            if (canReuse && existingIndex != null) {
                allToUnique[i] = existingIndex;
            } else {
                int newPos = uniqueCalls.size();
                if (canReuse) {
                    callToIndex.put(call, newPos);
                }
                uniqueCalls.add(call);
                allToUnique[i] = newPos;
            }
        }

        // Step 3: Build mapping from original projection positions to flattened indices
        int[] originalToDedup = new int[pythonRexCalls.size()];
        for (int i = 0; i < pythonRexCalls.size(); i++) {
            originalToDedup[i] = allToUnique[projectionRootPositions[i]];
        }

        // Step 4: Build refMap for sub-expression cross-referencing.
        // putIfAbsent preserves the first occurrence index, ensuring a parent references
        // its own child rather than a later structurally-equal duplicate.
        LinkedHashMap<RexCall, Integer> refMap = new LinkedHashMap<>();
        for (int i = 0; i < uniqueCalls.size(); i++) {
            refMap.putIfAbsent(uniqueCalls.get(i), i);
        }

        return new PythonCallCseResult(uniqueCalls, originalToDedup, refMap);
    }

    /** Counts the total number of flattened Python UDF calls. */
    public static int countFlattenedCalls(List<RexCall> pythonRexCalls) {
        int total = 0;
        for (RexCall call : pythonRexCalls) {
            total += collectAllPythonUdfCalls(call).size();
        }
        return total;
    }

    /**
     * Builds a CSE annotation showing top-level reuse relationships.
     *
     * <p>Example: given {@code SumFun(f1,f2) AS $1, SumFun(SumFun(f1,f2),f1) AS $2}, produces
     * {@code (CSE: $2->$1)} indicating $2's inner expression reuses $1.
     */
    public static String buildCseTopLevelAnnotation(
            List<RexNode> projection, List<String> outputFieldNames) {
        // Find all Python UDF RexCall indices in projection
        List<Integer> callIndices = new ArrayList<>();
        for (int i = 0; i < projection.size(); i++) {
            if (projection.get(i) instanceof RexCall
                    && PythonUtil.isPythonCall((RexCall) projection.get(i))) {
                callIndices.add(i);
            }
        }

        if (callIndices.size() <= 1) {
            return "";
        }

        List<String> parts = new ArrayList<>();
        for (int i = 1; i < callIndices.size(); i++) {
            int idx = callIndices.get(i);
            RexCall call = (RexCall) projection.get(idx);
            for (RexNode operand : call.getOperands()) {
                if (operand instanceof RexCall && PythonUtil.isPythonCall((RexCall) operand)) {
                    for (int j = 0; j < i; j++) {
                        int prevIdx = callIndices.get(j);
                        if (operand.equals(projection.get(prevIdx))) {
                            parts.add(
                                    outputFieldNames.get(idx)
                                            + "->"
                                            + outputFieldNames.get(prevIdx));
                            break;
                        }
                    }
                }
            }
        }

        if (parts.isEmpty()) {
            return "";
        }
        return " (CSE: " + String.join(", ", parts) + ")";
    }

    private PythonCallDeduplicator() {
        // Utility class, no instantiation
    }
}
