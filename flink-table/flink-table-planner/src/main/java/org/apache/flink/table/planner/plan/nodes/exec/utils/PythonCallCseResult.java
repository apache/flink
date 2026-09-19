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

import org.apache.calcite.rex.RexCall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Encapsulates the result of Python UDF call Common Sub-expression Elimination (CSE). */
@Internal
public class PythonCallCseResult {

    /** Marks an operand that is not read from the evaluated list. */
    public static final int NO_REF = -1;

    /**
     * The deduplicated Python UDF calls to be evaluated, in execution order.
     *
     * <p>Nested call trees are flattened so that a sub-expression shared between calls appears
     * exactly once. For example {@code SELECT udf1(x), udf2(udf1(x))} yields {@code [udf1(x),
     * udf2(<ref to udf1(x)>)]}.
     */
    private final List<RexCall> deduplicatedCalls;

    /**
     * For each entry of {@link #deduplicatedCalls}, one position per operand of that call: the
     * entry whose result the operand reads, or {@link #NO_REF} if the operand is not a Python call
     * evaluated as its own entry.
     *
     * <p>The positions are recorded per occurrence while flattening, so two occurrences of the same
     * expression are told apart even when Calcite represents them by the same {@link RexCall}
     * object. Post-order guarantees that every position is smaller than the index of the entry
     * holding it.
     */
    private final List<int[]> operandRefs;

    /**
     * For each projection entry, the position in {@link #deduplicatedCalls} holding its result.
     *
     * <p>Flattening appends intermediate sub-expressions that must not be emitted, and post-order
     * can leave a projected result before the end of the list, so this is not necessarily the
     * identity.
     */
    private final int[] outputIndices;

    public PythonCallCseResult(
            List<RexCall> deduplicatedCalls, List<int[]> operandRefs, int[] outputIndices) {
        this.deduplicatedCalls = deduplicatedCalls;
        this.operandRefs = operandRefs;
        this.outputIndices = outputIndices;
    }

    /** The given calls, unchanged: each is evaluated as a whole and nothing is referenced. */
    static PythonCallCseResult unchanged(List<RexCall> pythonRexCalls) {
        List<int[]> noRefs = new ArrayList<>(pythonRexCalls.size());
        int[] identity = new int[pythonRexCalls.size()];
        for (int i = 0; i < pythonRexCalls.size(); i++) {
            int[] refs = new int[pythonRexCalls.get(i).getOperands().size()];
            Arrays.fill(refs, NO_REF);
            noRefs.add(refs);
            identity[i] = i;
        }
        return new PythonCallCseResult(
                pythonRexCalls, Collections.unmodifiableList(noRefs), identity);
    }

    public List<RexCall> getDeduplicatedCalls() {
        return deduplicatedCalls;
    }

    /** See {@link #operandRefs}. */
    public int[] getOperandRefs(int entry) {
        return operandRefs.get(entry);
    }

    public int[] getOutputIndices() {
        return outputIndices;
    }
}
