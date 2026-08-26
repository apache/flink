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

import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.Map;

/** Encapsulates the result of Python UDF call Common Sub-expression Elimination (CSE). */
@Internal
public class PythonCallCseResult {

    /**
     * The flattened Python UDF calls to be evaluated, in execution order.
     *
     * <p>Nested call trees are flattened so that a sub-expression shared between calls appears
     * exactly once. For example {@code SELECT udf1(x), udf2(udf1(x))} yields {@code [udf1(x),
     * udf2(<ref to udf1(x)>)]}.
     */
    private final List<RexCall> uniqueCalls;

    /**
     * Sub-expression cross-reference map from a Python UDF call to its index in {@link
     * #uniqueCalls}.
     *
     * <p>Used when building {@code PythonFunctionInfo} to let a nested Python UDF call reference an
     * already-computed sub-expression result instead of recomputing it.
     */
    private final Map<RexCall, Integer> refMap;

    /**
     * Positions in {@link #uniqueCalls} holding the results of the projection entries, in
     * projection order.
     *
     * <p>Flattening adds entries for nested sub-expressions and preserves post-order, so the list
     * is generally wider than the projection and a top-level result is not necessarily last. Only
     * these positions belong to the operator output; the others are intermediate results consumed
     * through {@link #refMap}.
     */
    private final int[] outputIndices;

    public PythonCallCseResult(
            List<RexCall> uniqueCalls, Map<RexCall, Integer> refMap, int[] outputIndices) {
        this.uniqueCalls = uniqueCalls;
        this.refMap = refMap;
        this.outputIndices = outputIndices;
    }

    /** Returns the flattened Python UDF calls to be evaluated, in execution order. */
    public List<RexCall> getUniqueCalls() {
        return uniqueCalls;
    }

    /** Returns the sub-expression cross-reference map. */
    public Map<RexCall, Integer> getRefMap() {
        return refMap;
    }

    /** Returns the positions holding the projection results, in projection order. */
    public int[] getOutputIndices() {
        return outputIndices;
    }
}
