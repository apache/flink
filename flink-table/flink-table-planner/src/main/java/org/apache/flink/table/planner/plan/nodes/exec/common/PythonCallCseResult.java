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

    /** Unique Python UDF call list after deduplication (flattened and deduplicated). */
    private final List<RexCall> uniqueCalls;

    /**
     * Mapping from original projection index to deduplicated unique call index.
     *
     * <p>For example, given {@code SELECT udf1(x), udf1(x)}, the mapping is {@code [0, 0]},
     * indicating both projections point to the 0th call in uniqueCalls.
     */
    private final int[] originalToDedupMapping;

    /**
     * Sub-expression cross-reference map from unique RexCall to its index in uniqueCalls.
     *
     * <p>Used when building {@code PythonFunctionInfo} to allow nested Python UDF calls to reuse
     * already-computed sub-expression results via reference instead of recomputation.
     */
    private final Map<RexCall, Integer> refMap;

    public PythonCallCseResult(
            List<RexCall> uniqueCalls, int[] originalToDedupMapping, Map<RexCall, Integer> refMap) {
        this.uniqueCalls = uniqueCalls;
        this.originalToDedupMapping = originalToDedupMapping;
        this.refMap = refMap;
    }

    /** Returns the unique Python UDF call list after deduplication. */
    public List<RexCall> getUniqueCalls() {
        return uniqueCalls;
    }

    /** Returns the mapping array from original projection index to deduplicated index. */
    public int[] getOriginalToDedupMapping() {
        return originalToDedupMapping;
    }

    /** Returns the sub-expression cross-reference map. */
    public Map<RexCall, Integer> getRefMap() {
        return refMap;
    }

    /** Returns whether an expansion projection is needed to restore the original output schema. */
    public boolean needsExpansionProjection() {
        return originalToDedupMapping.length != uniqueCalls.size();
    }

    /**
     * Returns the number of top-level Python UDF calls in the original projection (before
     * deduplication). This equals the length of the originalToDedupMapping array.
     */
    public int getOriginalCount() {
        return originalToDedupMapping.length;
    }

    /** Returns the number of unique calls after deduplication. */
    public int getUniqueCount() {
        return uniqueCalls.size();
    }
}
