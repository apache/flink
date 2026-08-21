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
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexCall;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Utility for Python UDF Common Sub-expression Elimination (CSE).
 *
 * <p>Deduplicates top-level Python UDF calls in the projection by structural equivalence to reduce
 * cross-process (JVM &lt;-&gt; Python Worker) overhead. Only deterministic calls can be safely
 * reused; non-deterministic calls must be evaluated independently each time.
 */
@Internal
public class PythonCallDeduplicator {

    /**
     * Deduplicates top-level Python UDF calls by structural equivalence.
     *
     * <p>For example, in {@code SELECT udf1(x), udf1(x)}, the second call is structurally equal to
     * the first one and will be computed only once; an expansion projection restores the original
     * output schema afterwards.
     */
    public static PythonCallCseResult deduplicate(List<RexCall> pythonRexCalls) {
        LinkedHashMap<RexCall, Integer> callToIndex = new LinkedHashMap<>();
        List<RexCall> uniqueCalls = new ArrayList<>();
        int[] originalToDedup = new int[pythonRexCalls.size()];

        for (int i = 0; i < pythonRexCalls.size(); i++) {
            RexCall call = pythonRexCalls.get(i);
            boolean canReuse = ShortcutUtils.isDeterministicThroughProgram(call, null);
            Integer existingIndex = callToIndex.get(call);
            if (canReuse && existingIndex != null) {
                // Deterministic duplicate — reuse the existing call
                originalToDedup[i] = existingIndex;
            } else {
                int newPos = uniqueCalls.size();
                if (canReuse) {
                    callToIndex.put(call, newPos);
                }
                uniqueCalls.add(call);
                originalToDedup[i] = newPos;
            }
        }
        return new PythonCallCseResult(uniqueCalls, originalToDedup);
    }

    private PythonCallDeduplicator() {
        // Utility class, no instantiation
    }
}
