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
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexCall;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Encapsulates the result of Python UDF call Common Sub-expression Elimination (CSE). */
@Internal
public class PythonCallCseResult {

    /**
     * The deduplicated Python UDF calls to be evaluated, in execution order.
     *
     * <p>Nested call trees are flattened so that a sub-expression shared between calls appears
     * exactly once. For example {@code SELECT udf1(x), udf2(udf1(x))} yields {@code [udf1(x),
     * udf2(<ref to udf1(x)>)]}.
     */
    private final List<RexCall> deduplicatedCalls;

    /**
     * For each projection entry, the position in {@link #deduplicatedCalls} holding its result.
     *
     * <p>Flattening appends intermediate sub-expressions that must not be emitted, and post-order
     * can leave a projected result before the end of the list, so this is not necessarily the
     * identity.
     */
    private final int[] outputIndices;

    /**
     * Maps a call to the position in {@link #deduplicatedCalls} where its result is computed, so
     * that a parent reads the result of its child instead of evaluating the child again inline.
     *
     * <p>Every entry of the list can be found here, but the two kinds of calls are looked up
     * differently. A deterministic call is found by structural equality: all occurrences of the
     * same expression map to the single entry that computes it, which is what makes them shared. A
     * non-deterministic call is found by object identity only: each occurrence maps to its own
     * entry, so it is still evaluated once per occurrence, while its parent can still locate that
     * entry rather than evaluating the whole subtree a second time.
     */
    private final Map<RexCall, Integer> refMap;

    public PythonCallCseResult(List<RexCall> deduplicatedCalls, int[] outputIndices) {
        this.deduplicatedCalls = deduplicatedCalls;
        this.outputIndices = outputIndices;
        this.refMap = buildRefMap(deduplicatedCalls);
    }

    public List<RexCall> getDeduplicatedCalls() {
        return deduplicatedCalls;
    }

    public int[] getOutputIndices() {
        return outputIndices;
    }

    public Map<RexCall, Integer> getRefMap() {
        return refMap;
    }

    private static Map<RexCall, Integer> buildRefMap(List<RexCall> deduplicatedCalls) {
        // putIfAbsent keeps the first occurrence, so a parent references the entry that actually
        // computes the value rather than a later structurally equal duplicate. Non-deterministic
        // calls go into an identity-keyed map layered on top, so each occurrence resolves only to
        // itself. IdentityHashMap is consulted first: a non-deterministic call is also structurally
        // equal to itself, and must not fall through to a structurally equal deterministic entry
        // (there is none, but the order makes the intent explicit and cheap to verify).
        Map<RexCall, Integer> byStructure = new LinkedHashMap<>();
        Map<RexCall, Integer> byIdentity = new IdentityHashMap<>();
        for (int i = 0; i < deduplicatedCalls.size(); i++) {
            RexCall call = deduplicatedCalls.get(i);
            if (ShortcutUtils.isDeterministicThroughProgram(call, null)) {
                byStructure.putIfAbsent(call, i);
            } else {
                byIdentity.put(call, i);
            }
        }
        if (byIdentity.isEmpty()) {
            return Collections.unmodifiableMap(byStructure);
        }
        return new LayeredRefMap(byIdentity, Collections.unmodifiableMap(byStructure));
    }

    /**
     * Two maps consulted in order. Only the lookup methods are meaningful; the class is a {@link
     * Map} so that the existing {@code Map<RexCall, Integer>} plumbing stays unchanged.
     */
    private static final class LayeredRefMap extends AbstractMap<RexCall, Integer> {

        private final Map<RexCall, Integer> byIdentity;
        private final Map<RexCall, Integer> byStructure;
        private final Set<Entry<RexCall, Integer>> entries;

        private LayeredRefMap(Map<RexCall, Integer> byIdentity, Map<RexCall, Integer> byStructure) {
            this.byIdentity = byIdentity;
            this.byStructure = byStructure;
            Set<Entry<RexCall, Integer>> all = new LinkedHashSet<>(byStructure.entrySet());
            all.addAll(byIdentity.entrySet());
            this.entries = Collections.unmodifiableSet(all);
        }

        @Override
        public Integer get(Object key) {
            Integer index = byIdentity.get(key);
            return index != null ? index : byStructure.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            return byIdentity.containsKey(key) || byStructure.containsKey(key);
        }

        @Override
        public Set<Entry<RexCall, Integer>> entrySet() {
            return entries;
        }
    }
}
