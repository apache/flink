/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared helpers for the remote (e.g. Python) call common sub-expression elimination rules, namely
 * {@link RemoteCalcProjectionCseRule} and {@link RemoteCalcConditionProjectionCseRule}.
 *
 * <p>Keeping the reusability predicate in one place ensures both rules agree on which calls may
 * safely share a single evaluation.
 */
class RemoteCalcCseUtil {

    private RemoteCalcCseUtil() {}

    /** Expands the local refs of a calc's projection into self-contained expression trees. */
    static List<RexNode> expandProjects(FlinkLogicalCalc calc) {
        RexProgram program = calc.getProgram();
        return program.getProjectList().stream()
                .map(program::expandLocalRef)
                .collect(Collectors.toList());
    }

    /**
     * Returns true if the node is itself a remote call whose result may be reused. A
     * non-deterministic call must be evaluated once per occurrence, so it never qualifies.
     */
    static boolean isReusableRemoteCall(RexNode node, RemoteCallFinder callFinder) {
        return callFinder.isRemoteCall(node) && isReusable(node);
    }

    /**
     * Returns true if the node contains a remote call and its result may be reused. Unlike {@link
     * #isReusableRemoteCall}, the remote call may be nested inside the expression tree.
     */
    static boolean containsReusableRemoteCall(RexNode node, RemoteCallFinder callFinder) {
        return callFinder.containsRemoteCall(node) && isReusable(node);
    }

    /**
     * Maps each field a calc forwards unchanged to the output position at which it is exposed.
     *
     * <p>Only plain {@link RexInputRef} projections are forwarding; the first occurrence wins when a
     * field is projected more than once.
     */
    static Map<Integer, Integer> forwardedFieldPositions(List<RexNode> projects) {
        Map<Integer, Integer> positions = new HashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            RexNode project = projects.get(i);
            if (project instanceof RexInputRef) {
                positions.putIfAbsent(((RexInputRef) project).getIndex(), i);
            }
        }
        return positions;
    }

    /**
     * Rewrites an expression written against a calc's input so that it is written against that
     * calc's output instead, or returns {@code null} if it reads a field the calc does not forward.
     *
     * <p>Splitting a Calc changes what an input ref means: the lower Calc addresses the original
     * input while the upper one addresses the lower Calc's output, and the lower Calc forwards only
     * the fields still needed above. Two expressions that print identically may therefore read
     * different columns, so they must be brought into a common frame of reference before being
     * compared for reuse. A {@code null} result means no valid comparison exists, because the upper
     * Calc cannot express the expression at all.
     */
    static RexNode translateToOutputFrame(
            RexNode node, Map<Integer, Integer> forwardedFieldPositions) {
        try {
            return node.accept(
                    new RexShuttle() {
                        @Override
                        public RexNode visitInputRef(RexInputRef inputRef) {
                            Integer position = forwardedFieldPositions.get(inputRef.getIndex());
                            if (position == null) {
                                throw new NotForwardedException();
                            }
                            return new RexInputRef(position, inputRef.getType());
                        }
                    });
        } catch (NotForwardedException e) {
            return null;
        }
    }

    private static boolean isReusable(RexNode node) {
        return ShortcutUtils.isDeterministicThroughProgram(node, null);
    }

    /** Signals that an expression reads a field which the calc does not forward. */
    private static class NotForwardedException extends RuntimeException {
        private NotForwardedException() {
            super(null, null, false, false);
        }
    }
}
