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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Rule that eliminates common Python UDF sub-expressions between the condition and projection of a
 * Calc node.
 *
 * <p>After {@link RemoteCalcSplitConditionRule} splits a Calc with Python UDFs in its condition,
 * the result is a two-level Calc structure:
 *
 * <pre>
 * TopCalc(projection=[pyFunc(a, b) + 1, pyFunc(a, b) + 2], condition=[$2 > 0])
 *   BottomCalc(projection=[a, b, pyFunc(a, b) AS f0])
 * </pre>
 *
 * <p>The TopCalc's projection still contains {@code pyFunc(a, b)} which is structurally identical
 * to the already-computed {@code f0} in the BottomCalc. This rule detects such duplicates and
 * rewrites the TopCalc's projection to reference the BottomCalc's output directly:
 *
 * <pre>
 * TopCalc(projection=[$2 + 1, $2 + 2], condition=[$2 > 0])
 *   BottomCalc(projection=[a, b, pyFunc(a, b) AS f0])
 * </pre>
 */
@Value.Enclosing
public class RemoteCalcConditionProjectionCseRule
        extends RelRule<RemoteCalcConditionProjectionCseRule.Config> {

    protected RemoteCalcConditionProjectionCseRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc topCalc = call.rel(0);
        FlinkLogicalCalc bottomCalc = call.rel(1);
        RemoteCallFinder callFinder = config.remoteCallFinder();

        // Only applies when the top calc has a condition.
        if (topCalc.getProgram().getCondition() == null) {
            return false;
        }

        List<RexNode> topProjects = expandProjects(topCalc);
        Map<RexNode, Integer> bottomPythonCalls = buildBottomPythonCallMap(bottomCalc, callFinder);

        if (bottomPythonCalls.isEmpty()) {
            return false;
        }

        // Check if any top projection contains a call matching the bottom calc's output.
        return topProjects.stream()
                .anyMatch(node -> containsCallMatchingBottom(node, bottomPythonCalls, callFinder));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc topCalc = call.rel(0);
        FlinkLogicalCalc bottomCalc = call.rel(1);
        RemoteCallFinder callFinder = config.remoteCallFinder();
        RexBuilder rexBuilder = call.builder().getRexBuilder();

        List<RexNode> topProjects = expandProjects(topCalc);
        RexNode topCondition =
                topCalc.getProgram().getCondition() != null
                        ? topCalc.getProgram().expandLocalRef(topCalc.getProgram().getCondition())
                        : null;

        Map<RexNode, Integer> bottomPythonCalls = buildBottomPythonCallMap(bottomCalc, callFinder);
        RelDataType bottomRowType = bottomCalc.getRowType();

        // Rewrite top projections: replace matching calls with RexInputRef.
        CseRewriteShuttle rewriter = new CseRewriteShuttle(bottomPythonCalls, bottomRowType);
        List<RexNode> newTopProjects =
                topProjects.stream().map(p -> p.accept(rewriter)).collect(Collectors.toList());

        if (!rewriter.hasRewritten()) {
            return;
        }

        // Build the new top calc with rewritten projections.
        call.transformTo(
                topCalc.copy(
                        topCalc.getTraitSet(),
                        bottomCalc,
                        RexProgram.create(
                                bottomRowType,
                                newTopProjects,
                                topCondition,
                                topCalc.getRowType(),
                                rexBuilder)));
    }

    private List<RexNode> expandProjects(FlinkLogicalCalc calc) {
        RexProgram program = calc.getProgram();
        return program.getProjectList().stream()
                .map(program::expandLocalRef)
                .collect(Collectors.toList());
    }

    /**
     * Builds a map from deterministic Python UDF calls in the bottom calc's projection to their
     * output index.
     */
    private Map<RexNode, Integer> buildBottomPythonCallMap(
            FlinkLogicalCalc bottomCalc, RemoteCallFinder callFinder) {
        RexProgram program = bottomCalc.getProgram();
        List<RexNode> bottomProjects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());

        Map<RexNode, Integer> result = new HashMap<>();
        IntStream.range(0, bottomProjects.size())
                .filter(
                        i -> {
                            RexNode node = bottomProjects.get(i);
                            return callFinder.containsRemoteCall(node)
                                    && ShortcutUtils.isDeterministicThroughProgram(node, null);
                        })
                .forEach(i -> result.put(bottomProjects.get(i), i));
        return result;
    }

    private boolean containsCallMatchingBottom(
            RexNode node, Map<RexNode, Integer> bottomPythonCalls, RemoteCallFinder callFinder) {
        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if (callFinder.isRemoteCall(rexCall) && bottomPythonCalls.containsKey(rexCall)) {
                return true;
            }
            return rexCall.getOperands().stream()
                    .anyMatch(op -> containsCallMatchingBottom(op, bottomPythonCalls, callFinder));
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RemoteCalcConditionProjectionCseRule)) {
            return false;
        }
        RemoteCalcConditionProjectionCseRule other = (RemoteCalcConditionProjectionCseRule) obj;
        return super.equals(other)
                && config.remoteCallFinder()
                        .getClass()
                        .equals(other.config.remoteCallFinder().getClass());
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + config.remoteCallFinder().getClass().hashCode();
    }

    // -------------------------------------------------------------------------

    /**
     * Replaces Python UDF calls in the top calc's projection with RexInputRef pointing to the
     * bottom calc's output position where the same call was already computed.
     */
    private static class CseRewriteShuttle extends RexShuttle {
        private final Map<RexNode, Integer> bottomPythonCalls;
        private final RelDataType bottomRowType;
        private boolean rewritten = false;

        CseRewriteShuttle(Map<RexNode, Integer> bottomPythonCalls, RelDataType bottomRowType) {
            this.bottomPythonCalls = bottomPythonCalls;
            this.bottomRowType = bottomRowType;
        }

        boolean hasRewritten() {
            return rewritten;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            Integer idx = bottomPythonCalls.get(call);
            if (idx != null) {
                rewritten = true;
                return new RexInputRef(idx, bottomRowType.getFieldList().get(idx).getType());
            }
            return super.visitCall(call);
        }
    }

    // -------------------------------------------------------------------------

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableRemoteCalcConditionProjectionCseRule.Config.builder()
                        .operandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCalc.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalCalc.class)
                                                                        .anyInputs()))
                        .description("RemoteCalcConditionProjectionCseRule")
                        .build();

        @Value.Default
        default RemoteCallFinder remoteCallFinder() {
            return new PythonRemoteCallFinder();
        }

        /** Sets {@link #remoteCallFinder()}. */
        Config withRemoteCallFinder(RemoteCallFinder callFinder);

        @Override
        default RemoteCalcConditionProjectionCseRule toRule() {
            return new RemoteCalcConditionProjectionCseRule(this);
        }
    }
}
