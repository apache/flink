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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;

/**
 * Planner rule that tries to simplify emit behavior of {@link StreamPhysicalWindowTableFunction} to
 * emit per record instead of emit after watermark passed window end if time attribute is event-time
 * and followed by {@link StreamPhysicalWindowRank} or {@link StreamPhysicalWindowJoin}.
 */
public interface SimplifyWindowTableFunctionRules {
    SimplifyWindowTableFunctionWithWindowRankRule WITH_WINDOW_RANK =
            new SimplifyWindowTableFunctionWithWindowRankRule();
    SimplifyWindowTableFunctionWithCalcWindowRankRule WITH_CALC_WINDOW_RANK =
            new SimplifyWindowTableFunctionWithCalcWindowRankRule();
    SimplifyWindowTableFunctionRuleWithWindowJoinRule WITH_WINDOW_JOIN =
            new SimplifyWindowTableFunctionRuleWithWindowJoinRule();
    SimplifyWindowTableFunctionRuleWithLeftCalcWindowJoinRule WITH_LEFT_CALC_WINDOW_JOIN =
            new SimplifyWindowTableFunctionRuleWithLeftCalcWindowJoinRule();
    SimplifyWindowTableFunctionRuleWithRightCalcWindowJoinRule WITH_RIGHT_CALC_WINDOW_JOIN =
            new SimplifyWindowTableFunctionRuleWithRightCalcWindowJoinRule();
    SimplifyWindowTableFunctionRuleWithLeftRightCalcWindowJoinRule
            WITH_LEFT_RIGHT_CALC_WINDOW_JOIN =
                    new SimplifyWindowTableFunctionRuleWithLeftRightCalcWindowJoinRule();
}

abstract class SimplifyWindowTableFunctionRuleBase extends RelOptRule {
    SimplifyWindowTableFunctionRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    /**
     * Replace the leaf node, and build a new {@link RelNode} tree in the given nodes order which is
     * in root-down direction.
     */
    protected RelNode rebuild(List<RelNode> nodes) {
        final StreamPhysicalWindowTableFunction windowTVF =
                (StreamPhysicalWindowTableFunction) nodes.get(nodes.size() - 1);
        if (needSimplify(windowTVF)) {
            RelNode root = windowTVF.copy(true);
            for (int i = nodes.size() - 2; i >= 0; i--) {
                RelNode node = nodes.get(i);
                root = node.copy(node.getTraitSet(), Collections.singletonList(root));
            }
            return root;
        } else {
            return nodes.get(0);
        }
    }

    protected boolean needSimplify(StreamPhysicalWindowTableFunction windowTVF) {
        // excludes windowTVF which is already simplified to emit by per record
        return windowTVF.windowing().isRowtime() && !windowTVF.emitPerRecord();
    }
}

abstract class SimplifyWindowTableFunctionWithWindowRankRuleBase
        extends SimplifyWindowTableFunctionRuleBase {
    SimplifyWindowTableFunctionWithWindowRankRuleBase(
            RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        final int windowTVFIdx = rels.size() - 1;
        final StreamPhysicalWindowTableFunction windowTVF =
                (StreamPhysicalWindowTableFunction) rels.get(windowTVFIdx);
        return needSimplify(windowTVF);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        RelNode newTree = rebuild(rels);
        call.transformTo(newTree);
    }
}

class SimplifyWindowTableFunctionWithWindowRankRule
        extends SimplifyWindowTableFunctionWithWindowRankRuleBase {

    SimplifyWindowTableFunctionWithWindowRankRule() {
        super(
                operand(
                        StreamPhysicalWindowRank.class,
                        operand(
                                StreamPhysicalExchange.class,
                                operand(StreamPhysicalWindowTableFunction.class, any()))),
                "SimplifyWindowTableFunctionWithWindowRankRule");
    }
}

class SimplifyWindowTableFunctionWithCalcWindowRankRule
        extends SimplifyWindowTableFunctionWithWindowRankRuleBase {

    SimplifyWindowTableFunctionWithCalcWindowRankRule() {
        super(
                operand(
                        StreamPhysicalWindowRank.class,
                        operand(
                                StreamPhysicalExchange.class,
                                operand(
                                        StreamPhysicalCalc.class,
                                        operand(StreamPhysicalWindowTableFunction.class, any())))),
                "SimplifyWindowTableFunctionWithCalcWindowRankRule");
    }
}

abstract class SimplifyWindowTableFunctionWithWindowJoinRuleBase
        extends SimplifyWindowTableFunctionRuleBase {
    SimplifyWindowTableFunctionWithWindowJoinRuleBase(
            RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final StreamPhysicalWindowTableFunction leftWindowTVF = getLeftWindowTVF(call);
        List<RelNode> rels = call.getRelList();
        final StreamPhysicalWindowTableFunction rightWindowTVF =
                (StreamPhysicalWindowTableFunction) rels.get(rels.size() - 1);
        return needSimplify(leftWindowTVF) || needSimplify(rightWindowTVF);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalWindowJoin join = call.rel(0);
        final RelNode newLeft = buildNewLeftTree(call);
        final RelNode newRight = buildNewRightTree(call);
        final RelNode newJoin =
                join.copy(
                        join.getTraitSet(),
                        join.getCondition(),
                        newLeft,
                        newRight,
                        join.getJoinType(),
                        join.isSemiJoinDone());
        call.transformTo(newJoin);
    }

    abstract StreamPhysicalWindowTableFunction getLeftWindowTVF(RelOptRuleCall call);

    abstract RelNode buildNewLeftTree(RelOptRuleCall call);

    abstract RelNode buildNewRightTree(RelOptRuleCall call);
}

class SimplifyWindowTableFunctionRuleWithWindowJoinRule
        extends SimplifyWindowTableFunctionWithWindowJoinRuleBase {

    SimplifyWindowTableFunctionRuleWithWindowJoinRule() {
        super(
                operand(
                        StreamPhysicalWindowJoin.class,
                        operand(
                                StreamPhysicalExchange.class,
                                operand(StreamPhysicalWindowTableFunction.class, any())),
                        operand(
                                StreamPhysicalExchange.class,
                                operand(StreamPhysicalWindowTableFunction.class, any()))),
                "SimplifyWindowTableFunctionRuleWithWindowJoinRule");
    }

    @Override
    StreamPhysicalWindowTableFunction getLeftWindowTVF(RelOptRuleCall call) {
        return call.rel(2);
    }

    @Override
    RelNode buildNewLeftTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(1, 3));
    }

    @Override
    RelNode buildNewRightTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(3, 5));
    }
}

class SimplifyWindowTableFunctionRuleWithLeftCalcWindowJoinRule
        extends SimplifyWindowTableFunctionWithWindowJoinRuleBase {

    SimplifyWindowTableFunctionRuleWithLeftCalcWindowJoinRule() {
        super(
                operand(
                        StreamPhysicalWindowJoin.class,
                        operand(
                                StreamPhysicalExchange.class,
                                operand(
                                        StreamPhysicalCalc.class,
                                        operand(StreamPhysicalWindowTableFunction.class, any()))),
                        operand(
                                StreamPhysicalExchange.class,
                                operand(StreamPhysicalWindowTableFunction.class, any()))),
                "SimplifyWindowTableFunctionRuleWithLeftCalcWindowJoinRule");
    }

    @Override
    StreamPhysicalWindowTableFunction getLeftWindowTVF(RelOptRuleCall call) {
        return call.rel(3);
    }

    @Override
    RelNode buildNewLeftTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(1, 4));
    }

    @Override
    RelNode buildNewRightTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(4, 6));
    }
}

class SimplifyWindowTableFunctionRuleWithRightCalcWindowJoinRule
        extends SimplifyWindowTableFunctionWithWindowJoinRuleBase {

    SimplifyWindowTableFunctionRuleWithRightCalcWindowJoinRule() {
        super(
                operand(
                        StreamPhysicalWindowJoin.class,
                        operand(
                                StreamPhysicalExchange.class,
                                operand(StreamPhysicalWindowTableFunction.class, any())),
                        operand(
                                StreamPhysicalExchange.class,
                                operand(
                                        StreamPhysicalCalc.class,
                                        operand(StreamPhysicalWindowTableFunction.class, any())))),
                "SimplifyWindowTableFunctionRuleWithRightCalcWindowJoinRule");
    }

    @Override
    StreamPhysicalWindowTableFunction getLeftWindowTVF(RelOptRuleCall call) {
        return call.rel(2);
    }

    @Override
    RelNode buildNewLeftTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(1, 3));
    }

    @Override
    RelNode buildNewRightTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(3, 6));
    }
}

class SimplifyWindowTableFunctionRuleWithLeftRightCalcWindowJoinRule
        extends SimplifyWindowTableFunctionWithWindowJoinRuleBase {

    SimplifyWindowTableFunctionRuleWithLeftRightCalcWindowJoinRule() {
        super(
                operand(
                        StreamPhysicalWindowJoin.class,
                        operand(
                                StreamPhysicalExchange.class,
                                operand(
                                        StreamPhysicalCalc.class,
                                        operand(StreamPhysicalWindowTableFunction.class, any()))),
                        operand(
                                StreamPhysicalExchange.class,
                                operand(
                                        StreamPhysicalCalc.class,
                                        operand(StreamPhysicalWindowTableFunction.class, any())))),
                "SimplifyWindowTableFunctionRuleWithLeftRightCalcWindowJoinRule");
    }

    @Override
    StreamPhysicalWindowTableFunction getLeftWindowTVF(RelOptRuleCall call) {
        return call.rel(3);
    }

    @Override
    RelNode buildNewLeftTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(1, 4));
    }

    @Override
    RelNode buildNewRightTree(RelOptRuleCall call) {
        return rebuild(call.getRelList().subList(4, 7));
    }
}
