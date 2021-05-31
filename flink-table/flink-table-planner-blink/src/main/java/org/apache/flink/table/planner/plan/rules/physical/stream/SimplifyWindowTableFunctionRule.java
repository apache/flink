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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;

/**
 * Planner rule that tries to simplify emit behavior of {@link StreamPhysicalWindowTableFunction} to
 * emit per record instead of emit after watermark passed window end if the successor node is {@link
 * StreamPhysicalWindowRank} or {@link StreamPhysicalWindowJoin} which with
 * rowtime-WindowingStrategy.
 */
public class SimplifyWindowTableFunctionRule
        extends RelRule<SimplifyWindowTableFunctionRule.Config> {

    public static final RelOptRule WITH_WINDOW_RANK =
            Config.EMPTY
                    .withDescription("SimplifyWindowTableFunctionRuleWithWindowRank")
                    .as(Config.class)
                    .withWindowRank()
                    .toRule();

    public static final RelOptRule WITH_CALC_WINDOW_RANK =
            Config.EMPTY
                    .withDescription("SimplifyWindowTableFunctionRuleWithCalcWindowRank")
                    .as(Config.class)
                    .withCalcWindowRank()
                    .toRule();

    public static final RelOptRule WITH_WINDOW_JOIN =
            Config.EMPTY
                    .withDescription("SimplifyWindowTableFunctionRuleWithWindowJoin")
                    .as(Config.class)
                    .withWindowJoin()
                    .toRule();

    public static final RelOptRule WITH_LEFT_CALC_WINDOW_JOIN =
            Config.EMPTY
                    .withDescription("SimplifyWindowTableFunctionRuleWithLeftCalcWindowJoin")
                    .as(Config.class)
                    .withLeftCalcWindowJoin()
                    .toRule();

    public static final RelOptRule WITH_RIGHT_CALC_WINDOW_JOIN =
            Config.EMPTY
                    .withDescription("SimplifyWindowTableFunctionRuleWithRightCalcWindowJoin")
                    .as(Config.class)
                    .withRightCalcWindowJoin()
                    .toRule();

    public static final RelOptRule WITH_LEFT_RIGHT_CALC_WINDOW_JOIN =
            Config.EMPTY
                    .withDescription("SimplifyWindowTableFunctionRuleWithLeftRightCalcWindowJoin")
                    .as(Config.class)
                    .withLeftRightCalcWindowJoin()
                    .toRule();

    public SimplifyWindowTableFunctionRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        final RelNode node = rels.get(0);
        if (node instanceof StreamPhysicalWindowRank) {
            final int windowTVFIdx = rels.size() - 1;
            final StreamPhysicalWindowTableFunction windowTVF =
                    (StreamPhysicalWindowTableFunction) rels.get(windowTVFIdx);
            return needSimplify(windowTVF);
        } else if (node instanceof StreamPhysicalWindowJoin) {
            RelNode leftWindowTVFRel;
            if (call.rule == WITH_WINDOW_JOIN) {
                leftWindowTVFRel = rels.get(2);
            } else if (call.rule == WITH_RIGHT_CALC_WINDOW_JOIN) {
                leftWindowTVFRel = rels.get(2);
            } else if (call.rule == WITH_LEFT_CALC_WINDOW_JOIN) {
                leftWindowTVFRel = rels.get(3);
            } else if (call.rule == WITH_LEFT_RIGHT_CALC_WINDOW_JOIN) {
                leftWindowTVFRel = rels.get(3);
            } else {
                throw new TableException("This should never happen. Please file an issue.");
            }
            final StreamPhysicalWindowTableFunction leftWindowTVF =
                    (StreamPhysicalWindowTableFunction) leftWindowTVFRel;
            final StreamPhysicalWindowTableFunction rightWindowTVF =
                    (StreamPhysicalWindowTableFunction) rels.get(rels.size() - 1);
            return needSimplify(leftWindowTVF) || needSimplify(rightWindowTVF);
        } else {
            throw new IllegalStateException(
                    this.getClass().getName()
                            + " matches a wrong relation tree: "
                            + RelOptUtil.toString(node));
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        final RelNode node = rels.get(0);
        if (node instanceof StreamPhysicalWindowRank) {
            RelNode newTree = rebuild(rels);
            call.transformTo(newTree);
        } else if (node instanceof StreamPhysicalWindowJoin) {
            RelNode newLeft;
            RelNode newRight;
            if (call.rule == WITH_WINDOW_JOIN) {
                newLeft = rebuild(rels.subList(1, 3));
                newRight = rebuild(rels.subList(3, 5));
            } else if (call.rule == WITH_RIGHT_CALC_WINDOW_JOIN) {
                newLeft = rebuild(rels.subList(1, 3));
                newRight = rebuild(rels.subList(3, 6));
            } else if (call.rule == WITH_LEFT_CALC_WINDOW_JOIN) {
                newLeft = rebuild(rels.subList(1, 4));
                newRight = rebuild(rels.subList(4, 6));
            } else if (call.rule == WITH_LEFT_RIGHT_CALC_WINDOW_JOIN) {
                newLeft = rebuild(rels.subList(1, 4));
                newRight = rebuild(rels.subList(4, 7));
            } else {
                throw new TableException("This should never happen. Please file an issue.");
            }
            final StreamPhysicalWindowJoin join = call.rel(0);
            final RelNode newJoin =
                    join.copy(
                            join.getTraitSet(),
                            join.getCondition(),
                            newLeft,
                            newRight,
                            join.getJoinType(),
                            join.isSemiJoinDone());
            call.transformTo(newJoin);
        } else {
            throw new IllegalStateException(
                    this.getClass().getName()
                            + " matches a wrong relation tree: "
                            + RelOptUtil.toString(node));
        }
    }

    private boolean needSimplify(StreamPhysicalWindowTableFunction windowTVF) {
        return windowTVF.windowing().isRowtime() && !windowTVF.emitPerRecord();
    }

    /**
     * Replace the leaf node, and build a new {@link RelNode} tree in the given nodes order which is
     * in root-down direction.
     */
    private RelNode rebuild(List<RelNode> nodes) {
        final StreamPhysicalWindowTableFunction windowTVF =
                (StreamPhysicalWindowTableFunction) nodes.get(nodes.size() - 1);
        if (needSimplify(windowTVF)) {
            final StreamPhysicalWindowTableFunction newWindowTVF = windowTVF.copy(true);
            RelNode root = newWindowTVF;
            for (int i = nodes.size() - 2; i >= 0; i--) {
                RelNode node = nodes.get(i);
                root = node.copy(node.getTraitSet(), Collections.singletonList(root));
            }
            return root;
        } else {
            return nodes.get(0);
        }
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {

        @Override
        default SimplifyWindowTableFunctionRule toRule() {
            return new SimplifyWindowTableFunctionRule(this);
        }

        default Config withWindowRank() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWindowRank.class)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalWindowTableFunction
                                                                                                            .class)
                                                                                            .anyInputs())))
                    .as(Config.class);
        }

        default Config withCalcWindowRank() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWindowRank.class)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalCalc
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b3 ->
                                                                                                            b3.operand(
                                                                                                                            StreamPhysicalWindowTableFunction
                                                                                                                                    .class)
                                                                                                                    .anyInputs()))))
                    .as(Config.class);
        }

        default Config withWindowJoin() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWindowJoin.class)
                                            .inputs(
                                                    b1 ->
                                                            b1.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalWindowTableFunction
                                                                                                            .class)
                                                                                            .anyInputs()),
                                                    b1 ->
                                                            b1.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalWindowTableFunction
                                                                                                            .class)
                                                                                            .anyInputs())))
                    .as(Config.class);
        }

        default Config withLeftCalcWindowJoin() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWindowJoin.class)
                                            .inputs(
                                                    b ->
                                                            b.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b1 ->
                                                                                    b1.operand(
                                                                                                    StreamPhysicalCalc
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b2 ->
                                                                                                            b2.operand(
                                                                                                                            StreamPhysicalWindowTableFunction
                                                                                                                                    .class)
                                                                                                                    .anyInputs())),
                                                    b ->
                                                            b.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b1 ->
                                                                                    b1.operand(
                                                                                                    StreamPhysicalWindowTableFunction
                                                                                                            .class)
                                                                                            .anyInputs())))
                    .as(Config.class);
        }

        default Config withRightCalcWindowJoin() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWindowJoin.class)
                                            .inputs(
                                                    b ->
                                                            b.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b1 ->
                                                                                    b1.operand(
                                                                                                    StreamPhysicalWindowTableFunction
                                                                                                            .class)
                                                                                            .anyInputs()),
                                                    b ->
                                                            b.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b1 ->
                                                                                    b1.operand(
                                                                                                    StreamPhysicalCalc
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b2 ->
                                                                                                            b2.operand(
                                                                                                                            StreamPhysicalWindowTableFunction
                                                                                                                                    .class)
                                                                                                                    .anyInputs()))))
                    .as(Config.class);
        }

        default Config withLeftRightCalcWindowJoin() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWindowJoin.class)
                                            .inputs(
                                                    b ->
                                                            b.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b1 ->
                                                                                    b1.operand(
                                                                                                    StreamPhysicalCalc
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b2 ->
                                                                                                            b2.operand(
                                                                                                                            StreamPhysicalWindowTableFunction
                                                                                                                                    .class)
                                                                                                                    .anyInputs())),
                                                    b ->
                                                            b.operand(StreamPhysicalExchange.class)
                                                                    .oneInput(
                                                                            b1 ->
                                                                                    b1.operand(
                                                                                                    StreamPhysicalCalc
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b2 ->
                                                                                                            b2.operand(
                                                                                                                            StreamPhysicalWindowTableFunction
                                                                                                                                    .class)
                                                                                                                    .anyInputs()))))
                    .as(Config.class);
        }
    }
}
