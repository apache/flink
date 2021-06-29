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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Planner rule that tries to simplify emit behavior of {@link StreamPhysicalWindowTableFunction} to
 * emit per record instead of emit after watermark passed window end if followed by {@link
 * StreamPhysicalWindowJoin} and time attribute is event-time.
 */
public class SimplifyWindowTableFunctionWithWindowJoinRule
        extends RelRule<SimplifyWindowTableFunctionWithWindowJoinRule.Config> {

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

    public SimplifyWindowTableFunctionWithWindowJoinRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        RelNode leftWindowTVFRel;
        if (call.rule == WITH_WINDOW_JOIN || call.rule == WITH_RIGHT_CALC_WINDOW_JOIN) {
            leftWindowTVFRel = rels.get(2);
        } else {
            leftWindowTVFRel = rels.get(3);
        }
        final StreamPhysicalWindowTableFunction leftWindowTVF =
                (StreamPhysicalWindowTableFunction) leftWindowTVFRel;
        final StreamPhysicalWindowTableFunction rightWindowTVF =
                (StreamPhysicalWindowTableFunction) rels.get(rels.size() - 1);
        return SimplifyWindowTableFunctionRuleHelper.needSimplify(leftWindowTVF)
                || SimplifyWindowTableFunctionRuleHelper.needSimplify(rightWindowTVF);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        final StreamPhysicalWindowJoin join = call.rel(0);
        RelNode newLeft;
        RelNode newRight;
        if (call.rule == WITH_WINDOW_JOIN) {
            newLeft = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(1, 3));
            newRight = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(3, 5));
        } else if (call.rule == WITH_RIGHT_CALC_WINDOW_JOIN) {
            newLeft = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(1, 3));
            newRight = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(3, 6));
        } else if (call.rule == WITH_LEFT_CALC_WINDOW_JOIN) {
            newLeft = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(1, 4));
            newRight = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(4, 6));
        } else {
            newLeft = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(1, 4));
            newRight = SimplifyWindowTableFunctionRuleHelper.rebuild(rels.subList(4, 7));
        }
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

    /** Rule configuration. */
    public interface Config extends RelRule.Config {

        @Override
        default SimplifyWindowTableFunctionWithWindowJoinRule toRule() {
            return new SimplifyWindowTableFunctionWithWindowJoinRule(this);
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
