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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Planner rule that tries to simplify emit behavior of {@link StreamPhysicalWindowTableFunction} to
 * emit per record instead of emit after watermark passed window end if followed by {@link
 * StreamPhysicalWindowRank} and time attribute is event-time.
 */
public class SimplifyWindowTableFunctionWithWindowRankRule
        extends RelRule<SimplifyWindowTableFunctionWithWindowRankRule.Config> {

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

    public SimplifyWindowTableFunctionWithWindowRankRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        final int windowTVFIdx = rels.size() - 1;
        final StreamPhysicalWindowTableFunction windowTVF =
                (StreamPhysicalWindowTableFunction) rels.get(windowTVFIdx);
        return SimplifyWindowTableFunctionRuleHelper.needSimplify(windowTVF);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        List<RelNode> rels = call.getRelList();
        RelNode newTree = SimplifyWindowTableFunctionRuleHelper.rebuild(rels);
        call.transformTo(newTree);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {

        @Override
        default SimplifyWindowTableFunctionWithWindowRankRule toRule() {
            return new SimplifyWindowTableFunctionWithWindowRankRule(this);
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
    }
}
