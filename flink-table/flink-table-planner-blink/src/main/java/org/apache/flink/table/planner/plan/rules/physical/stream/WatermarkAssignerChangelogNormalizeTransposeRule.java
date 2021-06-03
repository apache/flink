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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Transpose {@link StreamPhysicalWatermarkAssigner} past into {@link
 * StreamPhysicalChangelogNormalize}.
 */
public class WatermarkAssignerChangelogNormalizeTransposeRule
        extends RelRule<WatermarkAssignerChangelogNormalizeTransposeRule.Config> {

    public static final RelOptRule WITH_CALC =
            Config.EMPTY
                    .withDescription("WatermarkAssignerChangelogNormalizeTransposeRuleWithCalc")
                    .as(Config.class)
                    .withCalc()
                    .toRule();

    public static final RelOptRule WITHOUT_CALC =
            Config.EMPTY
                    .withDescription("WatermarkAssignerChangelogNormalizeTransposeRuleWithoutCalc")
                    .as(Config.class)
                    .withoutCalc()
                    .toRule();

    public WatermarkAssignerChangelogNormalizeTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalWatermarkAssigner watermark = call.rel(0);
        final RelNode node = call.rel(1);
        if (node instanceof StreamPhysicalCalc) {
            // with calc
            final StreamPhysicalCalc calc = call.rel(1);
            final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(2);
            final StreamPhysicalExchange exchange = call.rel(3);

            final RelNode newTree =
                    buildTreeInOrder(
                            changelogNormalize, exchange, watermark, calc, exchange.getInput());
            call.transformTo(newTree);
        } else if (node instanceof StreamPhysicalChangelogNormalize) {
            // without calc
            final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);
            final StreamPhysicalExchange exchange = call.rel(2);

            final RelNode newTree =
                    buildTreeInOrder(changelogNormalize, exchange, watermark, exchange.getInput());
            call.transformTo(newTree);
        } else {
            throw new IllegalStateException(
                    this.getClass().getName()
                            + " matches a wrong relation tree: "
                            + RelOptUtil.toString(watermark));
        }
    }

    /**
     * Build a new {@link RelNode} tree in the given nodes order which is in root-down direction.
     */
    private RelNode buildTreeInOrder(RelNode... nodes) {
        checkArgument(nodes.length >= 2);
        RelNode root = nodes[nodes.length - 1];
        for (int i = nodes.length - 2; i >= 0; i--) {
            RelNode node = nodes[i];
            root = node.copy(node.getTraitSet(), Collections.singletonList(root));
        }
        return root;
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {

        @Override
        default WatermarkAssignerChangelogNormalizeTransposeRule toRule() {
            return new WatermarkAssignerChangelogNormalizeTransposeRule(this);
        }

        default Config withCalc() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWatermarkAssigner.class)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(StreamPhysicalCalc.class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalChangelogNormalize
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b3 ->
                                                                                                            b3.operand(
                                                                                                                            StreamPhysicalExchange
                                                                                                                                    .class)
                                                                                                                    .anyInputs()))))
                    .as(Config.class);
        }

        default Config withoutCalc() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWatermarkAssigner.class)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(
                                                                            StreamPhysicalChangelogNormalize
                                                                                    .class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalExchange
                                                                                                            .class)
                                                                                            .anyInputs())))
                    .as(Config.class);
        }
    }
}
