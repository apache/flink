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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.utils.FlinkRelUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.immutables.value.Value;

/**
 * This rule is copied from Calcite's {@link org.apache.calcite.rel.rules.CalcMergeRule}.
 *
 * <p>Modification: - Condition in the merged program will be simplified if it exists. - If the two
 * {@link org.apache.calcite.rel.core.Calc} can merge into one, each non-deterministic {@link
 * org.apache.calcite.rex.RexNode} of bottom {@link org.apache.calcite.rel.core.Calc} should appear
 * at most once in the project list and filter list of top {@link org.apache.calcite.rel.core.Calc}.
 */

/**
 * Planner rule that merges a {@link org.apache.calcite.rel.core.Calc} onto a {@link
 * org.apache.calcite.rel.core.Calc}.
 *
 * <p>The resulting {@link org.apache.calcite.rel.core.Calc} has the same project list as the upper
 * {@link org.apache.calcite.rel.core.Calc}, but expressed in terms of the lower {@link
 * org.apache.calcite.rel.core.Calc}'s inputs.
 */
@Value.Enclosing
public class FlinkCalcMergeRule extends RelRule<FlinkCalcMergeRule.FlinkCalcMergeRuleConfig> {

    public static final FlinkCalcMergeRule INSTANCE = FlinkCalcMergeRuleConfig.DEFAULT.toRule();
    public static final FlinkCalcMergeRule STREAM_PHYSICAL_INSTANCE =
            FlinkCalcMergeRuleConfig.STREAM_PHYSICAL.toRule();

    protected FlinkCalcMergeRule(FlinkCalcMergeRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        Calc topCalc = call.rel(0);
        Calc bottomCalc = call.rel(1);

        // Don't merge a calc which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter.
        RexProgram topProgram = topCalc.getProgram();
        if (RexOver.containsOver(topProgram)) {
            return false;
        }

        return FlinkRelUtil.isMergeable(topCalc, bottomCalc);
    }

    public void onMatch(RelOptRuleCall call) {
        Calc topCalc = call.rel(0);
        Calc bottomCalc = call.rel(1);

        Calc newCalc = FlinkRelUtil.merge(topCalc, bottomCalc);
        if (newCalc.getDigest() == bottomCalc.getDigest()) {
            // newCalc is equivalent to bottomCalc,
            // which means that topCalc
            // must be trivial. Take it out of the game.
            call.getPlanner().prune(topCalc);
        }
        call.transformTo(newCalc);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface FlinkCalcMergeRuleConfig extends RelRule.Config {
        FlinkCalcMergeRule.FlinkCalcMergeRuleConfig DEFAULT =
                ImmutableFlinkCalcMergeRule.FlinkCalcMergeRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(Calc.class)
                                                .inputs(b1 -> b1.operand(Calc.class).anyInputs()))
                        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .withDescription("FlinkCalcMergeRule");

        FlinkCalcMergeRule.FlinkCalcMergeRuleConfig STREAM_PHYSICAL =
                ImmutableFlinkCalcMergeRule.FlinkCalcMergeRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(StreamPhysicalCalc.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(StreamPhysicalCalc.class)
                                                                        .anyInputs()))
                        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .withDescription("FlinkCalcMergeRule");

        @Override
        default FlinkCalcMergeRule toRule() {
            return new FlinkCalcMergeRule(this);
        }
    }
}
