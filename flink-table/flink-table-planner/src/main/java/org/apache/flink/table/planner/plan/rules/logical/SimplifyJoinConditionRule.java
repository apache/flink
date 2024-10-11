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

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

/**
 * Planner rule that apply various simplifying transformations on join condition. e.g. reduce same
 * expressions: a=b AND b=a -> a=b, simplify boolean expressions: x = 1 AND FALSE -> FALSE, simplify
 * cast expressions: CAST('123' as integer) -> 123
 */
@Value.Enclosing
public class SimplifyJoinConditionRule
        extends RelRule<SimplifyJoinConditionRule.SimplifyJoinConditionRuleConfig> {
    public static final SimplifyJoinConditionRule INSTANCE =
            SimplifyJoinConditionRule.SimplifyJoinConditionRuleConfig.DEFAULT.toRule();

    protected SimplifyJoinConditionRule(SimplifyJoinConditionRuleConfig config) {
        super(config);
    }

    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RexNode condition = join.getCondition();

        if (join.getCondition().isAlwaysTrue()) {
            return;
        }

        RexNode simpleCondExp =
                FlinkRexUtil.simplify(
                        join.getCluster().getRexBuilder(),
                        condition,
                        join.getCluster().getPlanner().getExecutor());
        RexNode newCondExp = RexUtil.pullFactors(join.getCluster().getRexBuilder(), simpleCondExp);

        if (newCondExp.equals(condition)) {
            return;
        }

        LogicalJoin newJoin =
                join.copy(
                        join.getTraitSet(),
                        newCondExp,
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone());
        call.transformTo(newJoin);
        call.getPlanner().prune(join);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface SimplifyJoinConditionRuleConfig extends RelRule.Config {
        SimplifyJoinConditionRule.SimplifyJoinConditionRuleConfig DEFAULT =
                ImmutableSimplifyJoinConditionRule.SimplifyJoinConditionRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(LogicalJoin.class).anyInputs())
                        .withDescription("SimplifyJoinConditionRule");

        @Override
        default SimplifyJoinConditionRule toRule() {
            return new SimplifyJoinConditionRule(this);
        }
    }
}
