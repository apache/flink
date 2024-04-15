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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import java.util.List;

import static org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.generateEqualsCondition;

/**
 * Planner rule that replaces distinct {@link Intersect} with a distinct {@link Aggregate} on a SEMI
 * {@link Join}.
 *
 * <p>Only handle the case of input size 2.
 */
@Value.Enclosing
public class ReplaceIntersectWithSemiJoinRule
        extends RelRule<ReplaceIntersectWithSemiJoinRule.ReplaceIntersectWithSemiJoinRuleConfig> {

    public static final ReplaceIntersectWithSemiJoinRule INSTANCE =
            ReplaceIntersectWithSemiJoinRule.ReplaceIntersectWithSemiJoinRuleConfig.DEFAULT
                    .toRule();

    private ReplaceIntersectWithSemiJoinRule(ReplaceIntersectWithSemiJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Intersect intersect = call.rel(0);
        return !intersect.all && intersect.getInputs().size() == 2;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Intersect intersect = call.rel(0);
        RelNode left = intersect.getInput(0);
        RelNode right = intersect.getInput(1);

        RelBuilder relBuilder = call.builder();
        List<Integer> keys = Util.range(left.getRowType().getFieldCount());
        List<RexNode> conditions = generateEqualsCondition(relBuilder, left, right, keys);

        relBuilder.push(left);
        relBuilder.push(right);
        relBuilder
                .join(JoinRelType.SEMI, conditions)
                .aggregate(
                        relBuilder.groupKey(keys.stream().mapToInt(Integer::intValue).toArray()));
        RelNode rel = relBuilder.build();
        call.transformTo(rel);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ReplaceIntersectWithSemiJoinRuleConfig extends RelRule.Config {
        ReplaceIntersectWithSemiJoinRule.ReplaceIntersectWithSemiJoinRuleConfig DEFAULT =
                ImmutableReplaceIntersectWithSemiJoinRule.ReplaceIntersectWithSemiJoinRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Intersect.class).anyInputs())
                        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .withDescription("ReplaceIntersectWithSemiJoinRule");

        @Override
        default ReplaceIntersectWithSemiJoinRule toRule() {
            return new ReplaceIntersectWithSemiJoinRule(this);
        }
    }
}
