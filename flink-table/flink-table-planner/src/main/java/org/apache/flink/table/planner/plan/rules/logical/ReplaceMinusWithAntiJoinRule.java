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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import java.util.List;

import static org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.generateEqualsCondition;

/**
 * Planner rule that replaces distinct {@link org.apache.calcite.rel.core.Minus} (SQL keyword:
 * EXCEPT) with a distinct {@link org.apache.calcite.rel.core.Aggregate} on an ANTI {@link
 * org.apache.calcite.rel.core.Join}.
 *
 * <p>Only handle the case of input size 2.
 */
@Value.Enclosing
public class ReplaceMinusWithAntiJoinRule
        extends RelRule<ReplaceMinusWithAntiJoinRule.ReplaceMinusWithAntiJoinRuleConfig> {

    public static final ReplaceMinusWithAntiJoinRule INSTANCE =
            ReplaceMinusWithAntiJoinRule.ReplaceMinusWithAntiJoinRuleConfig.DEFAULT.toRule();

    private ReplaceMinusWithAntiJoinRule(ReplaceMinusWithAntiJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Minus minus = call.rel(0);
        return !minus.all && minus.getInputs().size() == 2;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Minus minus = call.rel(0);
        RelNode left = minus.getInput(0);
        RelNode right = minus.getInput(1);

        RelBuilder relBuilder = call.builder();
        List<Integer> keys = Util.range(left.getRowType().getFieldCount());
        List<RexNode> conditions = generateEqualsCondition(relBuilder, left, right, keys);

        relBuilder.push(left);
        relBuilder.push(right);
        relBuilder
                .join(JoinRelType.ANTI, conditions)
                .aggregate(
                        relBuilder.groupKey(keys.stream().mapToInt(Integer::intValue).toArray()));
        RelNode rel = relBuilder.build();
        call.transformTo(rel);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ReplaceMinusWithAntiJoinRuleConfig extends RelRule.Config {
        ReplaceMinusWithAntiJoinRule.ReplaceMinusWithAntiJoinRuleConfig DEFAULT =
                ImmutableReplaceMinusWithAntiJoinRule.ReplaceMinusWithAntiJoinRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Minus.class).anyInputs())
                        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .withDescription("ReplaceMinusWithAntiJoinRule");

        @Override
        default ReplaceMinusWithAntiJoinRule toRule() {
            return new ReplaceMinusWithAntiJoinRule(this);
        }
    }
}
