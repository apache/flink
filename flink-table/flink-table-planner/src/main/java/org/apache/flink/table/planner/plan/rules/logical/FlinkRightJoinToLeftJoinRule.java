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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to convert RIGHT JOIN to LEFT JOIN since Multi-Way Join operator works only with LEFT joins.
 */
@Value.Enclosing
public class FlinkRightJoinToLeftJoinRule extends RelRule<FlinkRightJoinToLeftJoinRule.Config>
        implements TransformationRule {

    public static final FlinkRightJoinToLeftJoinRule INSTANCE =
            FlinkRightJoinToLeftJoinRule.Config.DEFAULT.toRule();

    /** Creates a FlinkRightJoinToLeftJoinRule. */
    public FlinkRightJoinToLeftJoinRule(FlinkRightJoinToLeftJoinRule.Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join origJoin = call.rel(0);
        return origJoin.getJoinType() == JoinRelType.RIGHT;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join origJoin = call.rel(0);
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        RexNode newCondition = shiftCondition(origJoin, left, right);

        Join leftJoin =
                origJoin.copy(
                        origJoin.getTraitSet(), newCondition, right, left, JoinRelType.LEFT, false);

        RelBuilder relBuilder = call.builder();
        relBuilder.push(leftJoin);

        RelNode project = reorderProjectedFields(left, right, relBuilder);

        call.transformTo(project);
    }

    private RelNode reorderProjectedFields(RelNode left, RelNode right, RelBuilder relBuilder) {
        int nFieldsOnLeft = left.getRowType().getFieldList().size();
        int nFieldsOnRight = right.getRowType().getFieldList().size();
        List<RexNode> reorderedFields = new ArrayList<>();

        for (int i = 0; i < nFieldsOnLeft; i++) {
            reorderedFields.add(relBuilder.field(i + nFieldsOnRight));
        }

        for (int i = 0; i < nFieldsOnRight; i++) {
            reorderedFields.add(relBuilder.field(i));
        }

        return relBuilder.project(reorderedFields).build();
    }

    private RexNode shiftCondition(Join joinRel, RelNode left, RelNode right) {
        RexNode condition = joinRel.getCondition();
        int nFieldsOnLeft = left.getRowType().getFieldList().size();
        int nFieldsOnRight = right.getRowType().getFieldList().size();
        int[] adjustments = new int[nFieldsOnLeft + nFieldsOnRight];
        for (int i = 0; i < nFieldsOnLeft + nFieldsOnRight; i++) {
            adjustments[i] = i < nFieldsOnLeft ? nFieldsOnRight : -nFieldsOnLeft;
        }

        return condition.accept(
                new RelOptUtil.RexInputConverter(
                        joinRel.getCluster().getRexBuilder(),
                        joinRel.getRowType().getFieldList(),
                        adjustments));
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        FlinkRightJoinToLeftJoinRule.Config DEFAULT =
                ImmutableFlinkRightJoinToLeftJoinRule.Config.builder()
                        .build()
                        .as(FlinkRightJoinToLeftJoinRule.Config.class)
                        .withOperandFor(LogicalJoin.class);

        @Override
        default FlinkRightJoinToLeftJoinRule toRule() {
            return new FlinkRightJoinToLeftJoinRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default FlinkRightJoinToLeftJoinRule.Config withOperandFor(
                Class<? extends Join> joinClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(joinClass)
                                            .inputs(
                                                    b1 -> b1.operand(RelNode.class).anyInputs(),
                                                    b2 -> b2.operand(RelNode.class).anyInputs()))
                    .as(FlinkRightJoinToLeftJoinRule.Config.class);
        }
    }
}
