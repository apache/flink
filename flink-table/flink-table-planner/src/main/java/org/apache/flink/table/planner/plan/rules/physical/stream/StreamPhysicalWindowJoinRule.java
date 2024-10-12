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

import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.table.planner.plan.utils.WindowJoinUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import scala.Tuple2;
import scala.Tuple7;

/** Rule to convert a {@link FlinkLogicalJoin} into a {@link StreamPhysicalWindowJoin}. */
@Value.Enclosing
public class StreamPhysicalWindowJoinRule
        extends RelRule<StreamPhysicalWindowJoinRule.StreamPhysicalWindowJoinRuleConfig> {

    public static final StreamPhysicalWindowJoinRule INSTANCE =
            StreamPhysicalWindowJoinRuleConfig.DEFAULT.toRule();

    protected StreamPhysicalWindowJoinRule(StreamPhysicalWindowJoinRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        return WindowJoinUtil.satisfyWindowJoin(join);
    }

    private RelTraitSet toHashTraitByColumns(int[] columns, RelTraitSet inputTraitSet) {
        FlinkRelDistribution distribution =
                columns.length == 0
                        ? FlinkRelDistribution.SINGLETON()
                        : FlinkRelDistribution.hash(columns, true);

        return inputTraitSet.replace(FlinkConventions.STREAM_PHYSICAL()).replace(distribution);
    }

    private RelNode convertInput(RelNode input, int[] columns) {
        RelTraitSet requiredTraitSet = toHashTraitByColumns(columns, input.getTraitSet());
        return RelOptRule.convert(input, requiredTraitSet);
    }

    public void onMatch(RelOptRuleCall call) {

        FlinkLogicalJoin join = call.rel(0);
        Tuple7<int[], int[], int[], int[], int[], int[], RexNode> tuple7 =
                WindowJoinUtil.excludeWindowStartEqualityAndEndEqualityFromWindowJoinCondition(
                        join);
        int[] windowStartEqualityLeftKeys = tuple7._1();
        int[] windowEndEqualityLeftKeys = tuple7._2();
        int[] windowStartEqualityRightKeys = tuple7._3();
        int[] windowEndEqualityRightKeys = tuple7._4();
        int[] remainLeftKeys = tuple7._5();
        int[] remainRightKeys = tuple7._6();
        RexNode remainCondition = tuple7._7();
        RelTraitSet providedTraitSet =
                join.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        FlinkLogicalRel left = call.rel(1);
        FlinkLogicalRel right = call.rel(2);
        RelNode newLeft = convertInput(left, remainLeftKeys);
        RelNode newRight = convertInput(right, remainRightKeys);

        Tuple2<RelWindowProperties, RelWindowProperties> tuple2 =
                WindowJoinUtil.getChildWindowProperties(join);
        RelWindowProperties leftWindowProperties = tuple2._1();
        RelWindowProperties rightWindowProperties = tuple2._2();
        // It's safe to directly get first element from windowStartEqualityLeftKeys because window
        // start equality is required in join condition for window join.
        WindowAttachedWindowingStrategy leftWindowing =
                new WindowAttachedWindowingStrategy(
                        leftWindowProperties.getWindowSpec(),
                        leftWindowProperties.getTimeAttributeType(),
                        windowStartEqualityLeftKeys[0],
                        windowEndEqualityLeftKeys[0]);
        WindowAttachedWindowingStrategy rightWindowing =
                new WindowAttachedWindowingStrategy(
                        rightWindowProperties.getWindowSpec(),
                        rightWindowProperties.getTimeAttributeType(),
                        windowStartEqualityRightKeys[0],
                        windowEndEqualityRightKeys[0]);

        StreamPhysicalWindowJoin newWindowJoin =
                new StreamPhysicalWindowJoin(
                        join.getCluster(),
                        providedTraitSet,
                        newLeft,
                        newRight,
                        join.getJoinType(),
                        remainCondition,
                        leftWindowing,
                        rightWindowing);
        call.transformTo(newWindowJoin);
    }

    /** Configuration for {@link StreamPhysicalWindowJoinRule}. */
    @Value.Immutable(singleton = false)
    public interface StreamPhysicalWindowJoinRuleConfig extends RelRule.Config {
        StreamPhysicalWindowJoinRule.StreamPhysicalWindowJoinRuleConfig DEFAULT =
                ImmutableStreamPhysicalWindowJoinRule.StreamPhysicalWindowJoinRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(FlinkLogicalRel.class)
                                                                        .anyInputs()))
                        .withDescription("StreamPhysicalWindowJoinRule")
                        .as(StreamPhysicalWindowJoinRuleConfig.class);

        @Override
        default StreamPhysicalWindowJoinRule toRule() {
            return new StreamPhysicalWindowJoinRule(this);
        }
    }
}
