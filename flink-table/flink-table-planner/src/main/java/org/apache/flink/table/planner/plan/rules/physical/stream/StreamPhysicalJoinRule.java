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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.function.Function;

/**
 * Rule that converts {@link FlinkLogicalJoin} without window bounds in join condition to {@link
 * StreamPhysicalJoin}.
 */
@Value.Enclosing
public class StreamPhysicalJoinRule
        extends StreamPhysicalJoinRuleBase<StreamPhysicalJoinRule.StreamPhysicalJoinRuleConfig> {
    public static final StreamPhysicalJoinRule INSTANCE =
            StreamPhysicalJoinRuleConfig.DEFAULT.toRule();

    public StreamPhysicalJoinRule(StreamPhysicalJoinRule.StreamPhysicalJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalJoin join = call.rel(0);
        final FlinkLogicalRel left = call.rel(1);
        final FlinkLogicalRel right = call.rel(2);

        if (!JoinUtil.satisfyRegularJoin(join, left, right)) {
            return false;
        }

        // validate the join
        if (left instanceof FlinkLogicalSnapshot) {
            throw new TableException(
                    "Temporal table join only support apply FOR SYSTEM_TIME AS OF on the right table.");
        }

        // INITIAL_TEMPORAL_JOIN_CONDITION should not appear in physical phase in case which
        // fallback
        // to regular join
        Preconditions.checkState(
                !TemporalJoinUtil.containsInitialTemporalJoinCondition(join.getCondition()));

        // Time attributes must not be in the output type of a regular join
        boolean timeAttrInOutput =
                join.getRowType().getFieldList().stream()
                        .anyMatch(f -> FlinkTypeFactory.isTimeIndicatorType(f.getType()));
        Preconditions.checkState(!timeAttrInOutput);

        // Join condition must not access time attributes
        boolean remainingPredsAccessTime =
                JoinUtil.accessesTimeAttribute(
                        join.getCondition(), JoinUtil.combineJoinInputsRowType(join));
        Preconditions.checkState(!remainingPredsAccessTime);
        return true;
    }

    @Override
    public FlinkRelNode transform(
            FlinkLogicalJoin join,
            FlinkRelNode leftInput,
            Function<RelNode, RelNode> leftConversion,
            FlinkRelNode rightInput,
            Function<RelNode, RelNode> rightConversion,
            RelTraitSet providedTraitSet) {
        return new StreamPhysicalJoin(
                join.getCluster(),
                providedTraitSet,
                leftConversion.apply(leftInput),
                rightConversion.apply(rightInput),
                join.getCondition(),
                join.getJoinType(),
                join.getHints());
    }

    /** Configuration for {@link StreamPhysicalIntervalJoinRule}. */
    @Value.Immutable
    public interface StreamPhysicalJoinRuleConfig
            extends StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig {
        StreamPhysicalJoinRule.StreamPhysicalJoinRuleConfig DEFAULT =
                ImmutableStreamPhysicalJoinRule.StreamPhysicalJoinRuleConfig.builder()
                        .build()
                        .withOperandSupplier(OPERAND_TRANSFORM)
                        .withDescription("StreamPhysicalJoinRule")
                        .as(StreamPhysicalJoinRule.StreamPhysicalJoinRuleConfig.class);

        @Override
        default StreamPhysicalJoinRule toRule() {
            return new StreamPhysicalJoinRule(this);
        }
    }
}
