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

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalJoin;
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.function.Function;

/**
 * Rule that matches a temporal join node and converts it to {@link StreamPhysicalTemporalJoin}, the
 * temporal join node is a {@link FlinkLogicalJoin} which contains {@link TEMPORAL_JOIN_CONDITION}.
 */
@Value.Enclosing
public class StreamPhysicalTemporalJoinRule
        extends StreamPhysicalJoinRuleBase<
                StreamPhysicalTemporalJoinRule.StreamPhysicalTemporalJoinRuleConfig> {
    public static final StreamPhysicalTemporalJoinRule INSTANCE =
            StreamPhysicalTemporalJoinRuleConfig.DEFAULT.toRule();

    public StreamPhysicalTemporalJoinRule(StreamPhysicalTemporalJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        if (!TemporalJoinUtil.satisfyTemporalJoin(join)) {
            return false;
        }

        // validate the join
        // INITIAL_TEMPORAL_JOIN_CONDITION should not appear in physical phase.
        Preconditions.checkState(
                !TemporalJoinUtil.containsInitialTemporalJoinCondition(join.getCondition()));
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
        final RelNode newRight;
        if (rightInput instanceof FlinkLogicalSnapshot) {
            newRight = ((FlinkLogicalSnapshot) rightInput).getInput();
        } else {
            newRight = rightInput;
        }

        return new StreamPhysicalTemporalJoin(
                join.getCluster(),
                providedTraitSet,
                leftConversion.apply(leftInput),
                rightConversion.apply(newRight),
                join.getCondition(),
                join.getJoinType());
    }

    /** Configuration for {@link StreamPhysicalIntervalJoinRule}. */
    @Value.Immutable
    public interface StreamPhysicalTemporalJoinRuleConfig
            extends StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig {
        StreamPhysicalTemporalJoinRule.StreamPhysicalTemporalJoinRuleConfig DEFAULT =
                ImmutableStreamPhysicalTemporalJoinRule.StreamPhysicalTemporalJoinRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(OPERAND_TRANSFORM)
                        .withDescription("StreamPhysicalTemporalJoinRule")
                        .as(
                                StreamPhysicalTemporalJoinRule.StreamPhysicalTemporalJoinRuleConfig
                                        .class);

        @Override
        default StreamPhysicalTemporalJoinRule toRule() {
            return new StreamPhysicalTemporalJoinRule(this);
        }
    }
}
