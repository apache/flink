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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLateralSnapshotJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLateralSnapshotJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Converts a {@link FlinkLogicalLateralSnapshotJoin} (created by {@link
 * org.apache.flink.table.planner.plan.rules.logical.LogicalJoinToLateralSnapshotJoinRule}) into a
 * {@link StreamPhysicalLateralSnapshotJoin}. The SNAPSHOT arguments are carried on the logical
 * node, so the conversion is a straight pass-through.
 */
public class StreamPhysicalLateralSnapshotJoinRule extends ConverterRule {

    public static final StreamPhysicalLateralSnapshotJoinRule INSTANCE =
            new StreamPhysicalLateralSnapshotJoinRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalLateralSnapshotJoin.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalLateralSnapshotJoinRule"));

    private StreamPhysicalLateralSnapshotJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final FlinkLogicalLateralSnapshotJoin join = (FlinkLogicalLateralSnapshotJoin) rel;
        final RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        // Both inputs are hash-partitioned on their join keys.
        final JoinInfo joinInfo = join.analyzeCondition();
        final RelNode newLeft = convertInput(join.getLeft(), joinInfo.leftKeys);
        final RelNode newRight = convertInput(join.getRight(), joinInfo.rightKeys);

        return new StreamPhysicalLateralSnapshotJoin(
                join.getCluster(),
                providedTraitSet,
                newLeft,
                newRight,
                join.getCondition(),
                join.getJoinType(),
                join.getLoadCompletedCondition(),
                join.getLoadCompletedTime(),
                join.getLoadCompletedIdleTimeoutMs(),
                join.getStateTtlMs());
    }

    /**
     * Converts a join input to the stream-physical convention and requires it to be
     * hash-partitioned on the given join {@code keys} (or a singleton distribution if there are
     * none).
     */
    private static RelNode convertInput(RelNode input, ImmutableIntList keys) {
        final FlinkRelDistribution distribution =
                keys.isEmpty()
                        ? FlinkRelDistribution.SINGLETON()
                        : FlinkRelDistribution.hash(keys.toIntArray(), true);
        final RelTraitSet requiredTraitSet =
                input.getTraitSet()
                        .replace(FlinkConventions.STREAM_PHYSICAL())
                        .replace(distribution);
        return RelOptRule.convert(input, requiredTraitSet);
    }
}
