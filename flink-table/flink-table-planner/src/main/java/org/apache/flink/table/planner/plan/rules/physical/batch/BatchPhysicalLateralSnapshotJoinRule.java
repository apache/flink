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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLateralSnapshotJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin;
import org.apache.flink.table.planner.plan.rules.logical.LogicalJoinToLateralSnapshotJoinRule;
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalLateralSnapshotJoinRule;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Converts a {@link FlinkLogicalLateralSnapshotJoin} (created by {@link
 * LogicalJoinToLateralSnapshotJoinRule}) into a regular batch join for batch execution: a {@link
 * BatchPhysicalHashJoin} when the join has an equi-key, or a {@link BatchPhysicalNestedLoopJoin}
 * when it does not.
 *
 * <p>In batch all input is bounded and append-only (batch rejects or in the future materializes
 * non-insert-only sources up front), so the processing-time {@code LATERAL SNAPSHOT} join
 * degenerates to a regular join of the probe side against the (final) build side; the
 * SNAPSHOT-specific arguments are irrelevant and dropped. This rule mirrors the streaming {@link
 * StreamPhysicalLateralSnapshotJoinRule}, which converts the same logical node to a dedicated
 * stream operator.
 *
 * <p>The SNAPSHOT input is the build (right) side of the LATERAL join and is the dimension-like
 * side, expected to be smaller than the probe (left) side. With an equi-key the join is emitted as
 * a shuffle hash join that builds on the right input; without an equi-key (e.g. a non-equi
 * condition, or an equi-key that a local filter folded to a constant) it is emitted as a broadcast
 * nested-loop join that broadcasts the right input, keeping the probe side parallel.
 */
public class BatchPhysicalLateralSnapshotJoinRule extends ConverterRule {

    public static final BatchPhysicalLateralSnapshotJoinRule INSTANCE =
            new BatchPhysicalLateralSnapshotJoinRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalLateralSnapshotJoin.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.BATCH_PHYSICAL(),
                            "BatchPhysicalLateralSnapshotJoinRule"));

    private BatchPhysicalLateralSnapshotJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final FlinkLogicalLateralSnapshotJoin join = (FlinkLogicalLateralSnapshotJoin) rel;
        final RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());

        final JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            // Without an equi-key a hash join is not applicable (the join is non-equi, or its
            // equi-key was constant-folded away by a local filter on the join key). Mirror how
            // regular batch joins handle this and emit a broadcast nested-loop join that broadcasts
            // the (smaller, dimension-like) SNAPSHOT build side to the probe side, which keeps the
            // probe side parallel.
            final RelNode newLeft =
                    RelOptRule.convert(
                            join.getLeft(),
                            join.getLeft()
                                    .getTraitSet()
                                    .replace(FlinkConventions.BATCH_PHYSICAL()));
            final RelNode newRight =
                    RelOptRule.convert(
                            join.getRight(),
                            join.getRight()
                                    .getTraitSet()
                                    .replace(FlinkConventions.BATCH_PHYSICAL())
                                    .replace(FlinkRelDistribution.BROADCAST_DISTRIBUTED()));
            return new BatchPhysicalNestedLoopJoin(
                    join.getCluster(),
                    providedTraitSet,
                    newLeft,
                    newRight,
                    join.getCondition(),
                    join.getJoinType(),
                    // leftIsBuild = false: build the right (SNAPSHOT) side.
                    false,
                    // singleRowJoin = false: neither side is known to produce a single row.
                    false);
        }

        // Both inputs are hash-partitioned on their join keys (shuffle hash join).
        final RelNode newLeft = convertInput(join.getLeft(), joinInfo.leftKeys);
        final RelNode newRight = convertInput(join.getRight(), joinInfo.rightKeys);

        return new BatchPhysicalHashJoin(
                join.getCluster(),
                providedTraitSet,
                newLeft,
                newRight,
                join.getCondition(),
                join.getJoinType(),
                // leftIsBuild = false: build the right (SNAPSHOT) side, the smaller dimension side.
                false,
                // isBroadcast = false: shuffle hash join.
                false,
                // tryDistinctBuildRow = false: only relevant for semi/anti joins.
                false,
                // withJobStrategyHint = false: not driven by a user join hint.
                false);
    }

    /**
     * Converts a join input to the batch-physical convention and requires it to be hash-partitioned
     * on the given join {@code keys}.
     */
    private static RelNode convertInput(RelNode input, ImmutableIntList keys) {
        final RelTraitSet requiredTraitSet =
                input.getTraitSet()
                        .replace(FlinkConventions.BATCH_PHYSICAL())
                        .replace(FlinkRelDistribution.hash(keys.toIntArray(), true));
        return RelOptRule.convert(input, requiredTraitSet);
    }
}
