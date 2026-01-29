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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortMergeJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.util.ImmutableIntList;
import org.immutables.value.Value;

/**
 * Rule that converts {@link FlinkLogicalJoin} to {@link BatchPhysicalSortMergeJoin} if there exists
 * at least one equal-join condition and SortMergeJoin is enabled.
 */
@Value.Enclosing
public class BatchPhysicalSortMergeJoinRule
        extends RelRule<BatchPhysicalSortMergeJoinRule.BatchPhysicalSortMergeJoinRuleConfig>
        implements BatchPhysicalJoinRuleBase {

    public static final BatchPhysicalSortMergeJoinRule INSTANCE =
            BatchPhysicalSortMergeJoinRuleConfig.DEFAULT.toRule();

    protected BatchPhysicalSortMergeJoinRule(BatchPhysicalSortMergeJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(join);
        return canUseJoinStrategy(join, tableConfig, JoinStrategy.SHUFFLE_MERGE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final JoinInfo joinInfo = join.analyzeCondition();

        final RelNode left = join.getLeft();
        final RelNode right = join.getRight();

        final RelTraitSet leftRequiredTrait =
                getTraitSetByShuffleKeys(call, joinInfo.leftKeys, true);
        final RelTraitSet rightRequiredTrait =
                getTraitSetByShuffleKeys(call, joinInfo.rightKeys, true);

        final RelNode newLeft = RelRule.convert(left, leftRequiredTrait);
        final RelNode newRight = RelRule.convert(right, rightRequiredTrait);

        final RelTraitSet providedTraitSet =
                call.getPlanner().emptyTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        final boolean withJobStrategyHint = JoinUtil.containsJoinStrategyHint(join.getHints());
        // do not try to remove redundant sort for shorter optimization time
        final BatchPhysicalSortMergeJoin newJoin =
                new BatchPhysicalSortMergeJoin(
                        join.getCluster(),
                        providedTraitSet,
                        newLeft,
                        newRight,
                        join.getCondition(),
                        join.getJoinType(),
                        false,
                        false,
                        withJobStrategyHint);
        call.transformTo(newJoin);
    }

    private static RelTraitSet getTraitSetByShuffleKeys(
            RelOptRuleCall call, ImmutableIntList shuffleKeys, boolean requireStrict) {
        return call.getPlanner()
                .emptyTraitSet()
                .replace(FlinkConventions.BATCH_PHYSICAL())
                .replace(FlinkRelDistribution.hash(shuffleKeys, requireStrict));
    }

    /** Configuration for {@link BatchPhysicalSortMergeJoinRule}. */
    @Value.Immutable(singleton = false)
    public interface BatchPhysicalSortMergeJoinRuleConfig extends RelRule.Config {

        BatchPhysicalSortMergeJoinRuleConfig DEFAULT =
                ImmutableBatchPhysicalSortMergeJoinRule.BatchPhysicalSortMergeJoinRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalJoin.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(RelNode.class)
                                                                        .anyInputs()))
                        .withDescription("BatchPhysicalSortMergeJoinRule")
                        .as(BatchPhysicalSortMergeJoinRuleConfig.class);

        @Override
        default BatchPhysicalSortMergeJoinRule toRule() {
            return new BatchPhysicalSortMergeJoinRule(this);
        }
    }
}
