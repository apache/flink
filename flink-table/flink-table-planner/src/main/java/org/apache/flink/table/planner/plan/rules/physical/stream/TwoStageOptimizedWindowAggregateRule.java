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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.stream.IntStream;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy;

/**
 * Rule that matches {@link StreamPhysicalWindowAggregate} on {@link StreamPhysicalExchange} with
 * following condition:
 *
 * <ul>
 *   <li>the applied windowing is not on processing-time, because processing-time should be
 *       materialized in a single node.
 *   <li>two-phase aggregation is enabled in given TableConfig.
 *   <li>all aggregate functions support merge() method.
 *   <li>the input of exchange does not satisfy the shuffle distribution
 * </ul>
 *
 * <p>This optimization is as known as local-global optimization for reducing data-shuffling. It
 * splits window aggregation into two-stage window aggregation, i.e. local-aggregation and
 * global-aggregation. The local-aggregation produces a partial result for each group and window
 * slice before shuffle in stage 1, and then the partially aggregated results are shuffled by group
 * key to global-aggregation which produces the final result in stage 2.
 */
public class TwoStageOptimizedWindowAggregateRule extends RelOptRule {

    public static final TwoStageOptimizedWindowAggregateRule INSTANCE =
            new TwoStageOptimizedWindowAggregateRule();

    private TwoStageOptimizedWindowAggregateRule() {
        super(
                operand(
                        StreamPhysicalWindowAggregate.class,
                        operand(StreamPhysicalExchange.class, operand(RelNode.class, any()))),
                "TwoStageOptimizedWindowAggregateRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final StreamPhysicalWindowAggregate windowAgg = call.rel(0);
        final RelNode realInput = call.rel(2);
        final TableConfig tableConfig = unwrapContext(call.getPlanner()).getTableConfig();
        final WindowingStrategy windowing = windowAgg.windowing();

        // the two-phase optimization must be enabled
        if (getAggPhaseStrategy(tableConfig) == AggregatePhaseStrategy.ONE_PHASE) {
            return false;
        }

        // processing time window doesn't support two-phase,
        // otherwise the processing-time can't be materialized in a single node
        if (!windowing.isRowtime()) {
            return false;
        }

        // all aggregate function should support merge() method
        if (!AggregateUtil.doAllSupportPartialMerge(windowAgg.aggInfoList().aggInfos())) {
            return false;
        }

        return !isInputSatisfyRequiredDistribution(realInput, windowAgg.grouping());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalWindowAggregate windowAgg = call.rel(0);
        final RelNode realInput = call.rel(2);
        final WindowingStrategy windowing = windowAgg.windowing();

        RelTraitSet localTraitSet =
                realInput
                        .getTraitSet()
                        .plus(ModifyKindSetTrait.INSERT_ONLY())
                        .plus(UpdateKindTrait.NONE());
        StreamPhysicalLocalWindowAggregate localAgg =
                new StreamPhysicalLocalWindowAggregate(
                        windowAgg.getCluster(),
                        localTraitSet,
                        realInput,
                        windowAgg.grouping(),
                        windowAgg.aggCalls(),
                        windowing);

        // grouping keys is forwarded by local agg, use indices instead of groupings
        int[] globalGrouping = IntStream.range(0, windowAgg.grouping().length).toArray();
        FlinkRelDistribution globalDistribution = createDistribution(globalGrouping);
        // create exchange if needed
        RelNode newInput =
                FlinkExpandConversionRule.satisfyDistribution(
                        FlinkConventions.STREAM_PHYSICAL(), localAgg, globalDistribution);
        RelTraitSet globalAggProvidedTraitSet = windowAgg.getTraitSet();

        // we put sliceEnd/windowEnd at the end of local output fields
        int endIndex = localAgg.getRowType().getFieldCount() - 1;
        final WindowingStrategy globalWindowing;
        if (windowing instanceof TimeAttributeWindowingStrategy) {
            globalWindowing =
                    new SliceAttachedWindowingStrategy(
                            windowing.getWindow(), windowing.getTimeAttributeType(), endIndex);
        } else {
            globalWindowing =
                    new WindowAttachedWindowingStrategy(
                            windowing.getWindow(), windowing.getTimeAttributeType(), endIndex);
        }

        StreamPhysicalGlobalWindowAggregate globalAgg =
                new StreamPhysicalGlobalWindowAggregate(
                        windowAgg.getCluster(),
                        globalAggProvidedTraitSet,
                        newInput,
                        realInput.getRowType(),
                        globalGrouping,
                        windowAgg.aggCalls(),
                        globalWindowing,
                        windowAgg.namedWindowProperties());

        call.transformTo(globalAgg);
    }

    // ------------------------------------------------------------------------------------------

    private boolean isInputSatisfyRequiredDistribution(RelNode input, int[] keys) {
        FlinkRelDistribution requiredDistribution = createDistribution(keys);
        FlinkRelDistribution inputDistribution =
                input.getTraitSet().getTrait(FlinkRelDistributionTraitDef.INSTANCE());
        return inputDistribution.satisfies(requiredDistribution);
    }

    private FlinkRelDistribution createDistribution(int[] keys) {
        if (keys.length > 0) {
            return FlinkRelDistribution.hash(keys, true);
        } else {
            return FlinkRelDistribution.SINGLETON();
        }
    }
}
