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
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.RelModifiedMonotonicity;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Option;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy;

/**
 * Rule that matches {@link StreamPhysicalGroupAggregate} on {@link StreamPhysicalExchange} with the
 * following condition: 1. mini-batch is enabled in given TableConfig, 2. two-phase aggregation is
 * enabled in given TableConfig, 3. all aggregate functions are mergeable, 4. the input of exchange
 * does not satisfy the shuffle distribution,
 *
 * <p>and converts them to
 *
 * <pre>
 *   StreamPhysicalGlobalGroupAggregate
 *   +- StreamPhysicalExchange
 *      +- StreamPhysicalLocalGroupAggregate
 *         +- input of exchange
 * </pre>
 */
@Value.Enclosing
public class TwoStageOptimizedAggregateRule
        extends RelRule<TwoStageOptimizedAggregateRule.TwoStageOptimizedAggregateRuleConfig> {

    public static final TwoStageOptimizedAggregateRule INSTANCE =
            TwoStageOptimizedAggregateRule.TwoStageOptimizedAggregateRuleConfig.DEFAULT.toRule();

    private TwoStageOptimizedAggregateRule(TwoStageOptimizedAggregateRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        boolean isMiniBatchEnabled =
                tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        boolean isTwoPhaseEnabled =
                getAggPhaseStrategy(tableConfig) != AggregatePhaseStrategy.ONE_PHASE;

        return isMiniBatchEnabled && isTwoPhaseEnabled && matchesTwoStage(call.rel(0), call.rel(2));
    }

    public static boolean matchesTwoStage(StreamPhysicalGroupAggregate agg, RelNode realInput) {
        boolean needRetraction = !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) realInput);
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(agg.getCluster().getMetadataQuery());
        RelModifiedMonotonicity monotonicity = fmq.getRelModifiedMonotonicity(agg);
        boolean[] needRetractionArray =
                AggregateUtil.deriveAggCallNeedRetractions(
                        agg.grouping().length, agg.aggCalls(), needRetraction, monotonicity);

        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        unwrapTypeFactory(agg),
                        FlinkTypeFactory.toLogicalRowType(agg.getInput().getRowType()),
                        agg.aggCalls(),
                        needRetractionArray,
                        needRetraction,
                        true,
                        true);

        return AggregateUtil.doAllSupportPartialMerge(aggInfoList.aggInfos())
                && !isInputSatisfyRequiredDistribution(realInput, agg.grouping());
    }

    private static boolean isInputSatisfyRequiredDistribution(RelNode input, int[] keys) {
        RelDistribution requiredDistribution = createDistribution(keys);
        RelTraitSet traitSet = input.getTraitSet();
        RelDistribution inputDistribution =
                traitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE());
        return inputDistribution.satisfies(requiredDistribution);
    }

    private static FlinkRelDistribution createDistribution(int[] keys) {
        if (keys.length > 0) {
            List<Integer> fields = IntStream.of(keys).boxed().collect(Collectors.toList());
            return FlinkRelDistribution.hash(fields, true);
        } else {
            return FlinkRelDistribution.SINGLETON();
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalGroupAggregate originalAgg = call.rel(0);
        RelNode realInput = call.rel(2);
        boolean needRetraction = !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) realInput);
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery());
        RelModifiedMonotonicity monotonicity = fmq.getRelModifiedMonotonicity(originalAgg);
        boolean[] aggCallNeedRetractions =
                AggregateUtil.deriveAggCallNeedRetractions(
                        originalAgg.grouping().length,
                        originalAgg.aggCalls(),
                        needRetraction,
                        monotonicity);

        // Set the traits for the local aggregation
        RelTraitSet localAggTraitSet =
                realInput
                        .getTraitSet()
                        .plus(ModifyKindSetTrait.INSERT_ONLY())
                        .plus(UpdateKindTrait.NONE());
        StreamPhysicalLocalGroupAggregate localHashAgg =
                new StreamPhysicalLocalGroupAggregate(
                        originalAgg.getCluster(),
                        localAggTraitSet,
                        realInput,
                        originalAgg.grouping(),
                        originalAgg.aggCalls(),
                        aggCallNeedRetractions,
                        needRetraction,
                        originalAgg.partialFinalType());

        // Global grouping keys are forwarded by local agg, use identity keys
        int[] globalGrouping =
                java.util.stream.IntStream.range(0, originalAgg.grouping().length).toArray();
        FlinkRelDistribution globalDistribution = createDistribution(globalGrouping);
        // create exchange if needed
        RelNode newInput =
                FlinkExpandConversionRule.satisfyDistribution(
                        FlinkConventions.STREAM_PHYSICAL(), localHashAgg, globalDistribution);
        RelTraitSet globalAggProvidedTraitSet = originalAgg.getTraitSet();

        StreamPhysicalGlobalGroupAggregate globalAgg =
                new StreamPhysicalGlobalGroupAggregate(
                        originalAgg.getCluster(),
                        globalAggProvidedTraitSet,
                        newInput,
                        originalAgg.getRowType(),
                        globalGrouping,
                        originalAgg.aggCalls(),
                        aggCallNeedRetractions,
                        realInput.getRowType(),
                        needRetraction,
                        originalAgg.partialFinalType(),
                        Option.empty(),
                        originalAgg.hints());

        call.transformTo(globalAgg);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface TwoStageOptimizedAggregateRuleConfig extends RelRule.Config {
        TwoStageOptimizedAggregateRule.TwoStageOptimizedAggregateRuleConfig DEFAULT =
                ImmutableTwoStageOptimizedAggregateRule.TwoStageOptimizedAggregateRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(StreamPhysicalGroupAggregate.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                StreamPhysicalExchange
                                                                                        .class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        RelNode
                                                                                                                .class)
                                                                                                .anyInputs())))
                        .withDescription("TwoStageOptimizedAggregateRule");

        @Override
        default TwoStageOptimizedAggregateRule toRule() {
            return new TwoStageOptimizedAggregateRule(this);
        }
    }
}
