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
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.PartialFinalType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * Rule that matches final {@link StreamPhysicalGlobalGroupAggregate} on {@link
 * StreamPhysicalExchange} on final {@link StreamPhysicalLocalGroupAggregate} on partial {@link
 * StreamPhysicalGlobalGroupAggregate}, and combines the final {@link
 * StreamPhysicalLocalGroupAggregate} and the partial {@link StreamPhysicalGlobalGroupAggregate}
 * into a {@link StreamPhysicalIncrementalGroupAggregate}.
 */
@Value.Enclosing
public class IncrementalAggregateRule
        extends RelRule<IncrementalAggregateRule.IncrementalAggregateRuleConfig> {

    public static final IncrementalAggregateRule INSTANCE =
            IncrementalAggregateRule.IncrementalAggregateRuleConfig.DEFAULT.toRule();

    protected IncrementalAggregateRule(
            IncrementalAggregateRule.IncrementalAggregateRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        StreamPhysicalGlobalGroupAggregate finalGlobalAgg = call.rel(0);
        StreamPhysicalLocalGroupAggregate finalLocalAgg = call.rel(2);
        StreamPhysicalGlobalGroupAggregate partialGlobalAgg = call.rel(3);

        TableConfig tableConfig = unwrapTableConfig(call);

        // whether incremental aggregate is enabled
        boolean incrementalAggEnabled =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED);

        return partialGlobalAgg.partialFinalType() == PartialFinalType.PARTIAL
                && finalLocalAgg.partialFinalType() == PartialFinalType.FINAL
                && finalGlobalAgg.partialFinalType() == PartialFinalType.FINAL
                && incrementalAggEnabled;
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        StreamPhysicalGlobalGroupAggregate finalGlobalAgg = relOptRuleCall.rel(0);
        StreamPhysicalExchange exchange = relOptRuleCall.rel(1);
        StreamPhysicalLocalGroupAggregate finalLocalAgg = relOptRuleCall.rel(2);
        StreamPhysicalGlobalGroupAggregate partialGlobalAgg = relOptRuleCall.rel(3);
        RelDataType partialLocalAggInputRowType = partialGlobalAgg.localAggInputRowType();

        AggregateCall[] partialOriginalAggCalls =
                JavaScalaConversionUtil.toJava(partialGlobalAgg.aggCalls())
                        .toArray(new AggregateCall[0]);

        AggregateCall[] partialRealAggCalls =
                partialGlobalAgg.localAggInfoList().getActualAggregateCalls();

        AggregateCall[] finalRealAggCalls =
                finalGlobalAgg.globalAggInfoList().getActualAggregateCalls();

        StreamPhysicalIncrementalGroupAggregate incrAgg =
                new StreamPhysicalIncrementalGroupAggregate(
                        partialGlobalAgg.getCluster(),
                        finalLocalAgg.getTraitSet(), // extends final local agg traits (ACC trait)
                        partialGlobalAgg.getInput(),
                        partialGlobalAgg.grouping(),
                        partialRealAggCalls,
                        finalLocalAgg.grouping(),
                        finalRealAggCalls,
                        partialOriginalAggCalls,
                        partialGlobalAgg.aggCallNeedRetractions(),
                        partialGlobalAgg.needRetraction(),
                        partialLocalAggInputRowType,
                        partialGlobalAgg.getRowType(),
                        partialGlobalAgg.hints());
        RelDataType incrAggOutputRowType = incrAgg.getRowType();

        StreamPhysicalExchange newExchange =
                exchange.copy(exchange.getTraitSet(), incrAgg, exchange.distribution);

        boolean partialAggCountStarInserted =
                partialGlobalAgg.globalAggInfoList().countStarInserted();

        RelNode globalAgg;
        if (partialAggCountStarInserted) {
            RelDataType globalAggInputAccType = finalLocalAgg.getRowType();
            Preconditions.checkState(
                    RelOptUtil.areRowTypesEqual(
                            incrAggOutputRowType, globalAggInputAccType, false));

            globalAgg =
                    finalGlobalAgg.copy(
                            finalGlobalAgg.getTraitSet(), Collections.singletonList(newExchange));
        } else {
            // adapt the needRetract of final global agg to be same as that of partial agg
            AggregateInfoList localAggInfoList =
                    AggregateUtil.transformToStreamAggregateInfoList(
                            unwrapTypeFactory(finalGlobalAgg),
                            // the final agg input is partial agg
                            FlinkTypeFactory.toLogicalRowType(partialGlobalAgg.getRowType()),
                            JavaScalaConversionUtil.toScala(Arrays.asList(finalRealAggCalls)),
                            // use partial global agg's aggCallNeedRetractions
                            partialGlobalAgg.aggCallNeedRetractions(),
                            partialGlobalAgg.needRetraction(),
                            partialGlobalAgg.globalAggInfoList().indexOfCountStar(),
                            // the local agg is not works on state
                            false,
                            true);

            // check whether the global agg required input row type equals the incr agg output row
            // type
            RelDataType globalAggInputAccType =
                    AggregateUtil.inferLocalAggRowType(
                            localAggInfoList,
                            incrAgg.getRowType(),
                            finalGlobalAgg.grouping(),
                            (FlinkTypeFactory) finalGlobalAgg.getCluster().getTypeFactory());

            Preconditions.checkState(
                    RelOptUtil.areRowTypesEqual(
                            incrAggOutputRowType, globalAggInputAccType, false));

            globalAgg =
                    new StreamPhysicalGlobalGroupAggregate(
                            finalGlobalAgg.getCluster(),
                            finalGlobalAgg.getTraitSet(),
                            newExchange,
                            finalGlobalAgg.getRowType(),
                            finalGlobalAgg.grouping(),
                            JavaScalaConversionUtil.toScala(Arrays.asList(finalRealAggCalls)),
                            partialGlobalAgg.aggCallNeedRetractions(),
                            finalGlobalAgg.localAggInputRowType(),
                            partialGlobalAgg.needRetraction(),
                            finalGlobalAgg.partialFinalType(),
                            partialGlobalAgg.globalAggInfoList().indexOfCountStar(),
                            finalGlobalAgg.hints());
        }

        relOptRuleCall.transformTo(globalAgg);
    }

    /** Configuration for {@link IncrementalAggregateRule}. */
    @Value.Immutable(singleton = false)
    public interface IncrementalAggregateRuleConfig extends RelRule.Config {
        IncrementalAggregateRule.IncrementalAggregateRuleConfig DEFAULT =
                ImmutableIncrementalAggregateRule.IncrementalAggregateRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(StreamPhysicalGlobalGroupAggregate.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                StreamPhysicalExchange
                                                                                        .class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        StreamPhysicalLocalGroupAggregate
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        b3 ->
                                                                                                                b3.operand(
                                                                                                                                StreamPhysicalGlobalGroupAggregate
                                                                                                                                        .class)
                                                                                                                        .anyInputs()))))
                        .withDescription("IncrementalAggregateRule")
                        .as(IncrementalAggregateRule.IncrementalAggregateRuleConfig.class);

        @Override
        default IncrementalAggregateRule toRule() {
            return new IncrementalAggregateRule(this);
        }
    }
}
