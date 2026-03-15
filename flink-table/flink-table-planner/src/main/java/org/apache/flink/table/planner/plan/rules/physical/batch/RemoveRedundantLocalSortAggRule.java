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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortAggregate;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

/**
 * There maybe exist a subTree like localSortAggregate -> globalSortAggregate, or localSortAggregate
 * -> sort -> globalSortAggregate which the middle shuffle is removed. The rule could remove
 * redundant localSortAggregate node.
 */
@Value.Enclosing
public class RemoveRedundantLocalSortAggRule
        extends RelRule<RemoveRedundantLocalSortAggRule.RemoveRedundantLocalSortAggRuleConfig> {

    public static final RelRule<?> WITHOUT_SORT =
            RemoveRedundantLocalSortAggRuleConfig.WITHOUT_SORT.toRule();

    public static final RelRule<?> WITH_SORT =
            RemoveRedundantLocalSortAggRuleConfig.WITH_SORT.toRule();

    protected RemoveRedundantLocalSortAggRule(RemoveRedundantLocalSortAggRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BatchPhysicalSortAggregate globalAgg = call.rel(0);

        final BatchPhysicalLocalSortAggregate localAgg;
        final RelNode inputOfLocalAgg;

        if (call.rels.length == 3) {
            // WITHOUT_SORT
            localAgg = call.rel(1);
            inputOfLocalAgg = call.rel(2);
        } else {
            // WITH_SORT
            localAgg = call.rel(2);
            inputOfLocalAgg = call.rel(3);
        }

        final BatchPhysicalSortAggregate newGlobalAgg =
                new BatchPhysicalSortAggregate(
                        globalAgg.getCluster(),
                        globalAgg.getTraitSet(),
                        inputOfLocalAgg,
                        globalAgg.getRowType(),
                        inputOfLocalAgg.getRowType(),
                        inputOfLocalAgg.getRowType(),
                        localAgg.grouping(),
                        localAgg.auxGrouping(),
                        // Use the localAgg agg calls because the global agg call filters was
                        // removed,
                        // see BatchPhysicalSortAggRule for details.
                        localAgg.getAggCallToAggFunction(),
                        false);

        call.transformTo(newGlobalAgg);
    }

    /** Configuration for {@link RemoveRedundantLocalSortAggRule}. */
    @Value.Immutable(singleton = false)
    public interface RemoveRedundantLocalSortAggRuleConfig extends RelRule.Config {

        RemoveRedundantLocalSortAggRuleConfig WITHOUT_SORT =
                ImmutableRemoveRedundantLocalSortAggRule.RemoveRedundantLocalSortAggRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalSortAggregate.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                BatchPhysicalLocalSortAggregate
                                                                                        .class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        FlinkConventions
                                                                                                                .BATCH_PHYSICAL()
                                                                                                                .getInterface())
                                                                                                .noInputs())))
                        .withDescription("RemoveRedundantLocalSortAggWithoutSortRule")
                        .as(RemoveRedundantLocalSortAggRuleConfig.class);

        RemoveRedundantLocalSortAggRuleConfig WITH_SORT =
                ImmutableRemoveRedundantLocalSortAggRule.RemoveRedundantLocalSortAggRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalSortAggregate.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(BatchPhysicalSort.class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        BatchPhysicalLocalSortAggregate
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        b3 ->
                                                                                                                b3.operand(
                                                                                                                                FlinkConventions
                                                                                                                                        .BATCH_PHYSICAL()
                                                                                                                                        .getInterface())
                                                                                                                        .noInputs()))))
                        .withDescription("RemoveRedundantLocalSortAggWithSortRule")
                        .as(RemoveRedundantLocalSortAggRuleConfig.class);

        @Override
        default RemoveRedundantLocalSortAggRule toRule() {
            return new RemoveRedundantLocalSortAggRule(this);
        }
    }
}
