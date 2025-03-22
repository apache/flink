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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalHashAggregate;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

/**
 * There maybe exist a subTree like localHashAggregate -> globalHashAggregate which the middle
 * shuffle is removed. The rule could remove redundant localHashAggregate node.
 */
@Value.Enclosing
public class RemoveRedundantLocalHashAggRule
        extends RelRule<RemoveRedundantLocalHashAggRule.RemoveRedundantLocalHashAggRuleConfig> {

    public static final RemoveRedundantLocalHashAggRule INSTANCE =
            RemoveRedundantLocalHashAggRule.RemoveRedundantLocalHashAggRuleConfig.DEFAULT.toRule();

    protected RemoveRedundantLocalHashAggRule(RemoveRedundantLocalHashAggRuleConfig config) {
        super(config);
    }

    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalHashAggregate globalAgg = call.rel(0);
        BatchPhysicalLocalHashAggregate localAgg = call.rel(1);
        RelNode inputOfLocalAgg = localAgg.getInput();
        BatchPhysicalHashAggregate newGlobalAgg =
                new BatchPhysicalHashAggregate(
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
                        // see BatchPhysicalHashAggRule for details.
                        localAgg.getAggCallToAggFunction(),
                        false);
        call.transformTo(newGlobalAgg);
    }

    /** Configuration for {@link RemoveRedundantLocalHashAggRule}. */
    @Value.Immutable(singleton = false)
    public interface RemoveRedundantLocalHashAggRuleConfig extends RelRule.Config {
        RemoveRedundantLocalHashAggRule.RemoveRedundantLocalHashAggRuleConfig DEFAULT =
                ImmutableRemoveRedundantLocalHashAggRule.RemoveRedundantLocalHashAggRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalHashAggregate.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                BatchPhysicalLocalHashAggregate
                                                                                        .class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        RelNode
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        b3 ->
                                                                                                                b3.operand(
                                                                                                                                FlinkConventions
                                                                                                                                        .BATCH_PHYSICAL()
                                                                                                                                        .getInterface())
                                                                                                                        .noInputs()))))
                        .withDescription("RemoveRedundantLocalHashAggRule")
                        .as(
                                RemoveRedundantLocalHashAggRule
                                        .RemoveRedundantLocalHashAggRuleConfig.class);

        @Override
        default RemoveRedundantLocalHashAggRule toRule() {
            return new RemoveRedundantLocalHashAggRule(this);
        }
    }
}
