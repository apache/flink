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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRank;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.Collections;

/**
 * Planner rule that matches a global {@link BatchPhysicalRank} on a local {@link
 * BatchPhysicalRank}, and merge them into a global {@link BatchPhysicalRank}.
 */
@Value.Enclosing
public class RemoveRedundantLocalRankRule
        extends RelRule<RemoveRedundantLocalRankRule.RemoveRedundantLocalRankRuleConfig> {

    public static final RemoveRedundantLocalRankRule INSTANCE =
            RemoveRedundantLocalRankRuleConfig.DEFAULT.toRule();

    public RemoveRedundantLocalRankRule(RemoveRedundantLocalRankRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        BatchPhysicalRank globalRank = call.rel(0);
        BatchPhysicalRank localRank = call.rel(1);
        return globalRank.isGlobal()
                && !localRank.isGlobal()
                && globalRank.rankType() == localRank.rankType()
                && globalRank.partitionKey() == localRank.partitionKey()
                && globalRank.orderKey() == globalRank.orderKey()
                && globalRank.rankEnd() == localRank.rankEnd();
    }

    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalRank globalRank = call.rel(0);
        RelNode inputOfLocalRank = call.rel(2);
        RelNode newGlobalRank =
                globalRank.copy(
                        globalRank.getTraitSet(), Collections.singletonList(inputOfLocalRank));
        call.transformTo(newGlobalRank);
    }

    /** Configuration for {@link RemoveRedundantLocalRankRule}. */
    @Value.Immutable(singleton = false)
    public interface RemoveRedundantLocalRankRuleConfig extends RelRule.Config {
        RemoveRedundantLocalRankRule.RemoveRedundantLocalRankRuleConfig DEFAULT =
                ImmutableRemoveRedundantLocalRankRule.RemoveRedundantLocalRankRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalRank.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(BatchPhysicalRank.class)
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
                        .withDescription("RemoveRedundantLocalRankRule")
                        .as(RemoveRedundantLocalRankRuleConfig.class);

        @Override
        default RemoveRedundantLocalRankRule toRule() {
            return new RemoveRedundantLocalRankRule(this);
        }
    }
}
