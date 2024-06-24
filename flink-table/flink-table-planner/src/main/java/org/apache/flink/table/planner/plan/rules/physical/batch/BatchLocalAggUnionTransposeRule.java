/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalHashAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Rules to transpose the BatchPhysicalLocalHashAggregate and
 * BatchPhysicalLocalSortAggregate(without keys) and BatchPhysicalExpand with BatchPhysicalUnion .
 */
@Value.Enclosing
public class BatchLocalAggUnionTransposeRule
        extends RelRule<BatchLocalAggUnionTransposeRule.Config> {

    public static final BatchLocalAggUnionTransposeRule EXPAND_INSTANCE =
            new BatchLocalAggUnionTransposeRule(Config.EXPAND);

    public static final BatchLocalAggUnionTransposeRule LOCAL_HASH_AGG_INSTANCE =
            new BatchLocalAggUnionTransposeRule(Config.LOCAL_HASH_AGG);

    public static final BatchLocalAggUnionTransposeRule LOCAL_SORT_AGG_INSTANCE =
            new BatchLocalAggUnionTransposeRule(Config.LOCAL_SORT_AGG);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public BatchLocalAggUnionTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalRel localAgg = call.rel(0);
        BatchPhysicalUnion union = call.rel(1);

        TableConfig tableConfig = ShortcutUtils.unwrapContext(union).getTableConfig();
        if (!tableConfig.get(
                OptimizerConfigOptions.TABLE_OPTIMIZER_LOCAL_AGG_UNION_TRANSPOSE_ENABLED)) {
            return;
        }
        List<RelNode> unionInputs = union.getInputs();
        List<RelNode> pushedDownLocalHashes = new ArrayList<>(unionInputs.size());
        for (RelNode unionInput : unionInputs) {
            pushedDownLocalHashes.add(
                    localAgg.copy(localAgg.getTraitSet(), Collections.singletonList(unionInput)));
        }
        BatchPhysicalUnion transformed =
                new BatchPhysicalUnion(
                        union.getCluster(),
                        union.getTraitSet(),
                        pushedDownLocalHashes,
                        union.all,
                        localAgg.getRowType());
        call.transformTo(transformed);
    }

    /** Configuration for {@link BatchLocalAggUnionTransposeRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config EXPAND =
                ImmutableBatchLocalAggUnionTransposeRule.Config.builder().build().ofExpand();

        Config LOCAL_HASH_AGG =
                ImmutableBatchLocalAggUnionTransposeRule.Config.builder().build().ofLocalHashAgg();

        Config LOCAL_SORT_AGG =
                ImmutableBatchLocalAggUnionTransposeRule.Config.builder().build().ofLocalSortAgg();

        @Override
        default RelOptRule toRule() {
            return new BatchLocalAggUnionTransposeRule(this);
        }

        default Config ofExpand() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(BatchPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform expandTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(BatchPhysicalExpand.class)
                                    .oneInput(unionTransform);
            return withOperandSupplier(expandTransform)
                    .withDescription("BatchPhysicalExpandUnionTransposeRule")
                    .as(Config.class);
        }

        default Config ofLocalHashAgg() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(BatchPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform localHashAggTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(BatchPhysicalLocalHashAggregate.class)
                                    .oneInput(unionTransform);
            return withOperandSupplier(localHashAggTransform)
                    .withDescription("BatchPhysicalLocalHashAggregateUnionTransposeRule")
                    .as(Config.class);
        }

        default Config ofLocalSortAgg() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(BatchPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform localSortAggTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(BatchPhysicalLocalSortAggregate.class)
                                    .oneInput(unionTransform);
            return withOperandSupplier(localSortAggTransform)
                    .withDescription("BatchPhysicalLocalSortAggregateUnionTransposeRule")
                    .as(Config.class);
        }
    }
}
