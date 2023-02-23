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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Transpose the BatchPhysicalLocalSortAggregate(with keys) with BatchPhysicalUnion. */
@Value.Enclosing
public class BatchLocalSortAggWithKeysUnionTransposeRule
        extends RelRule<BatchLocalSortAggWithKeysUnionTransposeRule.Config> {

    public static final BatchLocalSortAggWithKeysUnionTransposeRule
            LOCAL_SORT_AGG_WITH_KEYS_INSTANCE =
                    new BatchLocalSortAggWithKeysUnionTransposeRule(Config.LOCAL_SORT);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public BatchLocalSortAggWithKeysUnionTransposeRule(
            BatchLocalSortAggWithKeysUnionTransposeRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalRel localSortAgg = call.rel(0);
        BatchPhysicalSort sort = call.rel(1);
        BatchPhysicalUnion union = call.rel(2);

        TableConfig tableConfig = ShortcutUtils.unwrapContext(union).getTableConfig();
        if (!tableConfig.get(
                OptimizerConfigOptions.TABLE_OPTIMIZER_LOCAL_AGG_UNION_TRANSPOSE_ENABLED)) {
            return;
        }
        List<RelNode> unionInputs = union.getInputs();
        List<RelNode> newLocalSorts = new ArrayList<>(unionInputs.size());
        for (RelNode unionInput : unionInputs) {
            Sort newSort = sort.copy(sort.getTraitSet(), Collections.singletonList(unionInput));
            RelNode newLocalSort =
                    localSortAgg.copy(
                            localSortAgg.getTraitSet(), Collections.singletonList(newSort));
            newLocalSorts.add(newLocalSort);
        }
        BatchPhysicalUnion transformed =
                new BatchPhysicalUnion(
                        union.getCluster(),
                        union.getTraitSet(),
                        newLocalSorts,
                        union.all,
                        localSortAgg.getRowType());
        call.transformTo(transformed);
    }

    /** Configuration for {@link BatchLocalSortAggWithKeysUnionTransposeRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config LOCAL_SORT =
                ImmutableBatchLocalSortAggWithKeysUnionTransposeRule.Config.builder()
                        .build()
                        .ofLocalSortAggWithKeys();

        @Override
        default RelOptRule toRule() {
            return new BatchLocalSortAggWithKeysUnionTransposeRule(this);
        }

        default Config ofLocalSortAggWithKeys() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(BatchPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform sortTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(BatchPhysicalSort.class)
                                    .oneInput(unionTransform);

            final RelRule.OperandTransform localSortAggTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(BatchPhysicalLocalSortAggregate.class)
                                    .oneInput(sortTransform);

            return withOperandSupplier(localSortAggTransform)
                    .withDescription("BatchPhysicalLocalSortAggregateWithKeysUnionTransposeRule")
                    .as(BatchLocalSortAggWithKeysUnionTransposeRule.Config.class);
        }
    }
}
