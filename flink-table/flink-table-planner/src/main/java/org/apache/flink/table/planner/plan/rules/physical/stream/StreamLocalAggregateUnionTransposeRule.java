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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
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
 * Rules to transpose the StreamPhysicalLocalGroupAggregate and StreamPhysicalLocalWindowAggregate
 * and StreamPhysicalExpand with StreamPhysicalUnion.
 */
@Value.Enclosing
public class StreamLocalAggregateUnionTransposeRule
        extends RelRule<StreamLocalAggregateUnionTransposeRule.Config> {

    public static final StreamLocalAggregateUnionTransposeRule EXPAND_INSTANCE =
            new StreamLocalAggregateUnionTransposeRule(Config.EXPAND);

    public static final StreamLocalAggregateUnionTransposeRule LOCAL_GROUP_AGG_INSTANCE =
            new StreamLocalAggregateUnionTransposeRule(Config.LOCAL_GROUP_AGG);

    public static final StreamLocalAggregateUnionTransposeRule LOCAL_WINDOW_AGG_INSTANCE =
            new StreamLocalAggregateUnionTransposeRule(Config.LOCAL_WINDOW_AGG);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public StreamLocalAggregateUnionTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalRel localAggregate = call.rel(0);
        StreamPhysicalUnion union = call.rel(1);

        TableConfig tableConfig = ShortcutUtils.unwrapContext(union).getTableConfig();
        if (!tableConfig.get(
                OptimizerConfigOptions.TABLE_OPTIMIZER_LOCAL_AGG_UNION_TRANSPOSE_ENABLED)) {
            return;
        }
        List<RelNode> unionInputs = union.getInputs();
        List<RelNode> pushedDownLocalHashes = new ArrayList<>(unionInputs.size());
        for (RelNode unionInput : unionInputs) {
            pushedDownLocalHashes.add(
                    localAggregate.copy(
                            localAggregate.getTraitSet(), Collections.singletonList(unionInput)));
        }
        StreamPhysicalUnion transformed =
                new StreamPhysicalUnion(
                        union.getCluster(),
                        union.getTraitSet(),
                        pushedDownLocalHashes,
                        union.all,
                        localAggregate.getRowType());
        call.transformTo(transformed);
    }

    /** Configuration for {@link StreamLocalAggregateUnionTransposeRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config EXPAND =
                ImmutableStreamLocalAggregateUnionTransposeRule.Config.builder().build().ofExpand();

        Config LOCAL_GROUP_AGG =
                ImmutableStreamLocalAggregateUnionTransposeRule.Config.builder()
                        .build()
                        .ofLocalGroupAgg();

        Config LOCAL_WINDOW_AGG =
                ImmutableStreamLocalAggregateUnionTransposeRule.Config.builder()
                        .build()
                        .ofLocalWindowAgg();

        @Override
        default RelOptRule toRule() {
            return new StreamLocalAggregateUnionTransposeRule(this);
        }

        default StreamLocalAggregateUnionTransposeRule.Config ofExpand() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(StreamPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform expandTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalExpand.class)
                                    .oneInput(unionTransform);
            return withOperandSupplier(expandTransform)
                    .withDescription("StreamExpandUnionTransposeRule")
                    .as(StreamLocalAggregateUnionTransposeRule.Config.class);
        }

        default StreamLocalAggregateUnionTransposeRule.Config ofLocalGroupAgg() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(StreamPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform localHashAggTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalLocalGroupAggregate.class)
                                    .oneInput(unionTransform);
            return withOperandSupplier(localHashAggTransform)
                    .withDescription("StreamLocalGroupAggregateUnionTransposeRule")
                    .as(StreamLocalAggregateUnionTransposeRule.Config.class);
        }

        default StreamLocalAggregateUnionTransposeRule.Config ofLocalWindowAgg() {

            final RelRule.OperandTransform unionTransform =
                    operandBuilder -> operandBuilder.operand(StreamPhysicalUnion.class).anyInputs();

            final RelRule.OperandTransform localWindowAggTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalLocalWindowAggregate.class)
                                    .oneInput(unionTransform);
            return withOperandSupplier(localWindowAggTransform)
                    .withDescription("StreamLocalWindowAggregateUnionTransposeRule")
                    .as(StreamLocalAggregateUnionTransposeRule.Config.class);
        }
    }
}
