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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTrait;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTraitDef;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that infers the mini-batch interval of minibatch assigner.
 *
 * <p>This rule could handle the following two kinds of operator: 1. supports operators which
 * supports mini-batch and does not require watermark, e.g. group aggregate. In this case, {@link
 * StreamPhysicalMiniBatchAssigner} with Proctime mode will be created if not exist, and the
 * interval value will be set as {@link ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY}.
 * 2. supports operators which requires watermark, e.g. window join, window aggregate. In this case,
 * {@link StreamPhysicalWatermarkAssigner} already exists, and its MiniBatchIntervalTrait will be
 * updated as the merged intervals from its outputs. Currently, mini-batched window aggregate is not
 * supported, and will be introduced later.
 *
 * <p>NOTES: This rule only supports HepPlanner with TOP_DOWN match order.
 */
@Value.Enclosing
public class MiniBatchIntervalInferRule
        extends RelRule<MiniBatchIntervalInferRule.MiniBatchIntervalInferRuleConfig> {

    public static final MiniBatchIntervalInferRule INSTANCE =
            MiniBatchIntervalInferRule.MiniBatchIntervalInferRuleConfig.DEFAULT.toRule();

    protected MiniBatchIntervalInferRule(
            MiniBatchIntervalInferRule.MiniBatchIntervalInferRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalRel rel = call.rel(0);
        MiniBatchIntervalTrait miniBatchIntervalTrait =
                rel.getTraitSet().getTrait(MiniBatchIntervalTraitDef.INSTANCE());
        List<RelNode> inputs = getInputs(rel);

        MiniBatchIntervalTrait updatedTrait = getUpdatedTrait(rel, miniBatchIntervalTrait);

        List<RelNode> updatedInputs = getUpdatedInput(inputs, updatedTrait);

        // update parent if a child was updated
        if (!inputs.equals(updatedInputs)) {
            RelNode newRel = rel.copy(rel.getTraitSet(), updatedInputs);
            call.transformTo(newRel);
        }
    }

    private MiniBatchIntervalTrait getUpdatedTrait(
            StreamPhysicalRel rel, MiniBatchIntervalTrait miniBatchIntervalTrait) {
        if (rel instanceof StreamPhysicalGroupWindowAggregate) {
            // TODO introduce mini-batch window aggregate later
            return MiniBatchIntervalTrait.NO_MINIBATCH();
        }

        if (rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalMiniBatchAssigner) {
            return MiniBatchIntervalTrait.NONE();
        }

        TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(rel);
        Boolean miniBatchEnabled =
                tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);

        if (rel.requireWatermark() && miniBatchEnabled) {
            MiniBatchInterval mergedInterval =
                    FlinkRelOptUtil.mergeMiniBatchInterval(
                            miniBatchIntervalTrait.getMiniBatchInterval(),
                            new MiniBatchInterval(0, MiniBatchMode.RowTime));
            return new MiniBatchIntervalTrait(mergedInterval);
        } else {
            return miniBatchIntervalTrait;
        }
    }

    // propagate parent's MiniBatchInterval to children.
    private List<RelNode> getUpdatedInput(
            List<RelNode> inputs, MiniBatchIntervalTrait updatedTrait) {
        final List<RelNode> updatedInputs = new ArrayList<>();
        for (RelNode input : inputs) {
            // add mini-batch watermark assigner node.
            if (shouldAppendMiniBatchAssignerNode(input)) {
                updatedInputs.add(
                        new StreamPhysicalMiniBatchAssigner(
                                input.getCluster(),
                                input.getTraitSet(),
                                // attach NONE trait for all of the inputs of MiniBatchAssigner,
                                // as they are leaf nodes and don't need to do propagate
                                input.copy(
                                        input.getTraitSet().plus(MiniBatchIntervalTrait.NONE()),
                                        input.getInputs())));
            } else {
                MiniBatchIntervalTrait originTrait =
                        input.getTraitSet().getTrait(MiniBatchIntervalTraitDef.INSTANCE());
                if (originTrait != updatedTrait) {
                    // calculate new MiniBatchIntervalTrait according parent's miniBatchInterval
                    // and the child's original miniBatchInterval.
                    MiniBatchInterval mergedMiniBatchInterval =
                            FlinkRelOptUtil.mergeMiniBatchInterval(
                                    originTrait.getMiniBatchInterval(),
                                    updatedTrait.getMiniBatchInterval());
                    MiniBatchIntervalTrait inferredTrait =
                            new MiniBatchIntervalTrait(mergedMiniBatchInterval);
                    updatedInputs.add(
                            input.copy(input.getTraitSet().plus(inferredTrait), input.getInputs()));
                } else {
                    updatedInputs.add(input);
                }
            }
        }
        return updatedInputs;
    }

    /**
     * Get all children RelNodes of a RelNode.
     *
     * @param parent The parent RelNode
     * @return All child nodes
     */
    private List<RelNode> getInputs(RelNode parent) {
        return parent.getInputs().stream()
                .map(i -> ((HepRelVertex) i).getCurrentRel())
                .collect(Collectors.toList());
    }

    private boolean shouldAppendMiniBatchAssignerNode(RelNode node) {
        final MiniBatchMode mode =
                node.getTraitSet()
                        .getTrait(MiniBatchIntervalTraitDef.INSTANCE())
                        .getMiniBatchInterval()
                        .getMode();
        if (node instanceof StreamPhysicalDataStreamScan
                || node instanceof StreamPhysicalLegacyTableSourceScan
                || node instanceof StreamPhysicalTableSourceScan) {
            // append minibatch node if the mode is not NONE and reach a source leaf node
            return mode == MiniBatchMode.RowTime || mode == MiniBatchMode.ProcTime;
        }
        if (node instanceof StreamPhysicalWatermarkAssigner) {
            // append minibatch node if it is rowtime mode and the child is watermark assigner
            // TODO: if it is ProcTime mode, we also append a minibatch node for now.
            //  Because the downstream can be a regular aggregate and the watermark assigner
            //  might be redundant. In FLINK-14621, we will remove redundant watermark assigner,
            //  then we can remove the ProcTime condition.
            return mode == MiniBatchMode.RowTime || mode == MiniBatchMode.ProcTime;
        }
        // others do not append minibatch node
        return false;
    }

    /** Configuration for {@link MiniBatchIntervalInferRule}. */
    @Value.Immutable(singleton = false)
    public interface MiniBatchIntervalInferRuleConfig extends RelRule.Config {
        MiniBatchIntervalInferRule.MiniBatchIntervalInferRuleConfig DEFAULT =
                ImmutableMiniBatchIntervalInferRule.MiniBatchIntervalInferRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalRel.class).anyInputs())
                        .withDescription("MiniBatchIntervalInferRule")
                        .as(MiniBatchIntervalInferRule.MiniBatchIntervalInferRuleConfig.class);

        @Override
        default MiniBatchIntervalInferRule toRule() {
            return new MiniBatchIntervalInferRule(this);
        }
    }
}
