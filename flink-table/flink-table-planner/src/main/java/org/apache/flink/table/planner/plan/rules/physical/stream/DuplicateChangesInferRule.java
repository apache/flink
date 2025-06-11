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

import org.apache.flink.annotation.Internal;
import org.apache.flink.legacy.table.sinks.RetractStreamTableSink;
import org.apache.flink.legacy.table.sinks.StreamTableSink;
import org.apache.flink.legacy.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.legacy.sinks.TableSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTrait;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTraitDef;
import org.apache.flink.table.planner.plan.utils.DuplicateChangesUtils;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.sinks.DataStreamTableSink;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A rule that infers the {@link DuplicateChanges} for each {@link StreamPhysicalRel} node.
 *
 * <p>The derivation of the trait {@link DuplicateChanges} flows from the root to the leaf, that is,
 * from the sink to the source.
 *
 * <p>Notes: This rule only supports HepPlanner with TOP_DOWN matching order.
 */
@Internal
@Value.Enclosing
public class DuplicateChangesInferRule extends RelRule<DuplicateChangesInferRule.Config> {

    public static final DuplicateChangesInferRule INSTANCE =
            DuplicateChangesInferRule.Config.DEFAULT.toRule();

    protected DuplicateChangesInferRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalRel rel = call.rel(0);
        DuplicateChangesTrait parentTrait =
                rel.getTraitSet().getTrait(DuplicateChangesTraitDef.INSTANCE);
        Preconditions.checkState(parentTrait != null);

        DuplicateChangesTrait requiredTrait;
        if (rel instanceof StreamPhysicalSink) {
            boolean canConsumeDuplicateChanges =
                    canConsumeDuplicateChanges((StreamPhysicalSink) rel);
            boolean isMaterialized = ((StreamPhysicalSink) rel).upsertMaterialize();
            requiredTrait =
                    canConsumeDuplicateChanges && !isMaterialized
                            ? DuplicateChangesTrait.ALLOW
                            : DuplicateChangesTrait.DISALLOW;
        } else if (rel instanceof StreamPhysicalLegacySink) {
            boolean canConsumeDuplicateChanges =
                    canConsumeDuplicateChanges((StreamPhysicalLegacySink<?>) rel);
            requiredTrait =
                    canConsumeDuplicateChanges
                            ? DuplicateChangesTrait.ALLOW
                            : DuplicateChangesTrait.DISALLOW;
        } else if (rel instanceof StreamPhysicalCalcBase) {
            RexProgram calcProgram = ((StreamPhysicalCalcBase) rel).getProgram();
            // if the calc contains non-deterministic fields, we should not allow duplicate
            if (!FlinkRexUtil.isDeterministic(calcProgram)) {
                requiredTrait = DuplicateChangesTrait.DISALLOW;
            } else {
                requiredTrait = parentTrait;
            }
        } else if (rel instanceof StreamPhysicalExchange
                || rel instanceof StreamPhysicalMiniBatchAssigner
                || rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalDropUpdateBefore
                || rel instanceof StreamPhysicalTableSourceScan
                || rel instanceof StreamPhysicalDataStreamScan
                || rel instanceof StreamPhysicalLegacyTableSourceScan
                || rel instanceof StreamPhysicalIntermediateTableScan) {
            // forward parent requirement
            requiredTrait = parentTrait;
        } else {
            // if not explicitly supported, all operators should not accept duplicate changes
            // TODO consider more operators to support consuming duplicate changes
            requiredTrait = DuplicateChangesTrait.DISALLOW;
        }

        boolean anyInputUpdated = false;
        List<RelNode> inputs = getInputs(rel);
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : inputs) {
            DuplicateChangesTrait inputOriginalTrait =
                    input.getTraitSet().getTrait(DuplicateChangesTraitDef.INSTANCE);
            if (!requiredTrait.equals(inputOriginalTrait)) {
                DuplicateChangesTrait mergedTrait =
                        mergeDuplicateChangesTrait(inputOriginalTrait, requiredTrait);
                RelNode newInput =
                        input.copy(input.getTraitSet().plus(mergedTrait), input.getInputs());
                newInputs.add(newInput);
                anyInputUpdated = true;
            } else {
                newInputs.add(input);
            }
        }
        // update parent if a child was updated
        if (anyInputUpdated) {
            RelNode newRel = rel.copy(rel.getTraitSet(), newInputs);
            call.transformTo(newRel);
        }
    }

    private static DuplicateChangesTrait mergeDuplicateChangesTrait(
            DuplicateChangesTrait inputOriginalTrait, DuplicateChangesTrait newRequiredTrait) {
        DuplicateChangesTrait mergedTrait;
        if (inputOriginalTrait == null) {
            mergedTrait = newRequiredTrait;
        } else {
            DuplicateChanges mergedDuplicateChanges =
                    DuplicateChangesUtils.mergeDuplicateChanges(
                            inputOriginalTrait.getDuplicateChanges(),
                            newRequiredTrait.getDuplicateChanges());
            mergedTrait = new DuplicateChangesTrait(mergedDuplicateChanges);
        }
        return mergedTrait;
    }

    private boolean canConsumeDuplicateChanges(StreamPhysicalSink sink) {
        boolean acceptUpdates = true;
        try {
            ChangelogMode sinkProvidedChangelogMode =
                    sink.tableSink().getChangelogMode(ChangelogMode.all());
            if (sinkProvidedChangelogMode.containsOnly(RowKind.INSERT)) {
                acceptUpdates = false;
            }
        } catch (Throwable t) {
            acceptUpdates = false;
        }

        return acceptUpdates
                && sink.contextResolvedTable().getResolvedSchema().getPrimaryKey().isPresent();
    }

    private boolean canConsumeDuplicateChanges(StreamPhysicalLegacySink<?> sink) {
        boolean onlyAcceptInsertOnly;
        TableSink tableSink = sink.sink();
        if (tableSink instanceof UpsertStreamTableSink
                || tableSink instanceof RetractStreamTableSink) {
            // if sink is upsert or retract
            onlyAcceptInsertOnly = false;
        } else if (tableSink instanceof StreamTableSink) {
            // if sink is append or other stream table sink
            onlyAcceptInsertOnly = true;
        } else if (tableSink instanceof DataStreamTableSink) {
            onlyAcceptInsertOnly = !((DataStreamTableSink) tableSink).withChangeFlag();
        } else {
            throw new IllegalStateException(
                    "Unknown legacy sink type: " + sink.getClass().getSimpleName());
        }

        return !onlyAcceptInsertOnly && sink.sink().getTableSchema().getPrimaryKey().isPresent();
    }

    private List<RelNode> getInputs(RelNode parent) {
        return parent.getInputs().stream()
                .map(
                        rel -> {
                            if (rel instanceof HepRelVertex) {
                                return ((HepRelVertex) rel).getCurrentRel();
                            } else {
                                return rel;
                            }
                        })
                .collect(Collectors.toList());
    }

    /** Configuration for {@link DuplicateChangesInferRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT =
                ImmutableDuplicateChangesInferRule.Config.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalRel.class).anyInputs())
                        .withDescription("DuplicateChangesInferRule")
                        .as(Config.class);

        @Override
        default DuplicateChangesInferRule toRule() {
            return new DuplicateChangesInferRule(this);
        }
    }
}
