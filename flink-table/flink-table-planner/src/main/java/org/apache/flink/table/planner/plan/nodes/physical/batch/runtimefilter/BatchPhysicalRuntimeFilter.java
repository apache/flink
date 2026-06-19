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

package org.apache.flink.table.planner.plan.nodes.physical.batch.runtimefilter;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.runtimefilter.BatchExecRuntimeFilter;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.optimize.program.FlinkRuntimeFilterProgram;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Batch physical RelNode responsible for filtering probe side data, see {@link
 * FlinkRuntimeFilterProgram} for more info. This RelNode has two inputs, left is the built filter,
 * and the right is the data to be filtered.
 */
public class BatchPhysicalRuntimeFilter extends BiRel implements BatchPhysicalRel {

    private final int[] probeIndices;
    private final double estimatedFilterRatio;

    public BatchPhysicalRuntimeFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            int[] probeIndices,
            double estimatedFilterRatio) {
        super(cluster, traitSet, left, right);
        this.probeIndices = probeIndices;
        this.estimatedFilterRatio = estimatedFilterRatio;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new BatchPhysicalRuntimeFilter(
                getCluster(),
                traitSet,
                inputs.get(0),
                inputs.get(1),
                probeIndices,
                estimatedFilterRatio);
    }

    @Override
    protected RelDataType deriveRowType() {
        return getRight().getRowType();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> fieldNames = getInput(1).getRowType().getFieldNames();
        return super.explainTerms(pw)
                .item(
                        "select",
                        String.join(
                                ", ",
                                Arrays.stream(probeIndices)
                                        .mapToObj(fieldNames::get)
                                        .toArray(String[]::new)))
                .item("estimatedFilterRatio", estimatedFilterRatio);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        // Not necessarily correct, but a better default than AbstractRelNode's 1.0
        return Math.ceil(mq.getRowCount(getInput(1)) * (1 - estimatedFilterRatio));
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        return new BatchExecRuntimeFilter(
                unwrapTableConfig(this),
                getInputProperties(),
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription(),
                probeIndices);
    }

    private List<InputProperty> getInputProperties() {
        InputProperty buildEdge =
                InputProperty.builder()
                        .requiredDistribution(InputProperty.BROADCAST_DISTRIBUTION)
                        .damBehavior(InputProperty.DamBehavior.BLOCKING)
                        .priority(0)
                        .build();

        InputProperty probeEdge =
                InputProperty.builder()
                        .requiredDistribution(InputProperty.ANY_DISTRIBUTION)
                        .damBehavior(InputProperty.DamBehavior.PIPELINED)
                        .priority(1)
                        .build();
        return Arrays.asList(buildEdge, probeEdge);
    }
}
