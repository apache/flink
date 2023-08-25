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
import org.apache.flink.table.planner.plan.nodes.exec.batch.runtimefilter.BatchExecLocalRuntimeFilterBuilder;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.optimize.program.FlinkRuntimeFilterProgram;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarBinaryType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Batch physical RelNode responsible for building a local runtime filter based on its local data.
 * See {@link FlinkRuntimeFilterProgram} for more info.
 */
public class BatchPhysicalLocalRuntimeFilterBuilder extends SingleRel implements BatchPhysicalRel {
    private final int estimatedRowCount;
    private final int maxRowCount;
    private final int[] buildIndices;
    private final String[] buildFieldNames;

    public BatchPhysicalLocalRuntimeFilterBuilder(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            int[] buildIndices,
            String[] buildFieldNames,
            int estimatedRowCount,
            int maxRowCount) {
        super(cluster, traits, input);
        this.buildIndices = buildIndices;
        this.buildFieldNames = buildFieldNames;
        this.estimatedRowCount = estimatedRowCount;
        this.maxRowCount = maxRowCount;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new BatchPhysicalLocalRuntimeFilterBuilder(
                getCluster(),
                traitSet,
                inputs.get(0),
                buildIndices,
                buildFieldNames,
                estimatedRowCount,
                maxRowCount);
    }

    @Override
    protected RelDataType deriveRowType() {
        return ((FlinkTypeFactory) getCluster().getTypeFactory())
                .buildRelNodeRowType(
                        Arrays.asList("actualRowCount", "filter"),
                        Arrays.asList(new IntType(), new VarBinaryType()));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("select", String.join(", ", buildFieldNames))
                .item("estimatedRowCount", estimatedRowCount)
                .item("maxRowCount", maxRowCount);
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        InputProperty inputProperty =
                InputProperty.builder().damBehavior(InputProperty.DamBehavior.END_INPUT).build();
        return new BatchExecLocalRuntimeFilterBuilder(
                unwrapTableConfig(this),
                Collections.singletonList(inputProperty),
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription(),
                buildIndices,
                estimatedRowCount,
                maxRowCount);
    }
}
