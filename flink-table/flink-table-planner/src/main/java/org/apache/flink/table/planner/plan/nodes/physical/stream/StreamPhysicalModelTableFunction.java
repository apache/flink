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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/** Stream physical RelNode for model table function. */
public class StreamPhysicalModelTableFunction extends SingleRel implements StreamPhysicalRel {

    private final RelDataType outputRowType;
    private final FlinkLogicalTableFunctionScan scan;

    public StreamPhysicalModelTableFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode inputRel,
            FlinkLogicalTableFunctionScan scan,
            RelDataType outputRowType) {
        super(cluster, traits, inputRel);
        this.scan = scan;
        this.outputRowType = outputRowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalModelTableFunction(
                getCluster(), traitSet, inputs.get(0), scan, getRowType());
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        return null;
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        return pw.item("invocation", scan.getCall())
                .item("input", getInput())
                .item("select", String.join(",", getRowType().getFieldNames()))
                .item("rowType", getRowType());
    }
}
