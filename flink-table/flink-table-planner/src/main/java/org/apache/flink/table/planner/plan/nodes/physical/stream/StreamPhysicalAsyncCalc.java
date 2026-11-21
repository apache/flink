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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecAsyncCalc;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/** Stream physical RelNode for {@link org.apache.flink.table.functions.AsyncScalarFunction}. */
public class StreamPhysicalAsyncCalc extends StreamPhysicalCalcBase {
    private final RelOptCluster cluster;
    private final RexProgram calcProgram;
    private final RelDataType outputRowType;

    public StreamPhysicalAsyncCalc(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode inputRel,
            RexProgram calcProgram,
            RelDataType outputRowType) {
        super(cluster, traitSet, inputRel, calcProgram, outputRowType);
        this.cluster = cluster;
        this.calcProgram = calcProgram;
        this.outputRowType = outputRowType;
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new StreamPhysicalAsyncCalc(cluster, traitSet, child, program, outputRowType);
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        List<RexNode> projection =
                AsyncUtil.expandAsyncCalcProjection(calcProgram, getRelTypeName());

        return new StreamExecAsyncCalc(
                unwrapTableConfig(this),
                projection,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }
}
