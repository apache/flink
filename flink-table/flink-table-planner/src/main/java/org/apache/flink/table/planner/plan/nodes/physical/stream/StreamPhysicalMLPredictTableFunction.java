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
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMLPredictTableFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalMLPredictTableFunction;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;

/** Stream physical RelNode for ml predict table function. */
public class StreamPhysicalMLPredictTableFunction extends CommonPhysicalMLPredictTableFunction
        implements StreamPhysicalRel {

    public StreamPhysicalMLPredictTableFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode inputRel,
            FlinkLogicalTableFunctionScan scan,
            RelDataType outputRowType,
            Map<String, String> runtimeConfig) {
        super(cluster, traits, inputRel, scan, outputRowType, runtimeConfig);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalMLPredictTableFunction(
                getCluster(), traitSet, inputs.get(0), scan, getRowType(), runtimeConfig);
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        RexModelCall modelCall = extractOperand(operand -> operand instanceof RexModelCall);
        return new StreamExecMLPredictTableFunction(
                ShortcutUtils.unwrapTableConfig(this),
                buildMLPredictSpec(runtimeConfig),
                buildModelSpec(modelCall),
                buildAsyncOptions(modelCall, runtimeConfig),
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }
}
