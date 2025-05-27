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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecAsyncCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.JoinTypeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/** Stream physical RelNode for {@link org.apache.flink.table.functions.AsyncScalarFunction}. */
public class StreamPhysicalAsyncCorrelate extends StreamPhysicalCorrelateBase {
    private final RelOptCluster cluster;
    private final FlinkLogicalTableFunctionScan scan;
    private final Optional<List<RexNode>> projections;
    private final Optional<RexNode> condition;
    private final JoinRelType joinType;

    public StreamPhysicalAsyncCorrelate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode inputRel,
            FlinkLogicalTableFunctionScan scan,
            Optional<List<RexNode>> projections,
            Optional<RexNode> condition,
            RelDataType outputRowType,
            JoinRelType joinType) {
        super(
                cluster,
                traitSet,
                inputRel,
                scan,
                JavaScalaConversionUtil.toScala(condition),
                outputRowType,
                joinType);
        this.cluster = cluster;
        this.scan = scan;
        this.projections = projections;
        this.condition = condition;
        this.joinType = joinType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, RelNode newChild, RelDataType outputType) {
        return new StreamPhysicalAsyncCorrelate(
                cluster, traitSet, newChild, scan, projections, condition, outputType, joinType);
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        if (projections.isPresent() || condition.isPresent()) {
            throw new TableException(
                    "Currently Async correlate does not support projections or conditions.");
        }
        return new StreamExecAsyncCorrelate(
                unwrapTableConfig(this),
                JoinTypeUtil.getFlinkJoinType(joinType),
                (RexCall) scan.getCall(),
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }
}
