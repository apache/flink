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

import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalAsyncCorrelate;
import org.apache.flink.table.planner.plan.rules.physical.common.CommonPhysicalAsyncCorrelateRule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Optional;

/**
 * A physical rule for identifying logical correlates containing {@link AsyncTableFunction} calls
 * and converting them to physical {@link StreamPhysicalAsyncCorrelate} RelNodes.
 */
public class StreamPhysicalAsyncCorrelateRule extends CommonPhysicalAsyncCorrelateRule {

    public static final RelOptRule INSTANCE =
            new StreamPhysicalAsyncCorrelateRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalCorrelate.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalAsyncCorrelateRule"));

    protected StreamPhysicalAsyncCorrelateRule(Config config) {
        super(config);
    }

    @Override
    protected RelNode convertToPhysicalAsyncCorrelate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            FlinkLogicalTableFunctionScan scan,
            Optional<List<RexNode>> projections,
            Optional<RexNode> condition,
            RelDataType outputRowType,
            JoinRelType joinType) {
        return new StreamPhysicalAsyncCorrelate(
                cluster, traitSet, input, scan, projections, condition, outputRowType, joinType);
    }
}
