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

import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalAsyncCorrelate;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A physical rule for identifying logical correlates containing {@link
 * org.apache.flink.table.functions.AsyncTableFunction} calls and converting them to physical {@link
 * StreamPhysicalAsyncCorrelate} RelNodes.
 */
public class StreamPhysicalAsyncCorrelateRule extends ConverterRule {

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

    // find only calc and table function
    private boolean findAsyncTableFunction(RelNode node) {
        if (node instanceof FlinkLogicalTableFunctionScan) {
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) node;
            return AsyncUtil.isAsyncCall(scan.getCall(), FunctionKind.ASYNC_TABLE);
        } else if (node instanceof FlinkLogicalCalc) {
            FlinkLogicalCalc calc = (FlinkLogicalCalc) node;
            RelNode child = ((RelSubset) calc.getInput()).getOriginal();
            return findAsyncTableFunction(child);
        }
        return false;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCorrelate correlate = call.rel(0);
        RelNode right = ((RelSubset) correlate.getRight()).getOriginal();
        return findAsyncTableFunction(right);
    }

    @Override
    public RelNode convert(RelNode rel) {
        FlinkLogicalCorrelate correlate = (FlinkLogicalCorrelate) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode convInput =
                RelOptRule.convert(correlate.getInput(0), FlinkConventions.STREAM_PHYSICAL());
        RelNode right = correlate.getInput(1);
        return convertToCorrelate(
                right, correlate, traitSet, convInput, Optional.empty(), Optional.empty());
    }

    public RelNode convertToCorrelate(
            RelNode relNode,
            FlinkLogicalCorrelate correlate,
            RelTraitSet traitSet,
            RelNode convInput,
            Optional<List<RexNode>> projections,
            Optional<RexNode> condition) {
        if (relNode instanceof RelSubset) {
            RelSubset rel = (RelSubset) relNode;
            return convertToCorrelate(
                    rel.getRelList().get(0),
                    correlate,
                    traitSet,
                    convInput,
                    projections,
                    condition);
        } else if (relNode instanceof FlinkLogicalCalc) {
            FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
            RelNode tableScan = StreamPhysicalCorrelateRule.getTableScan(calc);
            FlinkLogicalCalc newCalc = StreamPhysicalCorrelateRule.getMergedCalc(calc);
            // The projections are not handled here or in the base version, so currently we match
            // that functionality.
            return convertToCorrelate(
                    tableScan,
                    correlate,
                    traitSet,
                    convInput,
                    Optional.ofNullable(
                            newCalc.getProgram().getProjectList() == null
                                    ? null
                                    : newCalc.getProgram().getProjectList().stream()
                                            .map(newCalc.getProgram()::expandLocalRef)
                                            .collect(Collectors.toList())),
                    Optional.ofNullable(
                            newCalc.getProgram().getCondition() == null
                                    ? null
                                    : newCalc.getProgram()
                                            .expandLocalRef(newCalc.getProgram().getCondition())));
        } else {
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
            return new StreamPhysicalAsyncCorrelate(
                    correlate.getCluster(),
                    traitSet,
                    convInput,
                    scan,
                    projections,
                    condition,
                    correlate.getRowType(),
                    correlate.getJoinType());
        }
    }
}
