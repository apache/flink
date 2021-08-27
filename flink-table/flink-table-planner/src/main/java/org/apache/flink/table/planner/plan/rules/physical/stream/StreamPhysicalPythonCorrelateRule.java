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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonCorrelate;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;

import scala.Option;
import scala.Some;

/**
 * The physical rule is responsible for convert {@link FlinkLogicalCorrelate} to {@link
 * StreamPhysicalPythonCorrelate}.
 */
public class StreamPhysicalPythonCorrelateRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new StreamPhysicalPythonCorrelateRule();

    private StreamPhysicalPythonCorrelateRule() {
        super(
                FlinkLogicalCorrelate.class,
                FlinkConventions.LOGICAL(),
                FlinkConventions.STREAM_PHYSICAL(),
                "StreamPhysicalPythonCorrelateRule");
    }

    // find only calc and table function
    private boolean findTableFunction(FlinkLogicalCalc calc) {
        RelNode child = ((RelSubset) calc.getInput()).getOriginal();
        if (child instanceof FlinkLogicalTableFunctionScan) {
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) child;
            return PythonUtil.isPythonCall(scan.getCall(), null);
        } else if (child instanceof FlinkLogicalCalc) {
            FlinkLogicalCalc childCalc = (FlinkLogicalCalc) child;
            return findTableFunction(childCalc);
        }
        return false;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCorrelate correlate = call.rel(0);
        RelNode right = ((RelSubset) correlate.getRight()).getOriginal();
        if (right instanceof FlinkLogicalTableFunctionScan) {
            // right node is a table function
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) right;
            // return true if the table function is python table function
            return PythonUtil.isPythonCall(scan.getCall(), null);
        } else if (right instanceof FlinkLogicalCalc) {
            // a filter is pushed above the table function
            return findTableFunction((FlinkLogicalCalc) right);
        }
        return false;
    }

    @Override
    public RelNode convert(RelNode relNode) {
        StreamExecPythonCorrelateFactory factory = new StreamExecPythonCorrelateFactory(relNode);
        return factory.convertToCorrelate();
    }

    /** The factory is responsible for creating {@link StreamPhysicalPythonCorrelate}. */
    private static class StreamExecPythonCorrelateFactory {
        private final FlinkLogicalCorrelate correlate;
        private final RelTraitSet traitSet;
        private final RelNode convInput;
        private final RelNode right;

        StreamExecPythonCorrelateFactory(RelNode rel) {
            this.correlate = (FlinkLogicalCorrelate) rel;
            this.traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
            this.convInput =
                    RelOptRule.convert(correlate.getInput(0), FlinkConventions.STREAM_PHYSICAL());
            this.right = correlate.getInput(1);
        }

        StreamPhysicalPythonCorrelate convertToCorrelate() {
            return convertToCorrelate(right, Option.empty());
        }

        private StreamPhysicalPythonCorrelate convertToCorrelate(
                RelNode relNode, Option<RexNode> condition) {
            if (relNode instanceof RelSubset) {
                RelSubset rel = (RelSubset) relNode;
                return convertToCorrelate(rel.getRelList().get(0), condition);
            } else if (relNode instanceof FlinkLogicalCalc) {
                FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
                RelNode tableScan = StreamPhysicalCorrelateRule.getTableScan(calc);
                FlinkLogicalCalc newCalc = StreamPhysicalCorrelateRule.getMergedCalc(calc);
                return convertToCorrelate(
                        tableScan,
                        Some.apply(
                                newCalc.getProgram()
                                        .expandLocalRef(newCalc.getProgram().getCondition())));
            } else {
                FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
                return new StreamPhysicalPythonCorrelate(
                        correlate.getCluster(),
                        traitSet,
                        convInput,
                        scan,
                        condition,
                        correlate.getRowType(),
                        correlate.getJoinType());
            }
        }
    }
}
