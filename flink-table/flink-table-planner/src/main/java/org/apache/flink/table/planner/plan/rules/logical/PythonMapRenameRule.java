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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule which renames the field names of the Flatten {@link FlinkLogicalCalc} which is right after a
 * {@link FlinkLogicalCalc} representing a Python Map operation to the output names of the map
 * function.
 */
public class PythonMapRenameRule extends RelOptRule {

    public static final PythonMapRenameRule INSTANCE = new PythonMapRenameRule();

    private PythonMapRenameRule() {
        super(
                operand(FlinkLogicalCalc.class, operand(FlinkLogicalCalc.class, none())),
                "PythonMapRenameRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc topCalc = call.rel(0);
        FlinkLogicalCalc bottomCalc = call.rel(1);

        // the bottom node is a Python map function
        List<RexNode> bottomProjects =
                bottomCalc.getProgram().getProjectList().stream()
                        .map(bottomCalc.getProgram()::expandLocalRef)
                        .collect(Collectors.toList());
        if (bottomProjects.size() != 1
                || !PythonUtil.isPythonCall(bottomProjects.get(0), null)
                || bottomCalc.getProgram().getCondition() != null) {
            return false;
        }

        // the top calc is a flatten operation and the field names of the flatten are not equal to
        // the field names of the bottom calc
        return PythonUtil.isFlattenCalc(topCalc)
                && !bottomCalc
                        .getRowType()
                        .getFieldList()
                        .get(0)
                        .getValue()
                        .getFieldNames()
                        .equals(topCalc.getProgram().getOutputRowType().getFieldNames());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc topCalc = call.rel(0);
        FlinkLogicalCalc bottomCalc = call.rel(1);

        List<RexNode> topProjects =
                topCalc.getProgram().getProjectList().stream()
                        .map(topCalc.getProgram()::expandLocalRef)
                        .collect(Collectors.toList());

        FlinkLogicalCalc newCalc =
                new FlinkLogicalCalc(
                        topCalc.getCluster(),
                        topCalc.getTraitSet(),
                        bottomCalc,
                        RexProgram.create(
                                bottomCalc.getRowType(),
                                topProjects,
                                null,
                                bottomCalc
                                        .getRowType()
                                        .getFieldList()
                                        .get(0)
                                        .getValue()
                                        .getFieldNames(),
                                call.builder().getRexBuilder()));
        call.transformTo(newCalc);
    }
}
