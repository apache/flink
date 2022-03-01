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

import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule will merge Python {@link FlinkLogicalCalc} used in Map operation, Flatten {@link
 * FlinkLogicalCalc} and Python {@link FlinkLogicalCalc} used in Map operation together.
 */
public class PythonMapMergeRule extends RelOptRule {

    public static final PythonMapMergeRule INSTANCE = new PythonMapMergeRule();

    private PythonMapMergeRule() {
        super(
                operand(
                        FlinkLogicalCalc.class,
                        operand(FlinkLogicalCalc.class, operand(FlinkLogicalCalc.class, none()))),
                "PythonMapMergeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc topCalc = call.rel(0);
        FlinkLogicalCalc middleCalc = call.rel(1);
        FlinkLogicalCalc bottomCalc = call.rel(2);

        RexProgram topProgram = topCalc.getProgram();
        List<RexNode> topProjects =
                topProgram.getProjectList().stream()
                        .map(topProgram::expandLocalRef)
                        .collect(Collectors.toList());

        if (topProjects.size() != 1
                || !PythonUtil.isPythonCall(topProjects.get(0), null)
                || !PythonUtil.takesRowAsInput((RexCall) topProjects.get(0))) {
            return false;
        }

        RexProgram bottomProgram = bottomCalc.getProgram();
        List<RexNode> bottomProjects =
                bottomProgram.getProjectList().stream()
                        .map(bottomProgram::expandLocalRef)
                        .collect(Collectors.toList());
        if (bottomProjects.size() != 1 || !PythonUtil.isPythonCall(bottomProjects.get(0), null)) {
            return false;
        }

        // Only Python Functions with same Python function kind can be merged together.
        if (PythonUtil.isPythonCall(topProjects.get(0), PythonFunctionKind.GENERAL)
                ^ PythonUtil.isPythonCall(bottomProjects.get(0), PythonFunctionKind.GENERAL)) {
            return false;
        }

        RexProgram middleProgram = middleCalc.getProgram();
        if (topProgram.getCondition() != null
                || middleProgram.getCondition() != null
                || bottomProgram.getCondition() != null) {
            return false;
        }

        List<RexNode> middleProjects =
                middleProgram.getProjectList().stream()
                        .map(middleProgram::expandLocalRef)
                        .collect(Collectors.toList());
        int inputRowFieldCount =
                middleProgram
                        .getInputRowType()
                        .getFieldList()
                        .get(0)
                        .getValue()
                        .getFieldList()
                        .size();

        return isFlattenCalc(middleProjects, inputRowFieldCount)
                && isTopCalcTakesWholeMiddleCalcAsInputs(
                        (RexCall) topProjects.get(0), middleProjects.size());
    }

    private boolean isTopCalcTakesWholeMiddleCalcAsInputs(
            RexCall pythonCall, int inputColumnCount) {
        List<RexNode> pythonCallInputs = pythonCall.getOperands();
        if (pythonCallInputs.size() != inputColumnCount) {
            return false;
        }
        for (int i = 0; i < pythonCallInputs.size(); i++) {
            RexNode input = pythonCallInputs.get(i);
            if (input instanceof RexInputRef) {
                if (((RexInputRef) input).getIndex() != i) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private boolean isFlattenCalc(List<RexNode> middleProjects, int inputRowFieldCount) {
        if (inputRowFieldCount != middleProjects.size()) {
            return false;
        }
        for (int i = 0; i < inputRowFieldCount; i++) {
            RexNode middleProject = middleProjects.get(i);
            if (middleProject instanceof RexFieldAccess) {
                RexFieldAccess rexField = ((RexFieldAccess) middleProject);
                if (rexField.getField().getIndex() != i) {
                    return false;
                }
                RexNode expr = rexField.getReferenceExpr();
                if (expr instanceof RexInputRef) {
                    if (((RexInputRef) expr).getIndex() != 0) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc topCalc = call.rel(0);
        FlinkLogicalCalc middleCalc = call.rel(1);
        FlinkLogicalCalc bottomCalc = call.rel(2);

        RexProgram topProgram = topCalc.getProgram();
        List<RexCall> topProjects =
                topProgram.getProjectList().stream()
                        .map(topProgram::expandLocalRef)
                        .map(x -> (RexCall) x)
                        .collect(Collectors.toList());
        RexCall topPythonCall = topProjects.get(0);

        // merge topCalc and middleCalc
        RexCall newPythonCall =
                topPythonCall.clone(
                        topPythonCall.getType(),
                        Collections.singletonList(RexInputRef.of(0, bottomCalc.getRowType())));
        List<RexCall> topMiddleMergedProjects = Collections.singletonList(newPythonCall);
        FlinkLogicalCalc topMiddleMergedCalc =
                new FlinkLogicalCalc(
                        middleCalc.getCluster(),
                        middleCalc.getTraitSet(),
                        bottomCalc,
                        RexProgram.create(
                                bottomCalc.getRowType(),
                                topMiddleMergedProjects,
                                null,
                                Collections.singletonList("f0"),
                                call.builder().getRexBuilder()));

        // merge bottomCalc
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RexProgram mergedProgram =
                RexProgramBuilder.mergePrograms(
                        topMiddleMergedCalc.getProgram(), bottomCalc.getProgram(), rexBuilder);
        Calc newCalc =
                topMiddleMergedCalc.copy(
                        topMiddleMergedCalc.getTraitSet(), bottomCalc.getInput(), mergedProgram);
        call.transformTo(newCalc);
    }
}
