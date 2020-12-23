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
 * Rule will merge Python {@link FlinkLogicalCalc} used in Map operation, Flatten {@link FlinkLogicalCalc}
 * and Python {@link FlinkLogicalCalc} used in Map operation together.
 */
public class PythonCalcMergeRule extends RelOptRule {

	public static final PythonCalcMergeRule INSTANCE = new PythonCalcMergeRule();

	private PythonCalcMergeRule() {
		super(operand(FlinkLogicalCalc.class,
			operand(FlinkLogicalCalc.class,
				operand(FlinkLogicalCalc.class, none()))),
			"PythonCalcMergeRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalCalc bottomCalc = call.rel(0);
		FlinkLogicalCalc middleCalc = call.rel(1);
		FlinkLogicalCalc topCalc = call.rel(2);
		return isChainedRowBasedPythonFunction(bottomCalc, middleCalc, topCalc);
	}

	private boolean isChainedRowBasedPythonFunction(
		FlinkLogicalCalc bottomCalc, FlinkLogicalCalc middleCalc, FlinkLogicalCalc topCalc) {
		RexProgram bottomProgram = bottomCalc.getProgram();
		List<RexNode> bottomProjects = bottomProgram.getProjectList()
			.stream()
			.map(bottomProgram::expandLocalRef)
			.collect(Collectors.toList());

		if (bottomProjects.size() != 1 || PythonUtil.isNonPythonCall(bottomProjects.get(0))) {
			return false;
		}

		RexProgram topProgram = topCalc.getProgram();
		List<RexNode> topProjects = topProgram.getProjectList()
			.stream()
			.map(topProgram::expandLocalRef)
			.collect(Collectors.toList());
		if (topProjects.size() != 1 || PythonUtil.isNonPythonCall(topProjects.get(0))) {
			return false;
		}

		RexProgram middleProgram = middleCalc.getProgram();
		if (middleProgram.getCondition() != null) {
			return false;
		}

		List<RexNode> middleProjects = middleProgram.getProjectList()
			.stream()
			.map(middleProgram::expandLocalRef)
			.collect(Collectors.toList());
		int inputRowFieldCount = middleProgram.getInputRowType()
			.getFieldList()
			.get(0)
			.getValue()
			.getFieldList().size();
		if (inputRowFieldCount != middleProjects.size()) {
			return false;
		}

		return isInputsCorrespondWithUpstreamOutputs(bottomProjects, middleProjects) &&
			isFlattenCalc(middleProjects);
	}

	private boolean isInputsCorrespondWithUpstreamOutputs(List<RexNode> bottomProjects, List<RexNode> middleProjects) {
		RexCall pythonCall = (RexCall) bottomProjects.get(0);
		List<RexNode> pythonCallInputs = pythonCall.getOperands();
		if (pythonCallInputs.size() != middleProjects.size()) {
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

	private boolean isFlattenCalc(List<RexNode> middleProjects) {
		for (RexNode middleProject : middleProjects) {
			if (middleProject instanceof RexFieldAccess) {
				RexNode expr = ((RexFieldAccess) middleProject).getReferenceExpr();
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
		FlinkLogicalCalc bottomCalc = call.rel(0);
		FlinkLogicalCalc middleCalc = call.rel(1);
		FlinkLogicalCalc topCalc = call.rel(2);

		RexProgram bottomProgram = bottomCalc.getProgram();
		List<RexCall> bottomProjects = bottomProgram.getProjectList()
			.stream()
			.map(bottomProgram::expandLocalRef)
			.map(x -> (RexCall) x)
			.collect(Collectors.toList());
		RexCall bottomPythonCall = bottomProjects.get(0);

		// merge bottomCalc and middleCalc
		RexCall newPythonCall = bottomPythonCall.clone(bottomPythonCall.getType(),
			Collections.singletonList(RexInputRef.of(0, topCalc.getRowType())));
		List<RexCall> bottomMiddleMergedProjects = Collections.singletonList(newPythonCall);
		FlinkLogicalCalc bottomMiddleMergedCalc = new FlinkLogicalCalc(
			middleCalc.getCluster(),
			middleCalc.getTraitSet(),
			topCalc,
			RexProgram.create(
				topCalc.getRowType(),
				bottomMiddleMergedProjects,
				null,
				Collections.singletonList("f0"),
				call.builder().getRexBuilder()));

		// merge topCalc
		RexBuilder rexBuilder = call.builder().getRexBuilder();
		RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
			bottomMiddleMergedCalc.getProgram(), topCalc.getProgram(), rexBuilder);
		Calc newCalc = bottomMiddleMergedCalc.copy(
			bottomMiddleMergedCalc.getTraitSet(), topCalc.getInput(), mergedProgram);
		call.transformTo(newCalc);
	}
}
