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

package org.apache.flink.table.plan.rules.logical;

import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.plan.util.CorrelateUtil;
import org.apache.flink.table.plan.util.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import scala.Option;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;

/**
 * Rule will split the Python {@link FlinkLogicalTableFunctionScan} which includes java calls into a
 * {@link FlinkLogicalCalc} which will be the left input of the new {@link FlinkLogicalCorrelate}
 * and a new {@link FlinkLogicalTableFunctionScan} without java calls.
 */
public class PythonCorrelateSplitRule extends RelOptRule {

	public static final PythonCorrelateSplitRule INSTANCE = new PythonCorrelateSplitRule();

	private PythonCorrelateSplitRule() {
		super(operand(FlinkLogicalCorrelate.class, any()), "PythonCorrelateSplitRule");
	}

	private FlinkLogicalTableFunctionScan createNewScan(
		FlinkLogicalTableFunctionScan scan,
		ScalarFunctionSplitter splitter) {
		RexCall rightRexCall = (RexCall) scan.getCall();
		// extract java funcs.
		List<RexNode> rightCalcProjects = rightRexCall
			.getOperands()
			.stream()
			.map(x -> x.accept(splitter))
			.collect(Collectors.toList());

		RexCall newRightRexCall = rightRexCall.clone(rightRexCall.getType(), rightCalcProjects);
		return new FlinkLogicalTableFunctionScan(
			scan.getCluster(),
			scan.getTraitSet(),
			scan.getInputs(),
			newRightRexCall,
			scan.getElementType(),
			scan.getRowType(),
			scan.getColumnMappings());
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalCorrelate correlate = call.rel(0);
		RelNode right = ((HepRelVertex) correlate.getRight()).getCurrentRel();
		FlinkLogicalTableFunctionScan pythonTableFuncScan;
		if (right instanceof FlinkLogicalTableFunctionScan) {
			pythonTableFuncScan = (FlinkLogicalTableFunctionScan) right;
		} else if (right instanceof FlinkLogicalCalc) {
			Option<FlinkLogicalTableFunctionScan> scan = CorrelateUtil
				.getTableFunctionScan((FlinkLogicalCalc) right);
			if (scan.isEmpty()) {
				return false;
			}
			pythonTableFuncScan = scan.get();
		} else {
			return false;
		}
		RexNode rexNode = pythonTableFuncScan.getCall();
		if (rexNode instanceof RexCall) {
			return PythonUtil.isPythonCall(rexNode, null) && PythonUtil.containsNonPythonCall(rexNode);
		}
		return false;
	}

	private List<String> createNewFiledNames(
		RelDataType rowType,
		RexBuilder rexBuilder,
		int primitiveFieldCount,
		ArrayBuffer<RexCall> extractedJavaRexCalls,
		List<RexNode> calcProjects) {
		for (int i = 0; i < primitiveFieldCount; i++) {
			calcProjects.add(RexInputRef.of(i, rowType));
		}
		// add the fields of the extracted java rex calls.
		Iterator<RexCall> iterator = extractedJavaRexCalls.iterator();
		while (iterator.hasNext()) {
			calcProjects.add(iterator.next());
		}

		List<String> nameList = new LinkedList<>();
		for (int i = 0; i < primitiveFieldCount; i++) {
			nameList.add(rowType.getFieldNames().get(i));
		}
		Iterator<Object> indicesIterator = extractedJavaRexCalls.indices().iterator();
		while (indicesIterator.hasNext()) {
			nameList.add("f" + indicesIterator.next());
		}
		return SqlValidatorUtil.uniquify(
			nameList,
			rexBuilder.getTypeFactory().getTypeSystem().isSchemaCaseSensitive());
	}

	private FlinkLogicalCalc createNewLeftCalc(
		RelNode left,
		RexBuilder rexBuilder,
		ArrayBuffer<RexCall> extractedJavaRexCalls,
		FlinkLogicalCorrelate correlate) {
		// add the fields of the primitive left input.
		List<RexNode> leftCalcProjects = new LinkedList<>();
		RelDataType leftRowType = left.getRowType();
		List<String> leftCalcCalcFieldNames = createNewFiledNames(
			leftRowType,
			rexBuilder,
			leftRowType.getFieldCount(),
			extractedJavaRexCalls,
			leftCalcProjects);

		// create a new java calc
		return new FlinkLogicalCalc(
			correlate.getCluster(),
			correlate.getTraitSet(),
			left,
			RexProgram.create(
				leftRowType,
				leftCalcProjects,
				null,
				leftCalcCalcFieldNames,
				rexBuilder));
	}

	private FlinkLogicalCalc createTopCalc(
		int primitiveLeftFieldCount,
		RexBuilder rexBuilder,
		ArrayBuffer<RexCall> extractedJavaRexCalls,
		RelDataType calcRowType,
		FlinkLogicalCorrelate newCorrelate) {
		RexProgram rexProgram = new RexProgramBuilder(newCorrelate.getRowType(), rexBuilder).getProgram();
		int offset = extractedJavaRexCalls.size() + primitiveLeftFieldCount;

		// extract correlate output RexNode.
		List<RexNode> newTopCalcProjects = rexProgram
			.getExprList()
			.stream()
			.filter(x -> x instanceof RexInputRef)
			.filter(x -> {
				int index = ((RexInputRef) x).getIndex();
				return index < primitiveLeftFieldCount || index >= offset;
			})
			.collect(Collectors.toList());

		return new FlinkLogicalCalc(
			newCorrelate.getCluster(),
			newCorrelate.getTraitSet(),
			newCorrelate,
			RexProgram.create(
				newCorrelate.getRowType(),
				newTopCalcProjects,
				null,
				calcRowType,
				rexBuilder));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		FlinkLogicalCorrelate correlate = call.rel(0);
		RexBuilder rexBuilder = call.builder().getRexBuilder();
		RelNode left = ((HepRelVertex) correlate.getLeft()).getCurrentRel();
		RelNode right = ((HepRelVertex) correlate.getRight()).getCurrentRel();
		int primitiveLeftFieldCount = left.getRowType().getFieldCount();
		ArrayBuffer<RexCall> extractedJavaRexCalls = new ArrayBuffer<>();
		ScalarFunctionSplitter splitter = new ScalarFunctionSplitter(
			primitiveLeftFieldCount,
			extractedJavaRexCalls,
			PythonUtil::isNonPythonCall
		);

		RelNode rightNewInput;
		if (right instanceof FlinkLogicalTableFunctionScan) {
			FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) right;
			rightNewInput = createNewScan(scan, splitter);
		} else {
			FlinkLogicalCalc calc = (FlinkLogicalCalc) right;
			FlinkLogicalTableFunctionScan scan = CorrelateUtil.getTableFunctionScan(calc).get();
			FlinkLogicalCalc mergedCalc = CorrelateUtil.getMergedCalc(calc);
			FlinkLogicalTableFunctionScan newScan = createNewScan(scan, splitter);
			rightNewInput = mergedCalc.copy(mergedCalc.getTraitSet(), newScan, mergedCalc.getProgram());
		}

		FlinkLogicalCalc leftCalc = createNewLeftCalc(
			left,
			rexBuilder,
			extractedJavaRexCalls,
			correlate);

		FlinkLogicalCorrelate newCorrelate = new FlinkLogicalCorrelate(
			correlate.getCluster(),
			correlate.getTraitSet(),
			leftCalc,
			rightNewInput,
			correlate.getCorrelationId(),
			correlate.getRequiredColumns(),
			correlate.getJoinType());

		FlinkLogicalCalc newTopCalc = createTopCalc(
			primitiveLeftFieldCount,
			rexBuilder,
			extractedJavaRexCalls,
			correlate.getRowType(),
			newCorrelate);

		call.transformTo(newTopCalc);
	}
}
