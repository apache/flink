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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecPythonCorrelate;
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
 * The physical rule is responsible for convert {@link FlinkLogicalCorrelate} to
 * {@link BatchExecPythonCorrelate}.
 */
public class BatchExecPythonCorrelateRule extends ConverterRule {

	public static final RelOptRule INSTANCE = new BatchExecPythonCorrelateRule();

	private BatchExecPythonCorrelateRule() {
		super(FlinkLogicalCorrelate.class, FlinkConventions.LOGICAL(), FlinkConventions.BATCH_PHYSICAL(),
			"BatchExecPythonCorrelateRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalCorrelate join = call.rel(0);
		RelNode right = ((RelSubset) join.getRight()).getOriginal();

		if (right instanceof FlinkLogicalTableFunctionScan) {
			// right node is a table function
			FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) right;
			// return true if the table function is python table function
			return PythonUtil.isPythonCall(scan.getCall(), null);
		} else if (right instanceof FlinkLogicalCalc) {
			// a filter is pushed above the table function
			FlinkLogicalCalc calc = (FlinkLogicalCalc) right;
			RelNode input = ((RelSubset) calc.getInput()).getOriginal();
			if (input instanceof FlinkLogicalTableFunctionScan) {
				FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) input;
				// return true if the table function is python table function
				return PythonUtil.isPythonCall(scan.getCall(), null);
			}
		}
		return false;
	}

	@Override
	public RelNode convert(RelNode relNode) {
		BatchExecPythonCorrelateFactory factory = new BatchExecPythonCorrelateFactory(relNode);
		return factory.convertToCorrelate();
	}

	/**
	 * The factory is responsible for creating {@link BatchExecPythonCorrelate}.
	 */
	private static class BatchExecPythonCorrelateFactory {
		private final FlinkLogicalCorrelate correlate;
		private final RelTraitSet traitSet;
		private final RelNode convInput;
		private final RelNode right;

		BatchExecPythonCorrelateFactory(RelNode rel) {
			this.correlate = (FlinkLogicalCorrelate) rel;
			this.traitSet = rel.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
			this.convInput = RelOptRule.convert(
				correlate.getInput(0), FlinkConventions.BATCH_PHYSICAL());
			this.right = correlate.getInput(1);
		}

		BatchExecPythonCorrelate convertToCorrelate() {
			return convertToCorrelate(right, Option.empty());
		}

		private BatchExecPythonCorrelate convertToCorrelate(
			RelNode relNode,
			Option<RexNode> condition) {
			if (relNode instanceof RelSubset) {
				RelSubset rel = (RelSubset) relNode;
				return convertToCorrelate(rel.getRelList().get(0), condition);
			} else if (relNode instanceof FlinkLogicalCalc) {
				FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
				return convertToCorrelate(
					((RelSubset) calc.getInput()).getOriginal(),
					Some.apply(calc.getProgram().expandLocalRef(calc.getProgram().getCondition())));
			} else {
				FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
				return new BatchExecPythonCorrelate(
					correlate.getCluster(),
					traitSet,
					convInput,
					scan,
					condition,
					null,
					correlate.getRowType(),
					correlate.getJoinType());
			}
		}
	}
}
