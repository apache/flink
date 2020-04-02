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

package org.apache.flink.table.plan.rules.datastream;

import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DataStreamPythonCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.plan.util.CorrelateUtil;
import org.apache.flink.table.plan.util.PythonUtil;

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
 * {@link DataStreamPythonCorrelate}.
 */
public class DataStreamPythonCorrelateRule extends ConverterRule {

	public static final RelOptRule INSTANCE = new DataStreamPythonCorrelateRule();

	private DataStreamPythonCorrelateRule() {
		super(FlinkLogicalCorrelate.class, FlinkConventions.LOGICAL(), FlinkConventions.DATASTREAM(),
			"DataStreamPythonCorrelateRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalCorrelate join = call.rel(0);
		RelNode right = ((RelSubset) join.getRight()).getOriginal();

		if (right instanceof FlinkLogicalTableFunctionScan) {
			// right node is a python table function
			FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) right;
			return PythonUtil.isPythonCall(scan.getCall(), null);
		} else if (right instanceof FlinkLogicalCalc) {
			// a filter is pushed above the table function
			FlinkLogicalCalc calc = (FlinkLogicalCalc) right;
			Option<FlinkLogicalTableFunctionScan> scan = CorrelateUtil.getTableFunctionScan(calc);
			return scan.isDefined() && PythonUtil.isPythonCall(scan.get().getCall(), null);
		}
		return false;
	}

	@Override
	public RelNode convert(RelNode rel) {
		DataStreamPythonCorrelateFactory factory = new DataStreamPythonCorrelateFactory(rel);
		return factory.convertToCorrelate();
	}

	/**
	 * The factory is responsible to creating {@link DataStreamPythonCorrelate}.
	 */
	private static class DataStreamPythonCorrelateFactory {
		private final RelNode correlateRel;
		private final FlinkLogicalCorrelate join;
		private final RelTraitSet traitSet;
		private final RelNode convInput;
		private final RelNode right;

		DataStreamPythonCorrelateFactory(RelNode rel) {
			this.correlateRel = rel;
			this.join = (FlinkLogicalCorrelate) rel;
			this.traitSet = rel.getTraitSet().replace(FlinkConventions.DATASTREAM());
			this.convInput = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASTREAM());
			this.right = join.getInput(1);
		}

		DataStreamPythonCorrelate convertToCorrelate() {
			return convertToCorrelate(right, Option.empty());
		}

		private DataStreamPythonCorrelate convertToCorrelate(
			RelNode relNode,
			Option<RexNode> condition) {
			if (relNode instanceof RelSubset) {
				RelSubset rel = (RelSubset) relNode;
				return convertToCorrelate(rel.getRelList().get(0), condition);
			} else if (relNode instanceof FlinkLogicalCalc) {
				FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
				FlinkLogicalTableFunctionScan tableScan = CorrelateUtil.getTableFunctionScan(calc).get();
				FlinkLogicalCalc newCalc = CorrelateUtil.getMergedCalc(calc);
				return convertToCorrelate(
					tableScan,
					Some.apply(newCalc.getProgram().expandLocalRef(newCalc.getProgram().getCondition())));
			} else {
				FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
				return new DataStreamPythonCorrelate(
					relNode.getCluster(),
					traitSet,
					new RowSchema(convInput.getRowType()),
					convInput,
					scan,
					condition,
					new RowSchema(correlateRel.getRowType()),
					new RowSchema(join.getRowType()),
					join.getJoinType(),
					"DataStreamPythonCorrelateRule");
			}
		}
	}
}
