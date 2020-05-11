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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeRewriter;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link LogicalProject} into a {@link LogicalTableScan}
 * which wraps a {@link SupportsProjectionPushDown} dynamic table source.
 *
 * <p>NOTES: This rule does not support nested fields push down now,
 * instead it will push the top-level column down just like non-nested fields.
 */
public class PushProjectIntoTableSourceScanRule extends RelOptRule {
	public static final PushProjectIntoTableSourceScanRule INSTANCE = new PushProjectIntoTableSourceScanRule();

	public PushProjectIntoTableSourceScanRule() {
		super(operand(LogicalProject.class,
				operand(LogicalTableScan.class, none())),
				"PushProjectIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(1);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		if (tableSourceTable == null || !(tableSourceTable.tableSource() instanceof SupportsProjectionPushDown)) {
			return false;
		}
		SupportsProjectionPushDown pushDownSource = (SupportsProjectionPushDown) tableSourceTable.tableSource();
		if (pushDownSource.supportsNestedProjection()) {
			throw new TableException("Nested projection push down is unsupported now. \n" +
					"Please disable nested projection (SupportsProjectionPushDown#supportsNestedProjection returns false), " +
					"planner will push down the top-level columns.");
		} else {
			return true;
		}
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalProject project = call.rel(0);
		LogicalTableScan scan = call.rel(1);

		int[] usedFields = RexNodeExtractor.extractRefInputFields(project.getProjects());
		// if no fields can be projected, we keep the original plan.
		if (scan.getRowType().getFieldCount() == usedFields.length) {
			return;
		}

		TableSourceTable oldTableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();

		int[][] projectedFields = new int[usedFields.length][];
		List<String> fieldNames = new ArrayList<>();
		for (int i = 0; i < usedFields.length; ++i) {
			int usedField = usedFields[i];
			projectedFields[i] = new int[] { usedField };
			fieldNames.add(scan.getRowType().getFieldNames().get(usedField));
		}
		((SupportsProjectionPushDown) newTableSource).applyProjection(projectedFields);
		FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) oldTableSourceTable.getRelOptSchema().getTypeFactory();
		RelDataType newRowType = flinkTypeFactory.projectStructType(oldTableSourceTable.getRowType(), usedFields);

		// project push down does not change the statistic, we can reuse origin statistic
		TableSourceTable newTableSourceTable = oldTableSourceTable.copy(
				newTableSource, newRowType, new String[] { ("project=[" + String.join(", ", fieldNames) + "]") });

		LogicalTableScan newScan = new LogicalTableScan(
				scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTableSourceTable);
		// rewrite input field in projections
		List<RexNode> newProjects = RexNodeRewriter.rewriteWithNewFieldInput(project.getProjects(), usedFields);
		LogicalProject newProject = project.copy(
				project.getTraitSet(),
				newScan,
				newProjects,
				project.getRowType());

		if (ProjectRemoveRule.isTrivial(newProject)) {
			// drop project if the transformed program merely returns its input
			call.transformTo(newScan);
		} else {
			call.transformTo(newProject);
		}
	}
}
