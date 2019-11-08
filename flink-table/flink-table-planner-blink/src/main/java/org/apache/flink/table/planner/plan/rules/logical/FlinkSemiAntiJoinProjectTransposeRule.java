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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * This rules is copied from Calcite's {@link org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule}.
 * Modification:
 * - Match Join with SEMI or ANTI type instead of SemiJoin
 * - Add predicate to check all expressions in Project should be RexInputRef
 */

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.SemiJoin} down in a tree past
 * a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalProject(X), Y) &rarr; LogicalProject(SemiJoin(X, Y))
 *
 * @see org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule
 */
public class FlinkSemiAntiJoinProjectTransposeRule extends RelOptRule {

	public static final FlinkSemiAntiJoinProjectTransposeRule INSTANCE =
			new FlinkSemiAntiJoinProjectTransposeRule(RelFactories.LOGICAL_BUILDER);

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a FlinkSemiAntiJoinProjectTransposeRule.
	 */
	private FlinkSemiAntiJoinProjectTransposeRule(RelBuilderFactory relBuilderFactory) {
		super(operand(LogicalJoin.class,
				some(operand(LogicalProject.class, any()))),
				relBuilderFactory, null);
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalJoin join = call.rel(0);
		LogicalProject project = call.rel(1);

		// only accept SEMI/ANTI join
		JoinRelType joinType = join.getJoinType();
		if (joinType != JoinRelType.SEMI && joinType != JoinRelType.ANTI) {
			return false;
		}
		// all expressions in Project should be RexInputRef
		for (RexNode p : project.getProjects()) {
			if (!(p instanceof RexInputRef)) {
				return false;
			}
		}
		return true;
	}

	//~ Methods ----------------------------------------------------------------

	public void onMatch(RelOptRuleCall call) {
		LogicalJoin join = call.rel(0);
		LogicalProject project = call.rel(1);

		// convert the semi/anti join condition to reflect the LHS with the project
		// pulled up
		RexNode newCondition = adjustCondition(project, join);
		Join newJoin = LogicalJoin.create(
				project.getInput(), join.getRight(), newCondition, join.getVariablesSet(), join.getJoinType());

		// Create the new projection. Note that the projection expressions
		// are the same as the original because they only reference the LHS
		// of the semi/anti join and the semi/anti join only projects out the LHS
		final RelBuilder relBuilder = call.builder();
		relBuilder.push(newJoin);
		relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());

		call.transformTo(relBuilder.build());
	}

	/**
	 * Pulls the project above the semi/anti join and returns the resulting semi/anti join
	 * condition. As a result, the semi/anti join condition should be modified such
	 * that references to the LHS of a semi/anti join should now reference the
	 * children of the project that's on the LHS.
	 *
	 * @param project LogicalProject on the LHS of the semi/anti join
	 * @param join the semi/anti join
	 * @return the modified semi/anti join condition
	 */
	private RexNode adjustCondition(LogicalProject project, Join join) {
		// create two RexPrograms -- the bottom one representing a
		// concatenation of the project and the RHS of the semi/anti join and the
		// top one representing the semi/anti join condition

		RexBuilder rexBuilder = project.getCluster().getRexBuilder();
		RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
		RelNode rightChild = join.getRight();

		// for the bottom RexProgram, the input is a concatenation of the
		// child of the project and the RHS of the semi/anti join
		RelDataType bottomInputRowType =
				SqlValidatorUtil.deriveJoinRowType(
						project.getInput().getRowType(),
						rightChild.getRowType(),
						JoinRelType.INNER,
						typeFactory,
						null,
						join.getSystemFieldList());
		RexProgramBuilder bottomProgramBuilder =
				new RexProgramBuilder(bottomInputRowType, rexBuilder);

		// add the project expressions, then add input references for the RHS
		// of the semi/anti join
		for (Pair<RexNode, String> pair : project.getNamedProjects()) {
			bottomProgramBuilder.addProject(pair.left, pair.right);
		}
		int nLeftFields = project.getInput().getRowType().getFieldCount();
		List<RelDataTypeField> rightFields =
				rightChild.getRowType().getFieldList();
		int nRightFields = rightFields.size();
		for (int i = 0; i < nRightFields; i++) {
			final RelDataTypeField field = rightFields.get(i);
			RexNode inputRef =
					rexBuilder.makeInputRef(
							field.getType(), i + nLeftFields);
			bottomProgramBuilder.addProject(inputRef, field.getName());
		}
		RexProgram bottomProgram = bottomProgramBuilder.getProgram();

		// input rowtype into the top program is the concatenation of the
		// project and the RHS of the semi/anti join
		RelDataType topInputRowType =
				SqlValidatorUtil.deriveJoinRowType(
						project.getRowType(),
						rightChild.getRowType(),
						JoinRelType.INNER,
						typeFactory,
						null,
						join.getSystemFieldList());
		RexProgramBuilder topProgramBuilder =
				new RexProgramBuilder(
						topInputRowType,
						rexBuilder);
		topProgramBuilder.addIdentity();
		topProgramBuilder.addCondition(join.getCondition());
		RexProgram topProgram = topProgramBuilder.getProgram();

		// merge the programs and expand out the local references to form
		// the new semi/anti join condition; it now references a concatenation of
		// the project's child and the RHS of the semi/anti join
		RexProgram mergedProgram =
				RexProgramBuilder.mergePrograms(
						topProgram,
						bottomProgram,
						rexBuilder);

		return mergedProgram.expandLocalRef(
				mergedProgram.getCondition());
	}
}

// End FlinkSemiAntiJoinProjectTransposeRule.java
