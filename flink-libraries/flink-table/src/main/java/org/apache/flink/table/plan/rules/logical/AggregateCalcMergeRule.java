/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that recognizes a {@link org.apache.calcite.rel.core.Aggregate}
 * on top of a {@link org.apache.calcite.rel.core.Calc} and if possible
 * aggregate through the calc or removes the calc.
 *
 * <p>This is only possible when no condition in calc and the grouping expressions and arguments to
 * the aggregate functions are field references (i.e. not expressions).
 *
 * <p>In some cases, this rule has the effect of trimming: the aggregate will
 * use fewer columns than the project did.
 */
public class AggregateCalcMergeRule extends RelOptRule {
	public static final AggregateCalcMergeRule INSTANCE =
			new AggregateCalcMergeRule(Aggregate.class, Calc.class,
					RelFactories.LOGICAL_BUILDER);

	public AggregateCalcMergeRule(
			Class<? extends Aggregate> aggregateClass,
			Class<? extends Calc> calcClass,
			RelBuilderFactory relBuilderFactory) {
		super(
				operand(aggregateClass,
						operand(calcClass, any())),
				relBuilderFactory, null);
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		final Calc calc = call.rel(1);
		return calc.getProgram().getCondition() == null;
	}

	public void onMatch(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		final Calc calc = call.rel(1);
		final RexProgram program = calc.getProgram();
		final List<RexNode> projects = new ArrayList<>();
		for (RexLocalRef localRef : program.getProjectList()) {
			projects.add(program.expandLocalRef(localRef));
		}
		final Project project = LogicalProject.create(calc.getInput(), projects, calc.getRowType());
		RelNode x = AggregateProjectMergeRule.apply(call, aggregate, project);
		if (x != null) {
			call.transformTo(x);
		}
	}
}

// End AggregateCalcMergeRule.java

