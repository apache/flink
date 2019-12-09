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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.functions.sql.internal.SqlAuxiliaryGroupAggFunction;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule is copied from Calcite's {@link org.apache.calcite.rel.rules.AggregateRemoveRule}.
 * Modification:
 * - only matches aggregate with with SIMPLE group and non-empty group
 * - supports SUM, MIN, MAX, AUXILIARY_GROUP aggregate functions with no filterArgs
 */

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.core.Aggregate}
 * if its aggregate functions are SUM, MIN, MAX, AUXILIARY_GROUP with no filterArgs,
 * and the underlying relational expression is already distinct.
 */
public class FlinkAggregateRemoveRule extends RelOptRule {
	public static final FlinkAggregateRemoveRule INSTANCE =
			new FlinkAggregateRemoveRule(LogicalAggregate.class,
					RelFactories.LOGICAL_BUILDER);

	//~ Constructors -----------------------------------------------------------

	@Deprecated // to be removed before 2.0
	public FlinkAggregateRemoveRule(Class<? extends Aggregate> aggregateClass) {
		this(aggregateClass, RelFactories.LOGICAL_BUILDER);
	}

	/**
	 * Creates an FlinkAggregateRemoveRule.
	 */
	public FlinkAggregateRemoveRule(Class<? extends Aggregate> aggregateClass,
			RelBuilderFactory relBuilderFactory) {
		// REVIEW jvs 14-Mar-2006: We have to explicitly mention the child here
		// to make sure the rule re-fires after the child changes (e.g. via
		// ProjectRemoveRule), since that may change our information
		// about whether the child is distinct.  If we clean up the inference of
		// distinct to make it correct up-front, we can get rid of the reference
		// to the child here.
		super(
				operand(aggregateClass,
						operand(RelNode.class, any())),
				relBuilderFactory, null);
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		final RelNode input = call.rel(1);
		if (aggregate.getGroupCount() == 0 || aggregate.indicator ||
				aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
			return false;
		}
		for (AggregateCall aggCall : aggregate.getAggCallList()) {
			SqlKind aggCallKind = aggCall.getAggregation().getKind();
			// TODO supports more AggregateCalls
			boolean isAllowAggCall = aggCallKind == SqlKind.SUM ||
					aggCallKind == SqlKind.MIN ||
					aggCallKind == SqlKind.MAX ||
					aggCall.getAggregation() instanceof SqlAuxiliaryGroupAggFunction;
			if (!isAllowAggCall || aggCall.filterArg >= 0 || aggCall.getArgList().size() != 1) {
				return false;
			}
		}

		final RelMetadataQuery mq = call.getMetadataQuery();
		return SqlFunctions.isTrue(mq.areColumnsUnique(input, aggregate.getGroupSet()));
	}

	//~ Methods ----------------------------------------------------------------

	public void onMatch(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		final RelNode input = call.rel(1);

		// Distinct is "GROUP BY c1, c2" (where c1, c2 are a set of columns on
		// which the input is unique, i.e. contain a key) and has no aggregate
		// functions or the functions we enumerated. It can be removed.
		final RelNode newInput = convert(input, aggregate.getTraitSet().simplify());

		// If aggregate was projecting a subset of columns, add a project for the
		// same effect.
		final RelBuilder relBuilder = call.builder();
		relBuilder.push(newInput);
		List<Integer> projectIndices = new ArrayList<>(aggregate.getGroupSet().asList());
		for (AggregateCall aggCall : aggregate.getAggCallList()) {
			projectIndices.addAll(aggCall.getArgList());
		}
		relBuilder.project(relBuilder.fields(projectIndices));
		// Create a project if some of the columns have become
		// NOT NULL due to aggregate functions are removed
		relBuilder.convert(aggregate.getRowType(), true);
		call.transformTo(relBuilder.build());
	}
}

// End FlinkAggregateRemoveRule.java
