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

import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to extract a {@link org.apache.calcite.rel.core.Project}
 * from a {@link LogicalAggregate} or a {@link LogicalWindowAggregate}
 * and push it down towards the input.
 *
 * <p>Note: Most of the logic in this rule is same with {@link AggregateExtractProjectRule}. The
 * difference is this rule has also taken the {@link LogicalWindowAggregate} into consideration.
 */
public class FlinkAggregateExtractProjectRule extends AggregateExtractProjectRule {

	public FlinkAggregateExtractProjectRule(RelOptRuleOperand operand, RelBuilderFactory builderFactory) {
		super(operand, builderFactory);
	}

	private int getWindowTimeFieldIndex(LogicalWindowAggregate aggregate, RelNode input) {
		LogicalWindow logicalWindow = aggregate.getWindow();
		ResolvedFieldReference timeAttribute = (ResolvedFieldReference) logicalWindow.timeAttribute();
		return input.getRowType().getFieldNames().indexOf(timeAttribute.name());
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		return aggregate instanceof LogicalWindowAggregate || aggregate instanceof LogicalAggregate;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		final RelNode input = call.rel(1);
		// Compute which input fields are used.
		// 1. group fields are always used
		final ImmutableBitSet.Builder inputFieldsUsed =
			aggregate.getGroupSet().rebuild();
		// 2. agg functions
		for (AggregateCall aggCall : aggregate.getAggCallList()) {
			for (int i : aggCall.getArgList()) {
				inputFieldsUsed.set(i);
			}
			if (aggCall.filterArg >= 0) {
				inputFieldsUsed.set(aggCall.filterArg);
			}
		}
		// 3. window time field if the aggregate is a group window aggregate.
		if (aggregate instanceof LogicalWindowAggregate) {
			inputFieldsUsed.set(getWindowTimeFieldIndex((LogicalWindowAggregate) aggregate, input));
		}

		final RelBuilder relBuilder = call.builder().push(input);
		final List<RexNode> projects = new ArrayList<>();
		final Mapping mapping =
			Mappings.create(MappingType.INVERSE_SURJECTION,
				aggregate.getInput().getRowType().getFieldCount(),
				inputFieldsUsed.cardinality());
		int j = 0;
		for (int i : inputFieldsUsed.build()) {
			projects.add(relBuilder.field(i));
			mapping.set(i, j++);
		}

		if (input instanceof Project && input.getRowType().getFieldCount() == projects.size()) {
			// Avoid extracting a project same with the input project.
			return;
		}
		relBuilder.project(projects);

		final ImmutableBitSet newGroupSet =
			Mappings.apply(mapping, aggregate.getGroupSet());

		final Iterable<ImmutableBitSet> newGroupSets =
			Iterables.transform(aggregate.getGroupSets(),
				bitSet -> Mappings.apply(mapping, bitSet));
		final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
		for (AggregateCall aggCall : aggregate.getAggCallList()) {
			final ImmutableList<RexNode> args =
				relBuilder.fields(
					Mappings.apply2(mapping, aggCall.getArgList()));
			final RexNode filterArg = aggCall.filterArg < 0 ? null
				: relBuilder.field(Mappings.apply(mapping, aggCall.filterArg));
			newAggCallList.add(
				relBuilder.aggregateCall(aggCall.getAggregation(), args)
					.distinct(aggCall.isDistinct())
					.filter(filterArg)
					.approximate(aggCall.isApproximate())
					.sort(relBuilder.fields(aggCall.collation))
					.as(aggCall.name));
		}

		final RelBuilder.GroupKey groupKey =
			relBuilder.groupKey(newGroupSet, newGroupSets);

		if (aggregate instanceof LogicalWindowAggregate) {
			relBuilder.aggregate(groupKey, newAggCallList);
			Aggregate newAggregate = (Aggregate) relBuilder.build();
			LogicalWindowAggregate oldLogicalWindowAggregate = (LogicalWindowAggregate) aggregate;
			LogicalWindowAggregate newLogicalWindowAggegate =
				LogicalWindowAggregate.create(
					oldLogicalWindowAggregate.getWindow(),
					oldLogicalWindowAggregate.getNamedProperties(),
					newAggregate);
			call.transformTo(newLogicalWindowAggegate);
		} else {
			relBuilder.aggregate(groupKey, newAggCallList);
			call.transformTo(relBuilder.build());
		}
	}
}
