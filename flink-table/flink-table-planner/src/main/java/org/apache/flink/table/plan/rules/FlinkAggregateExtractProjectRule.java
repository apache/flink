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

package org.apache.flink.table.plan.rules;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate;

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
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

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

	@Override
	public boolean matches(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		return aggregate instanceof LogicalWindowAggregate || aggregate instanceof LogicalAggregate;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final Aggregate aggregate = call.rel(0);
		final RelNode input = call.rel(1);
		final RelBuilder relBuilder = call.builder().push(input);

		Tuple2<List<RexNode>, Mapping> projectsAndMapping =
			extractProjectsAndMapping(aggregate, input, relBuilder);
		final List<RexNode> projects = projectsAndMapping.f0;

		if (input instanceof Project && input.getRowType().getFieldCount() == projects.size()) {
			// Avoid extracting a trivial Project which simply projects all fields of the input Project.
			return;
		}
		relBuilder.project(projects, new LinkedList<>(), true);

		RelNode newAggregate = getNewAggregate(aggregate, relBuilder, projectsAndMapping.f1);
		call.transformTo(newAggregate);
	}

	/**
	 * Extract projects from the Aggregate and return the index mapping between the new projects
	 * and it's input.
	 */
	private Tuple2<List<RexNode>, Mapping> extractProjectsAndMapping(
		Aggregate aggregate, RelNode input, RelBuilder relBuilder) {

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
		return Tuple2.of(projects, mapping);
	}

	private RelNode getNewAggregate(Aggregate oldAggregate, RelBuilder relBuilder, Mapping mapping) {

		final ImmutableBitSet newGroupSet =
			Mappings.apply(mapping, oldAggregate.getGroupSet());

		final Iterable<ImmutableBitSet> newGroupSets =
			oldAggregate.getGroupSets().stream()
				.map(bitSet -> Mappings.apply(mapping, bitSet))
				.collect(Collectors.toList());

		final List<RelBuilder.AggCall> newAggCallList =
			getNewAggCallList(oldAggregate, relBuilder, mapping);

		final RelBuilder.GroupKey groupKey =
			relBuilder.groupKey(newGroupSet, newGroupSets);

		if (oldAggregate instanceof LogicalWindowAggregate) {
			relBuilder.aggregate(groupKey, newAggCallList);
			Aggregate newAggregate = (Aggregate) relBuilder.build();
			LogicalWindowAggregate oldLogicalWindowAggregate = (LogicalWindowAggregate) oldAggregate;
			return LogicalWindowAggregate.create(
					oldLogicalWindowAggregate.getWindow(),
					oldLogicalWindowAggregate.getNamedProperties(),
					newAggregate);
		} else {
			relBuilder.aggregate(groupKey, newAggCallList);
			return relBuilder.build();
		}
	}

	private int getWindowTimeFieldIndex(LogicalWindowAggregate aggregate, RelNode input) {
		LogicalWindow logicalWindow = aggregate.getWindow();
		ResolvedFieldReference timeAttribute = (ResolvedFieldReference) logicalWindow.timeAttribute();
		return input.getRowType().getFieldNames().indexOf(timeAttribute.name());
	}

	private List<RelBuilder.AggCall> getNewAggCallList(
		Aggregate oldAggregate, RelBuilder relBuilder, Mapping mapping) {

		final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();

		for (AggregateCall aggCall : oldAggregate.getAggCallList()) {
			final RexNode filterArg = aggCall.filterArg < 0 ? null
				: relBuilder.field(Mappings.apply(mapping, aggCall.filterArg));
			newAggCallList.add(
				relBuilder
					.aggregateCall(
						aggCall.getAggregation(),
						relBuilder.fields(Mappings.apply2(mapping, aggCall.getArgList())))
					.distinct(aggCall.isDistinct())
					.filter(filterArg)
					.approximate(aggCall.isApproximate())
					.sort(relBuilder.fields(aggCall.collation))
					.as(aggCall.name));
		}
		return newAggCallList;
	}
}
