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

import org.apache.flink.table.expressions.PlannerResolvedFieldReference;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.rel.LogicalTableAggregate;
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate;
import org.apache.flink.table.plan.logical.rel.LogicalWindowTableAggregate;
import org.apache.flink.table.plan.logical.rel.TableAggregate;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule to extract a {@link org.apache.calcite.rel.core.Project} from a {@link LogicalAggregate},
 * a {@link LogicalWindowAggregate} or a {@link TableAggregate} and push it down towards
 * the input.
 *
 * <p>Note: Most of the logic in this rule is same with {@link AggregateExtractProjectRule}. The
 * difference is this rule has also taken the {@link LogicalWindowAggregate} and
 * {@link TableAggregate} into consideration. Furthermore, this rule also creates trivial
 * {@link Project}s unless the input node is already a {@link Project}.
 */
public class ExtendedAggregateExtractProjectRule extends AggregateExtractProjectRule {

	public static final ExtendedAggregateExtractProjectRule INSTANCE =
		new ExtendedAggregateExtractProjectRule(
			operand(SingleRel.class,
				operand(RelNode.class, any())), RelFactories.LOGICAL_BUILDER);

	public ExtendedAggregateExtractProjectRule(
		RelOptRuleOperand operand,
		RelBuilderFactory builderFactory) {

		super(operand, builderFactory);
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		final SingleRel relNode = call.rel(0);
		return relNode instanceof LogicalWindowAggregate ||
			relNode instanceof LogicalAggregate ||
			relNode instanceof TableAggregate;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final RelNode relNode = call.rel(0);
		final RelNode input = call.rel(1);
		final RelBuilder relBuilder = call.builder().push(input);

		if (relNode instanceof Aggregate) {
			call.transformTo(performExtractForAggregate((Aggregate) relNode, input, relBuilder));
		} else if (relNode instanceof TableAggregate) {
			call.transformTo(performExtractForTableAggregate((TableAggregate) relNode, input, relBuilder));
		}
	}

	/**
	 * Extract a project from the input aggregate and return a new aggregate.
	 */
	private RelNode performExtractForAggregate(Aggregate aggregate, RelNode input, RelBuilder relBuilder) {
		Mapping mapping = extractProjectsAndMapping(aggregate, input, relBuilder);
		return getNewAggregate(aggregate, relBuilder, mapping);
	}

	/**
	 * Extract a project from the input table aggregate and return a new table aggregate.
	 */
	private RelNode performExtractForTableAggregate(TableAggregate aggregate, RelNode input, RelBuilder relBuilder) {
		RelNode newAggregate = performExtractForAggregate(aggregate.getCorrespondingAggregate(), input, relBuilder);
		if (aggregate instanceof LogicalTableAggregate) {
			return LogicalTableAggregate.create((Aggregate) newAggregate);
		} else {
			return LogicalWindowTableAggregate.create((LogicalWindowAggregate) newAggregate);
		}
	}

	/**
	 * Extract projects from the Aggregate and return the index mapping between the new projects
	 * and it's input.
	 */
	private Mapping extractProjectsAndMapping(
		Aggregate aggregate,
		RelNode input,
		RelBuilder relBuilder) {

		// Compute which input fields are used.
		final ImmutableBitSet.Builder inputFieldsUsed = getInputFieldUsed(aggregate, input);

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

		if (input instanceof Project) {
			// this will not create trivial projects
			relBuilder.project(projects);
		} else {
			relBuilder.project(projects, Collections.emptyList(), true);
		}

		return mapping;
	}

	/**
	 * Compute which input fields are used by the aggregate.
	 */
	private ImmutableBitSet.Builder getInputFieldUsed(Aggregate aggregate, RelNode input) {
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
			inputFieldsUsed.set(getWindowTimeFieldIndex(((LogicalWindowAggregate) aggregate).getWindow(), input));
		}
		return inputFieldsUsed;
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
			if (newGroupSet.size() == 0 && newAggCallList.size() == 0) {
				// Return the old LogicalWindowAggregate directly, as we can't get an empty Aggregate
				// from the relBuilder.
				return oldAggregate;
			} else {
				relBuilder.aggregate(groupKey, newAggCallList);
				Aggregate newAggregate = (Aggregate) relBuilder.build();
				LogicalWindowAggregate oldLogicalWindowAggregate = (LogicalWindowAggregate) oldAggregate;

				return LogicalWindowAggregate.create(
					oldLogicalWindowAggregate.getWindow(),
					oldLogicalWindowAggregate.getNamedProperties(),
					newAggregate);
			}
		} else {
			relBuilder.aggregate(groupKey, newAggCallList);
			return relBuilder.build();
		}
	}

	private int getWindowTimeFieldIndex(LogicalWindow logicalWindow, RelNode input) {
		PlannerResolvedFieldReference timeAttribute = (PlannerResolvedFieldReference) logicalWindow.timeAttribute();
		return input.getRowType().getFieldNames().indexOf(timeAttribute.name());
	}

	private List<RelBuilder.AggCall> getNewAggCallList(
		Aggregate oldAggregate,
		RelBuilder relBuilder,
		Mapping mapping) {

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
