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

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule is copied from Calcite's {@link
 * org.apache.calcite.rel.rules.FilterAggregateTransposeRule}. Modification: - Only when the filter
 * satisfies deterministic semantics, it will be allowed to be pushed down.
 */

/**
 * Planner rule that pushes a {@link Filter} past a {@link Aggregate}.
 *
 * @see AggregateFilterTransposeRule
 * @see CoreRules#FILTER_AGGREGATE_TRANSPOSE
 */
@Value.Enclosing
public class FlinkFilterAggregateTransposeRule
        extends RelRule<FlinkFilterAggregateTransposeRule.Config> implements TransformationRule {
    public static final RelOptRule INSTANCE = new FlinkFilterAggregateTransposeRule(Config.DEFAULT);

    /** Creates a FlinkFilterAggregateTransposeRule. */
    protected FlinkFilterAggregateTransposeRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkFilterAggregateTransposeRule(
            Class<? extends Filter> filterClass,
            RelBuilderFactory relBuilderFactory,
            Class<? extends Aggregate> aggregateClass) {
        this(
                Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(Config.class)
                        .withOperandFor(filterClass, aggregateClass));
    }

    @Deprecated // to be removed before 2.0
    protected FlinkFilterAggregateTransposeRule(
            RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory) {
        this(
                Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .withOperandSupplier(b -> b.exactly(operand))
                        .as(Config.class));
    }

    @Deprecated // to be removed before 2.0
    public FlinkFilterAggregateTransposeRule(
            Class<? extends Filter> filterClass,
            RelFactories.FilterFactory filterFactory,
            Class<? extends Aggregate> aggregateClass) {
        this(filterClass, RelBuilder.proto(Contexts.of(filterFactory)), aggregateClass);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filterRel = call.rel(0);
        final Aggregate aggRel = call.rel(1);

        final List<RexNode> conditions = RelOptUtil.conjunctions(filterRel.getCondition());
        final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        final List<RelDataTypeField> origFields = aggRel.getRowType().getFieldList();
        final int[] adjustments = new int[origFields.size()];
        int j = 0;
        for (int i : aggRel.getGroupSet()) {
            adjustments[j] = i - j;
            j++;
        }
        final List<RexNode> pushedConditions = new ArrayList<>();
        final List<RexNode> remainingConditions = new ArrayList<>();

        for (RexNode condition : conditions) {
            ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
            if (canPush(aggRel, rCols) && RexUtil.isDeterministic(condition)) {
                pushedConditions.add(
                        condition.accept(
                                new RelOptUtil.RexInputConverter(
                                        rexBuilder,
                                        origFields,
                                        aggRel.getInput(0).getRowType().getFieldList(),
                                        adjustments)));
            } else {
                remainingConditions.add(condition);
            }
        }

        final RelBuilder builder = call.builder();
        RelNode rel = builder.push(aggRel.getInput()).filter(pushedConditions).build();
        if (rel == aggRel.getInput(0)) {
            return;
        }
        rel = aggRel.copy(aggRel.getTraitSet(), ImmutableList.of(rel));
        rel = builder.push(rel).filter(remainingConditions).build();
        call.transformTo(rel);
    }

    private static boolean canPush(Aggregate aggregate, ImmutableBitSet rCols) {
        // If the filter references columns not in the group key, we cannot push
        final ImmutableBitSet groupKeys =
                ImmutableBitSet.range(0, aggregate.getGroupSet().cardinality());
        if (!groupKeys.contains(rCols)) {
            return false;
        }

        if (aggregate.getGroupType() != Group.SIMPLE) {
            // If grouping sets are used, the filter can be pushed if
            // the columns referenced in the predicate are present in
            // all the grouping sets.
            for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
                if (!groupingSet.contains(rCols)) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableFlinkFilterAggregateTransposeRule.Config.builder()
                        .build()
                        .withOperandFor(Filter.class, Aggregate.class);

        @Override
        default FlinkFilterAggregateTransposeRule toRule() {
            return new FlinkFilterAggregateTransposeRule(this);
        }

        /** Defines an operand tree for the given 2 classes. */
        default Config withOperandFor(
                Class<? extends Filter> filterClass, Class<? extends Aggregate> aggregateClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(filterClass)
                                            .oneInput(b1 -> b1.operand(aggregateClass).anyInputs()))
                    .as(Config.class);
        }

        /** Defines an operand tree for the given 3 classes. */
        default Config withOperandFor(
                Class<? extends Filter> filterClass,
                Class<? extends Aggregate> aggregateClass,
                Class<? extends RelNode> relClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(filterClass)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(aggregateClass)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    relClass)
                                                                                            .anyInputs())))
                    .as(Config.class);
        }
    }
}
