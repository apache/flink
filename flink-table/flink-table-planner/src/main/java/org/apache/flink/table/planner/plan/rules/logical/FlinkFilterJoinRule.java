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
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

/**
 * Planner rule that pushes filters above and within a join node into the join node and/or its
 * children nodes.
 *
 * <p>This rule is copied from {@link FilterJoinRule}.
 *
 * <p>Different from {@link FilterJoinRule}, this rule can handle more cases: for the above filter
 * of inner/left/right join or the join condition of inner join, the predicate which field
 * references are all from one side join condition can be pushed into another join side. Such as:
 * <li>SELECT * FROM MyTable1 join MyTable2 ON a1 = a2 AND a1 = 2
 * <li>SELECT * FROM MyTable1, MyTable2 WHERE a1 = a2 AND a1 = 2
 */
public abstract class FlinkFilterJoinRule<C extends FlinkFilterJoinRule.Config> extends RelRule<C>
        implements TransformationRule {

    public static final FlinkFilterIntoJoinRule FILTER_INTO_JOIN =
            FlinkFilterIntoJoinRule.FlinkFilterIntoJoinRuleConfig.DEFAULT.toRule();
    public static final FlinkJoinConditionPushRule JOIN_CONDITION_PUSH =
            FlinkJoinConditionPushRule.FlinkFilterJoinRuleConfig.DEFAULT.toRule();

    // For left/right join, not all filter conditions support push to another side after deduction.
    // This set specifies the supported filter conditions.
    public static final Set<SqlKind> SUITABLE_FILTER_TO_PUSH =
            new HashSet() {
                {
                    add(SqlKind.EQUALS);
                    add(SqlKind.GREATER_THAN);
                    add(SqlKind.GREATER_THAN_OR_EQUAL);
                    add(SqlKind.LESS_THAN);
                    add(SqlKind.LESS_THAN_OR_EQUAL);
                    add(SqlKind.NOT_EQUALS);
                }
            };

    /** Creates a FilterJoinRule. */
    protected FlinkFilterJoinRule(C config) {
        super(config);
    }

    // ~ Methods ----------------------------------------------------------------

    protected void perform(RelOptRuleCall call, Filter filter, Join join) {
        final List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        final List<RexNode> origJoinFilters =
                com.google.common.collect.ImmutableList.copyOf(joinFilters);

        // If there is only the joinRel,
        // make sure it does not match a cartesian product joinRel
        // (with "true" condition), otherwise this rule will be applied
        // again on the new cartesian product joinRel.
        if (filter == null && joinFilters.isEmpty()) {
            return;
        }

        final List<RexNode> aboveFilters =
                filter != null ? getConjunctions(filter) : new ArrayList<>();
        final com.google.common.collect.ImmutableList<RexNode> origAboveFilters =
                com.google.common.collect.ImmutableList.copyOf(aboveFilters);

        // Simplify Outer Joins
        JoinRelType joinType = join.getJoinType();
        if (config.isSmart()
                && !origAboveFilters.isEmpty()
                && join.getJoinType() != JoinRelType.INNER) {
            joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
        }

        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();

        // TODO - add logic to derive additional filters.  E.g., from
        // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
        // derive table filters:
        // (t1.a = 1 OR t1.b = 3)
        // (t2.a = 2 OR t2.b = 4)

        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        boolean filterPushed = false;
        if (RelOptUtil.classifyFilters(
                join,
                aboveFilters,
                joinType.canPushIntoFromAbove(),
                joinType.canPushLeftFromAbove(),
                joinType.canPushRightFromAbove(),
                joinFilters,
                leftFilters,
                rightFilters)) {
            filterPushed = true;
        }

        // Move join filters up if needed
        validateJoinFilters(aboveFilters, joinFilters, join, joinType);

        // If no filter got pushed after validate, reset filterPushed flag
        if (leftFilters.isEmpty()
                && rightFilters.isEmpty()
                && joinFilters.size() == origJoinFilters.size()
                && aboveFilters.size() == origAboveFilters.size()) {
            if (com.google.common.collect.Sets.newHashSet(joinFilters)
                    .equals(com.google.common.collect.Sets.newHashSet(origJoinFilters))) {
                filterPushed = false;
            }
        }

        // Try to push down filters in ON clause. A ON clause filter can only be
        // pushed down if it does not affect the non-matching set, i.e. it is
        // not on the side which is preserved.

        // Anti-join on conditions can not be pushed into left or right, e.g. for plan:
        //
        //     Join(condition=[AND(cond1, $2)], joinType=[anti])
        //     :  - prj(f0=[$0], f1=[$1], f2=[$2])
        //     :  - prj(f0=[$0])
        //
        // The semantic would change if join condition $2 is pushed into left,
        // that is, the result set may be smaller. The right can not be pushed
        // into for the same reason.
        if (RelOptUtil.classifyFilters(
                join,
                joinFilters,
                false,
                joinType.canPushLeftFromWithin(),
                joinType.canPushRightFromWithin(),
                joinFilters,
                leftFilters,
                rightFilters)) {
            filterPushed = true;
        }

        // if nothing actually got pushed and there is nothing leftover,
        // then this rule is a no-op
        if ((!filterPushed && joinType == join.getJoinType())
                || (joinFilters.isEmpty() && leftFilters.isEmpty() && rightFilters.isEmpty())) {
            return;
        }

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        // create the new join node referencing the new children and
        // containing its new join filters (if there are any)
        final com.google.common.collect.ImmutableList<RelDataType> fieldTypes =
                com.google.common.collect.ImmutableList.<RelDataType>builder()
                        .addAll(RelOptUtil.getFieldTypeList(join.getLeft().getRowType()))
                        .addAll(RelOptUtil.getFieldTypeList(join.getRight().getRowType()))
                        .build();
        final RexNode joinFilter =
                RexUtil.composeConjunction(
                        rexBuilder, RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

        // push above filters to another side for INNER, LEFT, RIGHT join
        pushFiltersToAnotherSide(
                join,
                joinType,
                origAboveFilters,
                joinFilter,
                leftFilters,
                rightFilters,
                Arrays.asList(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT));

        // push join filters to another side for INNER join
        pushFiltersToAnotherSide(
                join,
                joinType,
                origJoinFilters,
                null, // do not derive JoinInfo
                leftFilters,
                rightFilters,
                Collections.singletonList(JoinRelType.INNER));

        // create Filters on top of the children if any filters were
        // pushed to them
        final RelBuilder relBuilder = call.builder();
        final RelNode leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
        final RelNode rightRel = relBuilder.push(join.getRight()).filter(rightFilters).build();

        // If nothing actually got pushed and there is nothing leftover,
        // then this rule is a no-op
        if (joinFilter.isAlwaysTrue()
                && leftFilters.isEmpty()
                && rightFilters.isEmpty()
                && joinType == join.getJoinType()) {
            return;
        }

        RelNode newJoinRel =
                join.copy(
                        join.getTraitSet(),
                        joinFilter,
                        leftRel,
                        rightRel,
                        joinType,
                        join.isSemiJoinDone());
        call.getPlanner().onCopy(join, newJoinRel);
        if (!leftFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, leftRel);
        }
        if (!rightFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, rightRel);
        }

        relBuilder.push(newJoinRel);

        // Create a project on top of the join if some of the columns have become
        // NOT NULL due to the join-type getting stricter.
        relBuilder.convert(join.getRowType(), false);

        // create a FilterRel on top of the join if needed
        relBuilder.filter(
                RexUtil.fixUp(
                        rexBuilder,
                        aboveFilters,
                        RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
        call.transformTo(relBuilder.build());
    }

    /**
     * Get conjunctions of filter's condition but with collapsed {@code IS NOT DISTINCT FROM}
     * expressions if needed.
     *
     * @param filter filter containing condition
     * @return condition conjunctions with collapsed {@code IS NOT DISTINCT FROM} expressions if any
     * @see RelOptUtil#conjunctions(RexNode)
     */
    private List<RexNode> getConjunctions(Filter filter) {
        List<RexNode> conjunctions = conjunctions(filter.getCondition());
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        for (int i = 0; i < conjunctions.size(); i++) {
            RexNode node = conjunctions.get(i);
            if (node instanceof RexCall) {
                conjunctions.set(
                        i,
                        RelOptUtil.collapseExpandedIsNotDistinctFromExpr(
                                (RexCall) node, rexBuilder));
            }
        }
        return conjunctions;
    }

    /**
     * Validates that target execution framework can satisfy join filters.
     *
     * <p>If the join filter cannot be satisfied (for example, if it is {@code l.c1 > r.c2} and the
     * join only supports equi-join), removes the filter from {@code joinFilters} and adds it to
     * {@code aboveFilters}.
     *
     * <p>The default implementation does nothing; i.e. the join can handle all conditions.
     *
     * @param aboveFilters Filter above Join
     * @param joinFilters Filters in join condition
     * @param join Join
     * @param joinType JoinRelType could be different from type in Join due to outer join
     *     simplification.
     */
    protected void validateJoinFilters(
            List<RexNode> aboveFilters,
            List<RexNode> joinFilters,
            Join join,
            JoinRelType joinType) {
        final Iterator<RexNode> filterIter = joinFilters.iterator();
        while (filterIter.hasNext()) {
            RexNode exp = filterIter.next();
            // Do not pull up filter conditions for semi/anti join.
            if (!config.getPredicate().apply(join, joinType, exp) && joinType.projectsRight()) {
                aboveFilters.add(exp);
                filterIter.remove();
            }
        }
    }

    private void pushFiltersToAnotherSide(
            Join joinRel,
            JoinRelType joinType,
            List<RexNode> filtersToPush,
            @Nullable RexNode joinFilter,
            List<RexNode> leftFilters,
            List<RexNode> rightFilters,
            List<JoinRelType> expectedJoinTypes) {
        if (filtersToPush.isEmpty() || !expectedJoinTypes.contains(joinType)) {
            return;
        }

        JoinInfo joinInfo = joinRel.analyzeCondition();
        if (joinInfo.leftSet().isEmpty()) {
            if (joinFilter == null) {
                return;
            }
            // build the new JoinInfo from the join filter if the original JoinInfo has empty keys
            joinInfo = JoinInfo.of(joinRel.getLeft(), joinRel.getRight(), joinFilter);
            if (joinInfo.leftSet().isEmpty()) {
                return;
            }
        }

        int leftFieldCnt = joinRel.getLeft().getRowType().getFieldList().size();
        ImmutableBitSet rightKeyBitsWithOffset =
                ImmutableBitSet.of(
                        joinInfo.rightKeys.stream()
                                .map(i -> i + leftFieldCnt)
                                .collect(Collectors.toList()));

        for (RexNode filter : filtersToPush) {
            final RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(filter);
            final ImmutableBitSet inputBits = inputFinder.build();
            if (!isSuitableFilterToPush(filter, joinType)) {
                continue;
            }

            if (joinInfo.leftSet().contains(inputBits)) {
                final RexNode shiftedFilter =
                        remapFilter(
                                joinInfo.leftKeys,
                                joinInfo.rightKeys,
                                joinRel.getRight().getRowType(),
                                filter);
                if (!rightFilters.contains(shiftedFilter)) {
                    rightFilters.add(shiftedFilter);
                }
            } else if (rightKeyBitsWithOffset.contains(inputBits)) {
                ImmutableIntList rightKeysWithOffset =
                        ImmutableIntList.copyOf(
                                joinInfo.rightKeys.stream()
                                        .map(i -> i + leftFieldCnt)
                                        .collect(Collectors.toList()));
                final RexNode shiftedFilter =
                        remapFilter(
                                rightKeysWithOffset,
                                joinInfo.leftKeys,
                                joinRel.getLeft().getRowType(),
                                filter);
                if (!leftFilters.contains(shiftedFilter)) {
                    leftFilters.add(shiftedFilter);
                }
            }
        }
    }

    private boolean isSuitableFilterToPush(RexNode filter, JoinRelType joinType) {
        if (filter.isAlwaysTrue()) {
            return false;
        }
        if (joinType == JoinRelType.INNER) {
            return true;
        }
        // For left/right outer join, now, we only support to push special condition in set
        // SUITABLE_FILTER_TO_PUSH to other side. Take left outer join and IS_NULL condition as an
        // example, If the join right side contains an IS_NULL filter, while we try to push it to
        // the join left side and the left side have any other filter on this column, which will
        // conflict and generate wrong plan.
        if ((joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT)
                && filter instanceof RexCall) {
            RexCall rexCall = (RexCall) filter;
            if (SUITABLE_FILTER_TO_PUSH.contains(rexCall.op.kind)
                    && (rexCall.getOperands().get(0) instanceof RexLiteral
                            || rexCall.getOperands().get(1) instanceof RexLiteral)) {
                return true;
            }
        }
        return false;
    }

    private RexNode remapFilter(
            ImmutableIntList oldKeys,
            ImmutableIntList newKeys,
            RelDataType newInputType,
            RexNode filter) {
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < oldKeys.size(); ++i) {
            mapping.put(oldKeys.get(i), newKeys.get(i));
        }
        RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int newIndex = mapping.getOrDefault(inputRef.getIndex(), -1);
                        if (newIndex < 0) {
                            throw new TableException("should not happen");
                        }
                        return new RexInputRef(
                                newIndex, newInputType.getFieldList().get(newIndex).getType());
                    }
                };
        return filter.accept(shuttle);
    }

    /** Rule that pushes parts of the join condition to its inputs. */
    public static class FlinkJoinConditionPushRule
            extends FlinkFilterJoinRule<FlinkJoinConditionPushRule.FlinkFilterJoinRuleConfig> {
        /** Creates a JoinConditionPushRule. */
        protected FlinkJoinConditionPushRule(FlinkFilterJoinRuleConfig config) {
            super(config);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            Join join = call.rel(0);
            return !isEventTimeTemporalJoin(join.getCondition()) && super.matches(call);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Join join = call.rel(0);
            perform(call, null, join);
        }

        /** Rule configuration. */
        @Value.Immutable(singleton = false)
        @Value.Style(
                get = {"is*", "get*"},
                init = "with*",
                defaults = @Value.Immutable(copy = false))
        public interface FlinkFilterJoinRuleConfig extends FlinkFilterJoinRule.Config {
            FlinkFilterJoinRuleConfig DEFAULT =
                    ImmutableFlinkFilterJoinRuleConfig.of((join, joinType, exp) -> true)
                            .withOperandSupplier(b -> b.operand(Join.class).anyInputs())
                            .withSmart(true);

            @Override
            default FlinkJoinConditionPushRule toRule() {
                return new FlinkJoinConditionPushRule(this);
            }
        }
    }

    /**
     * Rule that tries to push filter expressions into a join condition and into the inputs of the
     * join.
     *
     * <p>Note: It never pushes a filter into an event time temporal join in streaming.
     *
     * @see CoreRules#FILTER_INTO_JOIN
     */
    public static class FlinkFilterIntoJoinRule
            extends FlinkFilterJoinRule<FlinkFilterIntoJoinRule.FlinkFilterIntoJoinRuleConfig> {
        /** Creates a FilterIntoJoinRule. */
        protected FlinkFilterIntoJoinRule(FlinkFilterIntoJoinRuleConfig config) {
            super(config);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            Join join = call.rel(1);
            return !isEventTimeTemporalJoin(join.getCondition()) && super.matches(call);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            Join join = call.rel(1);
            perform(call, filter, join);
        }

        /** Rule configuration. */
        @Value.Immutable(singleton = false)
        @Value.Style(
                get = {"is*", "get*"},
                init = "with*",
                defaults = @Value.Immutable(copy = false))
        public interface FlinkFilterIntoJoinRuleConfig extends FlinkFilterJoinRule.Config {
            FlinkFilterIntoJoinRuleConfig DEFAULT =
                    ImmutableFlinkFilterIntoJoinRuleConfig.of((join, joinType, exp) -> true)
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(Filter.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(Join.class)
                                                                            .anyInputs()))
                            .withSmart(true);

            @Override
            default FlinkFilterIntoJoinRule toRule() {
                return new FlinkFilterIntoJoinRule(this);
            }
        }
    }

    protected boolean isEventTimeTemporalJoin(RexNode joinCondition) {
        RexVisitor<Void> temporalConditionFinder =
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        if (call.getOperator() == TemporalJoinUtil.INITIAL_TEMPORAL_JOIN_CONDITION()
                                && TemporalJoinUtil.isInitialRowTimeTemporalTableJoin(call)) {
                            throw new Util.FoundOne(call);
                        }
                        return super.visitCall(call);
                    }
                };
        try {
            joinCondition.accept(temporalConditionFinder);
        } catch (Util.FoundOne found) {
            return true;
        }
        return false;
    }

    /**
     * Predicate that returns whether a filter is valid in the ON clause of a join for this
     * particular kind of join. If not, Calcite will push it back to above the join.
     */
    @FunctionalInterface
    public interface Predicate {
        boolean apply(Join join, JoinRelType joinType, RexNode exp);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        /** Whether to try to strengthen join-type, default false. */
        @Value.Default
        default boolean isSmart() {
            return false;
        }

        /** Sets {@link #isSmart()}. */
        Config withSmart(boolean smart);

        /**
         * Predicate that returns whether a filter is valid in the ON clause of a join for this
         * particular kind of join. If not, Calcite will push it back to above the join.
         */
        @Value.Parameter
        Predicate getPredicate();

        /** Sets {@link #getPredicate()}. */
        Config withPredicate(Predicate predicate);
    }
}
