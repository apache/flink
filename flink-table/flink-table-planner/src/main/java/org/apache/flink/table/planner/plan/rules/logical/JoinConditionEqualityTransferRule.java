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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.EQUALS;

/**
 * Planner rule that converts Join's conditions to the left or right table's own independent filter
 * as much as possible, so that the rules of filter-push-down can push down the filter to below.
 *
 * <p>e.g. join condition: l_a = r_b and l_a = r_c. The l_a is a field from left input, both r_b and
 * r_c are fields from the right input. After rewrite, condition will be: l_a = r_b and r_b = r_c.
 * r_b = r_c can be pushed down to the right input.
 */
@Value.Enclosing
public class JoinConditionEqualityTransferRule
        extends RelRule<JoinConditionEqualityTransferRule.JoinConditionEqualityTransferRuleConfig> {

    public static final JoinConditionEqualityTransferRule INSTANCE =
            JoinConditionEqualityTransferRuleConfig.DEFAULT.toRule();

    protected JoinConditionEqualityTransferRule(JoinConditionEqualityTransferRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.SEMI) {
            return false;
        }

        Tuple2<List<RexNode>, List<RexNode>> partitionJoinFilters = partitionJoinFilters(join);
        List<Set<RexInputRef>> groups = getEquiFilterRelationshipGroup(partitionJoinFilters.f0);
        for (Set<RexInputRef> group : groups) {
            if (group.size() > 2) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        Tuple2<List<RexNode>, List<RexNode>> optimizableAndRemainFilters =
                partitionJoinFilters(join);
        List<RexNode> optimizableFilters = optimizableAndRemainFilters.f0;
        List<RexNode> remainFilters = optimizableAndRemainFilters.f1;
        Map<Boolean, List<Set<RexInputRef>>> partitioned =
                getEquiFilterRelationshipGroup(optimizableFilters).stream()
                        .collect(Collectors.partitioningBy(t -> t.size() > 2));
        List<Set<RexInputRef>> equiFiltersToOpt = partitioned.get(true);
        List<Set<RexInputRef>> equiFiltersNotOpt = partitioned.get(false);

        RelBuilder builder = call.builder();
        RexBuilder rexBuilder = builder.getRexBuilder();
        List<RexNode> newEquiJoinFilters = new ArrayList<>();

        // add equiFiltersNotOpt.
        equiFiltersNotOpt.forEach(
                refs -> {
                    assert (refs.size() == 2);
                    Iterator<RexInputRef> iterator = refs.iterator();
                    newEquiJoinFilters.add(
                            rexBuilder.makeCall(EQUALS, iterator.next(), iterator.next()));
                });

        // new opt filters.
        equiFiltersToOpt.forEach(
                refs -> {
                    // partition to InputRef to left and right.
                    Map<Boolean, List<RexInputRef>> leftAndRightRefs =
                            refs.stream()
                                    .collect(Collectors.partitioningBy(t -> fromJoinLeft(join, t)));
                    List<RexInputRef> leftRefs = leftAndRightRefs.get(true);
                    List<RexInputRef> rightRefs = leftAndRightRefs.get(false);

                    // equals for each other.
                    List<RexNode> rexCalls = new ArrayList<>(makeCalls(rexBuilder, leftRefs));
                    rexCalls.addAll(makeCalls(rexBuilder, rightRefs));

                    // equals for left and right.
                    if (!leftRefs.isEmpty() && !rightRefs.isEmpty()) {
                        rexCalls.add(
                                rexBuilder.makeCall(EQUALS, leftRefs.get(0), rightRefs.get(0)));
                    }

                    // add to newEquiJoinFilters with deduplication.
                    newEquiJoinFilters.addAll(rexCalls);
                });

        remainFilters.add(
                FlinkRexUtil.simplify(
                        rexBuilder,
                        builder.and(newEquiJoinFilters),
                        join.getCluster().getPlanner().getExecutor()));
        RexNode newJoinFilter = builder.and(remainFilters);
        Join newJoin =
                join.copy(
                        join.getTraitSet(),
                        newJoinFilter,
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone());

        call.transformTo(newJoin);
    }

    /** Returns true if the given input ref is from join left, else false. */
    private boolean fromJoinLeft(Join join, RexInputRef ref) {
        assert join.getSystemFieldList().isEmpty();
        return ref.getIndex() < join.getLeft().getRowType().getFieldCount();
    }

    /** Partition join condition to leftRef-rightRef equals and others. */
    private Tuple2<List<RexNode>, List<RexNode>> partitionJoinFilters(Join join) {
        List<RexNode> left = new ArrayList<>();
        List<RexNode> right = new ArrayList<>();
        List<RexNode> conjunctions = RelOptUtil.conjunctions(join.getCondition());
        for (RexNode rexNode : conjunctions) {
            if (rexNode instanceof RexCall) {
                RexCall call = (RexCall) rexNode;
                if (call.isA(SqlKind.EQUALS)) {
                    if (call.operands.get(0) instanceof RexInputRef
                            && call.operands.get(1) instanceof RexInputRef) {
                        RexInputRef ref1 = (RexInputRef) call.operands.get(0);
                        RexInputRef ref2 = (RexInputRef) call.operands.get(1);
                        boolean isLeft1 = fromJoinLeft(join, ref1);
                        boolean isLeft2 = fromJoinLeft(join, ref2);
                        if (isLeft1 != isLeft2) {
                            left.add(rexNode);
                            continue;
                        }
                    }
                }
            }
            right.add(rexNode);
        }
        return Tuple2.of(left, right);
    }

    /** Put fields to a group that have equivalence relationships. */
    private List<Set<RexInputRef>> getEquiFilterRelationshipGroup(List<RexNode> equiJoinFilters) {
        List<Set<RexInputRef>> res = new ArrayList<>();
        for (RexNode rexNode : equiJoinFilters) {
            if (rexNode instanceof RexCall) {
                RexCall call = (RexCall) rexNode;
                if (call.isA(SqlKind.EQUALS)) {
                    RexInputRef left = (RexInputRef) call.operands.get(0);
                    RexInputRef right = (RexInputRef) call.operands.get(1);
                    boolean found = false;
                    for (Set<RexInputRef> refs : res) {
                        if (refs.contains(left) || refs.contains(right)) {
                            refs.add(left);
                            refs.add(right);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        Set<RexInputRef> set = new HashSet<>();
                        set.add(left);
                        set.add(right);
                        res.add(set);
                    }
                }
            }
        }
        return res;
    }

    /** Make calls to a number of inputRefs, make sure that they both have a relationship. */
    private List<RexNode> makeCalls(RexBuilder rexBuilder, List<RexInputRef> nodes) {
        final List<RexNode> calls = new ArrayList<>();
        if (nodes.size() > 1) {
            RexInputRef rex = nodes.get(0);
            nodes.subList(1, nodes.size())
                    .forEach(t -> calls.add(rexBuilder.makeCall(EQUALS, rex, t)));
        }
        return calls;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface JoinConditionEqualityTransferRuleConfig extends RelRule.Config {
        JoinConditionEqualityTransferRule.JoinConditionEqualityTransferRuleConfig DEFAULT =
                ImmutableJoinConditionEqualityTransferRule.JoinConditionEqualityTransferRuleConfig
                        .builder()
                        .description("JoinConditionEqualityTransferRule")
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Join.class).anyInputs());

        @Override
        default JoinConditionEqualityTransferRule toRule() {
            return new JoinConditionEqualityTransferRule(this);
        }
    }
}
