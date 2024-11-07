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
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Planner rule to apply transitive closure on {@link MultiJoin} for equi-join predicates.
 *
 * <p>e.g. MJ(A, B, C) ON A.a1=B.b1 AND B.b1=C.c1 &rarr; MJ(A, B, C) ON A.a1=B.b1 AND B.b1=C.c1 AND
 * A.a1=C.c1
 *
 * <p>The advantage of applying this rule is that it increases the choice of join reorder; at the
 * same time, the disadvantage is that it will use more CPU for additional join predicates.
 */
@Value.Enclosing
public class RewriteMultiJoinConditionRule
        extends RelRule<RewriteMultiJoinConditionRule.RewriteMultiJoinConditionRuleConfig> {

    public static final RewriteMultiJoinConditionRule INSTANCE =
            RewriteMultiJoinConditionRule.RewriteMultiJoinConditionRuleConfig.DEFAULT.toRule();

    private RewriteMultiJoinConditionRule(RewriteMultiJoinConditionRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        MultiJoin multiJoin = call.rel(0);
        // currently only supports all join types are INNER join
        boolean isAllInnerJoin =
                multiJoin.getJoinTypes().stream()
                        .allMatch(joinType -> joinType == JoinRelType.INNER);
        List<RexNode> equiJoinFilters = partitionJoinFilters(multiJoin).f0;
        int numJoinInputs = multiJoin.getInputs().size();
        // If the number of join inputs small than/equals bushy tree threshold, the
        // FlinkBushyJoinReorderRule will be used in FlinkJoinReorderRule. For bushy
        // join reorder rule, this is no need to rewrite multi join condition to
        // avoid increasing the search space.
        int bushyTreeThreshold =
                ShortcutUtils.unwrapContext(multiJoin)
                        .getTableConfig()
                        .get(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD);
        return numJoinInputs > bushyTreeThreshold
                && !multiJoin.isFullOuterJoin()
                && isAllInnerJoin
                && equiJoinFilters.size() > 1;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MultiJoin multiJoin = call.rel(0);
        Tuple2<List<RexNode>, List<RexNode>> partitions = partitionJoinFilters(multiJoin);
        List<RexNode> equiJoinFilters = partitions.f0;
        List<RexNode> nonEquiJoinFilters = partitions.f1;
        // there is no `equals` method in RexCall, so the key of this map should be String
        Map<RexNode, List<RexNode>> equiJoinFilterMap = new HashMap<>();
        equiJoinFilters.stream()
                .filter(node -> node instanceof RexCall)
                .forEach(
                        rexNode -> {
                            Preconditions.checkState(rexNode.isA(SqlKind.EQUALS));
                            RexNode left = ((RexCall) rexNode).getOperands().get(0);
                            RexNode right = ((RexCall) rexNode).getOperands().get(1);
                            equiJoinFilterMap
                                    .computeIfAbsent(left, k -> new ArrayList<>())
                                    .add(right);
                            equiJoinFilterMap
                                    .computeIfAbsent(right, k -> new ArrayList<>())
                                    .add(left);
                        });

        List<List<RexNode>> candidateJoinFilters =
                equiJoinFilterMap.values().stream()
                        .filter(list -> list.size() > 1)
                        .collect(Collectors.toList());
        if (candidateJoinFilters.isEmpty()) {
            // no transitive closure predicates
            return;
        }

        List<RexNode> newEquiJoinFilters = new ArrayList<>(equiJoinFilters);
        RexBuilder rexBuilder = multiJoin.getCluster().getRexBuilder();
        candidateJoinFilters.forEach(
                candidate -> {
                    IntStream.range(0, candidate.size())
                            .forEach(
                                    startIndex -> {
                                        RexNode op1 = candidate.get(startIndex);
                                        List<RexNode> restOps =
                                                candidate.subList(startIndex + 1, candidate.size());
                                        restOps.forEach(
                                                op2 -> {
                                                    RexNode newFilter =
                                                            rexBuilder.makeCall(
                                                                    SqlStdOperatorTable.EQUALS,
                                                                    op1,
                                                                    op2);
                                                    if (!containEquiJoinFilter(
                                                            newFilter, newEquiJoinFilters)) {
                                                        newEquiJoinFilters.add(newFilter);
                                                    }
                                                });
                                    });
                });

        if (newEquiJoinFilters.size() == equiJoinFilters.size()) {
            // no new join filters added
            return;
        }

        RexNode newJoinFilter =
                call.builder()
                        .and(
                                Stream.concat(
                                                newEquiJoinFilters.stream(),
                                                nonEquiJoinFilters.stream())
                                        .collect(Collectors.toList()));
        MultiJoin newMultiJoin =
                new MultiJoin(
                        multiJoin.getCluster(),
                        multiJoin.getInputs(),
                        newJoinFilter,
                        multiJoin.getRowType(),
                        multiJoin.isFullOuterJoin(),
                        multiJoin.getOuterJoinConditions(),
                        multiJoin.getJoinTypes(),
                        multiJoin.getProjFields(),
                        multiJoin.getJoinFieldRefCountsMap(),
                        multiJoin.getPostJoinFilter());

        call.transformTo(newMultiJoin);
    }

    private boolean containEquiJoinFilter(RexNode joinFilter, List<RexNode> equiJoinFiltersList) {
        return equiJoinFiltersList.stream().anyMatch(f -> f.equals(joinFilter));
    }

    /** Partitions MultiJoin condition in equi join filters and non-equi join filters. */
    private Tuple2<List<RexNode>, List<RexNode>> partitionJoinFilters(MultiJoin multiJoin) {
        List<RexNode> joinFilters = RelOptUtil.conjunctions(multiJoin.getJoinFilter());

        Map<Boolean, List<RexNode>> partitioned =
                joinFilters.stream()
                        .collect(Collectors.partitioningBy(filter -> filter.isA(SqlKind.EQUALS)));
        List<RexNode> equiJoinFilters = partitioned.get(true);
        List<RexNode> nonEquiJoinFilters = partitioned.get(false);
        return new Tuple2<>(equiJoinFilters, nonEquiJoinFilters);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface RewriteMultiJoinConditionRuleConfig extends RelRule.Config {
        RewriteMultiJoinConditionRule.RewriteMultiJoinConditionRuleConfig DEFAULT =
                ImmutableRewriteMultiJoinConditionRule.RewriteMultiJoinConditionRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(MultiJoin.class).anyInputs())
                        .withDescription("RewriteMultiJoinConditionRule");

        @Override
        default RewriteMultiJoinConditionRule toRule() {
            return new RewriteMultiJoinConditionRule(this);
        }
    }
}
