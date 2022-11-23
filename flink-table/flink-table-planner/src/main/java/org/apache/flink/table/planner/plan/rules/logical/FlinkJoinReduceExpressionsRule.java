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

import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * This rule is copied from Calcite's {@link
 * org.apache.calcite.rel.rules.ReduceExpressionsRule.JoinReduceExpressionsRule}. Modification: Do
 * not reduce constants in join conditions when it is a temporal join.
 */

/**
 * Rule that reduces constants inside a {@link Join}.
 *
 * @see CoreRules#JOIN_REDUCE_EXPRESSIONS
 */
public class FlinkJoinReduceExpressionsRule
        extends ReduceExpressionsRule<FlinkJoinReduceExpressionsRule.Config> {

    public static final FlinkJoinReduceExpressionsRule INSTANCE = Config.DEFAULT.toRule();

    /** Creates a JoinReduceExpressionsRule. */
    protected FlinkJoinReduceExpressionsRule(FlinkJoinReduceExpressionsRule.Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinReduceExpressionsRule(
            Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
        this(
                FlinkJoinReduceExpressionsRule.Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(FlinkJoinReduceExpressionsRule.Config.class)
                        .withOperandFor(joinClass)
                        .withMatchNullability(true)
                        .as(FlinkJoinReduceExpressionsRule.Config.class));
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinReduceExpressionsRule(
            Class<? extends Join> joinClass,
            boolean matchNullability,
            RelBuilderFactory relBuilderFactory) {
        this(
                FlinkJoinReduceExpressionsRule.Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(FlinkJoinReduceExpressionsRule.Config.class)
                        .withOperandFor(joinClass)
                        .withMatchNullability(matchNullability)
                        .as(FlinkJoinReduceExpressionsRule.Config.class));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final List<RexNode> expList = Lists.newArrayList(join.getCondition());
        final int fieldCount = join.getLeft().getRowType().getFieldCount();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final RelOptPredicateList leftPredicates = mq.getPulledUpPredicates(join.getLeft());
        final RelOptPredicateList rightPredicates = mq.getPulledUpPredicates(join.getRight());
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RelOptPredicateList predicates =
                leftPredicates.union(rexBuilder, rightPredicates.shift(rexBuilder, fieldCount));
        if (TemporalJoinUtil.containsInitialTemporalJoinCondition(join.getCondition())
                || !reduceExpressions(join, expList, predicates, true, config.matchNullability())) {
            return;
        }
        call.transformTo(
                join.copy(
                        join.getTraitSet(),
                        expList.get(0),
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone()));

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(join);
    }

    /** Rule configuration. */
    public interface Config extends ReduceExpressionsRule.Config {
        FlinkJoinReduceExpressionsRule.Config DEFAULT =
                EMPTY.as(FlinkJoinReduceExpressionsRule.Config.class)
                        .withMatchNullability(false)
                        .withOperandFor(Join.class)
                        .withDescription("ReduceExpressionsRule(Join)")
                        .as(FlinkJoinReduceExpressionsRule.Config.class);

        @Override
        default FlinkJoinReduceExpressionsRule toRule() {
            return new FlinkJoinReduceExpressionsRule(this);
        }
    }
}
