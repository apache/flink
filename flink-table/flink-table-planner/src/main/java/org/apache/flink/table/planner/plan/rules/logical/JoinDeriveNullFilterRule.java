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

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;

import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that filters null values before join if the count null value from join input is
 * greater than null filter threshold.
 *
 * <p>Since the key of the Null value is impossible to match in the inner join, and there is a
 * single point skew due to too many Null values. We should push down a not-null filter into the
 * child node of join.
 */
@Value.Enclosing
public class JoinDeriveNullFilterRule
        extends RelRule<JoinDeriveNullFilterRule.JoinDeriveNullFilterRuleConfig> {

    // To avoid the impact of null values on the single join node,
    // We will add a null filter (possibly be pushed down) before the join to filter
    // null values when the source of InnerJoin has nullCount more than this value.
    public static final Long JOIN_NULL_FILTER_THRESHOLD = 2000000L;

    public static final JoinDeriveNullFilterRule INSTANCE =
            JoinDeriveNullFilterRule.JoinDeriveNullFilterRuleConfig.DEFAULT.toRule();

    private JoinDeriveNullFilterRule(JoinDeriveNullFilterRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        return join.getJoinType() == JoinRelType.INNER
                && !join.analyzeCondition().pairs().isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(join.getCluster().getMetadataQuery());

        JoinInfo joinInfo = join.analyzeCondition();
        RelNode newLeft =
                createIsNotNullFilter(
                        relBuilder, rexBuilder, mq, join.getLeft(), joinInfo.leftKeys);
        RelNode newRight =
                createIsNotNullFilter(
                        relBuilder, rexBuilder, mq, join.getRight(), joinInfo.rightKeys);

        if ((newLeft != join.getLeft()) || (newRight != join.getRight())) {
            Join newJoin = join.copy(join.getTraitSet(), Lists.newArrayList(newLeft, newRight));
            call.transformTo(newJoin);
        }
    }

    private RelNode createIsNotNullFilter(
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            FlinkRelMetadataQuery mq,
            RelNode input,
            ImmutableIntList keys) {
        List<RexNode> filters = new ArrayList<>();
        for (int key : keys) {
            Double nullCount = mq.getColumnNullCount(input, key);
            if (nullCount != null && nullCount > JOIN_NULL_FILTER_THRESHOLD) {
                filters.add(
                        relBuilder.call(
                                SqlStdOperatorTable.IS_NOT_NULL,
                                rexBuilder.makeInputRef(input, key)));
            }
        }

        if (!filters.isEmpty()) {
            return relBuilder.push(input).filter(filters).build();
        } else {
            return input;
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface JoinDeriveNullFilterRuleConfig extends RelRule.Config {
        JoinDeriveNullFilterRule.JoinDeriveNullFilterRuleConfig DEFAULT =
                ImmutableJoinDeriveNullFilterRule.JoinDeriveNullFilterRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(LogicalJoin.class).anyInputs())
                        .withDescription("JoinDeriveNullFilterRule");

        @Override
        default JoinDeriveNullFilterRule toRule() {
            return new JoinDeriveNullFilterRule(this);
        }
    }
}
