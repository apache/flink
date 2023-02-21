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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * This rules is copied from Calcite's {@link
 * org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule}. Modification: - Match Join with SEMI
 * or ANTI type instead of SemiJoin
 */

/**
 * Planner rule that pushes semi-join down in a tree past a {@link
 * org.apache.calcite.rel.core.Filter}.
 *
 * <p>The intention is to trigger other rules that will convert {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalFilter(X), Y) &rarr; LogicalFilter(SemiJoin(X, Y))
 *
 * @see org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule
 */
public class FlinkSemiAntiJoinFilterTransposeRule extends RelOptRule {
    public static final FlinkSemiAntiJoinFilterTransposeRule INSTANCE =
            new FlinkSemiAntiJoinFilterTransposeRule(RelFactories.LOGICAL_BUILDER);

    // ~ Constructors -----------------------------------------------------------

    /** Creates a FlinkSemiAntiJoinFilterTransposeRule. */
    public FlinkSemiAntiJoinFilterTransposeRule(RelBuilderFactory relBuilderFactory) {
        super(
                operand(LogicalJoin.class, some(operand(LogicalFilter.class, any()))),
                relBuilderFactory,
                null);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        return join.getJoinType() == JoinRelType.SEMI || join.getJoinType() == JoinRelType.ANTI;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        LogicalFilter filter = call.rel(1);

        RelNode newJoin =
                LogicalJoin.create(
                        filter.getInput(),
                        join.getRight(),
                        join.getHints(),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType());

        final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
        RelNode newFilter = factory.createFilter(newJoin, filter.getCondition());

        call.transformTo(newFilter);
    }
}

// End FlinkSemiAntiJoinFilterTransposeRule.java
