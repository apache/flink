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

import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

/**
 * FilterIntoJoinRule for streaming, it doesn't push a filter into join which is a temporal join
 * using event time.
 */
public class StreamFilterIntoJoinRule extends FilterJoinRule<StreamFilterIntoJoinRule.Config> {

    public static final RelOptRule FILTER_INTO_JOIN = Config.DEFAULT.toRule();

    protected StreamFilterIntoJoinRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(1);
        boolean existTemporalJoinCondition = existTemporalJoinCondition(join.getCondition());
        return !existTemporalJoinCondition && super.matches(call);
    }

    private boolean existTemporalJoinCondition(RexNode joinCondition) {
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

    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);
        this.perform(call, filter, join);
    }

    /** Config for StreamFilterIntoJoinRule. */
    public interface Config extends FilterJoinRule.Config {
        StreamFilterIntoJoinRule.Config DEFAULT =
                EMPTY.withOperandSupplier(
                                b0 ->
                                        b0.operand(Filter.class)
                                                .oneInput(b1 -> b1.operand(Join.class).anyInputs()))
                        .as(FilterJoinRule.Config.class)
                        .withSmart(true)
                        .withPredicate((join, joinType, exp) -> true)
                        .as(StreamFilterIntoJoinRule.Config.class);

        @Override
        default RelOptRule toRule() {
            return new StreamFilterIntoJoinRule(this);
        }
    }
}
