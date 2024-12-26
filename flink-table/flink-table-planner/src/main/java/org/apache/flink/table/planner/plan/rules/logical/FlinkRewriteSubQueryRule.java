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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * Planner rule that rewrites scalar query in filter like: {@code select * from T1 where (select
 * count(*) from T2) > 0} to {@code select * from T1 where exists (select * from T2)}, which could
 * be converted to SEMI join by {@link FlinkSubQueryRemoveRule}.
 *
 * <p>Without this rule, the original query will be rewritten to a filter on a join on an aggregate
 * by {@link SubQueryRemoveRule}. the full logical plan is
 *
 * <pre>{@code
 * LogicalProject(a=[$0], b=[$1], c=[$2])
 * +- LogicalJoin(condition=[$3], joinType=[semi])
 *    :- LogicalTableScan(table=[[x, source: [TestTableSource(a, b, c)]]])
 *    +- LogicalProject($f0=[IS NOT NULL($0)])
 *       +- LogicalAggregate(group=[{}], m=[MIN($0)])
 *          +- LogicalProject(i=[true])
 *             +- LogicalTableScan(table=[[y, source: [TestTableSource(d, e, f)]]])
 * }</pre>
 */
@Value.Enclosing
public class FlinkRewriteSubQueryRule
        extends RelRule<FlinkRewriteSubQueryRule.FlinkRewriteSubQueryRuleConfig> {

    public static final FlinkRewriteSubQueryRule FILTER =
            FlinkRewriteSubQueryRuleConfig.DEFAULT.toRule();

    protected FlinkRewriteSubQueryRule(
            FlinkRewriteSubQueryRule.FlinkRewriteSubQueryRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RexNode condition = filter.getCondition();
        RexNode newCondition = rewriteScalarQuery(condition);
        if (condition.equals(newCondition)) {
            return;
        }

        Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
        call.transformTo(newFilter);
    }

    // scalar query like: `(select count(*) from T) > 0` can be converted to `exists(select * from
    // T)`
    private RexNode rewriteScalarQuery(RexNode condition) {
        return condition.accept(
                new RexShuttle() {
                    public RexNode visitCall(RexCall call) {
                        Optional<RexSubQuery> subQuery = getSupportedScalarQuery(call);
                        if (subQuery.isPresent()) {
                            return RexSubQuery.exists(subQuery.get().rel.getInput(0));
                        }
                        return super.visitCall(call);
                    }
                });
    }

    private boolean isScalarQuery(RexNode n) {
        return n.isA(SqlKind.SCALAR_QUERY);
    }

    private boolean isBetween0And1(RexNode n, boolean include0, boolean include1) {
        if (n instanceof RexLiteral) {
            RexLiteral l = (RexLiteral) n;
            if (l.getTypeName().getFamily() == SqlTypeFamily.NUMERIC && l.getValue() != null) {
                double v = Double.parseDouble(l.getValue().toString());
                return (0.0 < v && v < 1.0) || (include0 && v == 0.0) || (include1 && v == 1.0);
            }
        }
        return false;
    }

    private boolean isCountStarAggWithoutGroupBy(RelNode n) {
        if (n instanceof Aggregate) {
            Aggregate agg = (Aggregate) n;
            if (agg.getGroupCount() == 0 && agg.getAggCallList().size() == 1) {
                AggregateCall aggCall = agg.getAggCallList().get(0);
                return !aggCall.isDistinct()
                        && aggCall.filterArg < 0
                        && aggCall.getArgList().isEmpty()
                        && aggCall.getAggregation() instanceof SqlCountAggFunction;
            }
        }
        return false;
    }

    Optional<RexSubQuery> getSupportedScalarQuery(RexCall call) {
        // check the RexNode is a RexLiteral which's value is between 0 and 1

        // check the RelNode is a Aggregate which has only count aggregate call with empty args

        switch (call.getKind()) {
            // (select count(*) from T) > X (X is between 0 (inclusive) and 1 (exclusive))
            case GREATER_THAN:
                if (isScalarQuery(call.operands.get(0))) {
                    RexSubQuery subQuery = (RexSubQuery) call.operands.get(0);
                    if (isCountStarAggWithoutGroupBy(subQuery.rel)
                            && isBetween0And1(call.operands.get(1), true, false)) {
                        return Optional.of(subQuery);
                    }
                }
                break;
            case GREATER_THAN_OR_EQUAL:
                if (isScalarQuery(call.operands.get(0))) {
                    RexSubQuery subQuery = (RexSubQuery) call.operands.get(0);
                    if (isCountStarAggWithoutGroupBy(subQuery.rel)
                            && isBetween0And1(call.operands.get(1), false, true)) {
                        return Optional.of(subQuery);
                    }
                }
                break;
            case LESS_THAN:
                if (isScalarQuery(call.operands.get(1))) {
                    RexSubQuery subQuery = (RexSubQuery) call.operands.get(1);
                    if (isCountStarAggWithoutGroupBy(subQuery.rel)
                            && isBetween0And1(call.operands.get(0), true, false)) {
                        return Optional.of(subQuery);
                    }
                }
                break;
            case LESS_THAN_OR_EQUAL:
                if (isScalarQuery(call.operands.get(1))) {
                    RexSubQuery subQuery = (RexSubQuery) call.operands.get(1);
                    if (isCountStarAggWithoutGroupBy(subQuery.rel)
                            && isBetween0And1(call.operands.get(0), false, true)) {
                        return Optional.of(subQuery);
                    }
                }
                break;
        }
        return Optional.empty();
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface FlinkRewriteSubQueryRuleConfig extends RelRule.Config {
        FlinkRewriteSubQueryRule.FlinkRewriteSubQueryRuleConfig DEFAULT =
                ImmutableFlinkRewriteSubQueryRule.FlinkRewriteSubQueryRuleConfig.builder()
                        .operandSupplier(
                                b0 ->
                                        b0.operand(Filter.class)
                                                .predicate(RexUtil.SubQueryFinder.FILTER_PREDICATE)
                                                .anyInputs())
                        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .description("FlinkRewriteSubQueryRule:Filter")
                        .build();

        @Override
        default FlinkRewriteSubQueryRule toRule() {
            return new FlinkRewriteSubQueryRule(this);
        }
    }
}
