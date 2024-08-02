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
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkRelFactories;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that rewrites sort correlation to a Rank. Typically, the following plan
 *
 * <pre>{@code
 * LogicalProject(state=[$0], name=[$1])
 * +- LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
 *    :- LogicalAggregate(group=[{0}])
 *    :  +- LogicalProject(state=[$1])
 *    :     +- LogicalTableScan(table=[[default_catalog, default_database, cities]])
 *    +- LogicalSort(sort0=[$1], dir0=[DESC-nulls-last], fetch=[3])
 *       +- LogicalProject(name=[$0], pop=[$2])
 *          +- LogicalFilter(condition=[=($1, $cor0.state)])
 *             +- LogicalTableScan(table=[[default_catalog, default_database, cities]])
 * }</pre>
 *
 * <p>would be transformed to
 *
 * <pre>{@code
 * LogicalProject(state=[$0], name=[$1])
 *  +- LogicalProject(state=[$1], name=[$0], pop=[$2])
 *     +- LogicalRank(rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=3],
 *          partitionBy=[$1], orderBy=[$2 DESC], select=[name=$0, state=$1, pop=$2])
 *        +- LogicalTableScan(table=[[default_catalog, default_database, cities]])
 * }</pre>
 *
 * <p>To match the Correlate, the LHS needs to be a global Aggregate on a scan, the RHS should be a
 * Sort with an equal Filter predicate whose keys are same with the LHS grouping keys.
 *
 * <p>This rule can only be used in {@link HepPlanner}.
 */
@Value.Enclosing
public class CorrelateSortToRankRule
        extends RelRule<CorrelateSortToRankRule.CorrelateSortToRankRuleConfig> {

    public static final CorrelateSortToRankRule INSTANCE =
            CorrelateSortToRankRule.CorrelateSortToRankRuleConfig.DEFAULT.toRule();

    protected CorrelateSortToRankRule(CorrelateSortToRankRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Correlate correlate = call.rel(0);
        if (correlate.getJoinType() != JoinRelType.INNER) {
            return false;
        }
        Aggregate agg = call.rel(1);
        if (!agg.getAggCallList().isEmpty() || agg.getGroupSets().size() > 1) {
            return false;
        }
        Project aggInput = call.rel(2);
        if (!aggInput.isMapping()) {
            return false;
        }
        Sort sort = call.rel(3);
        if (sort.offset != null || sort.fetch == null) {
            // 1. we can not describe the offset using rank
            // 2. there is no need to transform to rank if no fetch limit
            return false;
        }
        Project sortInput = call.rel(4);
        if (!sortInput.isMapping()) {
            return false;
        }
        Filter filter = call.rel(5);

        List<RexNode> cnfCond = RelOptUtil.conjunctions(filter.getCondition());
        if (cnfCond.stream().anyMatch(c -> !isValidCondition(c, correlate))) {
            return false;
        }

        return aggInput.getInput().getDigest().equals(filter.getInput().getDigest());
    }

    private boolean isValidCondition(RexNode condition, Correlate correlate) {
        // must be equiv condition
        if (condition.getKind() != SqlKind.EQUALS) {
            return false;
        }
        Tuple2<RexInputRef, RexFieldAccess> tuple = resolveFilterCondition(condition);
        if (tuple.f0 == null) {
            return false;
        }
        RexCorrelVariable variable = (RexCorrelVariable) tuple.f1.getReferenceExpr();
        return variable.id.equals(correlate.getCorrelationId());
    }

    /**
     * Resolves the filter condition with specific pattern: input ref and field access.
     *
     * @param condition The join condition
     * @return tuple of operands (RexInputRef, RexFieldAccess), or null if the pattern does not
     *     match
     */
    private Tuple2<RexInputRef, RexFieldAccess> resolveFilterCondition(RexNode condition) {
        RexCall condCall = (RexCall) condition;
        RexNode operand0 = condCall.getOperands().get(0);
        RexNode operand1 = condCall.getOperands().get(1);
        if (operand0.isA(SqlKind.INPUT_REF) && operand1.isA(SqlKind.FIELD_ACCESS)) {
            return Tuple2.of((RexInputRef) operand0, (RexFieldAccess) operand1);
        } else if (operand0.isA(SqlKind.FIELD_ACCESS) && operand1.isA(SqlKind.INPUT_REF)) {
            return Tuple2.of((RexInputRef) operand1, (RexFieldAccess) operand0);
        } else {
            return Tuple2.of(null, null);
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelBuilder builder = call.builder();

        Sort sort = call.rel(3);
        Project sortInput = call.rel(4);
        Filter filter = call.rel(5);

        List<RexNode> cnfCond = RelOptUtil.conjunctions(filter.getCondition());
        ImmutableBitSet partitionKey =
                ImmutableBitSet.of(
                        cnfCond.stream()
                                .map(c -> resolveFilterCondition(c).f0.getIndex())
                                .collect(Collectors.toList()));

        RelDataType baseType = sortInput.getInput().getRowType();
        List<RexNode> projects = new ArrayList<>();
        partitionKey.asList().forEach(k -> projects.add(RexInputRef.of(k, baseType)));
        projects.addAll(sortInput.getProjects());

        RelCollation oriCollation = sort.getCollation();
        List<RelFieldCollation> newFieldCollations =
                oriCollation.getFieldCollations().stream()
                        .map(
                                fc -> {
                                    int newFieldIdx =
                                            ((RexInputRef)
                                                            sortInput
                                                                    .getProjects()
                                                                    .get(fc.getFieldIndex()))
                                                    .getIndex();
                                    return fc.withFieldIndex(newFieldIdx);
                                })
                        .collect(Collectors.toList());
        RelCollation newCollation = RelCollations.of(newFieldCollations);

        RelNode newRel =
                ((FlinkRelBuilder) (builder.push(filter.getInput())))
                        .rank(
                                partitionKey,
                                newCollation,
                                RankType.ROW_NUMBER,
                                new ConstantRankRange(
                                        1, ((RexLiteral) sort.fetch).getValueAs(Long.class)),
                                null,
                                false)
                        .project(projects)
                        .build();

        call.transformTo(newRel);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface CorrelateSortToRankRuleConfig extends RelRule.Config {
        CorrelateSortToRankRule.CorrelateSortToRankRuleConfig DEFAULT =
                ImmutableCorrelateSortToRankRule.CorrelateSortToRankRuleConfig.builder()
                        .operandSupplier(
                                b0 ->
                                        b0.operand(Correlate.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(Aggregate.class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        Project
                                                                                                                .class)
                                                                                                .anyInputs()),
                                                        b2 ->
                                                                b2.operand(Sort.class)
                                                                        .inputs(
                                                                                b3 ->
                                                                                        b3.operand(
                                                                                                        Project
                                                                                                                .class)
                                                                                                .inputs(
                                                                                                        b4 ->
                                                                                                                b4.operand(
                                                                                                                                Filter
                                                                                                                                        .class)
                                                                                                                        .anyInputs()))))
                        .relBuilderFactory(FlinkRelFactories.FLINK_REL_BUILDER())
                        .description("CorrelateSortToRankRule")
                        .build();

        @Override
        default CorrelateSortToRankRule toRule() {
            return new CorrelateSortToRankRule(this);
        }
    }
}
