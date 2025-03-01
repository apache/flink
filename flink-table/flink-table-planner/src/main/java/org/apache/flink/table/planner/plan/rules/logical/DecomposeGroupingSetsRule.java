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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkRelFactories;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ExpandUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.collection.JavaConverters;

/**
 * This rule rewrites an aggregation query with grouping sets into an regular aggregation query with
 * expand.
 *
 * <p>This rule duplicates the input data by two or more times (# number of groupSets + an optional
 * non-distinct group). This will put quite a bit of memory pressure of the used aggregate and
 * exchange operators.
 *
 * <p>This rule will be used for the plan with grouping sets or the plan with distinct aggregations
 * after {@link FlinkAggregateExpandDistinctAggregatesRule} applied.
 *
 * <p>`FlinkAggregateExpandDistinctAggregatesRule` rewrites an aggregate query with distinct
 * aggregations into an expanded double aggregation. The first aggregate has grouping sets in which
 * the regular aggregation expressions and every distinct clause are aggregated in a separate group.
 * The results are then combined in a second aggregate.
 *
 * <pre>Examples:
 *
 * MyTable: a: INT, b: BIGINT, c: VARCHAR(32), d: VARCHAR(32)
 *
 * Original records:
 * | a | b | c  | d  |
 * |:-:|:-:|:--:|:--:|
 * | 1 | 1 | c1 | d1 |
 * | 1 | 2 | c1 | d2 |
 * | 2 | 1 | c1 | d1 |
 *
 * Example1 (expand for DISTINCT aggregates):
 *
 * SQL: SELECT a, SUM(DISTINCT b) as t1, COUNT(DISTINCT c) as t2, COUNT(d) as t3 FROM MyTable GROUP
 * BY a
 *
 * Logical plan:
 * {@code
 * LogicalAggregate(group=[{0}], t1=[SUM(DISTINCT $1)], t2=[COUNT(DISTINCT $2)], t3=[COUNT($3)])
 *  LogicalTableScan(table=[[builtin, default, MyTable]])
 * }
 *
 * Logical plan after `FlinkAggregateExpandDistinctAggregatesRule` applied:
 * {@code
 * LogicalProject(a=[$0], t1=[$1], t2=[$2], t3=[CAST($3):BIGINT NOT NULL])
 *  LogicalProject(a=[$0], t1=[$1], t2=[$2], $f3=[CASE(IS NOT NULL($3), $3, 0)])
 *   LogicalAggregate(group=[{0}], t1=[SUM($1) FILTER $4], t2=[COUNT($2) FILTER $5],
 *     t3=[MIN($3) FILTER $6])
 *    LogicalProject(a=[$0], b=[$1], c=[$2], t3=[$3], $g_1=[=($4, 1)], $g_2=[=($4, 2)],
 *      $g_3=[=($4, 3)])
 *     LogicalAggregate(group=[{0, 1, 2}], groups=[[{0, 1}, {0, 2}, {0}]], t3=[COUNT($3)],
 *       $g=[GROUPING($0, $1, $2)])
 *      LogicalTableScan(table=[[builtin, default, MyTable]])
 * }
 *
 * Logical plan after this rule applied:
 * {@code
 * LogicalCalc(expr#0..3=[{inputs}], expr#4=[IS NOT NULL($t3)], ...)
 *  LogicalAggregate(group=[{0}], t1=[SUM($1) FILTER $4], t2=[COUNT($2) FILTER $5],
 *    t3=[MIN($3) FILTER $6])
 *   LogicalCalc(expr#0..4=[{inputs}], ... expr#10=[CASE($t6, $t5, $t8, $t7, $t9)],
 *      expr#11=[1], expr#12=[=($t10, $t11)], ... $g_1=[$t12], ...)
 *    LogicalAggregate(group=[{0, 1, 2, 4}], groups=[[]], t3=[COUNT($3)])
 *     LogicalExpand(projects=[{a=[$0], b=[$1], c=[null], d=[$3], $e=[1]},
 *       {a=[$0], b=[null], c=[$2], d=[$3], $e=[2]}, {a=[$0], b=[null], c=[null], d=[$3], $e=[3]}])
 *      LogicalTableSourceScan(table=[[builtin, default, MyTable]], fields=[a, b, c, d])
 * }
 *
 * '$e = 1' is equivalent to 'group by a, b' '$e = 2' is equivalent to 'group by a, c' '$e = 3' is
 * equivalent to 'group by a'
 *
 * Expanded records: \+-----+-----+-----+-----+-----+ \| a | b | c | d | $e |
 * \+-----+-----+-----+-----+-----+ ---+--- \| 1 | 1 | null| d1 | 1 | |
 * \+-----+-----+-----+-----+-----+ | \| 1 | null| c1 | d1 | 2 | records expanded by record1
 * \+-----+-----+-----+-----+-----+ | \| 1 | null| null| d1 | 3 | | \+-----+-----+-----+-----+-----+
 * ---+--- \| 1 | 2 | null| d2 | 1 | | \+-----+-----+-----+-----+-----+ | \| 1 | null| c1 | d2 | 2 |
 * records expanded by record2 \+-----+-----+-----+-----+-----+ | \| 1 | null| null| d2 | 3 | |
 * \+-----+-----+-----+-----+-----+ ---+--- \| 2 | 1 | null| d1 | 1 | |
 * \+-----+-----+-----+-----+-----+ | \| 2 | null| c1 | d1 | 2 | records expanded by record3
 * \+-----+-----+-----+-----+-----+ | \| 2 | null| null| d1 | 3 | | \+-----+-----+-----+-----+-----+
 * ---+---
 *
 * Example2 (Some fields are both in DISTINCT aggregates and non-DISTINCT aggregates):
 *
 * SQL: SELECT MAX(a) as t1, COUNT(DISTINCT a) as t2, count(DISTINCT d) as t3 FROM MyTable
 *
 * Field `a` is both in DISTINCT aggregate and `MAX` aggregate, so, `a` should be outputted as two
 * individual fields, one is for `MAX` aggregate, another is for DISTINCT aggregate.
 *
 * Expanded records: \+-----+-----+-----+-----+ \| a | d | $e | a_0 | \+-----+-----+-----+-----+
 * ---+--- \| 1 | null| 1 | 1 | | \+-----+-----+-----+-----+ | \| null| d1 | 2 | 1 | records
 * expanded by record1 \+-----+-----+-----+-----+ | \| null| null| 3 | 1 | |
 * \+-----+-----+-----+-----+ ---+--- \| 1 | null| 1 | 1 | | \+-----+-----+-----+-----+ | \| null|
 * d2 | 2 | 1 | records expanded by record2 \+-----+-----+-----+-----+ | \| null| null| 3 | 1 | |
 * \+-----+-----+-----+-----+ ---+--- \| 2 | null| 1 | 2 | | \+-----+-----+-----+-----+ | \| null|
 * d1 | 2 | 2 | records expanded by record3 \+-----+-----+-----+-----+ | \| null| null| 3 | 2 | |
 * \+-----+-----+-----+-----+ ---+---
 *
 * Example3 (expand for CUBE/ROLLUP/GROUPING SETS):
 *
 * SQL: SELECT a, c, SUM(b) as b FROM MyTable GROUP BY GROUPING SETS (a, c)
 *
 * Logical plan:
 * {@code
 * LogicalAggregate(group=[{0, 1}], groups=[[{0}, {1}]], b=[SUM($2)])
 *  LogicalProject(a=[$0], c=[$2], b=[$1])
 *   LogicalTableScan(table=[[builtin, default, MyTable]])
 * }
 *
 * Logical plan after this rule applied:
 * {@code
 * LogicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], b=[$t3])
 *  LogicalAggregate(group=[{0, 2, 3}], groups=[[]], b=[SUM($1)])
 *   LogicalExpand(projects=[{a=[$0], b=[$1], c=[null], $e=[1]},
 *     {a=[null], b=[$1], c=[$2], $e=[2]}])
 *    LogicalNativeTableScan(table=[[builtin, default, MyTable]])
 * }
 *
 * '$e = 1' is equivalent to 'group by a' '$e = 2' is equivalent to 'group by c'
 *
 * Expanded records: \+-----+-----+-----+-----+ \| a | b | c | $e | \+-----+-----+-----+-----+
 * ---+--- \| 1 | 1 | null| 1 | | \+-----+-----+-----+-----+ records expanded by record1 \| null| 1
 * \| c1 | 2 | | \+-----+-----+-----+-----+ ---+--- \| 1 | 2 | null| 1 | |
 * \+-----+-----+-----+-----+ records expanded by record2 \| null| 2 | c1 | 2 | |
 * \+-----+-----+-----+-----+ ---+--- \| 2 | 1 | null| 1 | | \+-----+-----+-----+-----+ records
 * expanded by record3 \| null| 1 | c1 | 2 | | \+-----+-----+-----+-----+ ---+---
 * </pre>
 */
@Value.Enclosing
public class DecomposeGroupingSetsRule
        extends RelRule<DecomposeGroupingSetsRule.DecomposeGroupingSetsRuleConfig> {
    public static final DecomposeGroupingSetsRule INSTANCE =
            DecomposeGroupingSetsRule.DecomposeGroupingSetsRuleConfig.DEFAULT.toRule();

    protected DecomposeGroupingSetsRule(DecomposeGroupingSetsRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        List<Object> groupIdExprs =
                JavaConverters.seqAsJavaList(
                        AggregateUtil.getGroupIdExprIndexes(
                                JavaConverters.asScalaBufferConverter(agg.getAggCallList())
                                        .asScala()));
        return agg.getGroupSets().size() > 1 || !groupIdExprs.isEmpty();
    }

    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        // Long data type is used to store groupValue in FlinkAggregateExpandDistinctAggregatesRule,
        // and the result of grouping function is a positive value,
        // so the max groupCount must be less than 64.
        if (agg.getGroupCount() >= 64) {
            throw new TableException("group count must be less than 64.");
        }

        RelNode aggInput = agg.getInput();
        List<Object> groupIdExprs =
                JavaConverters.seqAsJavaList(
                        AggregateUtil.getGroupIdExprIndexes(
                                JavaConverters.asScalaBufferConverter(agg.getAggCallList())
                                        .asScala()));
        List<Tuple2<AggregateCall, Integer>> aggCallsWithIndexes =
                IntStream.range(0, agg.getAggCallList().size())
                        .mapToObj(i -> Tuple2.of(agg.getAggCallList().get(i), i))
                        .collect(Collectors.toList());

        RelOptCluster cluster = agg.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        boolean needExpand = agg.getGroupSets().size() > 1;

        FlinkRelBuilder relBuilder = (FlinkRelBuilder) call.builder();
        relBuilder.push(aggInput);

        ImmutableBitSet newGroupSet;
        Map<Integer, Integer> duplicateFieldMap;
        if (needExpand) {
            Tuple2<scala.collection.immutable.Map<Integer, Integer>, Integer> expandResult =
                    JavaScalaConversionUtil.toJava(
                            ExpandUtil.buildExpandNode(
                                    relBuilder,
                                    JavaConverters.asScalaBufferConverter(agg.getAggCallList())
                                            .asScala(),
                                    agg.getGroupSet(),
                                    agg.getGroupSets()));

            // new groupSet contains original groupSet and expand_id('$e') field
            newGroupSet = agg.getGroupSet().union(ImmutableBitSet.of(expandResult.f1));
            duplicateFieldMap = JavaConverters.mapAsJavaMap(expandResult.f0);
        } else {
            // no need add expand node, only need care about group functions
            newGroupSet = agg.getGroupSet();
            duplicateFieldMap = new HashMap<>();
        }

        int newGroupCount = newGroupSet.cardinality();
        List<AggregateCall> newAggCalls =
                aggCallsWithIndexes.stream()
                        .filter(p -> !groupIdExprs.contains(p.f1))
                        .map(
                                p -> {
                                    AggregateCall aggCall = p.f0;
                                    List<Integer> newArgList =
                                            aggCall.getArgList().stream()
                                                    .map(a -> duplicateFieldMap.getOrDefault(a, a))
                                                    .collect(Collectors.toList());
                                    int newFilterArg =
                                            duplicateFieldMap.getOrDefault(
                                                    aggCall.filterArg, aggCall.filterArg);
                                    return aggCall.adaptTo(
                                            relBuilder.peek(),
                                            newArgList,
                                            newFilterArg,
                                            agg.getGroupCount(),
                                            newGroupCount);
                                })
                        .collect(Collectors.toList());

        // create simple aggregate
        relBuilder.aggregate(relBuilder.groupKey(newGroupSet, List.of(newGroupSet)), newAggCalls);
        RelNode newAgg = relBuilder.peek();

        // create a project to mapping original aggregate's output
        // get names of original grouping fields
        List<String> groupingFieldsName =
                IntStream.range(0, agg.getGroupCount())
                        .mapToObj(x -> agg.getRowType().getFieldNames().get(x))
                        .collect(Collectors.toList());

        // create field access for all original grouping fields
        List<RexNode> groupingFields =
                IntStream.range(0, agg.getGroupSet().cardinality())
                        .mapToObj(idx -> rexBuilder.makeInputRef(newAgg, idx))
                        .collect(Collectors.toList());

        List<Tuple2<ImmutableBitSet, Integer>> groupSetsWithIndexes =
                IntStream.range(0, agg.getGroupSets().size())
                        .mapToObj(i -> Tuple2.of(agg.getGroupSets().get(i), i))
                        .collect(Collectors.toList());
        // output aggregate calls including `normal` agg call and grouping agg call
        int aggCnt = 0;
        List<RexNode> aggFields = new ArrayList<>();
        for (Tuple2<AggregateCall, Integer> aggCallWithIndex : aggCallsWithIndexes) {
            AggregateCall aggCall = aggCallWithIndex.f0;
            int idx = aggCallWithIndex.f1;
            if (groupIdExprs.contains(idx)) {
                if (needExpand) {
                    // reference to expand_id('$e') field in new aggregate
                    int expandIdIdxInNewAgg = newGroupCount - 1;
                    RexInputRef expandIdField =
                            rexBuilder.makeInputRef(newAgg, expandIdIdxInNewAgg);
                    // create case when for group expression
                    List<RexNode> whenThenElse =
                            groupSetsWithIndexes.stream()
                                    .flatMap(
                                            tuple -> {
                                                int i = tuple.f1;
                                                RexNode groupExpr =
                                                        lowerGroupExpr(
                                                                rexBuilder,
                                                                aggCall,
                                                                groupSetsWithIndexes,
                                                                i);
                                                if (i < agg.getGroupSets().size() - 1) {
                                                    // WHEN/THEN
                                                    long expandIdVal =
                                                            ExpandUtil.genExpandId(
                                                                    agg.getGroupSet(), tuple.f0);
                                                    RelDataType expandIdType =
                                                            newAgg.getRowType()
                                                                    .getFieldList()
                                                                    .get(expandIdIdxInNewAgg)
                                                                    .getType();
                                                    RexNode expandIdLit =
                                                            rexBuilder.makeLiteral(
                                                                    expandIdVal,
                                                                    expandIdType,
                                                                    false);
                                                    return Stream.of(
                                                            // when $e = $e_value
                                                            rexBuilder.makeCall(
                                                                    SqlStdOperatorTable.EQUALS,
                                                                    expandIdField,
                                                                    expandIdLit),
                                                            // then return group expression literal
                                                            // value
                                                            groupExpr);
                                                } else {
                                                    // ELSE
                                                    return Stream.of(
                                                            // else return group expression literal
                                                            // value
                                                            groupExpr);
                                                }
                                            })
                                    .collect(Collectors.toList());
                    aggFields.add(rexBuilder.makeCall(SqlStdOperatorTable.CASE, whenThenElse));
                } else {
                    // create literal for group expression
                    aggFields.add(lowerGroupExpr(rexBuilder, aggCall, groupSetsWithIndexes, 0));
                }
            } else {
                // create access to aggregation result
                RexInputRef aggResult = rexBuilder.makeInputRef(newAgg, newGroupCount + aggCnt);
                aggCnt += 1;
                aggFields.add(aggResult);
            }
        }

        // add a projection to establish the result schema and set the values of the group
        // expressions.
        RelNode project =
                relBuilder
                        .project(
                                Stream.concat(groupingFields.stream(), aggFields.stream())
                                        .collect(Collectors.toList()),
                                Stream.concat(
                                                groupingFieldsName.stream(),
                                                agg.getAggCallList().stream()
                                                        .map(AggregateCall::getName))
                                        .collect(Collectors.toList()))
                        .convert(agg.getRowType(), true)
                        .build();

        call.transformTo(project);
    }

    /** Returns a literal for a given group expression. */
    private RexNode lowerGroupExpr(
            RexBuilder builder,
            AggregateCall call,
            List<Tuple2<ImmutableBitSet, Integer>> groupSetsWithIndexes,
            int indexInGroupSets) {

        ImmutableBitSet groupSet = groupSetsWithIndexes.get(indexInGroupSets).f0;
        Set<Integer> groups = groupSet.asSet();

        switch (call.getAggregation().getKind()) {
            case GROUP_ID:
                // https://issues.apache.org/jira/browse/CALCITE-1824
                // GROUP_ID is not in the SQL standard. It is implemented only by Oracle.
                // GROUP_ID is useful only if you have duplicate grouping sets,
                // If grouping sets are distinct, GROUP_ID() will always return zero;
                // Else return the index in the duplicate grouping sets.
                // e.g. SELECT deptno, GROUP_ID() AS g FROM Emp GROUP BY GROUPING SETS (deptno, (),
                // ())
                // As you can see, the grouping set () occurs twice.
                // So there is one row in the result for each occurrence:
                // the first occurrence has g = 0; the second has g = 1.

                List<Integer> duplicateGroupSetsIndices =
                        groupSetsWithIndexes.stream()
                                .filter(p -> p.f0.compareTo(groupSet) == 0)
                                .map(tuple2 -> tuple2.f1)
                                .collect(Collectors.toList());
                Preconditions.checkArgument(
                        !duplicateGroupSetsIndices.isEmpty(), "requirement failed");
                long id = duplicateGroupSetsIndices.indexOf(indexInGroupSets);
                return builder.makeLiteral(id, call.getType(), false);
            case GROUPING:
            case GROUPING_ID:
                // GROUPING function is defined in the SQL standard,
                // but the definition of GROUPING is different from in Oracle and in SQL standard:
                // https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions064.htm#SQLRF00647
                //
                // GROUPING_ID function is not defined in the SQL standard, and has the same
                // functionality with GROUPING function in Calcite.
                // our implementation is consistent with Oracle about GROUPING_ID function.
                //
                // NOTES:
                // In Calcite, the java-document of SqlGroupingFunction is not consistent with
                // agg.iq.
                long res = 0L;
                for (Integer arg : call.getArgList()) {
                    res = (res << 1L) + (groups.contains(arg) ? 0L : 1L);
                }
                return builder.makeLiteral(res, call.getType(), false);
            default:
                return builder.makeNullLiteral(call.getType());
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface DecomposeGroupingSetsRuleConfig extends RelRule.Config {
        DecomposeGroupingSetsRule.DecomposeGroupingSetsRuleConfig DEFAULT =
                ImmutableDecomposeGroupingSetsRule.DecomposeGroupingSetsRuleConfig.builder()
                        .operandSupplier(b0 -> b0.operand(LogicalAggregate.class).anyInputs())
                        .relBuilderFactory(FlinkRelFactories.FLINK_REL_BUILDER())
                        .description("DecomposeGroupingSetsRule")
                        .build();

        @Override
        default DecomposeGroupingSetsRule toRule() {
            return new DecomposeGroupingSetsRule(this);
        }
    }
}
