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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Util.last;

/**
 * Default implementation of {@link org.apache.calcite.rel.rules.SubQueryRemoveRule}, the class was
 * copied over because of LITERAL_AGG function which should be implemented in Flink first.
 *
 * <p>Lines 740 ~ 744, Use Calcite 1.34.0 behavior.
 */
@Value.Enclosing
public class SubQueryRemoveRule extends RelRule<SubQueryRemoveRule.Config>
        implements TransformationRule {

    /** Creates a SubQueryRemoveRule. */
    protected SubQueryRemoveRule(Config config) {
        super(config);
        Objects.requireNonNull(config.matchHandler());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    protected RexNode apply(
            RexSubQuery e,
            Set<CorrelationId> variablesSet,
            RelOptUtil.Logic logic,
            RelBuilder builder,
            int inputCount,
            int offset,
            int subQueryIndex) {
        switch (e.getKind()) {
            case SCALAR_QUERY:
                return rewriteScalarQuery(e, variablesSet, builder, inputCount, offset);
            case ARRAY_QUERY_CONSTRUCTOR:
            case MAP_QUERY_CONSTRUCTOR:
            case MULTISET_QUERY_CONSTRUCTOR:
                return rewriteCollection(e, variablesSet, builder, inputCount, offset);
            case SOME:
                return rewriteSome(e, variablesSet, builder, subQueryIndex);
            case IN:
                return rewriteIn(e, variablesSet, logic, builder, offset, subQueryIndex);
            case EXISTS:
                return rewriteExists(e, variablesSet, logic, builder);
            case UNIQUE:
                return rewriteUnique(e, builder);
            default:
                throw new AssertionError(e.getKind());
        }
    }

    /**
     * Rewrites a scalar sub-query into an {@link org.apache.calcite.rel.core.Aggregate}.
     *
     * @param e Scalar sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified
     *     RexSubQuery
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteScalarQuery(
            RexSubQuery e,
            Set<CorrelationId> variablesSet,
            RelBuilder builder,
            int inputCount,
            int offset) {
        builder.push(e.rel);
        final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
        final Boolean unique = mq.areColumnsUnique(builder.peek(), ImmutableBitSet.of());
        if (unique == null || !unique) {
            builder.aggregate(
                    builder.groupKey(),
                    builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, builder.field(0)));
        }
        builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
        return field(builder, inputCount, offset);
    }

    /**
     * Rewrites a sub-query into a {@link org.apache.calcite.rel.core.Collect}.
     *
     * @param e Sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified
     *     RexSubQuery
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteCollection(
            RexSubQuery e,
            Set<CorrelationId> variablesSet,
            RelBuilder builder,
            int inputCount,
            int offset) {
        builder.push(e.rel);
        builder.push(Collect.create(builder.build(), e.getKind(), "x"));
        builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
        return field(builder, inputCount, offset);
    }

    /**
     * Rewrites a SOME sub-query into a {@link Join}.
     *
     * @param e SOME sub-query to rewrite
     * @param builder Builder
     * @param subQueryIndex sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteSome(
            RexSubQuery e, Set<CorrelationId> variablesSet, RelBuilder builder, int subQueryIndex) {
        // Most general case, where the left and right keys might have nulls, and
        // caller requires 3-valued logic return.
        //
        // select e.deptno, e.deptno < some (select deptno from emp) as v
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when q.c = 0 then false // sub-query is empty
        //   when (e.deptno < q.m) is true then true
        //   when q.c > q.d then unknown // sub-query has at least one null
        //   else e.deptno < q.m
        //   end as v
        // from emp as e
        // cross join (
        //   select max(deptno) as m, count(*) as c, count(deptno) as d
        //   from emp) as q
        //
        final SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;
        switch (op.comparisonKind) {
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN_OR_EQUAL:
            case LESS_THAN:
            case GREATER_THAN:
            case NOT_EQUALS:
                break;

            default:
                // "SOME =" should have been rewritten into IN.
                throw new AssertionError("unexpected " + op);
        }

        final RexNode caseRexNode;
        final RexNode literalFalse = builder.literal(false);
        final RexNode literalTrue = builder.literal(true);
        final RexLiteral literalUnknown =
                builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

        final SqlAggFunction minMax =
                op.comparisonKind == SqlKind.GREATER_THAN
                                || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
                        ? SqlStdOperatorTable.MIN
                        : SqlStdOperatorTable.MAX;

        String qAlias = "q";
        if (subQueryIndex != 0) {
            qAlias = "q" + subQueryIndex;
        }

        if (variablesSet.isEmpty()) {
            switch (op.comparisonKind) {
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN:
                case GREATER_THAN:
                    // for non-correlated case queries such as
                    // select e.deptno, e.deptno < some (select deptno from emp) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when q.c = 0 then false // sub-query is empty
                    //   when (e.deptno < q.m) is true then true
                    //   when q.c > q.d then unknown // sub-query has at least one null
                    //   else e.deptno < q.m
                    //   end as v
                    // from emp as e
                    // cross join (
                    //   select max(deptno) as m, count(*) as c, count(deptno) as d
                    //   from emp) as q
                    builder.push(e.rel)
                            .aggregate(
                                    builder.groupKey(),
                                    builder.aggregateCall(minMax, builder.field(0)).as("m"),
                                    builder.count(false, "c"),
                                    builder.count(false, "d", builder.field(0)))
                            .as(qAlias)
                            .join(JoinRelType.INNER);
                    caseRexNode =
                            builder.call(
                                    SqlStdOperatorTable.CASE,
                                    builder.equals(builder.field(qAlias, "c"), builder.literal(0)),
                                    literalFalse,
                                    builder.call(
                                            SqlStdOperatorTable.IS_TRUE,
                                            builder.call(
                                                    RexUtil.op(op.comparisonKind),
                                                    e.operands.get(0),
                                                    builder.field(qAlias, "m"))),
                                    literalTrue,
                                    builder.greaterThan(
                                            builder.field(qAlias, "c"), builder.field(qAlias, "d")),
                                    literalUnknown,
                                    builder.call(
                                            RexUtil.op(op.comparisonKind),
                                            e.operands.get(0),
                                            builder.field(qAlias, "m")));
                    break;

                case NOT_EQUALS:
                    // for non-correlated case queries such as
                    // select e.deptno, e.deptno <> some (select deptno from emp) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when q.c = 0 then false // sub-query is empty
                    //   when e.deptno is null then unknown
                    //   when q.c <> q.d && q.d <= 1 then e.deptno != m || unknown
                    //   when q.d = 1
                    //     then e.deptno != m // sub-query has the distinct result
                    //   else true
                    //   end as v
                    // from emp as e
                    // cross join (
                    //   select count(*) as c, count(deptno) as d, max(deptno) as m
                    //   from (select distinct deptno from emp)) as q
                    builder.push(e.rel);
                    builder.distinct()
                            .aggregate(
                                    builder.groupKey(),
                                    builder.count(false, "c"),
                                    builder.count(false, "d", builder.field(0)),
                                    builder.max(builder.field(0)).as("m"))
                            .as(qAlias)
                            .join(JoinRelType.INNER);
                    caseRexNode =
                            builder.call(
                                    SqlStdOperatorTable.CASE,
                                    builder.equals(builder.field("c"), builder.literal(0)),
                                    literalFalse,
                                    builder.isNull(e.getOperands().get(0)),
                                    literalUnknown,
                                    builder.and(
                                            builder.notEquals(
                                                    builder.field("d"), builder.field("c")),
                                            builder.lessThanOrEqual(
                                                    builder.field("d"), builder.literal(1))),
                                    builder.or(
                                            builder.notEquals(
                                                    e.operands.get(0), builder.field(qAlias, "m")),
                                            literalUnknown),
                                    builder.equals(builder.field("d"), builder.literal(1)),
                                    builder.notEquals(
                                            e.operands.get(0), builder.field(qAlias, "m")),
                                    literalTrue);
                    break;

                default:
                    throw new AssertionError("not possible - per above check");
            }
        } else {
            final String indicator = "trueLiteral";
            final List<RexNode> parentQueryFields = new ArrayList<>();
            switch (op.comparisonKind) {
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN:
                case GREATER_THAN:
                    // for correlated case queries such as
                    //
                    // select e.deptno, e.deptno < some (
                    //   select deptno from emp where emp.name = e.name) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when indicator is null then false // sub-query is empty for corresponding
                    // corr value
                    //   when q.c = 0 then false // sub-query is empty
                    //   when (e.deptno < q.m) is true then true
                    //   when q.c > q.d then unknown // sub-query has at least one null
                    //   else e.deptno < q.m
                    //   end as v
                    // from emp as e
                    // left outer join (
                    //   select name, max(deptno) as m, count(*) as c, count(deptno) as d,
                    //       "alwaysTrue" as indicator
                    //   from emp group by name) as q on e.name = q.name
                    builder.push(e.rel)
                            .aggregate(
                                    builder.groupKey(),
                                    builder.aggregateCall(minMax, builder.field(0)).as("m"),
                                    builder.count(false, "c"),
                                    builder.count(false, "d", builder.field(0)));

                    parentQueryFields.addAll(builder.fields());
                    parentQueryFields.add(builder.alias(literalTrue, indicator));
                    builder.project(parentQueryFields).as(qAlias);
                    builder.join(JoinRelType.LEFT, literalTrue, variablesSet);
                    caseRexNode =
                            builder.call(
                                    SqlStdOperatorTable.CASE,
                                    builder.isNull(builder.field(qAlias, indicator)),
                                    literalFalse,
                                    builder.equals(builder.field(qAlias, "c"), builder.literal(0)),
                                    literalFalse,
                                    builder.call(
                                            SqlStdOperatorTable.IS_TRUE,
                                            builder.call(
                                                    RexUtil.op(op.comparisonKind),
                                                    e.operands.get(0),
                                                    builder.field(qAlias, "m"))),
                                    literalTrue,
                                    builder.greaterThan(
                                            builder.field(qAlias, "c"), builder.field(qAlias, "d")),
                                    literalUnknown,
                                    builder.call(
                                            RexUtil.op(op.comparisonKind),
                                            e.operands.get(0),
                                            builder.field(qAlias, "m")));
                    break;

                case NOT_EQUALS:
                    // for correlated case queries such as
                    //
                    // select e.deptno, e.deptno <> some (
                    //   select deptno from emp where emp.name = e.name) as v
                    // from emp as e
                    //
                    // becomes
                    //
                    // select e.deptno,
                    //   case
                    //   when indicator is null
                    //     then false // sub-query is empty for corresponding corr value
                    //   when q.c = 0 then false // sub-query is empty
                    //   when e.deptno is null then unknown
                    //   when q.c <> q.d && q.d <= 1
                    //     then e.deptno != m || unknown
                    //   when q.d = 1
                    //     then e.deptno != m // sub-query has the distinct result
                    //   else true
                    //   end as v
                    // from emp as e
                    // left outer join (
                    //   select name, count(distinct *) as c, count(distinct deptno) as d,
                    //       max(deptno) as m, "alwaysTrue" as indicator
                    //   from emp group by name) as q on e.name = q.name
                    builder.push(e.rel)
                            .aggregate(
                                    builder.groupKey(),
                                    builder.count(true, "c"),
                                    builder.count(true, "d", builder.field(0)),
                                    builder.max(builder.field(0)).as("m"));

                    parentQueryFields.addAll(builder.fields());
                    parentQueryFields.add(builder.alias(literalTrue, indicator));
                    builder.project(parentQueryFields).as(qAlias); // TODO use projectPlus
                    builder.join(JoinRelType.LEFT, literalTrue, variablesSet);
                    caseRexNode =
                            builder.call(
                                    SqlStdOperatorTable.CASE,
                                    builder.isNull(builder.field(qAlias, indicator)),
                                    literalFalse,
                                    builder.equals(builder.field("c"), builder.literal(0)),
                                    literalFalse,
                                    builder.isNull(e.getOperands().get(0)),
                                    literalUnknown,
                                    builder.and(
                                            builder.notEquals(
                                                    builder.field("d"), builder.field("c")),
                                            builder.lessThanOrEqual(
                                                    builder.field("d"), builder.literal(1))),
                                    builder.or(
                                            builder.notEquals(
                                                    e.operands.get(0), builder.field(qAlias, "m")),
                                            literalUnknown),
                                    builder.equals(builder.field("d"), builder.literal(1)),
                                    builder.notEquals(
                                            e.operands.get(0), builder.field(qAlias, "m")),
                                    literalTrue);
                    break;

                default:
                    throw new AssertionError("not possible - per above check");
            }
        }

        // CASE statement above is created with nullable boolean type, but it might
        // not be correct.  If the original sub-query node's type is not nullable it
        // is guaranteed for case statement to not produce NULLs. Therefore to avoid
        // planner complaining we need to add cast.  Note that nullable type is
        // created due to the MIN aggregate call, since there is no GROUP BY.
        if (!e.getType().isNullable()) {
            return builder.cast(caseRexNode, e.getType().getSqlTypeName());
        }
        return caseRexNode;
    }

    /**
     * Rewrites an EXISTS RexSubQuery into a {@link Join}.
     *
     * @param e EXISTS sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified
     *     RexSubQuery
     * @param logic Logic for evaluating
     * @param builder Builder
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteExists(
            RexSubQuery e,
            Set<CorrelationId> variablesSet,
            RelOptUtil.Logic logic,
            RelBuilder builder) {
        // If the sub-query is guaranteed to produce at least one row, just return
        // TRUE.
        final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
        final Double minRowCount = mq.getMinRowCount(e.rel);
        if (minRowCount != null && minRowCount >= 1D) {
            return builder.literal(true);
        }
        final Double maxRowCount = mq.getMaxRowCount(e.rel);
        if (maxRowCount != null && maxRowCount < 1D) {
            return builder.literal(false);
        }
        builder.push(e.rel);
        builder.project(builder.alias(builder.literal(true), "i"));
        switch (logic) {
            case TRUE:
                // Handles queries with single EXISTS in filter condition:
                // select e.deptno from emp as e
                // where exists (select deptno from emp)
                builder.aggregate(builder.groupKey(0));
                builder.as("dt");
                builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
                return builder.literal(true);
            default:
                builder.distinct();
        }

        builder.as("dt");

        builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);

        return builder.isNotNull(last(builder.fields()));
    }

    /**
     * Rewrites a UNIQUE RexSubQuery into an EXISTS RexSubQuery.
     *
     * <p>For example, rewrites the UNIQUE sub-query:
     *
     * <pre>{@code
     * UNIQUE (SELECT PUBLISHED_IN
     * FROM BOOK
     * WHERE AUTHOR_ID = 3)
     * }</pre>
     *
     * <p>to the following EXISTS sub-query:
     *
     * <pre>{@code
     * NOT EXISTS (
     *   SELECT * FROM (
     *     SELECT PUBLISHED_IN
     *     FROM BOOK
     *     WHERE AUTHOR_ID = 3
     *   ) T
     *   WHERE (T.PUBLISHED_IN) IS NOT NULL
     *   GROUP BY T.PUBLISHED_IN
     *   HAVING COUNT(*) > 1
     * )
     * }</pre>
     *
     * @param e UNIQUE sub-query to rewrite
     * @param builder Builder
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteUnique(RexSubQuery e, RelBuilder builder) {
        // if sub-query always return unique value.
        final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
        Boolean isUnique = mq.areRowsUnique(e.rel, true);
        if (isUnique != null && isUnique) {
            return builder.getRexBuilder().makeLiteral(true);
        }
        builder.push(e.rel);
        List<RexNode> notNullCondition =
                builder.fields().stream().map(builder::isNotNull).collect(Collectors.toList());
        builder.filter(notNullCondition)
                .aggregate(builder.groupKey(builder.fields()), builder.countStar("c"))
                .filter(builder.greaterThan(last(builder.fields()), builder.literal(1)));
        RelNode relNode = builder.build();
        return builder.call(SqlStdOperatorTable.NOT, RexSubQuery.exists(relNode));
    }

    /**
     * Rewrites an IN RexSubQuery into a {@link Join}.
     *
     * @param e IN sub-query to rewrite
     * @param variablesSet A set of variables used by a relational expression of the specified
     *     RexSubQuery
     * @param logic Logic for evaluating
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @param subQueryIndex sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteIn(
            RexSubQuery e,
            Set<CorrelationId> variablesSet,
            RelOptUtil.Logic logic,
            RelBuilder builder,
            int offset,
            int subQueryIndex) {
        // Most general case, where the left and right keys might have nulls, and
        // caller requires 3-valued logic return.
        //
        // select e.deptno, e.deptno in (select deptno from emp)
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when ct.c = 0 then false
        //   when e.deptno is null then null
        //   when dt.i is not null then true
        //   when ct.ck < ct.c then null
        //   else false
        //   end
        // from emp as e
        // left join (
        //   (select count(*) as c, count(deptno) as ck from emp) as ct
        //   cross join (select distinct deptno, true as i from emp)) as dt
        //   on e.deptno = dt.deptno
        //
        // If keys are not null we can remove "ct" and simplify to
        //
        // select e.deptno,
        //   case
        //   when dt.i is not null then true
        //   else false
        //   end
        // from emp as e
        // left join (select distinct deptno, true as i from emp) as dt
        //   on e.deptno = dt.deptno
        //
        // We could further simplify to
        //
        // select e.deptno,
        //   dt.i is not null
        // from emp as e
        // left join (select distinct deptno, true as i from emp) as dt
        //   on e.deptno = dt.deptno
        //
        // but have not yet.
        //
        // If the logic is TRUE we can just kill the record if the condition
        // evaluates to FALSE or UNKNOWN. Thus the query simplifies to an inner
        // join:
        //
        // select e.deptno,
        //   true
        // from emp as e
        // inner join (select distinct deptno from emp) as dt
        //   on e.deptno = dt.deptno
        //

        builder.push(e.rel);
        final List<RexNode> fields = new ArrayList<>(builder.fields());

        // for the case when IN has only literal operands, it may be handled
        // in the simpler way:
        //
        // select e.deptno, 123456 in (select deptno from emp)
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when dt.c IS NULL THEN FALSE
        //   when e.deptno IS NULL THEN NULL
        //   when dt.cs IS FALSE THEN NULL
        //   when dt.cs IS NOT NULL THEN TRUE
        //   else false
        //   end
        // from emp AS e
        // cross join (
        //   select distinct deptno is not null as cs, count(*) as c
        //   from emp
        //   where deptno = 123456 or deptno is null or e.deptno is null
        //   order by cs desc limit 1) as dt
        //

        String ctAlias = "ct";
        if (subQueryIndex != 0) {
            ctAlias = "ct" + subQueryIndex;
        }

        boolean allLiterals = RexUtil.allLiterals(e.getOperands());
        final List<RexNode> expressionOperands = new ArrayList<>(e.getOperands());

        final List<RexNode> keyIsNulls =
                e.getOperands().stream()
                        .filter(operand -> operand.getType().isNullable())
                        .map(builder::isNull)
                        .collect(Collectors.toList());

        final RexLiteral trueLiteral = builder.literal(true);
        final RexLiteral falseLiteral = builder.literal(false);
        final RexLiteral unknownLiteral =
                builder.getRexBuilder().makeNullLiteral(trueLiteral.getType());
        if (allLiterals) {
            final List<RexNode> conditions =
                    Pair.zip(expressionOperands, fields).stream()
                            .map(pair -> builder.equals(pair.left, pair.right))
                            .collect(Collectors.toList());
            switch (logic) {
                case TRUE:
                case TRUE_FALSE:
                    builder.filter(conditions);
                    builder.project(builder.alias(trueLiteral, "cs"));
                    builder.distinct();
                    break;
                default:
                    List<RexNode> isNullOperands =
                            fields.stream().map(builder::isNull).collect(Collectors.toList());
                    // uses keyIsNulls conditions in the filter to avoid empty results
                    isNullOperands.addAll(keyIsNulls);
                    builder.filter(builder.or(builder.and(conditions), builder.or(isNullOperands)));
                    RexNode project =
                            builder.and(
                                    fields.stream()
                                            .map(builder::isNotNull)
                                            .collect(Collectors.toList()));
                    builder.project(builder.alias(project, "cs"));

                    if (variablesSet.isEmpty()) {
                        builder.aggregate(
                                builder.groupKey(builder.field("cs")), builder.count(false, "c"));

                        // sorts input with desc order since we are interested
                        // only in the case when one of the values is true.
                        // When true value is absent then we are interested
                        // only in false value.
                        builder.sortLimit(
                                0, 1, ImmutableList.of(builder.desc(builder.field("cs"))));
                    } else {
                        builder.distinct();
                    }
            }
            // clears expressionOperands and fields lists since
            // all expressions were used in the filter
            expressionOperands.clear();
            fields.clear();
        } else {
            switch (logic) {
                case TRUE:
                    builder.aggregate(builder.groupKey(fields));
                    break;
                case TRUE_FALSE_UNKNOWN:
                case UNKNOWN_AS_TRUE:
                    // Builds the cross join
                    builder.aggregate(
                            builder.groupKey(),
                            builder.count(false, "c"),
                            builder.count(builder.fields()).as("ck"));
                    builder.as(ctAlias);
                    if (!variablesSet.isEmpty()) {
                        builder.join(JoinRelType.LEFT, trueLiteral, variablesSet);
                    } else {
                        builder.join(JoinRelType.INNER, trueLiteral, variablesSet);
                    }
                    offset += 2;
                    builder.push(e.rel);
                // fall through
                default:
                    // FLINK MODIFICATION BEGIN
                    fields.add(builder.alias(trueLiteral, "i"));
                    builder.project(fields);
                    builder.distinct();
                    // FLINK MODIFICATION END
            }
        }

        String dtAlias = "dt";
        if (subQueryIndex != 0) {
            dtAlias = "dt" + subQueryIndex;
        }
        builder.as(dtAlias);
        int refOffset = offset;
        final List<RexNode> conditions =
                Pair.zip(expressionOperands, builder.fields()).stream()
                        .map(
                                pair ->
                                        builder.equals(
                                                pair.left, RexUtil.shift(pair.right, refOffset)))
                        .collect(Collectors.toList());
        switch (logic) {
            case TRUE:
                builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet);
                return trueLiteral;
            default:
                break;
        }
        // Now the left join
        builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet);

        final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
        RexLiteral b = trueLiteral;
        switch (logic) {
            case TRUE_FALSE_UNKNOWN:
                b = unknownLiteral;
            // fall through
            case UNKNOWN_AS_TRUE:
                if (allLiterals) {
                    // Considers case when right side of IN is empty
                    // for the case of non-correlated sub-queries
                    if (variablesSet.isEmpty()) {
                        operands.add(builder.isNull(builder.field(dtAlias, "c")), falseLiteral);
                    }
                    operands.add(builder.equals(builder.field(dtAlias, "cs"), falseLiteral), b);
                } else {
                    operands.add(
                            builder.equals(builder.field(ctAlias, "c"), builder.literal(0)),
                            falseLiteral);
                }
                break;
            default:
                break;
        }

        if (!keyIsNulls.isEmpty()) {
            operands.add(builder.or(keyIsNulls), unknownLiteral);
        }

        if (allLiterals) {
            operands.add(builder.isNotNull(builder.field(dtAlias, "cs")), trueLiteral);
        } else {
            operands.add(builder.isNotNull(last(builder.fields())), trueLiteral);
        }

        if (!allLiterals) {
            switch (logic) {
                case TRUE_FALSE_UNKNOWN:
                case UNKNOWN_AS_TRUE:
                    operands.add(
                            builder.lessThan(
                                    builder.field(ctAlias, "ck"), builder.field(ctAlias, "c")),
                            b);
                    break;
                default:
                    break;
            }
        }
        operands.add(falseLiteral);
        return builder.call(SqlStdOperatorTable.CASE, operands.build());
    }

    /**
     * Returns a reference to a particular field, by offset, across several inputs on a {@link
     * RelBuilder}'s stack.
     */
    private static RexInputRef field(RelBuilder builder, int inputCount, int offset) {
        for (int inputOrdinal = 0; ; ) {
            final RelNode r = builder.peek(inputCount, inputOrdinal);
            if (offset < r.getRowType().getFieldCount()) {
                return builder.field(inputCount, inputOrdinal, offset);
            }
            ++inputOrdinal;
            offset -= r.getRowType().getFieldCount();
        }
    }

    /**
     * Returns a list of expressions that project the first {@code fieldCount} fields of the top
     * input on a {@link RelBuilder}'s stack.
     */
    private static List<RexNode> fields(RelBuilder builder, int fieldCount) {
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            projects.add(builder.field(i));
        }
        return projects;
    }

    private static void matchProject(SubQueryRemoveRule rule, RelOptRuleCall call) {
        final Project project = call.rel(0);
        final RelBuilder builder = call.builder();
        final RexSubQuery e = RexUtil.SubQueryFinder.find(project.getProjects());
        assert e != null;
        final RelOptUtil.Logic logic =
                LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN, project.getProjects(), e);
        builder.push(project.getInput());
        final int fieldCount = builder.peek().getRowType().getFieldCount();
        final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
        final RexNode target = rule.apply(e, variablesSet, logic, builder, 1, fieldCount, 0);
        final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
        builder.project(shuttle.apply(project.getProjects()), project.getRowType().getFieldNames());
        call.transformTo(builder.build());
    }

    private static void matchFilter(SubQueryRemoveRule rule, RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Set<CorrelationId> filterVariablesSet = filter.getVariablesSet();
        final RelBuilder builder = call.builder();
        builder.push(filter.getInput());
        int count = 0;
        RexNode c = filter.getCondition();
        while (true) {
            final RexSubQuery e = RexUtil.SubQueryFinder.find(c);
            if (e == null) {
                assert count > 0;
                break;
            }
            ++count;
            final RelOptUtil.Logic logic =
                    LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(c), e);
            final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
            // Filter without variables could be handled before this change, we do not want
            // to break it yet for compatibility reason.
            if (!filterVariablesSet.isEmpty()) {
                // Only consider the correlated variables which originated from this sub-query
                // level.
                variablesSet.retainAll(filterVariablesSet);
            }
            final RexNode target =
                    rule.apply(
                            e,
                            variablesSet,
                            logic,
                            builder,
                            1,
                            builder.peek().getRowType().getFieldCount(),
                            count);
            final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
            c = c.accept(shuttle);
        }
        builder.filter(c);
        builder.project(fields(builder, filter.getRowType().getFieldCount()));
        call.transformTo(builder.build());
    }

    private static void matchJoin(SubQueryRemoveRule rule, RelOptRuleCall call) {
        final Join join = call.rel(0);
        final RelBuilder builder = call.builder();
        final RexSubQuery e = RexUtil.SubQueryFinder.find(join.getCondition());
        assert e != null;
        final RelOptUtil.Logic logic =
                LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(join.getCondition()), e);
        builder.push(join.getLeft());
        builder.push(join.getRight());
        final int fieldCount = join.getRowType().getFieldCount();
        final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
        final RexNode target = rule.apply(e, variablesSet, logic, builder, 2, fieldCount, 0);
        final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
        builder.join(join.getJoinType(), shuttle.apply(join.getCondition()));
        builder.project(fields(builder, join.getRowType().getFieldCount()));
        call.transformTo(builder.build());
    }

    /**
     * Shuttle that replaces occurrences of a given {@link org.apache.calcite.rex.RexSubQuery} with
     * a replacement expression.
     */
    private static class ReplaceSubQueryShuttle extends RexShuttle {
        private final RexSubQuery subQuery;
        private final RexNode replacement;

        ReplaceSubQueryShuttle(RexSubQuery subQuery, RexNode replacement) {
            this.subQuery = subQuery;
            this.replacement = replacement;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            return subQuery.equals(this.subQuery) ? replacement : subQuery;
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config PROJECT =
                ImmutableSubQueryRemoveRule.Config.builder()
                        .withMatchHandler(SubQueryRemoveRule::matchProject)
                        .build()
                        .withOperandSupplier(
                                b ->
                                        b.operand(Project.class)
                                                .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                                                .anyInputs())
                        .withDescription("SubQueryRemoveRule:Project");

        Config FILTER =
                ImmutableSubQueryRemoveRule.Config.builder()
                        .withMatchHandler(SubQueryRemoveRule::matchFilter)
                        .build()
                        .withOperandSupplier(
                                b ->
                                        b.operand(Filter.class)
                                                .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                                                .anyInputs())
                        .withDescription("SubQueryRemoveRule:Filter");

        Config JOIN =
                ImmutableSubQueryRemoveRule.Config.builder()
                        .withMatchHandler(SubQueryRemoveRule::matchJoin)
                        .build()
                        .withOperandSupplier(
                                b ->
                                        b.operand(Join.class)
                                                .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                                                .anyInputs())
                        .withDescription("SubQueryRemoveRule:Join");

        @Override
        default SubQueryRemoveRule toRule() {
            return new SubQueryRemoveRule(this);
        }

        /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
        MatchHandler<SubQueryRemoveRule> matchHandler();

        /** Sets {@link #matchHandler()}. */
        Config withMatchHandler(MatchHandler<SubQueryRemoveRule> matchHandler);
    }
}
