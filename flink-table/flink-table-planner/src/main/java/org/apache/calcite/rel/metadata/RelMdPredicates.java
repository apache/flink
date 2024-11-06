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
package org.apache.calcite.rel.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Utility to infer Predicates that are applicable above a RelNode.
 *
 * <p>The class was copied over because of * CALCITE-6317 * and should be removed after upgraded to
 * calcite-1.37.0.
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Port fix of CALCITE-6317 (Calcite 1.37.0): Lines 328~351, 396~398
 * </ol>
 *
 * <p>This is currently used by {@link
 * org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule} to infer <em>Predicates</em> that
 * can be inferred from one side of a Join to the other.
 *
 * <p>The PullUp Strategy is sound but not complete. Here are some of the limitations:
 *
 * <ol>
 *   <li>For Aggregations we only PullUp predicates that only contain Grouping Keys. This can be
 *       extended to infer predicates on Aggregation expressions from expressions on the aggregated
 *       columns. For e.g.
 *       <pre>
 * select a, max(b) from R1 where b &gt; 7
 *   &rarr; max(b) &gt; 7 or max(b) is null
 * </pre>
 *   <li>For Projections we only look at columns that are projected without any function applied.
 *       So:
 *       <pre>
 * select a from R1 where a &gt; 7
 *   &rarr; "a &gt; 7" is pulled up from the Projection.
 * select a + 1 from R1 where a + 1 &gt; 7
 *   &rarr; "a + 1 gt; 7" is not pulled up
 * </pre>
 *   <li>There are several restrictions on Joins:
 *       <ul>
 *         <li>We only pullUp inferred predicates for now. Pulling up existing predicates causes an
 *             explosion of duplicates. The existing predicates are pushed back down as new
 *             predicates. Once we have rules to eliminate duplicate Filter conditions, we should
 *             pullUp all predicates.
 *         <li>For Left Outer: we infer new predicates from the left and set them as applicable on
 *             the Right side. No predicates are pulledUp.
 *         <li>Right Outer Joins are handled in an analogous manner.
 *         <li>For Full Outer Joins no predicates are pulledUp or inferred.
 *       </ul>
 * </ol>
 */
public class RelMdPredicates implements MetadataHandler<BuiltInMetadata.Predicates> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new RelMdPredicates(), BuiltInMetadata.Predicates.Handler.class);

    private static final List<RexNode> EMPTY_LIST = ImmutableList.of();

    @Override
    public MetadataDef<BuiltInMetadata.Predicates> getDef() {
        return BuiltInMetadata.Predicates.DEF;
    }

    /**
     * Catch-all implementation for {@link BuiltInMetadata.Predicates#getPredicates()}, invoked
     * using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getPulledUpPredicates(RelNode)
     */
    public RelOptPredicateList getPredicates(RelNode rel, RelMetadataQuery mq) {
        return RelOptPredicateList.EMPTY;
    }

    /** Infers predicates for a table scan. */
    public RelOptPredicateList getPredicates(TableScan scan, RelMetadataQuery mq) {
        final BuiltInMetadata.Predicates.Handler handler =
                scan.getTable().unwrap(BuiltInMetadata.Predicates.Handler.class);
        if (handler != null) {
            return handler.getPredicates(scan, mq);
        }
        return RelOptPredicateList.EMPTY;
    }

    /**
     * Infers predicates for a project.
     *
     * <ol>
     *   <li>create a mapping from input to projection. Map only positions that directly reference
     *       an input column.
     *   <li>Expressions that only contain above columns are retained in the Project's
     *       pullExpressions list.
     *   <li>For e.g. expression 'a + e = 9' below will not be pulled up because 'e' is not in the
     *       projection list.
     *       <blockquote>
     *       <pre>
     * inputPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
     * projectionExprs:       {a, b, c, e / 2}
     * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
     * </pre>
     *       </blockquote>
     * </ol>
     */
    public RelOptPredicateList getPredicates(Project project, RelMetadataQuery mq) {
        final RelNode input = project.getInput();
        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);
        final List<RexNode> projectPullUpPredicates = new ArrayList<>();

        ImmutableBitSet.Builder columnsMappedBuilder = ImmutableBitSet.builder();
        Mapping m =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION,
                        input.getRowType().getFieldCount(),
                        project.getRowType().getFieldCount());

        for (Ord<RexNode> expr : Ord.zip(project.getProjects())) {
            if (expr.e instanceof RexInputRef) {
                int sIdx = ((RexInputRef) expr.e).getIndex();
                m.set(sIdx, expr.i);
                columnsMappedBuilder.set(sIdx);
                // Project can also generate constants. We need to include them.
            } else if (RexLiteral.isNullLiteral(expr.e)) {
                projectPullUpPredicates.add(
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.IS_NULL,
                                rexBuilder.makeInputRef(project, expr.i)));
            } else if (RexUtil.isConstant(expr.e)) {
                final List<RexNode> args =
                        ImmutableList.of(rexBuilder.makeInputRef(project, expr.i), expr.e);
                final SqlOperator op =
                        args.get(0).getType().isNullable() || args.get(1).getType().isNullable()
                                ? SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
                                : SqlStdOperatorTable.EQUALS;
                projectPullUpPredicates.add(rexBuilder.makeCall(op, args));
            }
        }

        // Go over childPullUpPredicates. If a predicate only contains columns in
        // 'columnsMapped' construct a new predicate based on mapping.
        final ImmutableBitSet columnsMapped = columnsMappedBuilder.build();
        for (RexNode r : inputInfo.pulledUpPredicates) {
            RexNode r2 = projectPredicate(rexBuilder, input, r, columnsMapped);
            if (!r2.isAlwaysTrue()) {
                r2 = r2.accept(new RexPermuteInputsShuttle(m, input));
                projectPullUpPredicates.add(r2);
            }
        }
        return RelOptPredicateList.of(rexBuilder, projectPullUpPredicates);
    }

    /**
     * Converts a predicate on a particular set of columns into a predicate on a subset of those
     * columns, weakening if necessary.
     *
     * <p>If not possible to simplify, returns {@code true}, which is the weakest possible
     * predicate.
     *
     * <p>Examples:
     *
     * <ol>
     *   <li>The predicate {@code $7 = $9} on columns [7] becomes {@code $7 is not null}
     *   <li>The predicate {@code $7 = $9 + $11} on columns [7, 9] becomes {@code $7 is not null or
     *       $9 is not null}
     *   <li>The predicate {@code $7 = $9 and $9 = 5} on columns [7] becomes {@code $7 = 5}
     *   <li>The predicate {@code $7 = $9 and ($9 = $1 or $9 = $2) and $1 > 3 and $2 > 10} on
     *       columns [7] becomes {@code $7 > 3}
     * </ol>
     *
     * <p>We currently only handle examples 1 and 2.
     *
     * @param rexBuilder Rex builder
     * @param input Input relational expression
     * @param r Predicate expression
     * @param columnsMapped Columns which the final predicate can reference
     * @return Predicate expression narrowed to reference only certain columns
     */
    private static RexNode projectPredicate(
            final RexBuilder rexBuilder, RelNode input, RexNode r, ImmutableBitSet columnsMapped) {
        ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
        if (columnsMapped.contains(rCols)) {
            // All required columns are present. No need to weaken.
            return r;
        }
        if (columnsMapped.intersects(rCols)) {
            final List<RexNode> list = new ArrayList<>();
            for (int c : columnsMapped.intersect(rCols)) {
                if (input.getRowType().getFieldList().get(c).getType().isNullable()
                        && Strong.isNull(r, ImmutableBitSet.of(c))) {
                    list.add(
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.IS_NOT_NULL,
                                    rexBuilder.makeInputRef(input, c)));
                }
            }
            if (!list.isEmpty()) {
                return RexUtil.composeDisjunction(rexBuilder, list);
            }
        }
        // Cannot weaken to anything non-trivial
        return rexBuilder.makeLiteral(true);
    }

    /** Add the Filter condition to the pulledPredicates list from the input. */
    public RelOptPredicateList getPredicates(Filter filter, RelMetadataQuery mq) {
        final RelNode input = filter.getInput();
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);

        // Simplify condition using RexSimplify.
        final RexNode condition = filter.getCondition();
        final RexExecutor executor =
                Util.first(filter.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
        final RexSimplify simplify =
                new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor);
        final RexNode simplifiedCondition = simplify.simplify(condition);

        return Util.first(inputInfo, RelOptPredicateList.EMPTY)
                .union(
                        rexBuilder,
                        RelOptPredicateList.of(
                                rexBuilder,
                                RexUtil.retainDeterministic(
                                        RelOptUtil.conjunctions(simplifiedCondition))));
    }

    /**
     * Infers predicates for a {@link org.apache.calcite.rel.core.Join} (including {@code
     * SemiJoin}).
     */
    public RelOptPredicateList getPredicates(Join join, RelMetadataQuery mq) {
        RelOptCluster cluster = join.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        final RexExecutor executor =
                Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
        final RelNode left = join.getInput(0);
        final RelNode right = join.getInput(1);

        final RelOptPredicateList leftInfo = mq.getPulledUpPredicates(left);
        final RelOptPredicateList rightInfo = mq.getPulledUpPredicates(right);

        JoinConditionBasedPredicateInference joinInference =
                new JoinConditionBasedPredicateInference(
                        join,
                        RexUtil.composeConjunction(rexBuilder, leftInfo.pulledUpPredicates),
                        RexUtil.composeConjunction(rexBuilder, rightInfo.pulledUpPredicates),
                        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor));

        return joinInference.inferPredicates(false);
    }

    // FLINK MODIFICATION BEGIN
    /**
     * Check whether the fields specified by the predicateColumns appear in all the groupSets of the
     * aggregate.
     *
     * @param predicateColumns A list of columns used in a pulled predicate.
     * @param aggregate An aggregation operation.
     * @return Whether all columns appear in all groupsets.
     */
    boolean allGroupSetsOverlap(ImmutableBitSet predicateColumns, Aggregate aggregate) {
        // Consider this example:
        // select deptno, sal, count(*)
        // from emp where deptno = 10
        // group by rollup(sal, deptno)
        // Because of the ROLLUP, we cannot assume
        // that deptno = 10 in the result: deptno may be NULL as well.
        for (ImmutableBitSet groupSet : aggregate.groupSets) {
            if (!groupSet.contains(predicateColumns)) {
                return false;
            }
        }
        return true;
    }

    // FLINK MODIFICATION END

    /**
     * Infers predicates for an Aggregate.
     *
     * <p>Pulls up predicates that only contains references to columns in the GroupSet. For e.g.
     *
     * <blockquote>
     *
     * <pre>
     * inputPullUpExprs : { a &gt; 7, b + c &lt; 10, a + e = 9}
     * groupSet         : { a, b}
     * pulledUpExprs    : { a &gt; 7}
     * </pre>
     *
     * </blockquote>
     */
    public RelOptPredicateList getPredicates(Aggregate agg, RelMetadataQuery mq) {
        final RelNode input = agg.getInput();
        final RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);
        final List<RexNode> aggPullUpPredicates = new ArrayList<>();

        ImmutableBitSet groupKeys = agg.getGroupSet();
        if (groupKeys.isEmpty()) {
            // "GROUP BY ()" can convert an empty relation to a non-empty relation, so
            // it is not valid to pull up predicates. In particular, consider the
            // predicate "false": it is valid on all input rows (trivially - there are
            // no rows!) but not on the output (there is one row).
            return RelOptPredicateList.EMPTY;
        }
        Mapping m =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION,
                        input.getRowType().getFieldCount(),
                        agg.getRowType().getFieldCount());

        int i = 0;
        for (int j : groupKeys) {
            m.set(j, i++);
        }

        for (RexNode r : inputInfo.pulledUpPredicates) {
            ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);

            // FLINK MODIFICATION BEGIN
            if (groupKeys.contains(rCols) && this.allGroupSetsOverlap(rCols, agg)) {
                // FLINK MODIFICATION END
                r = r.accept(new RexPermuteInputsShuttle(m, input));
                aggPullUpPredicates.add(r);
            }
        }
        return RelOptPredicateList.of(rexBuilder, aggPullUpPredicates);
    }

    /** Infers predicates for a Union. */
    public RelOptPredicateList getPredicates(Union union, RelMetadataQuery mq) {
        final RexBuilder rexBuilder = union.getCluster().getRexBuilder();

        Set<RexNode> finalPredicates = new HashSet<>();
        final List<RexNode> finalResidualPredicates = new ArrayList<>();
        for (Ord<RelNode> input : Ord.zip(union.getInputs())) {
            RelOptPredicateList info = mq.getPulledUpPredicates(input.e);
            if (info.pulledUpPredicates.isEmpty()) {
                return RelOptPredicateList.EMPTY;
            }
            final Set<RexNode> predicates = new HashSet<>();
            final List<RexNode> residualPredicates = new ArrayList<>();
            for (RexNode pred : info.pulledUpPredicates) {
                if (input.i == 0) {
                    predicates.add(pred);
                    continue;
                }
                if (finalPredicates.contains(pred)) {
                    predicates.add(pred);
                } else {
                    residualPredicates.add(pred);
                }
            }
            // Add new residual predicates
            finalResidualPredicates.add(RexUtil.composeConjunction(rexBuilder, residualPredicates));
            // Add those that are not part of the final set to residual
            for (RexNode e : finalPredicates) {
                if (!predicates.contains(e)) {
                    // This node was in previous union inputs, but it is not in this one
                    for (int j = 0; j < input.i; j++) {
                        finalResidualPredicates.set(
                                j,
                                RexUtil.composeConjunction(
                                        rexBuilder,
                                        Arrays.asList(finalResidualPredicates.get(j), e)));
                    }
                }
            }
            // Final predicates
            finalPredicates = predicates;
        }

        final List<RexNode> predicates = new ArrayList<>(finalPredicates);
        final RelOptCluster cluster = union.getCluster();
        final RexExecutor executor =
                Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
        RexNode disjunctivePredicate =
                new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
                        .simplifyUnknownAs(
                                rexBuilder.makeCall(
                                        SqlStdOperatorTable.OR, finalResidualPredicates),
                                RexUnknownAs.FALSE);
        if (!disjunctivePredicate.isAlwaysTrue()) {
            predicates.add(disjunctivePredicate);
        }
        return RelOptPredicateList.of(rexBuilder, predicates);
    }

    /** Infers predicates for a Intersect. */
    public RelOptPredicateList getPredicates(Intersect intersect, RelMetadataQuery mq) {
        final RexBuilder rexBuilder = intersect.getCluster().getRexBuilder();

        final RexExecutor executor =
                Util.first(intersect.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);

        final RexImplicationChecker rexImplicationChecker =
                new RexImplicationChecker(rexBuilder, executor, intersect.getRowType());

        Set<RexNode> finalPredicates = new HashSet<>();

        for (Ord<RelNode> input : Ord.zip(intersect.getInputs())) {
            RelOptPredicateList info = mq.getPulledUpPredicates(input.e);
            if (info == null || info.pulledUpPredicates.isEmpty()) {
                continue;
            }

            for (RexNode pred : info.pulledUpPredicates) {
                if (finalPredicates.stream()
                        .anyMatch(finalPred -> rexImplicationChecker.implies(finalPred, pred))) {
                    // There's already a stricter predicate in finalPredicates,
                    // thus no need to count this one.
                    continue;
                }
                // Remove looser predicate and add this one into finalPredicates
                finalPredicates =
                        finalPredicates.stream()
                                .filter(
                                        finalPred ->
                                                !rexImplicationChecker.implies(pred, finalPred))
                                .collect(Collectors.toSet());
                finalPredicates.add(pred);
            }
        }

        return RelOptPredicateList.of(rexBuilder, finalPredicates);
    }

    /** Infers predicates for a Minus. */
    public RelOptPredicateList getPredicates(Minus minus, RelMetadataQuery mq) {
        return mq.getPulledUpPredicates(minus.getInput(0));
    }

    /** Infers predicates for a Sort. */
    public RelOptPredicateList getPredicates(Sort sort, RelMetadataQuery mq) {
        RelNode input = sort.getInput();
        return mq.getPulledUpPredicates(input);
    }

    /** Infers predicates for a TableModify. */
    public RelOptPredicateList getPredicates(TableModify tableModify, RelMetadataQuery mq) {
        return mq.getPulledUpPredicates(tableModify.getInput());
    }

    /** Infers predicates for an Exchange. */
    public RelOptPredicateList getPredicates(Exchange exchange, RelMetadataQuery mq) {
        RelNode input = exchange.getInput();
        return mq.getPulledUpPredicates(input);
    }

    // CHECKSTYLE: IGNORE 1
    /**
     * Returns the {@link BuiltInMetadata.Predicates#getPredicates()} statistic.
     *
     * @see RelMetadataQuery#getPulledUpPredicates(RelNode)
     */
    public RelOptPredicateList getPredicates(RelSubset r, RelMetadataQuery mq) {
        if (!Bug.CALCITE_1048_FIXED) {
            return RelOptPredicateList.EMPTY;
        }
        final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
        RelOptPredicateList list = null;
        for (RelNode r2 : r.getRels()) {
            RelOptPredicateList list2 = mq.getPulledUpPredicates(r2);
            if (list2 != null) {
                list = list == null ? list2 : list.union(rexBuilder, list2);
            }
        }
        return Util.first(list, RelOptPredicateList.EMPTY);
    }

    /**
     * Utility to infer predicates from one side of the join that apply on the other side.
     *
     * <p>Contract is:
     *
     * <ul>
     *   <li>initialize with a {@link org.apache.calcite.rel.core.Join} and optional predicates
     *       applicable on its left and right subtrees.
     *   <li>you can then ask it for equivalentPredicate(s) given a predicate.
     * </ul>
     *
     * <p>So for:
     *
     * <ol>
     *   <li>'<code>R1(x) join R2(y) on x = y</code>' a call for equivalentPredicates on '<code>
     *       x &gt; 7</code>' will return ' <code>[y &gt; 7]</code>'
     *   <li>'<code>R1(x) join R2(y) on x = y join R3(z) on y = z</code>' a call for
     *       equivalentPredicates on the second join '<code>x &gt; 7</code>' will return
     * </ol>
     */
    static class JoinConditionBasedPredicateInference {
        final Join joinRel;
        final int nSysFields;
        final int nFieldsLeft;
        final int nFieldsRight;
        final ImmutableBitSet leftFieldsBitSet;
        final ImmutableBitSet rightFieldsBitSet;
        final ImmutableBitSet allFieldsBitSet;

        @SuppressWarnings("JdkObsolete")
        SortedMap<Integer, BitSet> equivalence;

        final Map<RexNode, ImmutableBitSet> exprFields;
        final Set<RexNode> allExprs;
        final Set<RexNode> equalityPredicates;
        final @Nullable RexNode leftChildPredicates;
        final @Nullable RexNode rightChildPredicates;
        final RexSimplify simplify;

        @SuppressWarnings("JdkObsolete")
        JoinConditionBasedPredicateInference(
                Join joinRel,
                @Nullable RexNode leftPredicates,
                @Nullable RexNode rightPredicates,
                RexSimplify simplify) {
            super();
            this.joinRel = joinRel;
            this.simplify = simplify;
            nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
            nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
            nSysFields = joinRel.getSystemFieldList().size();
            leftFieldsBitSet = ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
            rightFieldsBitSet =
                    ImmutableBitSet.range(
                            nSysFields + nFieldsLeft, nSysFields + nFieldsLeft + nFieldsRight);
            allFieldsBitSet = ImmutableBitSet.range(0, nSysFields + nFieldsLeft + nFieldsRight);

            exprFields = new HashMap<>();
            allExprs = new HashSet<>();

            if (leftPredicates == null) {
                leftChildPredicates = null;
            } else {
                Mappings.TargetMapping leftMapping =
                        Mappings.createShiftMapping(
                                nSysFields + nFieldsLeft, nSysFields, 0, nFieldsLeft);
                leftChildPredicates =
                        leftPredicates.accept(
                                new RexPermuteInputsShuttle(leftMapping, joinRel.getInput(0)));

                allExprs.add(leftChildPredicates);
                for (RexNode r : RelOptUtil.conjunctions(leftChildPredicates)) {
                    exprFields.put(r, RelOptUtil.InputFinder.bits(r));
                    allExprs.add(r);
                }
            }
            if (rightPredicates == null) {
                rightChildPredicates = null;
            } else {
                Mappings.TargetMapping rightMapping =
                        Mappings.createShiftMapping(
                                nSysFields + nFieldsLeft + nFieldsRight,
                                nSysFields + nFieldsLeft,
                                0,
                                nFieldsRight);
                rightChildPredicates =
                        rightPredicates.accept(
                                new RexPermuteInputsShuttle(rightMapping, joinRel.getInput(1)));

                allExprs.add(rightChildPredicates);
                for (RexNode r : RelOptUtil.conjunctions(rightChildPredicates)) {
                    exprFields.put(r, RelOptUtil.InputFinder.bits(r));
                    allExprs.add(r);
                }
            }

            equivalence = new TreeMap<>();
            equalityPredicates = new HashSet<>();
            for (int i = 0; i < nSysFields + nFieldsLeft + nFieldsRight; i++) {
                equivalence.put(i, BitSets.of(i));
            }

            // Only process equivalences found in the join conditions. Processing
            // Equivalences from the left or right side infer predicates that are
            // already present in the Tree below the join.
            List<RexNode> exprs = RelOptUtil.conjunctions(joinRel.getCondition());

            final EquivalenceFinder eF = new EquivalenceFinder();
            exprs.forEach(input -> input.accept(eF));

            equivalence = BitSets.closure(equivalence);
        }

        /**
         * The PullUp Strategy is sound but not complete.
         *
         * <ol>
         *   <li>We only pullUp inferred predicates for now. Pulling up existing predicates causes
         *       an explosion of duplicates. The existing predicates are pushed back down as new
         *       predicates. Once we have rules to eliminate duplicate Filter conditions, we should
         *       pullUp all predicates.
         *   <li>For Left Outer: we infer new predicates from the left and set them as applicable on
         *       the Right side. No predicates are pulledUp.
         *   <li>Right Outer Joins are handled in an analogous manner.
         *   <li>For Full Outer Joins no predicates are pulledUp or inferred.
         * </ol>
         */
        public RelOptPredicateList inferPredicates(boolean includeEqualityInference) {
            final List<RexNode> inferredPredicates = new ArrayList<>();
            final Set<RexNode> allExprs = new HashSet<>(this.allExprs);
            final JoinRelType joinType = joinRel.getJoinType();
            switch (joinType) {
                case SEMI:
                case INNER:
                case LEFT:
                    infer(
                            leftChildPredicates,
                            allExprs,
                            inferredPredicates,
                            includeEqualityInference,
                            joinType == JoinRelType.LEFT ? rightFieldsBitSet : allFieldsBitSet);
                    break;
                default:
                    break;
            }
            switch (joinType) {
                case SEMI:
                case INNER:
                case RIGHT:
                    infer(
                            rightChildPredicates,
                            allExprs,
                            inferredPredicates,
                            includeEqualityInference,
                            joinType == JoinRelType.RIGHT ? leftFieldsBitSet : allFieldsBitSet);
                    break;
                default:
                    break;
            }

            Mappings.TargetMapping rightMapping =
                    Mappings.createShiftMapping(
                            nSysFields + nFieldsLeft + nFieldsRight,
                            0,
                            nSysFields + nFieldsLeft,
                            nFieldsRight);
            final RexPermuteInputsShuttle rightPermute =
                    new RexPermuteInputsShuttle(rightMapping, joinRel);
            Mappings.TargetMapping leftMapping =
                    Mappings.createShiftMapping(
                            nSysFields + nFieldsLeft, 0, nSysFields, nFieldsLeft);
            final RexPermuteInputsShuttle leftPermute =
                    new RexPermuteInputsShuttle(leftMapping, joinRel);
            final List<RexNode> leftInferredPredicates = new ArrayList<>();
            final List<RexNode> rightInferredPredicates = new ArrayList<>();

            for (RexNode iP : inferredPredicates) {
                ImmutableBitSet iPBitSet = RelOptUtil.InputFinder.bits(iP);
                if (leftFieldsBitSet.contains(iPBitSet)) {
                    leftInferredPredicates.add(iP.accept(leftPermute));
                } else if (rightFieldsBitSet.contains(iPBitSet)) {
                    rightInferredPredicates.add(iP.accept(rightPermute));
                }
            }

            final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
            switch (joinType) {
                case SEMI:
                    Iterable<RexNode> pulledUpPredicates;
                    pulledUpPredicates =
                            Iterables.concat(
                                    RelOptUtil.conjunctions(leftChildPredicates),
                                    leftInferredPredicates);
                    return RelOptPredicateList.of(
                            rexBuilder,
                            pulledUpPredicates,
                            leftInferredPredicates,
                            rightInferredPredicates);
                case INNER:
                    pulledUpPredicates =
                            Iterables.concat(
                                    RelOptUtil.conjunctions(leftChildPredicates),
                                    RelOptUtil.conjunctions(rightChildPredicates),
                                    RexUtil.retainDeterministic(
                                            RelOptUtil.conjunctions(joinRel.getCondition())),
                                    inferredPredicates);
                    return RelOptPredicateList.of(
                            rexBuilder,
                            pulledUpPredicates,
                            leftInferredPredicates,
                            rightInferredPredicates);
                case LEFT:
                    return RelOptPredicateList.of(
                            rexBuilder,
                            RelOptUtil.conjunctions(leftChildPredicates),
                            leftInferredPredicates,
                            rightInferredPredicates);
                case RIGHT:
                    return RelOptPredicateList.of(
                            rexBuilder,
                            RelOptUtil.conjunctions(rightChildPredicates),
                            inferredPredicates,
                            EMPTY_LIST);
                default:
                    assert inferredPredicates.size() == 0;
                    return RelOptPredicateList.EMPTY;
            }
        }

        public @Nullable RexNode left() {
            return leftChildPredicates;
        }

        public @Nullable RexNode right() {
            return rightChildPredicates;
        }

        private void infer(
                @Nullable RexNode predicates,
                Set<RexNode> allExprs,
                List<RexNode> inferredPredicates,
                boolean includeEqualityInference,
                ImmutableBitSet inferringFields) {
            for (RexNode r : RelOptUtil.conjunctions(predicates)) {
                if (!includeEqualityInference && equalityPredicates.contains(r)) {
                    continue;
                }
                for (Mapping m : mappings(r)) {
                    RexNode tr =
                            r.accept(
                                    new RexPermuteInputsShuttle(
                                            m, joinRel.getInput(0), joinRel.getInput(1)));
                    // Filter predicates can be already simplified, so we should work with
                    // simplified RexNode versions as well. It also allows prevent of having
                    // some duplicates in in result pulledUpPredicates
                    RexNode simplifiedTarget =
                            simplify.simplifyFilterPredicates(RelOptUtil.conjunctions(tr));
                    if (simplifiedTarget == null) {
                        simplifiedTarget = joinRel.getCluster().getRexBuilder().makeLiteral(false);
                    }
                    if (checkTarget(inferringFields, allExprs, tr)
                            && checkTarget(inferringFields, allExprs, simplifiedTarget)) {
                        inferredPredicates.add(simplifiedTarget);
                        allExprs.add(simplifiedTarget);
                    }
                }
            }
        }

        Iterable<Mapping> mappings(final RexNode predicate) {
            final ImmutableBitSet fields =
                    requireNonNull(
                            exprFields.get(predicate),
                            () -> "exprFields.get(predicate) is null for " + predicate);
            if (fields.cardinality() == 0) {
                return Collections.emptyList();
            }
            return () -> new ExprsItr(fields);
        }

        private static boolean checkTarget(
                ImmutableBitSet inferringFields, Set<RexNode> allExprs, RexNode tr) {
            return inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
                    && !allExprs.contains(tr)
                    && !isAlwaysTrue(tr);
        }

        @SuppressWarnings("JdkObsolete")
        private void markAsEquivalent(int p1, int p2) {
            BitSet b = requireNonNull(equivalence.get(p1), () -> "equivalence.get(p1) for " + p1);
            b.set(p2);

            b = requireNonNull(equivalence.get(p2), () -> "equivalence.get(p2) for " + p2);
            b.set(p1);
        }

        /** Find expressions of the form 'col_x = col_y'. */
        class EquivalenceFinder extends RexVisitorImpl<Void> {
            protected EquivalenceFinder() {
                super(true);
            }

            @Override
            public Void visitCall(RexCall call) {
                if (call.getOperator().getKind() == SqlKind.EQUALS) {
                    int lPos = pos(call.getOperands().get(0));
                    int rPos = pos(call.getOperands().get(1));
                    if (lPos != -1 && rPos != -1) {
                        markAsEquivalent(lPos, rPos);
                        equalityPredicates.add(call);
                    }
                }
                return null;
            }
        }

        /**
         * Given an expression returns all the possible substitutions.
         *
         * <p>For example, for an expression 'a + b + c' and the following equivalences:
         *
         * <pre>
         * a : {a, b}
         * b : {a, b}
         * c : {c, e}
         * </pre>
         *
         * <p>The following Mappings will be returned:
         *
         * <pre>
         * {a &rarr; a, b &rarr; a, c &rarr; c}
         * {a &rarr; a, b &rarr; a, c &rarr; e}
         * {a &rarr; a, b &rarr; b, c &rarr; c}
         * {a &rarr; a, b &rarr; b, c &rarr; e}
         * {a &rarr; b, b &rarr; a, c &rarr; c}
         * {a &rarr; b, b &rarr; a, c &rarr; e}
         * {a &rarr; b, b &rarr; b, c &rarr; c}
         * {a &rarr; b, b &rarr; b, c &rarr; e}
         * </pre>
         *
         * <p>which imply the following inferences:
         *
         * <pre>
         * a + a + c
         * a + a + e
         * a + b + c
         * a + b + e
         * b + a + c
         * b + a + e
         * b + b + c
         * b + b + e
         * </pre>
         */
        class ExprsItr implements Iterator<Mapping> {
            final int[] columns;
            final BitSet[] columnSets;
            final int[] iterationIdx;
            @Nullable Mapping nextMapping;
            boolean firstCall;

            @SuppressWarnings("JdkObsolete")
            ExprsItr(ImmutableBitSet fields) {
                nextMapping = null;
                columns = new int[fields.cardinality()];
                columnSets = new BitSet[fields.cardinality()];
                iterationIdx = new int[fields.cardinality()];
                for (int j = 0, i = fields.nextSetBit(0);
                        i >= 0;
                        i = fields.nextSetBit(i + 1), j++) {
                    columns[j] = i;
                    int fieldIndex = i;
                    columnSets[j] =
                            requireNonNull(
                                    equivalence.get(i),
                                    () ->
                                            "equivalence.get(i) is null for "
                                                    + fieldIndex
                                                    + ", "
                                                    + equivalence);
                    iterationIdx[j] = 0;
                }
                firstCall = true;
            }

            @Override
            public boolean hasNext() {
                if (firstCall) {
                    initializeMapping();
                    firstCall = false;
                } else {
                    computeNextMapping(iterationIdx.length - 1);
                }
                return nextMapping != null;
            }

            @Override
            public Mapping next() {
                if (nextMapping == null) {
                    throw new NoSuchElementException();
                }
                return nextMapping;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void computeNextMapping(int level) {
                int t = columnSets[level].nextSetBit(iterationIdx[level]);
                if (t < 0) {
                    if (level == 0) {
                        nextMapping = null;
                    } else {
                        int tmp = columnSets[level].nextSetBit(0);
                        requireNonNull(nextMapping, "nextMapping").set(columns[level], tmp);
                        iterationIdx[level] = tmp + 1;
                        computeNextMapping(level - 1);
                    }
                } else {
                    requireNonNull(nextMapping, "nextMapping").set(columns[level], t);
                    iterationIdx[level] = t + 1;
                }
            }

            private void initializeMapping() {
                nextMapping =
                        Mappings.create(
                                MappingType.PARTIAL_FUNCTION,
                                nSysFields + nFieldsLeft + nFieldsRight,
                                nSysFields + nFieldsLeft + nFieldsRight);
                for (int i = 0; i < columnSets.length; i++) {
                    BitSet c = columnSets[i];
                    int t = c.nextSetBit(iterationIdx[i]);
                    if (t < 0) {
                        nextMapping = null;
                        return;
                    }
                    nextMapping.set(columns[i], t);
                    iterationIdx[i] = t + 1;
                }
            }
        }

        private static int pos(RexNode expr) {
            if (expr instanceof RexInputRef) {
                return ((RexInputRef) expr).getIndex();
            }
            return -1;
        }

        private static boolean isAlwaysTrue(RexNode predicate) {
            if (predicate instanceof RexCall) {
                RexCall c = (RexCall) predicate;
                if (c.getOperator().getKind() == SqlKind.EQUALS) {
                    int lPos = pos(c.getOperands().get(0));
                    int rPos = pos(c.getOperands().get(1));
                    return lPos != -1 && lPos == rPos;
                }
            }
            return predicate.isAlwaysTrue();
        }
    }
}
