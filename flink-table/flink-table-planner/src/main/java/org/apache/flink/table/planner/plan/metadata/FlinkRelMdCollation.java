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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.calcite.adapter.enumerable.EnumerableCorrelate;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * FlinkRelMdCollation supplies a default implementation of {@link
 * org.apache.calcite.rel.metadata.RelMetadataQuery#collations} for the standard logical algebra.
 */
public class FlinkRelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.COLLATIONS.method, new FlinkRelMdCollation());

    // ~ Constructors -----------------------------------------------------------

    private FlinkRelMdCollation() {}

    // ~ Methods ----------------------------------------------------------------

    public MetadataDef<BuiltInMetadata.Collation> getDef() {
        return BuiltInMetadata.Collation.DEF;
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            TableScan scan, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(table(scan.getTable()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Values values, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                values(mq, values.getRowType(), values.getTuples()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Project project, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                project(mq, project.getInput(), project.getProjects()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Filter rel, RelMetadataQuery mq) {
        return mq.collations(rel.getInput());
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Calc calc, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                calc(mq, calc.getInput(), calc.getProgram()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            SortExchange sort, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(sort(sort.getCollation()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Sort sort, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(sort(sort.getCollation()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Window rel, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                window(mq, rel.getInput(), rel.groups));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            EnumerableCorrelate join, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                enumerableCorrelate(mq, join.getLeft(), join.getRight(), join.getJoinType()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            EnumerableMergeJoin join, RelMetadataQuery mq) {
        // In general a join is not sorted. But a merge join preserves the sort
        // order of the left and right sides.
        return com.google.common.collect.ImmutableList.copyOf(
                mergeJoin(
                        mq,
                        join.getLeft(),
                        join.getRight(),
                        join.analyzeCondition().leftKeys,
                        join.analyzeCondition().rightKeys));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            EnumerableHashJoin join, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                enumerableHashJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            EnumerableNestedLoopJoin join, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                enumerableNestedLoopJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            Match rel, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.copyOf(
                match(
                        mq,
                        rel.getInput(),
                        rel.getRowType(),
                        rel.getPattern(),
                        rel.isStrictStart(),
                        rel.isStrictEnd(),
                        rel.getPatternDefinitions(),
                        rel.getMeasures(),
                        rel.getAfter(),
                        rel.getSubsets(),
                        rel.isAllRows(),
                        rel.getPartitionKeys(),
                        rel.getOrderKeys(),
                        rel.getInterval()));
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            TableModify rel, RelMetadataQuery mq) {
        return mq.collations(rel.getInput());
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            HepRelVertex rel, RelMetadataQuery mq) {
        return mq.collations(rel.getCurrentRel());
    }

    public com.google.common.collect.ImmutableList<RelCollation> collations(
            RelSubset subset, RelMetadataQuery mq) {
        if (!Bug.CALCITE_1048_FIXED) {
            // if the best node is null, so we can get the collation based original node, due to
            // the original node is logically equivalent as the rel.
            RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
            return mq.collations(rel);
        } else {
            throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!");
        }
    }

    /**
     * Catch-all implementation for {@link BuiltInMetadata.Collation#collations()}, invoked using
     * reflection, for any relational expression not handled by a more specific method.
     *
     * <p>{@link org.apache.calcite.rel.core.Union}, {@link org.apache.calcite.rel.core.Intersect},
     * {@link org.apache.calcite.rel.core.Minus}, {@link org.apache.calcite.rel.core.Join}, {@link
     * org.apache.calcite.rel.core.Correlate} do not in general return sorted results (but
     * implementations using particular algorithms may).
     *
     * @param rel Relational expression
     * @return Relational expression's collations
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#collations(RelNode)
     */
    public com.google.common.collect.ImmutableList<RelCollation> collations(
            RelNode rel, RelMetadataQuery mq) {
        return com.google.common.collect.ImmutableList.of();
    }

    // Helper methods

    /** Helper method to determine a {@link org.apache.calcite.rel.core.TableScan}'s collation. */
    public static List<RelCollation> table(RelOptTable table) {
        // Behavior change since CALCITE-4215: the default collations is null.
        // In Flink, the default is an empty list.
        List<RelCollation> collations = table.getCollationList();
        return collations == null ? Collections.emptyList() : collations;
    }

    /**
     * Helper method to determine a {@link org.apache.calcite.rel.core.Values}'s collation.
     *
     * <p>We actually under-report the collations. A Values with 0 or 1 rows - an edge case, but
     * legitimate and very common - is ordered by every permutation of every subset of the columns.
     *
     * <p>So, our algorithm aims to:
     *
     * <ul>
     *   <li>produce at most N collations (where N is the number of columns);
     *   <li>make each collation as long as possible;
     *   <li>do not repeat combinations already emitted - if we've emitted {@code (a, b)} do not
     *       later emit {@code (b, a)};
     *   <li>probe the actual values and make sure that each collation is consistent with the data
     * </ul>
     *
     * <p>So, for an empty Values with 4 columns, we would emit {@code (a, b, c, d), (b, c, d), (c,
     * d), (d)}.
     */
    public static List<RelCollation> values(
            RelMetadataQuery mq,
            RelDataType rowType,
            com.google.common.collect.ImmutableList<
                            com.google.common.collect.ImmutableList<RexLiteral>>
                    tuples) {
        Util.discard(mq); // for future use
        final List<RelCollation> list = new ArrayList<>();
        final int n = rowType.getFieldCount();
        final List<Pair<RelFieldCollation, com.google.common.collect.Ordering<List<RexLiteral>>>>
                pairs = new ArrayList<>();
        outer:
        for (int i = 0; i < n; i++) {
            pairs.clear();
            for (int j = i; j < n; j++) {
                final RelFieldCollation fieldCollation = new RelFieldCollation(j);
                com.google.common.collect.Ordering<List<RexLiteral>> comparator =
                        comparator(fieldCollation);
                com.google.common.collect.Ordering<List<RexLiteral>> ordering;
                if (pairs.isEmpty()) {
                    ordering = comparator;
                } else {
                    ordering = Util.last(pairs).right.compound(comparator);
                }
                pairs.add(Pair.of(fieldCollation, ordering));
                if (!ordering.isOrdered(tuples)) {
                    if (j == i) {
                        continue outer;
                    }
                    pairs.remove(pairs.size() - 1);
                }
            }
            if (!pairs.isEmpty()) {
                list.add(RelCollations.of(Pair.left(pairs)));
            }
        }
        return list;
    }

    private static com.google.common.collect.Ordering<List<RexLiteral>> comparator(
            RelFieldCollation fieldCollation) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        switch (fieldCollation.direction) {
            case ASCENDING:
                return new com.google.common.collect.Ordering<List<RexLiteral>>() {
                    public int compare(List<RexLiteral> o1, List<RexLiteral> o2) {
                        final Comparable c1 = o1.get(x).getValueAs(Comparable.class);
                        final Comparable c2 = o2.get(x).getValueAs(Comparable.class);
                        return RelFieldCollation.compare(c1, c2, nullComparison);
                    }
                };
            default:
                return new com.google.common.collect.Ordering<List<RexLiteral>>() {
                    public int compare(List<RexLiteral> o1, List<RexLiteral> o2) {
                        final Comparable c1 = o1.get(x).getValueAs(Comparable.class);
                        final Comparable c2 = o2.get(x).getValueAs(Comparable.class);
                        return RelFieldCollation.compare(c2, c1, -nullComparison);
                    }
                };
        }
    }

    /** Helper method to determine a {@link Project}'s collation. */
    public static List<RelCollation> project(
            RelMetadataQuery mq, RelNode input, List<? extends RexNode> projects) {
        final SortedSet<RelCollation> collations = new TreeSet<>();
        final List<RelCollation> inputCollations = mq.collations(input);
        if (inputCollations == null || inputCollations.isEmpty()) {
            return com.google.common.collect.ImmutableList.of();
        }
        final com.google.common.collect.Multimap<Integer, Integer> targets =
                com.google.common.collect.LinkedListMultimap.create();
        final Map<Integer, SqlMonotonicity> targetsWithMonotonicity = new HashMap<>();
        for (Ord<RexNode> project : Ord.<RexNode>zip(projects)) {
            if (project.e instanceof RexInputRef) {
                targets.put(((RexInputRef) project.e).getIndex(), project.i);
            } else if (project.e instanceof RexCall) {
                final RexCall call = (RexCall) project.e;
                final RexCallBinding binding =
                        RexCallBinding.create(
                                input.getCluster().getTypeFactory(), call, inputCollations);
                targetsWithMonotonicity.put(project.i, call.getOperator().getMonotonicity(binding));
            }
        }
        final List<RelFieldCollation> fieldCollations = new ArrayList<>();
        loop:
        for (RelCollation ic : inputCollations) {
            if (ic.getFieldCollations().isEmpty()) {
                continue;
            }
            fieldCollations.clear();
            for (RelFieldCollation ifc : ic.getFieldCollations()) {
                final Collection<Integer> integers = targets.get(ifc.getFieldIndex());
                if (integers.isEmpty()) {
                    continue loop; // cannot do this collation
                }
                fieldCollations.add(ifc.withFieldIndex(integers.iterator().next()));
            }
            assert !fieldCollations.isEmpty();
            collations.add(RelCollations.of(fieldCollations));
        }

        final List<RelFieldCollation> fieldCollationsForRexCalls = new ArrayList<>();
        for (Map.Entry<Integer, SqlMonotonicity> entry : targetsWithMonotonicity.entrySet()) {
            final SqlMonotonicity value = entry.getValue();
            switch (value) {
                case NOT_MONOTONIC:
                case CONSTANT:
                    break;
                default:
                    fieldCollationsForRexCalls.add(
                            new RelFieldCollation(
                                    entry.getKey(), RelFieldCollation.Direction.of(value)));
                    break;
            }
        }

        if (!fieldCollationsForRexCalls.isEmpty()) {
            collations.add(RelCollations.of(fieldCollationsForRexCalls));
        }

        return com.google.common.collect.ImmutableList.copyOf(collations);
    }

    /** Helper method to determine a {@link org.apache.calcite.rel.core.Filter}'s collation. */
    public static List<RelCollation> filter(RelMetadataQuery mq, RelNode input) {
        return mq.collations(input);
    }

    /** Helper method to determine a {@link org.apache.calcite.rel.core.Calc}'s collation. */
    public static List<RelCollation> calc(RelMetadataQuery mq, RelNode input, RexProgram program) {
        final List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        return project(mq, input, projects);
    }

    /** Helper method to determine a {@link org.apache.calcite.rel.core.Snapshot}'s collation. */
    public static List<RelCollation> snapshot(RelMetadataQuery mq, RelNode input) {
        return mq.collations(input);
    }

    /** Helper method to determine a {@link org.apache.calcite.rel.core.Sort}'s collation. */
    public static List<RelCollation> sort(RelCollation collation) {
        return com.google.common.collect.ImmutableList.of(collation);
    }

    /** Helper method to determine a limit's collation. */
    public static List<RelCollation> limit(RelMetadataQuery mq, RelNode input) {
        return mq.collations(input);
    }

    /**
     * Helper method to determine a {@link org.apache.calcite.rel.core.Window}'s collation.
     *
     * <p>A Window projects the fields of its input first, followed by the output from each of its
     * windows. Assuming (quite reasonably) that the implementation does not re-order its input
     * rows, then any collations of its input are preserved.
     */
    public static List<RelCollation> window(
            RelMetadataQuery mq,
            RelNode input,
            com.google.common.collect.ImmutableList<Window.Group> groups) {
        return mq.collations(input);
    }

    /** Helper method to determine a {@link org.apache.calcite.rel.core.Match}'s collation. */
    public static List<RelCollation> match(
            RelMetadataQuery mq,
            RelNode input,
            RelDataType rowType,
            RexNode pattern,
            boolean strictStart,
            boolean strictEnd,
            Map<String, RexNode> patternDefinitions,
            Map<String, RexNode> measures,
            RexNode after,
            Map<String, ? extends SortedSet<String>> subsets,
            boolean allRows,
            ImmutableBitSet partitionKeys,
            RelCollation orderKeys,
            RexNode interval) {
        return mq.collations(input);
    }

    /**
     * Helper method to determine a {@link Join}'s collation assuming that it uses a merge-join
     * algorithm.
     *
     * <p>If the inputs are sorted on other keys <em>in addition to</em> the join key, the result
     * preserves those collations too.
     */
    public static List<RelCollation> mergeJoin(
            RelMetadataQuery mq,
            RelNode left,
            RelNode right,
            ImmutableIntList leftKeys,
            ImmutableIntList rightKeys) {
        final com.google.common.collect.ImmutableList.Builder<RelCollation> builder =
                com.google.common.collect.ImmutableList.builder();

        final com.google.common.collect.ImmutableList<RelCollation> leftCollations =
                mq.collations(left);
        assert RelCollations.contains(leftCollations, leftKeys)
                : "cannot merge join: left input is not sorted on left keys";
        builder.addAll(leftCollations);

        final com.google.common.collect.ImmutableList<RelCollation> rightCollations =
                mq.collations(right);
        assert RelCollations.contains(rightCollations, rightKeys)
                : "cannot merge join: right input is not sorted on right keys";
        final int leftFieldCount = left.getRowType().getFieldCount();
        for (RelCollation collation : rightCollations) {
            builder.add(RelCollations.shift(collation, leftFieldCount));
        }
        return builder.build();
    }

    /**
     * Returns the collation of {@link EnumerableHashJoin} based on its inputs and the join type.
     */
    public static List<RelCollation> enumerableHashJoin(
            RelMetadataQuery mq, RelNode left, RelNode right, JoinRelType joinType) {
        if (joinType == JoinRelType.SEMI) {
            return enumerableSemiJoin(mq, left, right);
        } else {
            return enumerableJoin0(mq, left, right, joinType);
        }
    }

    /**
     * Returns the collation of {@link EnumerableNestedLoopJoin} based on its inputs and the join
     * type.
     */
    public static List<RelCollation> enumerableNestedLoopJoin(
            RelMetadataQuery mq, RelNode left, RelNode right, JoinRelType joinType) {
        return enumerableJoin0(mq, left, right, joinType);
    }

    public static List<RelCollation> enumerableCorrelate(
            RelMetadataQuery mq, RelNode left, RelNode right, JoinRelType joinType) {
        // The current implementation always preserve the sort order of the left input
        return mq.collations(left);
    }

    public static List<RelCollation> enumerableSemiJoin(
            RelMetadataQuery mq, RelNode left, RelNode right) {
        // The current implementation always preserve the sort order of the left input
        return mq.collations(left);
    }

    public static List<RelCollation> enumerableBatchNestedLoopJoin(
            RelMetadataQuery mq, RelNode left, RelNode right, JoinRelType joinType) {
        // The current implementation always preserve the sort order of the left input
        return mq.collations(left);
    }

    private static List<RelCollation> enumerableJoin0(
            RelMetadataQuery mq, RelNode left, RelNode right, JoinRelType joinType) {
        // The current implementation can preserve the sort order of the left input if one of the
        // following conditions hold:
        // (i) join type is INNER or LEFT;
        // (ii) RelCollation always orders nulls last.
        final com.google.common.collect.ImmutableList<RelCollation> leftCollations =
                mq.collations(left);
        switch (joinType) {
            case SEMI:
            case ANTI:
            case INNER:
            case LEFT:
                return leftCollations;
            case RIGHT:
            case FULL:
                for (RelCollation collation : leftCollations) {
                    for (RelFieldCollation field : collation.getFieldCollations()) {
                        if (!(RelFieldCollation.NullDirection.LAST == field.nullDirection)) {
                            return com.google.common.collect.ImmutableList.of();
                        }
                    }
                }
                return leftCollations;
        }
        return com.google.common.collect.ImmutableList.of();
    }
}
