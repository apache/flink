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

import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * This rule is copied from Calcite's {@link
 * org.apache.calcite.rel.rules.AggregateJoinTransposeRule}. Modification: - Do not match temporal
 * join since lookup table source doesn't support aggregate. - Fix type mismatch error - Support
 * aggregate with AUXILIARY_GROUP
 */

/**
 * Planner rule that pushes an {@link org.apache.calcite.rel.core.Aggregate} past a {@link
 * org.apache.calcite.rel.core.Join}.
 */
public class FlinkAggregateJoinTransposeRule extends RelOptRule {
    public static final FlinkAggregateJoinTransposeRule INSTANCE =
            new FlinkAggregateJoinTransposeRule(
                    LogicalAggregate.class, LogicalJoin.class, RelFactories.LOGICAL_BUILDER, false);

    /** Extended instance of the rule that can push down aggregate functions. */
    public static final FlinkAggregateJoinTransposeRule EXTENDED =
            new FlinkAggregateJoinTransposeRule(
                    LogicalAggregate.class, LogicalJoin.class, RelFactories.LOGICAL_BUILDER, true);

    private final boolean allowFunctions;

    /** Creates an FlinkAggregateJoinTransposeRule. */
    public FlinkAggregateJoinTransposeRule(
            Class<? extends Aggregate> aggregateClass,
            Class<? extends Join> joinClass,
            RelBuilderFactory relBuilderFactory,
            boolean allowFunctions) {
        super(
                operandJ(
                        aggregateClass,
                        null,
                        aggregate -> aggregate.getGroupType() == Aggregate.Group.SIMPLE,
                        operand(joinClass, any())),
                relBuilderFactory,
                null);

        this.allowFunctions = allowFunctions;
    }

    @Deprecated // to be removed before 2.0
    public FlinkAggregateJoinTransposeRule(
            Class<? extends Aggregate> aggregateClass,
            RelFactories.AggregateFactory aggregateFactory,
            Class<? extends Join> joinClass,
            RelFactories.JoinFactory joinFactory) {
        this(aggregateClass, joinClass, RelBuilder.proto(aggregateFactory, joinFactory), false);
    }

    @Deprecated // to be removed before 2.0
    public FlinkAggregateJoinTransposeRule(
            Class<? extends Aggregate> aggregateClass,
            RelFactories.AggregateFactory aggregateFactory,
            Class<? extends Join> joinClass,
            RelFactories.JoinFactory joinFactory,
            boolean allowFunctions) {
        this(
                aggregateClass,
                joinClass,
                RelBuilder.proto(aggregateFactory, joinFactory),
                allowFunctions);
    }

    @Deprecated // to be removed before 2.0
    public FlinkAggregateJoinTransposeRule(
            Class<? extends Aggregate> aggregateClass,
            RelFactories.AggregateFactory aggregateFactory,
            Class<? extends Join> joinClass,
            RelFactories.JoinFactory joinFactory,
            RelFactories.ProjectFactory projectFactory) {
        this(
                aggregateClass,
                joinClass,
                RelBuilder.proto(aggregateFactory, joinFactory, projectFactory),
                false);
    }

    @Deprecated // to be removed before 2.0
    public FlinkAggregateJoinTransposeRule(
            Class<? extends Aggregate> aggregateClass,
            RelFactories.AggregateFactory aggregateFactory,
            Class<? extends Join> joinClass,
            RelFactories.JoinFactory joinFactory,
            RelFactories.ProjectFactory projectFactory,
            boolean allowFunctions) {
        this(
                aggregateClass,
                joinClass,
                RelBuilder.proto(aggregateFactory, joinFactory, projectFactory),
                allowFunctions);
    }

    private boolean containsSnapshot(RelNode relNode) {
        RelNode original = null;
        if (relNode instanceof RelSubset) {
            original = ((RelSubset) relNode).getOriginal();
        } else if (relNode instanceof HepRelVertex) {
            original = ((HepRelVertex) relNode).getCurrentRel();
        } else {
            original = relNode;
        }
        if (original instanceof LogicalSnapshot) {
            return true;
        } else if (original instanceof SingleRel) {
            return containsSnapshot(((SingleRel) original).getInput());
        } else {
            return false;
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // avoid push aggregates through dim join
        Join join = call.rel(1);
        RelNode right = join.getRight();
        // right tree should not contain temporal table
        return !containsSnapshot(right);
    }

    public void onMatch(RelOptRuleCall call) {
        final Aggregate origAgg = call.rel(0);
        final Join join = call.rel(1);
        final RexBuilder rexBuilder = origAgg.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();

        // converts an aggregate with AUXILIARY_GROUP to a regular aggregate.
        // if the converted aggregate can be push down,
        // AggregateReduceGroupingRule will try reduce grouping of new aggregates created by this
        // rule
        final Pair<Aggregate, List<RexNode>> newAggAndProject = toRegularAggregate(origAgg);
        final Aggregate aggregate = newAggAndProject.left;
        final List<RexNode> projectAfterAgg = newAggAndProject.right;

        // If any aggregate functions do not support splitting, bail out
        // If any aggregate call has a filter or distinct, bail out
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            if (aggregateCall.getAggregation().unwrap(SqlSplittableAggFunction.class) == null) {
                return;
            }
            if (aggregateCall.filterArg >= 0 || aggregateCall.isDistinct()) {
                return;
            }
        }

        if (join.getJoinType() != JoinRelType.INNER) {
            return;
        }

        if (!allowFunctions && !aggregate.getAggCallList().isEmpty()) {
            return;
        }

        // Do the columns used by the join appear in the output of the aggregate?
        final ImmutableBitSet aggregateColumns = aggregate.getGroupSet();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final ImmutableBitSet keyColumns =
                keyColumns(aggregateColumns, mq.getPulledUpPredicates(join).pulledUpPredicates);
        final ImmutableBitSet joinColumns = RelOptUtil.InputFinder.bits(join.getCondition());
        final boolean allColumnsInAggregate = keyColumns.contains(joinColumns);
        final ImmutableBitSet belowAggregateColumns = aggregateColumns.union(joinColumns);

        // Split join condition
        final List<Integer> leftKeys = com.google.common.collect.Lists.newArrayList();
        final List<Integer> rightKeys = com.google.common.collect.Lists.newArrayList();
        final List<Boolean> filterNulls = com.google.common.collect.Lists.newArrayList();
        RexNode nonEquiConj =
                RelOptUtil.splitJoinCondition(
                        join.getLeft(),
                        join.getRight(),
                        join.getCondition(),
                        leftKeys,
                        rightKeys,
                        filterNulls);
        // If it contains non-equi join conditions, we bail out
        if (!nonEquiConj.isAlwaysTrue()) {
            return;
        }

        // Push each aggregate function down to each side that contains all of its
        // arguments. Note that COUNT(*), because it has no arguments, can go to
        // both sides.
        final Map<Integer, Integer> map = new HashMap<>();
        final List<Side> sides = new ArrayList<>();
        int uniqueCount = 0;
        int offset = 0;
        int belowOffset = 0;
        for (int s = 0; s < 2; s++) {
            final Side side = new Side();
            final RelNode joinInput = join.getInput(s);
            int fieldCount = joinInput.getRowType().getFieldCount();
            final ImmutableBitSet fieldSet = ImmutableBitSet.range(offset, offset + fieldCount);
            final ImmutableBitSet belowAggregateKeyNotShifted =
                    belowAggregateColumns.intersect(fieldSet);
            for (Ord<Integer> c : Ord.zip(belowAggregateKeyNotShifted)) {
                map.put(c.e, belowOffset + c.i);
            }
            final Mappings.TargetMapping mapping =
                    s == 0
                            ? Mappings.createIdentity(fieldCount)
                            : Mappings.createShiftMapping(
                                    fieldCount + offset, 0, offset, fieldCount);

            final ImmutableBitSet belowAggregateKey = belowAggregateKeyNotShifted.shift(-offset);
            final boolean unique;
            if (!allowFunctions) {
                assert aggregate.getAggCallList().isEmpty();
                // If there are no functions, it doesn't matter as much whether we
                // aggregate the inputs before the join, because there will not be
                // any functions experiencing a cartesian product effect.
                //
                // But finding out whether the input is already unique requires a call
                // to areColumnsUnique that currently (until [CALCITE-1048] "Make
                // metadata more robust" is fixed) places a heavy load on
                // the metadata system.
                //
                // So we choose to imagine the the input is already unique, which is
                // untrue but harmless.
                //
                Util.discard(Bug.CALCITE_1048_FIXED);
                unique = true;
            } else {
                final Boolean unique0 = mq.areColumnsUnique(joinInput, belowAggregateKey);
                unique = unique0 != null && unique0;
            }
            if (unique) {
                ++uniqueCount;
                side.aggregate = false;
                relBuilder.push(joinInput);
                final Map<Integer, Integer> belowAggregateKeyToNewProjectMap = new HashMap<>();
                final List<RexNode> projects = new ArrayList<>();
                for (Integer i : belowAggregateKey) {
                    belowAggregateKeyToNewProjectMap.put(i, projects.size());
                    projects.add(relBuilder.field(i));
                }
                for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
                    final SqlAggFunction aggregation = aggCall.e.getAggregation();
                    final SqlSplittableAggFunction splitter =
                            Preconditions.checkNotNull(
                                    aggregation.unwrap(SqlSplittableAggFunction.class));
                    if (!aggCall.e.getArgList().isEmpty()
                            && fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
                        final RexNode singleton =
                                splitter.singleton(
                                        rexBuilder,
                                        joinInput.getRowType(),
                                        aggCall.e.transform(mapping));
                        final RexNode targetSingleton =
                                rexBuilder.ensureType(aggCall.e.type, singleton, false);

                        if (targetSingleton instanceof RexInputRef) {
                            final int index = ((RexInputRef) targetSingleton).getIndex();
                            if (!belowAggregateKey.get(index)) {
                                projects.add(targetSingleton);
                                side.split.put(aggCall.i, projects.size() - 1);
                            } else {
                                side.split.put(
                                        aggCall.i, belowAggregateKeyToNewProjectMap.get(index));
                            }
                        } else {
                            projects.add(targetSingleton);
                            side.split.put(aggCall.i, projects.size() - 1);
                        }
                    }
                }
                relBuilder.project(projects);
                side.newInput = relBuilder.build();
            } else {
                side.aggregate = true;
                List<AggregateCall> belowAggCalls = new ArrayList<>();
                final SqlSplittableAggFunction.Registry<AggregateCall> belowAggCallRegistry =
                        registry(belowAggCalls);
                final int oldGroupKeyCount = aggregate.getGroupCount();
                final int newGroupKeyCount = belowAggregateKey.cardinality();
                for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
                    final SqlAggFunction aggregation = aggCall.e.getAggregation();
                    final SqlSplittableAggFunction splitter =
                            Preconditions.checkNotNull(
                                    aggregation.unwrap(SqlSplittableAggFunction.class));
                    final AggregateCall call1;
                    if (fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
                        final AggregateCall splitCall = splitter.split(aggCall.e, mapping);
                        call1 =
                                splitCall.adaptTo(
                                        joinInput,
                                        splitCall.getArgList(),
                                        splitCall.filterArg,
                                        oldGroupKeyCount,
                                        newGroupKeyCount);
                    } else {
                        call1 = splitter.other(rexBuilder.getTypeFactory(), aggCall.e);
                    }
                    if (call1 != null) {
                        side.split.put(
                                aggCall.i,
                                belowAggregateKey.cardinality()
                                        + belowAggCallRegistry.register(call1));
                    }
                }
                side.newInput =
                        relBuilder
                                .push(joinInput)
                                .aggregate(
                                        relBuilder.groupKey(belowAggregateKey, null), belowAggCalls)
                                .build();
            }
            offset += fieldCount;
            belowOffset += side.newInput.getRowType().getFieldCount();
            sides.add(side);
        }

        if (uniqueCount == 2) {
            // Both inputs to the join are unique. There is nothing to be gained by
            // this rule. In fact, this aggregate+join may be the result of a previous
            // invocation of this rule; if we continue we might loop forever.
            return;
        }

        // Update condition
        final Mapping mapping =
                (Mapping) Mappings.target(map::get, join.getRowType().getFieldCount(), belowOffset);
        final RexNode newCondition = RexUtil.apply(mapping, join.getCondition());

        // Create new join
        relBuilder
                .push(sides.get(0).newInput)
                .push(sides.get(1).newInput)
                .join(join.getJoinType(), newCondition);

        // Aggregate above to sum up the sub-totals
        final List<AggregateCall> newAggCalls = new ArrayList<>();
        final int groupIndicatorCount = aggregate.getGroupCount() + aggregate.getIndicatorCount();
        final int newLeftWidth = sides.get(0).newInput.getRowType().getFieldCount();
        final List<RexNode> projects =
                new ArrayList<>(rexBuilder.identityProjects(relBuilder.peek().getRowType()));
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
            final SqlAggFunction aggregation = aggCall.e.getAggregation();
            final SqlSplittableAggFunction splitter =
                    Preconditions.checkNotNull(aggregation.unwrap(SqlSplittableAggFunction.class));
            final Integer leftSubTotal = sides.get(0).split.get(aggCall.i);
            final Integer rightSubTotal = sides.get(1).split.get(aggCall.i);
            newAggCalls.add(
                    splitter.topSplit(
                            rexBuilder,
                            registry(projects),
                            groupIndicatorCount,
                            relBuilder.peek().getRowType(),
                            aggCall.e,
                            leftSubTotal == null ? -1 : leftSubTotal,
                            rightSubTotal == null ? -1 : rightSubTotal + newLeftWidth));
        }

        relBuilder.project(projects);

        boolean aggConvertedToProjects = false;
        if (allColumnsInAggregate) {
            // let's see if we can convert aggregate into projects
            List<RexNode> projects2 = new ArrayList<>();
            for (int key : Mappings.apply(mapping, aggregate.getGroupSet())) {
                projects2.add(relBuilder.field(key));
            }
            int aggCallIdx = projects2.size();
            for (AggregateCall newAggCall : newAggCalls) {
                final SqlSplittableAggFunction splitter =
                        newAggCall.getAggregation().unwrap(SqlSplittableAggFunction.class);
                if (splitter != null) {
                    final RelDataType rowType = relBuilder.peek().getRowType();
                    final RexNode singleton = splitter.singleton(rexBuilder, rowType, newAggCall);
                    final RelDataType originalAggCallType =
                            aggregate.getRowType().getFieldList().get(aggCallIdx).getType();
                    final RexNode targetSingleton =
                            rexBuilder.ensureType(originalAggCallType, singleton, false);
                    projects2.add(targetSingleton);
                }
                aggCallIdx += 1;
            }
            if (projects2.size() == aggregate.getGroupSet().cardinality() + newAggCalls.size()) {
                // We successfully converted agg calls into projects.
                relBuilder.project(projects2);
                aggConvertedToProjects = true;
            }
        }

        if (!aggConvertedToProjects) {
            relBuilder.aggregate(
                    relBuilder.groupKey(
                            Mappings.apply(mapping, aggregate.getGroupSet()),
                            Mappings.apply2(mapping, aggregate.getGroupSets())),
                    newAggCalls);
        }
        if (projectAfterAgg != null) {
            relBuilder.project(projectAfterAgg, origAgg.getRowType().getFieldNames());
        }

        call.transformTo(relBuilder.build());
    }

    /**
     * Convert aggregate with AUXILIARY_GROUP to regular aggregate. Return original aggregate and
     * null project if the given aggregate does not contain AUXILIARY_GROUP, else new aggregate
     * without AUXILIARY_GROUP and a project to permute output columns if needed.
     */
    private Pair<Aggregate, List<RexNode>> toRegularAggregate(Aggregate aggregate) {
        Tuple2<int[], Seq<AggregateCall>> auxGroupAndRegularAggCalls =
                AggregateUtil.checkAndSplitAggCalls(aggregate);
        final int[] auxGroup = auxGroupAndRegularAggCalls._1;
        final Seq<AggregateCall> regularAggCalls = auxGroupAndRegularAggCalls._2;
        if (auxGroup.length != 0) {
            int[] fullGroupSet = AggregateUtil.checkAndGetFullGroupSet(aggregate);
            ImmutableBitSet newGroupSet = ImmutableBitSet.of(fullGroupSet);
            List<AggregateCall> aggCalls =
                    JavaConverters.seqAsJavaListConverter(regularAggCalls).asJava();
            final Aggregate newAgg =
                    aggregate.copy(
                            aggregate.getTraitSet(),
                            aggregate.getInput(),
                            aggregate.indicator,
                            newGroupSet,
                            com.google.common.collect.ImmutableList.of(newGroupSet),
                            aggCalls);
            final List<RelDataTypeField> aggFields = aggregate.getRowType().getFieldList();
            final List<RexNode> projectAfterAgg = new ArrayList<>();
            for (int i = 0; i < fullGroupSet.length; ++i) {
                int group = fullGroupSet[i];
                int index = newGroupSet.indexOf(group);
                projectAfterAgg.add(new RexInputRef(index, aggFields.get(i).getType()));
            }
            int fieldCntOfAgg = aggFields.size();
            for (int i = fullGroupSet.length; i < fieldCntOfAgg; ++i) {
                projectAfterAgg.add(new RexInputRef(i, aggFields.get(i).getType()));
            }
            Preconditions.checkArgument(projectAfterAgg.size() == fieldCntOfAgg);
            return new Pair<>(newAgg, projectAfterAgg);
        } else {
            return new Pair<>(aggregate, null);
        }
    }

    /**
     * Computes the closure of a set of columns according to a given list of constraints. Each 'x =
     * y' constraint causes bit y to be set if bit x is set, and vice versa.
     */
    private static ImmutableBitSet keyColumns(
            ImmutableBitSet aggregateColumns,
            com.google.common.collect.ImmutableList<RexNode> predicates) {
        SortedMap<Integer, BitSet> equivalence = new TreeMap<>();
        for (RexNode predicate : predicates) {
            populateEquivalences(equivalence, predicate);
        }
        ImmutableBitSet keyColumns = aggregateColumns;
        for (Integer aggregateColumn : aggregateColumns) {
            final BitSet bitSet = equivalence.get(aggregateColumn);
            if (bitSet != null) {
                keyColumns = keyColumns.union(bitSet);
            }
        }
        return keyColumns;
    }

    private static void populateEquivalences(Map<Integer, BitSet> equivalence, RexNode predicate) {
        switch (predicate.getKind()) {
            case EQUALS:
                RexCall call = (RexCall) predicate;
                final List<RexNode> operands = call.getOperands();
                if (operands.get(0) instanceof RexInputRef) {
                    final RexInputRef ref0 = (RexInputRef) operands.get(0);
                    if (operands.get(1) instanceof RexInputRef) {
                        final RexInputRef ref1 = (RexInputRef) operands.get(1);
                        populateEquivalence(equivalence, ref0.getIndex(), ref1.getIndex());
                        populateEquivalence(equivalence, ref1.getIndex(), ref0.getIndex());
                    }
                }
        }
    }

    private static void populateEquivalence(Map<Integer, BitSet> equivalence, int i0, int i1) {
        BitSet bitSet = equivalence.get(i0);
        if (bitSet == null) {
            bitSet = new BitSet();
            equivalence.put(i0, bitSet);
        }
        bitSet.set(i1);
    }

    /**
     * Creates a {@link org.apache.calcite.sql.SqlSplittableAggFunction.Registry} that is a view of
     * a list.
     */
    private static <E> SqlSplittableAggFunction.Registry<E> registry(final List<E> list) {
        return new SqlSplittableAggFunction.Registry<E>() {
            public int register(E e) {
                int i = list.indexOf(e);
                if (i < 0) {
                    i = list.size();
                    list.add(e);
                }
                return i;
            }
        };
    }

    /** Work space for an input to a join. */
    private static class Side {
        final Map<Integer, Integer> split = new HashMap<>();
        RelNode newInput;
        boolean aggregate;
    }
}

// End FlinkAggregateJoinTransposeRule.java
