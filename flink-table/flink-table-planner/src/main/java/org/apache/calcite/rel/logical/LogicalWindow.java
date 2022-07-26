/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.logical;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Window} not targeted at any particular engine or
 * calling convention.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 */
public final class LogicalWindow extends Window {
    /**
     * Creates a LogicalWindow.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param hints hints for this node
     * @param input Input relational expression
     * @param constants List of constants that are additional inputs
     * @param rowType Output row type
     * @param groups Window groups
     */
    public LogicalWindow(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            List<RexLiteral> constants,
            RelDataType rowType,
            List<Group> groups) {
        super(cluster, traitSet, hints, input, constants, rowType, groups);
    }

    /**
     * Creates a LogicalWindow.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public LogicalWindow(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<RexLiteral> constants,
            RelDataType rowType,
            List<Group> groups) {
        this(cluster, traitSet, Collections.emptyList(), input, constants, rowType, groups);
    }

    @Override
    public LogicalWindow copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalWindow(
                getCluster(), traitSet, hints, sole(inputs), constants, rowType, groups);
    }

    /**
     * Creates a LogicalWindow.
     *
     * @param input Input relational expression
     * @param traitSet Trait set
     * @param constants List of constants that are additional inputs
     * @param rowType Output row type
     * @param groups Window groups
     */
    public static LogicalWindow create(
            RelTraitSet traitSet,
            RelNode input,
            List<RexLiteral> constants,
            RelDataType rowType,
            List<Window.Group> groups) {
        return new LogicalWindow(input.getCluster(), traitSet, input, constants, rowType, groups);
    }

    /** Creates a LogicalWindow by parsing a {@link RexProgram}. */
    public static RelNode create(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelBuilder relBuilder,
            RelNode child,
            final RexProgram program) {
        final RelDataType outRowType = program.getOutputRowType();
        // Build a list of distinct groups, partitions and aggregate
        // functions.
        final com.google.common.collect.Multimap<WindowKey, RexOver> windowMap =
                com.google.common.collect.LinkedListMultimap.create();

        final int inputFieldCount = child.getRowType().getFieldCount();

        final Map<RexLiteral, RexInputRef> constantPool = new HashMap<>();
        final List<RexLiteral> constants = new ArrayList<>();

        // Identify constants in the expression tree and replace them with
        // references to newly generated constant pool.
        RexShuttle replaceConstants =
                new RexShuttle() {
                    @Override
                    public RexNode visitLiteral(RexLiteral literal) {
                        RexInputRef ref = constantPool.get(literal);
                        if (ref != null) {
                            return ref;
                        }
                        constants.add(literal);
                        ref =
                                new RexInputRef(
                                        constantPool.size() + inputFieldCount, literal.getType());
                        constantPool.put(literal, ref);
                        return ref;
                    }
                };

        // Build a list of groups, partitions, and aggregate functions. Each
        // aggregate function will add its arguments as outputs of the input
        // program.
        final Map<RexOver, RexOver> origToNewOver = new IdentityHashMap<>();
        for (RexNode agg : program.getExprList()) {
            if (agg instanceof RexOver) {
                final RexOver origOver = (RexOver) agg;
                final RexOver newOver = (RexOver) origOver.accept(replaceConstants);
                origToNewOver.put(origOver, newOver);
                addWindows(windowMap, newOver, inputFieldCount);
            }
        }

        final Map<RexOver, Window.RexWinAggCall> aggMap = new HashMap<>();
        List<Group> groups = new ArrayList<>();
        for (Map.Entry<WindowKey, Collection<RexOver>> entry : windowMap.asMap().entrySet()) {
            final WindowKey windowKey = entry.getKey();
            final List<RexWinAggCall> aggCalls = new ArrayList<>();
            for (RexOver over : entry.getValue()) {
                final RexWinAggCall aggCall =
                        new RexWinAggCall(
                                over.getAggOperator(),
                                over.getType(),
                                toInputRefs(over.operands),
                                aggMap.size(),
                                over.isDistinct(),
                                over.ignoreNulls());
                aggCalls.add(aggCall);
                aggMap.put(over, aggCall);
            }
            RexShuttle toInputRefs =
                    new RexShuttle() {
                        @Override
                        public RexNode visitLocalRef(RexLocalRef localRef) {
                            return new RexInputRef(localRef.getIndex(), localRef.getType());
                        }
                    };
            groups.add(
                    new Group(
                            windowKey.groupSet,
                            windowKey.isRows,
                            windowKey.lowerBound.accept(toInputRefs),
                            windowKey.upperBound.accept(toInputRefs),
                            windowKey.orderKeys,
                            aggCalls));
        }

        // Figure out the type of the inputs to the output program.
        // They are: the inputs to this rel, followed by the outputs of
        // each window.
        final List<Window.RexWinAggCall> flattenedAggCallList = new ArrayList<>();
        final List<Map.Entry<String, RelDataType>> fieldList =
                new ArrayList<>(child.getRowType().getFieldList());
        final int offset = fieldList.size();

        // Use better field names for agg calls that are projected.
        final Map<Integer, String> fieldNames = new HashMap<>();
        for (Ord<RexLocalRef> ref : Ord.zip(program.getProjectList())) {
            final int index = ref.e.getIndex();
            if (index >= offset) {
                fieldNames.put(index - offset, outRowType.getFieldNames().get(ref.i));
            }
        }

        for (Ord<Group> window : Ord.zip(groups)) {
            for (Ord<RexWinAggCall> over : Ord.zip(window.e.aggCalls)) {
                // Add the k-th over expression of
                // the i-th window to the output of the program.
                String name = fieldNames.get(over.i);
                if (name == null || name.startsWith("$")) {
                    name = "w" + window.i + "$o" + over.i;
                }
                fieldList.add(Pair.of(name, over.e.getType()));
                flattenedAggCallList.add(over.e);
            }
        }
        final RelDataType intermediateRowType =
                cluster.getTypeFactory().createStructType(fieldList);

        // The output program is the windowed agg's program, combined with
        // the output calc (if it exists).
        RexShuttle shuttle =
                new RexShuttle() {
                    public RexNode visitOver(RexOver over) {
                        // Look up the aggCall which this expr was translated to.
                        final Window.RexWinAggCall aggCall = aggMap.get(origToNewOver.get(over));
                        assert aggCall != null;
                        assert RelOptUtil.eq(
                                "over", over.getType(), "aggCall", aggCall.getType(), Litmus.THROW);

                        // Find the index of the aggCall among all partitions of all
                        // groups.
                        final int aggCallIndex = flattenedAggCallList.indexOf(aggCall);
                        assert aggCallIndex >= 0;

                        // Replace expression with a reference to the window slot.
                        final int index = inputFieldCount + aggCallIndex;
                        assert RelOptUtil.eq(
                                "over",
                                over.getType(),
                                "intermed",
                                intermediateRowType.getFieldList().get(index).getType(),
                                Litmus.THROW);
                        return new RexInputRef(index, over.getType());
                    }

                    public RexNode visitLocalRef(RexLocalRef localRef) {
                        final int index = localRef.getIndex();
                        if (index < inputFieldCount) {
                            // Reference to input field.
                            return localRef;
                        }
                        return new RexLocalRef(
                                flattenedAggCallList.size() + index, localRef.getType());
                    }
                };

        final LogicalWindow window =
                LogicalWindow.create(traitSet, child, constants, intermediateRowType, groups);

        // The order that the "over" calls occur in the groups and
        // partitions may not match the order in which they occurred in the
        // original expression.
        // Add a project to permute them.
        final List<RexNode> refToWindow = toInputRefs(shuttle.visitList(program.getExprList()));

        final List<RexNode> projectList = new ArrayList<>();
        for (RexLocalRef inputRef : program.getProjectList()) {
            final int index = inputRef.getIndex();
            final RexInputRef ref = (RexInputRef) refToWindow.get(index);
            projectList.add(ref);
        }

        return relBuilder.push(window).project(projectList, outRowType.getFieldNames()).build();
    }

    private static List<RexNode> toInputRefs(final List<? extends RexNode> operands) {
        return new AbstractList<RexNode>() {
            public int size() {
                return operands.size();
            }

            public RexNode get(int index) {
                final RexNode operand = operands.get(index);
                if (operand instanceof RexInputRef) {
                    return operand;
                }
                assert operand instanceof RexLocalRef;
                final RexLocalRef ref = (RexLocalRef) operand;
                return new RexInputRef(ref.getIndex(), ref.getType());
            }
        };
    }

    /**
     * Group specification. All windowed aggregates over the same window (regardless of how it is
     * specified, in terms of a named window or specified attribute by attribute) will end up with
     * the same window key.
     */
    private static class WindowKey {
        private final ImmutableBitSet groupSet;
        private final RelCollation orderKeys;
        private final boolean isRows;
        private final RexWindowBound lowerBound;
        private final RexWindowBound upperBound;

        WindowKey(
                ImmutableBitSet groupSet,
                RelCollation orderKeys,
                boolean isRows,
                RexWindowBound lowerBound,
                RexWindowBound upperBound) {
            this.groupSet = groupSet;
            this.orderKeys = orderKeys;
            this.isRows = isRows;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupSet, orderKeys, isRows, lowerBound, upperBound);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || obj instanceof WindowKey
                            && groupSet.equals(((WindowKey) obj).groupSet)
                            && orderKeys.equals(((WindowKey) obj).orderKeys)
                            && Objects.equals(lowerBound, ((WindowKey) obj).lowerBound)
                            && Objects.equals(upperBound, ((WindowKey) obj).upperBound)
                            && isRows == ((WindowKey) obj).isRows;
        }
    }

    private static void addWindows(
            com.google.common.collect.Multimap<WindowKey, RexOver> windowMap,
            RexOver over,
            final int inputFieldCount) {
        final RexWindow aggWindow = over.getWindow();

        // Look up or create a window.
        RelCollation orderKeys =
                getCollation(
                        com.google.common.collect.Lists.newArrayList(
                                Util.filter(
                                        aggWindow.orderKeys,
                                        rexFieldCollation ->
                                                // If ORDER BY references constant (i.e.
                                                // RexInputRef),
                                                // then we can ignore such ORDER BY key.
                                                rexFieldCollation.left instanceof RexLocalRef)));
        ImmutableBitSet groupSet = ImmutableBitSet.of(getProjectOrdinals(aggWindow.partitionKeys));
        final int groupLength = groupSet.length();
        if (inputFieldCount < groupLength) {
            // If PARTITION BY references constant, we can ignore such partition key.
            // All the inputs after inputFieldCount are literals, thus we can clear.
            groupSet = groupSet.except(ImmutableBitSet.range(inputFieldCount, groupLength));
        }

        WindowKey windowKey =
                new WindowKey(
                        groupSet,
                        orderKeys,
                        aggWindow.isRows(),
                        aggWindow.getLowerBound(),
                        aggWindow.getUpperBound());
        windowMap.put(windowKey, over);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalWindow(
                getCluster(), traitSet, hintList, input, constants, rowType, groups);
    }
}
