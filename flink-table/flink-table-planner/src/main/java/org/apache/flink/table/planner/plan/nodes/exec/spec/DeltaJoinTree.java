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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinRuntimeTree;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.combineOutputRowType;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.splitProjectionAndFilter;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * A delta join tree used to describe the relationships among one or more joins in the input.
 *
 * <p>Each node in the tree will have two types:
 *
 * <ol>
 *   <li>{@link BinaryInputNode}: the leaf node of the tree, which represents that it is a source
 *       for delta join.
 *   <li>{@link JoinNode}: the non-leaf node of the tree, which represents a join between two
 *       inputs.
 * </ol>
 *
 * <p>Take the following sql pattern as an example:
 *
 * <pre>{@code
 *              DeltaJoin
 *           /            \
 *       Calc3             \
 *        /                 \
 *   DeltaJoin           DeltaJoin
 *     /    \             /     \
 *  Calc1    \          /      Calc2
 *   /        \       /           \
 * #0 A     #1 B    #2 C          #3 D
 * }</pre>
 *
 * <p>The tree converted from the above sql pattern is:
 *
 * <pre>{@code
 *                     Join
 *              /                 \
 *   Join with Calc3              Join
 *     /             \        /            \
 * #0 with Calc1     #1     #2           #3 with Calc2
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeltaJoinTree {

    public static final String FIELD_NAME_ROOT = "root";

    @JsonProperty(FIELD_NAME_ROOT)
    public final Node root;

    @JsonCreator
    public DeltaJoinTree(@JsonProperty(FIELD_NAME_ROOT) Node root) {
        this(root, true);
    }

    /**
     * Construct a delta join tree.
     *
     * @param root the root node of the delta join tree
     * @param shouldValidate whether the delta join tree should be validated
     */
    private DeltaJoinTree(Node root, boolean shouldValidate) {
        this.root = root;

        if (shouldValidate) {
            List<Integer> allInputOrdinals = getAllInputOrdinals(root);
            Preconditions.checkArgument(
                    allInputOrdinals.equals(
                            IntStream.range(0, allInputOrdinals.size())
                                    .boxed()
                                    .collect(Collectors.toList())));
        }
    }

    /** Shift all input ordinals in {@link BinaryInputNode} in the join with the given shift. */
    public DeltaJoinTree shiftInputIndex(int shift) {
        return new DeltaJoinTree(shiftInputIndexInternal(root, shift), false);
    }

    private Node shiftInputIndexInternal(Node node, int shift) {
        if (node instanceof BinaryInputNode) {
            BinaryInputNode binaryInputNode = (BinaryInputNode) node;
            return new BinaryInputNode(
                    binaryInputNode.inputOrdinal + shift, binaryInputNode.rowType, node.rexProgram);
        }
        JoinNode joinNode = (JoinNode) node;
        Node newLeft = shiftInputIndexInternal(joinNode.left, shift);
        Node newRight = shiftInputIndexInternal(joinNode.right, shift);
        return new JoinNode(
                joinNode.joinType,
                joinNode.condition,
                joinNode.leftJoinKey,
                joinNode.rightJoinKey,
                newLeft,
                newRight,
                node.rexProgram);
    }

    /**
     * Get the output row type of the delta join tree on the given input ordinals.
     *
     * <p>In the {@link BinaryInputNode}, we are concerned with its {@link
     * BinaryInputNode#inputOrdinal}. In the {@link JoinNode}, we focus on the {@link
     * BinaryInputNode#inputOrdinal} of all the {@link BinaryInputNode} within its input.
     *
     * <p>Take the following delta join tree as an example:
     *
     * <pre>{@code
     *                      Join
     *              /                 \
     *  Join with Calc3               Join
     *     /             \        /            \
     * #0 with Calc1     #1     #2           #3 with Calc2
     * }</pre>
     *
     * <p>When {@code caresInputOrdinals = [0, 1]} is given, the output row type is the row type of
     * Calc3.
     */
    public RowType getOutputRowTypeOnNode(int[] caresInputOrdinals, FlinkTypeFactory typeFactory) {
        Preconditions.checkArgument(caresInputOrdinals.length > 0);
        return getOutputTypeOnNodeInternal(
                Arrays.stream(caresInputOrdinals).boxed().collect(Collectors.toSet()),
                root,
                typeFactory);
    }

    private RowType getOutputTypeOnNodeInternal(
            Set<Integer> caresInputOrdinals, Node node, FlinkTypeFactory typeFactory) {
        Set<Integer> allInputOrdinalsInThisSubTree = node.getAllInputOrdinals();
        Preconditions.checkArgument(allInputOrdinalsInThisSubTree.containsAll(caresInputOrdinals));
        if (allInputOrdinalsInThisSubTree.equals(caresInputOrdinals)) {
            return node.getRowTypeAfterCalc(typeFactory);
        }

        Preconditions.checkArgument(node instanceof JoinNode);
        JoinNode joinNode = (JoinNode) node;
        if (joinNode.left.getAllInputOrdinals().containsAll(caresInputOrdinals)) {
            return getOutputTypeOnNodeInternal(caresInputOrdinals, joinNode.left, typeFactory);
        }
        Preconditions.checkArgument(
                joinNode.right.getAllInputOrdinals().containsAll(caresInputOrdinals));
        return getOutputTypeOnNodeInternal(caresInputOrdinals, joinNode.right, typeFactory);
    }

    /** Convert this {@link DeltaJoinTree} to {@link DeltaJoinRuntimeTree}. */
    public DeltaJoinRuntimeTree convert2RuntimeTree(PlannerBase planner, ExecNodeConfig config) {
        return new DeltaJoinRuntimeTree(convert2RuntimeTreeInternal(planner, config, root));
    }

    private DeltaJoinRuntimeTree.Node convert2RuntimeTreeInternal(
            PlannerBase planner, ExecNodeConfig config, Node node) {
        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        FlinkTypeFactory typeFactory = unwrapTypeFactory(planner);
        RowType rowTypeBeforeCalc = node.getRowTypeBeforeCalc(typeFactory);
        RowType rowTypePassThroughCalc = node.getRowTypeAfterCalc(typeFactory);

        String generatedCalcName =
                node instanceof BinaryInputNode
                        ? "BinaryInputNodeCalcFunction"
                        : "JoinNodeCalcFunction";
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                Optional.ofNullable(node.projection)
                        .map(
                                projection ->
                                        LookupJoinCodeGenerator.generateCalcMapFunction(
                                                config,
                                                classLoader,
                                                JavaScalaConversionUtil.toScala(node.projection),
                                                node.filter,
                                                rowTypePassThroughCalc,
                                                rowTypeBeforeCalc,
                                                generatedCalcName,
                                                typeFactory))
                        .orElse(null);

        if (node instanceof BinaryInputNode) {
            return new DeltaJoinRuntimeTree.BinaryInputNode(
                    ((BinaryInputNode) node).inputOrdinal,
                    generatedCalc,
                    InternalSerializers.create(rowTypePassThroughCalc));
        }
        JoinNode joinNode = (JoinNode) node;

        DeltaJoinRuntimeTree.Node newLeft =
                convert2RuntimeTreeInternal(planner, config, joinNode.left);
        DeltaJoinRuntimeTree.Node newRight =
                convert2RuntimeTreeInternal(planner, config, joinNode.right);

        GeneratedFilterCondition generatedJoinCondition =
                FilterCodeGenerator.generateFilterCondition(
                        config,
                        classLoader,
                        joinNode.condition,
                        joinNode.getRowTypeBeforeCalc(typeFactory),
                        "JoinCondition");

        return new DeltaJoinRuntimeTree.JoinNode(
                joinNode.joinType,
                generatedJoinCondition,
                generatedCalc,
                newLeft,
                newRight,
                InternalSerializers.create(rowTypePassThroughCalc));
    }

    private static List<Integer> getAllInputOrdinals(Node node) {
        List<Integer> collector = new ArrayList<>();
        collectAllInputOrdinals(node, collector);
        return collector;
    }

    private static void collectAllInputOrdinals(Node node, List<Integer> collector) {
        if (node instanceof BinaryInputNode) {
            collector.add(((BinaryInputNode) node).inputOrdinal);
            return;
        }
        JoinNode joinNode = (JoinNode) node;
        collectAllInputOrdinals(joinNode.left, collector);
        collectAllInputOrdinals(joinNode.right, collector);
    }

    /**
     * An abstract node for {@link BinaryInputNode} and {@link JoinNode}.
     *
     * <p>The {@link #projection} and {@link #filter} represents a calc on this {@link Node}. If
     * they are null, that means there is no calc on this {@link Node}.
     *
     * <p>The {@link #rowTypeAfterCalc} represents the row type after the calc. If it is null, that
     * means there is no calc on this {@link Node}.
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = BinaryInputNode.class),
        @JsonSubTypes.Type(value = JoinNode.class),
    })
    public abstract static class Node {

        public static final String FIELD_NAME_PROJECTION = "projection";
        public static final String FIELD_NAME_FILTER = "filter";
        public static final String FIELD_NAME_ROW_TYPE_AFTER_CALC = "rowTypeAfterCalc";

        @JsonProperty(FIELD_NAME_PROJECTION)
        @Nullable
        public final List<RexNode> projection;

        @JsonProperty(FIELD_NAME_FILTER)
        @Nullable
        public final RexNode filter;

        @JsonProperty(FIELD_NAME_ROW_TYPE_AFTER_CALC)
        @Nullable
        public final RowType rowTypeAfterCalc;

        @JsonIgnore @Nullable public final RexProgram rexProgram;

        private Node(@Nullable RexProgram rexProgram) {
            this.rexProgram = rexProgram;
            Tuple2<Optional<List<RexNode>>, Optional<RexNode>> projectAndFilter =
                    splitProjectionAndFilter(rexProgram);
            this.projection = projectAndFilter.f0.orElse(null);
            this.filter = projectAndFilter.f1.orElse(null);
            if (this.projection != null) {
                Preconditions.checkArgument(rexProgram != null);
                rowTypeAfterCalc = FlinkTypeFactory.toLogicalRowType(rexProgram.getOutputRowType());
            } else {
                rowTypeAfterCalc = null;
            }
        }

        /** A construct used for restoring. */
        private Node(
                @Nullable List<RexNode> projection,
                @Nullable RexNode filter,
                @Nullable RowType rowTypeAfterCalc) {
            this.rexProgram = null;
            this.projection = projection;
            this.filter = filter;
            this.rowTypeAfterCalc = rowTypeAfterCalc;
        }

        @JsonIgnore
        public abstract RowType getRowTypeBeforeCalc(FlinkTypeFactory typeFactory);

        @JsonIgnore
        public RowType getRowTypeAfterCalc(FlinkTypeFactory typeFactory) {
            if (projection == null) {
                return getRowTypeBeforeCalc(typeFactory);
            }
            Preconditions.checkState(null != rowTypeAfterCalc);
            return rowTypeAfterCalc;
        }

        @Nullable
        @JsonIgnore
        public List<RexNode> getProjection() {
            return projection;
        }

        @Nullable
        @JsonIgnore
        public RexNode getFilter() {
            return filter;
        }

        @JsonIgnore
        public abstract Set<Integer> getAllInputOrdinals();
    }

    /**
     * A leaf {@link Node} in this tree. It represents a source used for delta join to scan and
     * lookup.
     *
     * <p>The {@link #inputOrdinal} is the index of the source in the {@link DeltaJoinAssociation}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("BinaryInputNode")
    public static class BinaryInputNode extends Node {
        public static final String FIELD_NAME_INPUT_ORDINAL = "inputOrdinal";
        public static final String FIELD_NAME_ROW_TYPE = "rowType";

        @JsonProperty(FIELD_NAME_INPUT_ORDINAL)
        public final int inputOrdinal;

        @JsonProperty(FIELD_NAME_ROW_TYPE)
        public final RowType rowType;

        public BinaryInputNode(
                int inputOrdinal, RowType rowTypeBeforeCalc, @Nullable RexProgram rexProgram) {
            super(rexProgram);
            this.inputOrdinal = inputOrdinal;
            this.rowType = rowTypeBeforeCalc;
        }

        @JsonCreator
        public BinaryInputNode(
                @JsonProperty(FIELD_NAME_INPUT_ORDINAL) int inputOrdinal,
                @JsonProperty(FIELD_NAME_PROJECTION) @Nullable List<RexNode> projection,
                @JsonProperty(FIELD_NAME_FILTER) @Nullable RexNode filter,
                @JsonProperty(FIELD_NAME_ROW_TYPE_AFTER_CALC) @Nullable RowType rowTypeAfterCalc,
                @JsonProperty(FIELD_NAME_ROW_TYPE) RowType rowType) {
            super(projection, filter, rowTypeAfterCalc);
            this.inputOrdinal = inputOrdinal;
            this.rowType = rowType;
        }

        @Override
        @JsonIgnore
        public RowType getRowTypeBeforeCalc(FlinkTypeFactory typeFactory) {
            return rowType;
        }

        @Override
        @JsonIgnore
        public Set<Integer> getAllInputOrdinals() {
            return Collections.singleton(inputOrdinal);
        }
    }

    /** A {@link Node} in the tree representing a join operation between two inputs. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("JoinNode")
    public static class JoinNode extends Node {
        public static final String FIELD_NAME_JOIN_TYPE = "joinType";
        public static final String FIELD_NAME_CONDITION = "condition";
        public static final String FIELD_NAME_LEFT_JOIN_KEY = "leftJoinKey";
        public static final String FIELD_NAME_RIGHT_JOIN_KEY = "rightJoinKey";
        public static final String FIELD_NAME_LEFT = "left";
        public static final String FIELD_NAME_RIGHT = "right";

        @JsonProperty(FIELD_NAME_JOIN_TYPE)
        public final FlinkJoinType joinType;

        @JsonProperty(FIELD_NAME_CONDITION)
        public final RexNode condition;

        @JsonProperty(FIELD_NAME_LEFT_JOIN_KEY)
        public final int[] leftJoinKey;

        @JsonProperty(FIELD_NAME_RIGHT_JOIN_KEY)
        public final int[] rightJoinKey;

        @JsonProperty(FIELD_NAME_LEFT)
        public final Node left;

        @JsonProperty(FIELD_NAME_RIGHT)
        public final Node right;

        public JoinNode(
                FlinkJoinType joinType,
                RexNode condition,
                int[] leftJoinKey,
                int[] rightJoinKey,
                Node left,
                Node right,
                @Nullable RexProgram rexProgram) {
            super(rexProgram);
            this.joinType = joinType;
            this.condition = condition;
            this.leftJoinKey = leftJoinKey;
            this.rightJoinKey = rightJoinKey;
            this.left = left;
            this.right = right;

            Preconditions.checkArgument(DeltaJoinUtil.isJoinTypeSupported(joinType));
        }

        @JsonCreator
        public JoinNode(
                @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
                @JsonProperty(FIELD_NAME_CONDITION) RexNode condition,
                @JsonProperty(FIELD_NAME_PROJECTION) @Nullable List<RexNode> projection,
                @JsonProperty(FIELD_NAME_FILTER) @Nullable RexNode filter,
                @JsonProperty(FIELD_NAME_ROW_TYPE_AFTER_CALC) @Nullable RowType rowTypeAfterCalc,
                @JsonProperty(FIELD_NAME_LEFT_JOIN_KEY) int[] leftJoinKey,
                @JsonProperty(FIELD_NAME_RIGHT_JOIN_KEY) int[] rightJoinKey,
                @JsonProperty(FIELD_NAME_LEFT) Node left,
                @JsonProperty(FIELD_NAME_RIGHT) Node right) {
            super(projection, filter, rowTypeAfterCalc);
            this.joinType = joinType;
            this.condition = condition;
            this.leftJoinKey = leftJoinKey;
            this.rightJoinKey = rightJoinKey;
            this.left = left;
            this.right = right;
        }

        @Override
        @JsonIgnore
        public RowType getRowTypeBeforeCalc(FlinkTypeFactory typeFactory) {
            return combineOutputRowType(
                    left.getRowTypeAfterCalc(typeFactory),
                    right.getRowTypeAfterCalc(typeFactory),
                    joinType,
                    typeFactory);
        }

        @Override
        @JsonIgnore
        public Set<Integer> getAllInputOrdinals() {
            return Stream.concat(
                            left.getAllInputOrdinals().stream(),
                            right.getAllInputOrdinals().stream())
                    .collect(Collectors.toSet());
        }

        @JsonIgnore
        public JoinNode addCalcOnJoinNode(RexProgram calcOnJoinNode) {
            return new JoinNode(
                    joinType, condition, leftJoinKey, rightJoinKey, left, right, calcOnJoinNode);
        }
    }
}
