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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkMultiJoinNode;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Flink Planner rule to flatten a tree of {@link LogicalJoin}s into a single {@link MultiJoin} with
 * N inputs.
 *
 * <p>Join conditions are also pulled up from the inputs into the topmost {@link MultiJoin}.
 *
 * <p>Here are examples of the {@link MultiJoin}s constructed after this rule has been applied on
 * following join trees.
 *
 * <ul>
 *   <li>A JOIN B JOIN C &rarr; MJ(A, B, C)
 *   <li>A LEFT JOIN B &rarr; MJ(A, B)
 *   <li>A LEFT JOIN (B JOIN C) &rarr; MJ(A, B, C)
 *   <li>(A JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A RIGHT JOIN B) RIGHT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) RIGHT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A RIGHT JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) FULL JOIN (C RIGHT JOIN D) &rarr; MJ[full](MJ(A, B), MJ(C, D))
 *   <li>SEMI, ANTI and NESTED joins are not supported yet.
 * </ul>
 *
 * <p>The constructor is parameterized to allow any sub-class of {@link Join}, not just {@link
 * LogicalJoin}.
 */
@Value.Enclosing
public class FlinkStreamJoinToMultiJoinRule extends RelRule<FlinkStreamJoinToMultiJoinRule.Config>
        implements TransformationRule {

    public static final FlinkStreamJoinToMultiJoinRule INSTANCE = Config.DEFAULT.toRule();

    /** Creates a JoinToMultiJoinRule. */
    public FlinkStreamJoinToMultiJoinRule(Config config) {
        super(config);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        if (origJoin.getJoinType() != JoinRelType.INNER
                && origJoin.getJoinType() != JoinRelType.LEFT) {
            return false;
        }

        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        // If left or right child is Project then there was RightToLeftJoinRule
        if (left instanceof Project) {
            Project project = (Project) left;
            if (!RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                left = ((HepRelVertex) left.getInput(0)).getCurrentRel();
            }
        }

        if (right instanceof Project) {
            Project project = (Project) right;
            if (!RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                right = ((HepRelVertex) right.getInput(0)).getCurrentRel();
            }
        }

        if (left instanceof Join) {
            return joinConditionsMatch(origJoin, (Join) left);
        }

        if (right instanceof Join) {
            return joinConditionsMatch(origJoin, (Join) right);
        }

        if (left instanceof FlinkMultiJoinNode) {
            return joinConditionsMatch(origJoin, (FlinkMultiJoinNode) left);
        }

        if (right instanceof FlinkMultiJoinNode) {
            return joinConditionsMatch(origJoin, (FlinkMultiJoinNode) right);
        }

        return false;
    }

    /**
     * Checks if left Join and original Join have at least one common join key. Currently only
     * conditions which are SqlKind.EQUALS are supported.
     *
     * @param origJoin original Join
     * @param otherJoin left child of original Join which is also Join
     * @return true if both nodes have at least one common join key
     */
    public boolean joinConditionsMatch(Join origJoin, Join otherJoin) {
        List<RexCall> thisConjunctions = collectConjunctions(origJoin.getCondition());
        List<RexCall> otherConjunctions = collectConjunctions(otherJoin.getCondition());

        // Only =($x, $y) conditions supported
        if (!areAllConditionsEquals(thisConjunctions)
                || !areAllConditionsEquals(otherConjunctions)) {
            return false;
        }

        // We need actual field names. not idxs cause there can be order changing projection
        Set<String> origJoinKeys = getJoinKeys(origJoin);
        Set<String> otherJoinKeys = getJoinKeys(otherJoin);

        origJoinKeys.retainAll(otherJoinKeys);

        return !origJoinKeys.isEmpty();
    }

    /**
     * Checks if left FlinkMultiJoinNode and original Join have at least one common join key.
     * Currently only conditions which are SqlKind.EQUALS are supported.
     *
     * @param origJoin original Join
     * @param otherJoin left child of original Join which is FlinkMultiJoinNode
     * @return true if both nodes have at least one common join key
     */
    public boolean joinConditionsMatch(Join origJoin, FlinkMultiJoinNode otherJoin) {
        List<RexCall> thisConjunctions = collectConjunctions(origJoin.getCondition());

        if (!areAllConditionsEquals(thisConjunctions)) {
            return false;
        }

        Set<String> origJoinKeys = getJoinKeys(origJoin);
        Set<String> otherJoinKeys = otherJoin.getCommonJoinKeys();

        origJoinKeys.retainAll(otherJoinKeys);

        return !origJoinKeys.isEmpty();
    }

    public Set<String> getJoinKeys(Join join) {
        Set<String> joinKeys = new HashSet<>();

        List<RexCall> conditions = collectConjunctions(join.getCondition());
        RelMetadataQuery mq = join.getCluster().getMetadataQuery();
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        for (RexCall condition : conditions) {
            for (RexNode operand : condition.getOperands()) {
                if (operand instanceof RexInputRef) {
                    fillJoinKeys((RexInputRef) operand, left, right, mq, joinKeys);
                }
            }
        }

        return joinKeys;
    }

    private void fillJoinKeys(
            RexInputRef ref,
            RelNode left,
            RelNode right,
            RelMetadataQuery mq,
            Set<String> joinKeys) {
        int inputRefIndex = ref.getIndex();
        RelNode targetInput;
        int inputFieldIndex;
        int leftFieldCount = left.getRowType().getFieldCount();

        if (inputRefIndex < leftFieldCount) {
            targetInput = left;
            inputFieldIndex = inputRefIndex;
        } else {
            targetInput = right;
            inputFieldIndex = inputRefIndex - leftFieldCount;
        }

        targetInput =
                (targetInput instanceof HepRelVertex)
                        ? ((HepRelVertex) targetInput).getCurrentRel()
                        : targetInput;

        if (targetInput instanceof FlinkMultiJoinNode) {
            Tuple2<RelNode, Integer> tuple2 =
                    ((FlinkMultiJoinNode) targetInput).getInputByFieldIdx(inputFieldIndex);
            targetInput = tuple2.f0;
            inputFieldIndex = tuple2.f1;
        }

        Set<RelColumnOrigin> origins = mq.getColumnOrigins(targetInput, inputFieldIndex);
        if (origins != null) {
            for (RelColumnOrigin origin : origins) {
                RelOptTable originTable = origin.getOriginTable();
                List<String> qualifiedName = originTable.getQualifiedName();
                String fieldName =
                        originTable
                                .getRowType()
                                .getFieldList()
                                .get(origin.getOriginColumnOrdinal())
                                .getName();
                joinKeys.add(qualifiedName.get(qualifiedName.size() - 1) + "." + fieldName);
            }
        }
    }

    private boolean areAllConditionsEquals(List<RexCall> conjunctions) {
        return conjunctions.stream()
                .allMatch(
                        rexCall ->
                                rexCall.isA(SqlKind.EQUALS)
                                        && rexCall.operands.get(0).isA(SqlKind.INPUT_REF)
                                        && rexCall.operands.get(1).isA(SqlKind.INPUT_REF));
    }

    private List<RexCall> collectConjunctions(RexNode joinCondition) {
        return RelOptUtil.conjunctions(joinCondition).stream()
                .map(rexNode -> (RexCall) rexNode)
                .collect(Collectors.toList());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);

        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RexNode> newProjects = null;
        List<String> fieldNames = null;

        Project leftProject = null;
        Project rightProject = null;
        if (left instanceof Project) {
            Project project = (Project) left;
            if (!RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                leftProject = project;
                left = ((HepRelVertex) left.getInput(0)).getCurrentRel();

                newProjects = new ArrayList<>(project.getProjects());
                fieldNames = new ArrayList<>(project.getRowType().getFieldNames());
                int rightSize = right.getRowType().getFieldCount();
                int leftSize = left.getRowType().getFieldCount();
                RelDataType rightRowType = right.getRowType();

                for (int i = 0; i < rightSize; i++) {
                    RelDataTypeField field = rightRowType.getFieldList().get(i);
                    RexInputRef ref = rexBuilder.makeInputRef(field.getType(), leftSize + i);
                    newProjects.add(ref);
                }
            }
        }

        if (right instanceof Project) {
            Project project = (Project) right;
            if (!RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                rightProject = project;
                right = ((HepRelVertex) right.getInput(0)).getCurrentRel();

                int leftSize = left.getRowType().getFieldCount();
                RelDataType leftRowType = left.getRowType();
                newProjects = new ArrayList<>();
                fieldNames = new ArrayList<>();

                for (int i = 0; i < leftSize; i++) {
                    RelDataTypeField field = leftRowType.getFieldList().get(i);
                    RexInputRef ref = rexBuilder.makeInputRef(field.getType(), i);
                    newProjects.add(ref);
                    fieldNames.add(field.getName());
                }
                newProjects.addAll(
                        project.getProjects().stream()
                                .map(expr -> shiftRex(expr, leftSize, rexBuilder))
                                .collect(Collectors.toList()));
                fieldNames.addAll(project.getRowType().getFieldNames());
            }
        }

        if (left instanceof Join) {
            RelNode leftChild = ((Join) left).getLeft();
            RelNode rightChild = ((Join) left).getRight();
            left =
                    convertToMultiJoin(
                            (Join) left,
                            leftChild instanceof HepRelVertex
                                    ? ((HepRelVertex) leftChild).getCurrentRel()
                                    : leftChild,
                            rightChild instanceof HepRelVertex
                                    ? ((HepRelVertex) rightChild).getCurrentRel()
                                    : rightChild);
        }

        if (right instanceof Join) {
            RelNode leftChild = ((Join) right).getLeft();
            RelNode rightChild = ((Join) right).getRight();
            right =
                    convertToMultiJoin(
                            (Join) right,
                            leftChild instanceof HepRelVertex
                                    ? ((HepRelVertex) leftChild).getCurrentRel()
                                    : leftChild,
                            rightChild instanceof HepRelVertex
                                    ? ((HepRelVertex) rightChild).getCurrentRel()
                                    : rightChild);
        }

        RelNode transformer;

        if (leftProject != null) {
            RelNode multiJoin = convertToMultiJoin(origJoin, left, right);
            multiJoin = convertJoinCondition(multiJoin, newProjects);
            RelNode newProject =
                    leftProject.copy(
                            multiJoin.getTraitSet(), multiJoin, newProjects, origJoin.getRowType());
            relBuilder.push(multiJoin);
            transformer = newProject;
        } else if (rightProject != null) {
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            List<RelDataType> types =
                    newProjects.stream().map(RexNode::getType).collect(Collectors.toList());

            RelDataType newRowType = typeFactory.createStructType(types, fieldNames);

            RelNode multiJoin = convertToMultiJoin(origJoin, left, right);
            multiJoin = convertJoinCondition(multiJoin, newProjects);

            RelNode newProject =
                    rightProject.copy(multiJoin.getTraitSet(), multiJoin, newProjects, newRowType);
            relBuilder.push(multiJoin);
            transformer = newProject;
        } else {
            transformer = convertToMultiJoin(origJoin, left, right);
        }

        call.transformTo(transformer);
    }

    private RexNode shiftRex(RexNode expr, int offset, RexBuilder rexBuilder) {
        if (expr instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) expr;
            return rexBuilder.makeInputRef(ref.getType(), ref.getIndex() + offset);
        }
        return expr;
    }

    private RelNode convertJoinCondition(RelNode multiJoin, List<RexNode> newProjections) {
        RexBuilder rexBuilder = multiJoin.getCluster().getRexBuilder();
        List<RexNode> joinConditions =
                new ArrayList<>(((FlinkMultiJoinNode) multiJoin).getJoinConditions());
        RexCall origJoinCondition = (RexCall) joinConditions.get(joinConditions.size() - 1);
        List<RexNode> newOperands = new ArrayList<>();

        for (RexNode operand : origJoinCondition.operands) {
            if (operand instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) operand;
                RexNode projectedExpr = newProjections.get(inputRef.getIndex());
                newOperands.add(projectedExpr);
            } else {
                newOperands.add(operand);
            }
        }
        joinConditions.set(
                joinConditions.size() - 1,
                rexBuilder.makeCall(origJoinCondition.getOperator(), newOperands));

        return ((FlinkMultiJoinNode) multiJoin).copy(joinConditions);
    }

    @Nonnull
    private RelNode convertToMultiJoin(Join origJoin, RelNode left, RelNode right) {
        List<RelNode> inputs = combineInputs(left, right);
        List<RexNode> joinFilters = combineJoinFilters(origJoin, left, right);
        List<JoinRelType> joinTypes = combineJoinTypes(origJoin, left, right);
        Set<String> commonJoinKeys = combineCommonJoinKeys(origJoin, left, right);

        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();
        final RelDataType rowType = buildOutputRowType(origJoin.getCluster(), inputs);

        return new FlinkMultiJoinNode(
                origJoin.getCluster(),
                inputs,
                RexUtil.composeConjunction(rexBuilder, joinFilters),
                rowType,
                joinFilters,
                joinTypes,
                commonJoinKeys);
    }

    private RelDataType buildOutputRowType(RelOptCluster cluster, List<RelNode> inputs) {
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        List<RelDataTypeField> allFields = new ArrayList<>();

        for (RelNode input : inputs) {
            allFields.addAll(input.getRowType().getFieldList());
        }

        List<RelDataType> types =
                allFields.stream().map(RelDataTypeField::getType).collect(Collectors.toList());

        List<String> names =
                allFields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());

        return typeFactory.createStructType(types, names);
    }

    private Set<String> combineCommonJoinKeys(Join origJoin, RelNode left, RelNode right) {
        Set<String> commonJoinKeys = getJoinKeys(origJoin);
        if (left instanceof FlinkMultiJoinNode) {
            commonJoinKeys.retainAll(((FlinkMultiJoinNode) left).getCommonJoinKeys());
        }
        if (right instanceof FlinkMultiJoinNode) {
            commonJoinKeys.retainAll(((FlinkMultiJoinNode) right).getCommonJoinKeys());
        }
        return commonJoinKeys;
    }

    /**
     * Combines JoinTypes of origJoin and its children.
     *
     * @param origJoin original Join
     * @param left left child
     * @param right right child
     * @return List of combined Join types if left and/or right child is FlinkMultiJoinNode
     */
    private List<JoinRelType> combineJoinTypes(Join origJoin, RelNode left, RelNode right) {
        List<JoinRelType> newJoinRelTypes = new ArrayList<>();

        if (left instanceof FlinkMultiJoinNode) {
            newJoinRelTypes.addAll(((FlinkMultiJoinNode) left).getJoinTypes());
        }

        if (right instanceof FlinkMultiJoinNode) {
            newJoinRelTypes.addAll(((FlinkMultiJoinNode) right).getJoinTypes());
        }

        newJoinRelTypes.add(origJoin.getJoinType());

        return newJoinRelTypes;
    }

    /**
     * Combines the inputs into a LogicalJoin into an array of inputs.
     *
     * @param left left input into join
     * @param right right input into join
     * @return combined left and right inputs in an array
     */
    private List<RelNode> combineInputs(RelNode left, RelNode right) {
        final List<RelNode> newInputs = new ArrayList<>();
        newInputs.addAll(extractInputs(left));
        newInputs.addAll(extractInputs(right));

        return newInputs;
    }

    /**
     * Extract the inputs into a RelNode into an array of inputs.
     *
     * @param node RelNode to extract inputs from
     * @return List of inputs
     */
    private List<RelNode> extractInputs(RelNode node) {
        final List<RelNode> inputs = new ArrayList<>();
        if (node instanceof FlinkMultiJoinNode) {
            inputs.addAll(node.getInputs());
        } else {
            inputs.add(node);
        }

        return inputs;
    }

    /**
     * Combines the join filters from the left and right inputs with the join filter in the joinrel
     * into a list of join conditions.
     *
     * @param origJoin Join
     * @param left Left input of the join
     * @param right Right input of the join
     * @return combined join filters AND-ed together
     */
    private List<RexNode> combineJoinFilters(Join origJoin, RelNode left, RelNode right) {
        List<RexNode> newJoinFilters = new ArrayList<>();

        if (left instanceof FlinkMultiJoinNode) {
            newJoinFilters.add(((FlinkMultiJoinNode) left).getJoinFilter());
        }

        if (right instanceof FlinkMultiJoinNode) {
            newJoinFilters.add(
                    shiftRightFilter(
                            origJoin,
                            left,
                            (FlinkMultiJoinNode) right,
                            ((FlinkMultiJoinNode) right).getJoinFilter()));
        }

        newJoinFilters.add(origJoin.getCondition());

        return newJoinFilters;
    }

    /**
     * Shifts a filter originating from the right child of the LogicalJoin to the right, to reflect
     * the filter now being applied on the resulting MultiJoin.
     *
     * @param joinRel the original LogicalJoin
     * @param left the left child of the LogicalJoin
     * @param right the right child of the LogicalJoin
     * @param rightFilter the filter originating from the right child
     * @return the adjusted right filter
     */
    private RexNode shiftRightFilter(
            Join joinRel, RelNode left, FlinkMultiJoinNode right, RexNode rightFilter) {
        if (rightFilter == null) {
            return null;
        }

        int nFieldsOnLeft = left.getRowType().getFieldList().size();
        int nFieldsOnRight = right.getRowType().getFieldList().size();
        int[] adjustments = new int[nFieldsOnRight];
        for (int i = 0; i < nFieldsOnRight; i++) {
            adjustments[i] = nFieldsOnLeft;
        }
        rightFilter =
                rightFilter.accept(
                        new RelOptUtil.RexInputConverter(
                                joinRel.getCluster().getRexBuilder(),
                                right.getRowType().getFieldList(),
                                joinRel.getRowType().getFieldList(),
                                adjustments));
        return rightFilter;
    }

    // ~ Inner Classes ----------------------------------------------------------

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableFlinkStreamJoinToMultiJoinRule.Config.builder()
                        .build()
                        .as(Config.class)
                        .withOperandFor(LogicalJoin.class);

        @Override
        default FlinkStreamJoinToMultiJoinRule toRule() {
            return new FlinkStreamJoinToMultiJoinRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Join> joinClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(joinClass)
                                            .inputs(
                                                    b1 -> b1.operand(RelNode.class).anyInputs(),
                                                    b2 -> b2.operand(RelNode.class).anyInputs()))
                    .as(Config.class);
        }
    }
}
