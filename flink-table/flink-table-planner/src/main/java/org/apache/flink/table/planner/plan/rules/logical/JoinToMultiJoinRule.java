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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkOrderPreservingProjection;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Flink Planner rule to flatten a tree of {@link Join}s into a single {@link MultiJoin} with N
 * inputs.
 *
 * <p>This rule is copied and adjusted from {@link org.apache.calcite.rel.rules.JoinToMultiJoinRule}
 * and {@link JoinToMultiJoinForReorderRule}. Unlike {@link JoinToMultiJoinForReorderRule}, this
 * rule:
 *
 * <ul>
 *   <li>Supports a broader set of left and inner joins by rewriting the $canCombine() method
 *   <li>Right joins are supported in combination with {@link FlinkRightJoinToLeftJoinRule}
 *   <li>Is specifically designed for stream processing, as the resulting MultiJoin will be
 *       converted into a {@link StreamPhysicalMultiJoin}
 *   <li>Does not expect resulting multi join to be reordered. Reordering should be applied before
 *       this rule.
 * </ul>
 *
 * <p>Join conditions are pulled up from the inputs into the topmost {@link MultiJoin}.
 *
 * <p>Join information is stored in the {@link MultiJoin}. Join conditions are stored in arrays in
 * the {@link MultiJoin}. This join information is associated with the null generating input in the
 * outer join. So, in the case of a left outer join between A and B, the information is associated
 * with B, not A.
 *
 * <p>Here are examples of the {@link MultiJoin}s constructed after this rule has been applied on
 * following join trees. Note that RIGHT joins are handled by {@link FlinkRightJoinToLeftJoinRule}
 * before this rule is applied.
 *
 * <ul>
 *   <li>A JOIN B &rarr; MJ(A, B)
 *   <li>A JOIN B JOIN C &rarr; MJ(A, B, C)
 *   <li>A LEFT JOIN B &rarr; MJ(A, B)
 *   <li>A LEFT JOIN (B JOIN C) &rarr; MJ(A, B, C)
 *   <li>(A JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 * </ul>
 *
 * <p>The following join types are not supported:
 *
 * <ul>
 *   <li>FULL OUTER JOIN
 *   <li>SEMI JOIN
 *   <li>ANTI JOIN
 * </ul>
 *
 * <p>The constructor is parameterized to allow any sub-class of {@link Join}, not just {@link
 * LogicalJoin}.
 *
 * @see FilterMultiJoinMergeRule
 * @see ProjectMultiJoinMergeRule
 * @see CoreRules#JOIN_TO_MULTI_JOIN
 */
@Value.Enclosing
public class JoinToMultiJoinRule extends RelRule<JoinToMultiJoinRule.Config>
        implements TransformationRule {

    public static final JoinToMultiJoinRule INSTANCE = JoinToMultiJoinRule.Config.DEFAULT.toRule();

    /** Creates a JoinToMultiJoinRule. */
    public JoinToMultiJoinRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public JoinToMultiJoinRule(Class<? extends Join> clazz) {
        this(Config.DEFAULT.withOperandFor(clazz));
    }

    @Deprecated // to be removed before 2.0
    public JoinToMultiJoinRule(
            Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
        this(
                Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(Config.class)
                        .withOperandFor(joinClass));
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * This rule matches only INNER and LEFT joins. Right joins are expected to be rewritten to left
     * joins by the optimizer with {@link FlinkRightJoinToLeftJoinRule}
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        if (origJoin.getJoinType() != JoinRelType.INNER
                && origJoin.getJoinType() != JoinRelType.LEFT) {
            return false;
        }

        // Check for interval joins and temporal join - these should not be merged
        // as they have special time semantics
        if (isIntervalJoin(origJoin) || isTemporalJoin(call)) {
            return false;
        }

        return origJoin.getJoinType().projectsRight();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RexNode> newProjects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        Project leftProject = null;
        Project rightProject = null;

        // this case covers A RIGHT JOIN B INNER JOIN C
        if (left instanceof FlinkOrderPreservingProjection) {
            Project project = (Project) left;
            if (!RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                leftProject = mergeOrderPreservingProjections(project);
                left = ((HepRelVertex) leftProject.getInput(0)).getCurrentRel();

                createNewProjects(
                        origJoin,
                        rexBuilder,
                        newProjects,
                        fieldNames,
                        left,
                        right,
                        leftProject,
                        true);
            }
        }

        // this case covers A RIGHT JOIN B RIGHT JOIN C
        if (right instanceof FlinkOrderPreservingProjection) {
            Project project = (Project) right;
            if (!RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                rightProject = mergeOrderPreservingProjections(project);
                right = ((HepRelVertex) rightProject.getInput(0)).getCurrentRel();

                createNewProjects(
                        origJoin,
                        rexBuilder,
                        newProjects,
                        fieldNames,
                        left,
                        right,
                        rightProject,
                        false);
            }
        }

        // inputNullGenFieldList records whether the field in originJoin is null generate field.
        List<Boolean> inputNullGenFieldList = new ArrayList<>();
        // Build null generate field list.
        buildInputNullGenFieldList(left, right, origJoin.getJoinType(), inputNullGenFieldList);

        // Combine the children MultiJoin inputs into an array of inputs for the new MultiJoin.
        final List<ImmutableBitSet> projFieldsList = new ArrayList<>();
        final List<int[]> joinFieldRefCountsList = new ArrayList<>();
        final List<Integer> newLevels = new ArrayList<>();
        final List<RelNode> newInputs =
                combineInputs(origJoin, left, right, projFieldsList, joinFieldRefCountsList);

        // Combine the join information from the left and right inputs, and include the
        // join information from the current join.
        final List<Pair<JoinRelType, RexNode>> joinSpecs = new ArrayList<>();
        int origJoinIdx = combineJoinInfo(origJoin, left, right, joinSpecs, newLevels);
        List<RexNode> joinConditions = Pair.right(joinSpecs);
        List<JoinRelType> joinTypes = Pair.left(joinSpecs);

        // Pull up the join filters from the children MultiJoinRels and combine them with the join
        // filter associated with this LogicalJoin to form the join filter for the new MultiJoin.
        final List<RexNode> newJoinFilters = combineJoinFilters(origJoin, left, right);

        // Add on the join field reference counts for the join condition associated with this
        // LogicalJoin.
        final Map<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
                addOnJoinFieldRefCounts(
                        newInputs,
                        origJoin.getRowType().getFieldCount(),
                        origJoin.getCondition(),
                        joinFieldRefCountsList);

        List<RexNode> newPostJoinFilters = combinePostJoinFilters(origJoin, left, right);

        // creates a RelNode that the original RelNode will be transformed into
        RelNode transformer =
                constructTransformer(
                        relBuilder,
                        rexBuilder,
                        origJoin,
                        origJoinIdx,
                        newInputs,
                        joinConditions,
                        newJoinFilters,
                        joinTypes,
                        newLevels,
                        projFieldsList,
                        newJoinFieldRefCountsMap,
                        newPostJoinFilters,
                        newProjects,
                        fieldNames,
                        leftProject,
                        rightProject);

        call.transformTo(transformer);
    }

    /**
     * Merge consecutive projections to get real input.
     *
     * @param topProject topmost projection
     * @return bottommost FlinkOrderPreservingProjection
     */
    private Project mergeOrderPreservingProjections(Project topProject) {
        RelNode input = ((HepRelVertex) topProject.getInput(0)).getCurrentRel();

        while (input instanceof FlinkOrderPreservingProjection) {
            Project botProject = (Project) input;

            List<RexNode> topProjects = topProject.getProjects();
            List<RexNode> botProjects = botProject.getProjects();

            List<RexNode> mergedProjects = new ArrayList<>();
            for (RexNode project : topProjects) {
                mergedProjects.add(
                        project.accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        return botProjects.get(inputRef.getIndex());
                                    }
                                }));
            }

            topProject =
                    topProject.copy(
                            topProject.getTraitSet(),
                            botProject.getInput(),
                            mergedProjects,
                            topProject.getRowType());
            input = ((HepRelVertex) topProject.getInput(0)).getCurrentRel();
        }

        return topProject;
    }

    /**
     * Creates a RelNode that the original RelNode will be transformed into. Typically, this is a
     * MultiJoin, or a Projection when a RIGHT JOIN is involved.
     *
     * @param relBuilder a rel builder
     * @param rexBuilder a rex builder
     * @param origJoin an original join node
     * @param origJoinIdx index of original join condition
     * @param newInputs combined inputs
     * @param joinConditions combined join conditions
     * @param newJoinFilters combined join filters
     * @param joinTypes combined join types
     * @param newLevels array of join tree levels
     * @param projFieldsList combined projected fields
     * @param newJoinFieldRefCountsMap combined ref counts map
     * @param newPostJoinFilters combined post-join filters
     * @param newProjects list of rex nodes for new projection
     * @param fieldNames list of field names for new projection
     * @param leftProject a left project in case right join takes place
     * @param rightProject a right project in case two right join in a row
     * @return a RelNode that the original RelNode will be transformed into
     */
    private RelNode constructTransformer(
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            Join origJoin,
            int origJoinIdx,
            List<RelNode> newInputs,
            List<RexNode> joinConditions,
            List<RexNode> newJoinFilters,
            List<JoinRelType> joinTypes,
            List<Integer> newLevels,
            List<ImmutableBitSet> projFieldsList,
            Map<Integer, ImmutableIntList> newJoinFieldRefCountsMap,
            List<RexNode> newPostJoinFilters,
            List<RexNode> newProjects,
            List<String> fieldNames,
            Project leftProject,
            Project rightProject) {
        RelNode transformer;
        if (leftProject != null || rightProject != null) {
            RelDataType newRowType =
                    combineRowTypes(relBuilder.getTypeFactory(), newProjects, fieldNames);
            joinConditions = convertJoinConditions(joinConditions, newProjects, origJoinIdx);
            newJoinFilters =
                    convertJoinFilters(origJoin.getJoinType(), newJoinFilters, newProjects);

            RelNode multiJoin =
                    new MultiJoin(
                            origJoin.getCluster(),
                            newInputs,
                            RexUtil.composeConjunction(rexBuilder, newJoinFilters),
                            newRowType,
                            origJoin.getJoinType() == JoinRelType.FULL,
                            joinConditions,
                            joinTypes,
                            projFieldsList,
                            com.google.common.collect.ImmutableMap.copyOf(newJoinFieldRefCountsMap),
                            RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true),
                            newLevels);

            Project targetProject = leftProject != null ? leftProject : rightProject;
            transformer =
                    targetProject.copy(
                            multiJoin.getTraitSet(), multiJoin, newProjects, origJoin.getRowType());
            relBuilder.push(multiJoin);
        } else {
            transformer =
                    new MultiJoin(
                            origJoin.getCluster(),
                            newInputs,
                            RexUtil.composeConjunction(rexBuilder, newJoinFilters),
                            origJoin.getRowType(),
                            origJoin.getJoinType() == JoinRelType.FULL,
                            joinConditions,
                            joinTypes,
                            projFieldsList,
                            com.google.common.collect.ImmutableMap.copyOf(newJoinFieldRefCountsMap),
                            RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true),
                            newLevels);
        }

        return transformer;
    }

    /**
     * Converts multijoin filters according to new projections.
     *
     * @param joinType original join type
     * @param newJoinFilters list of multijoin filters
     * @param newProjects list of rex nodes for new projection
     * @return a list of converted join filters.
     */
    private List<RexNode> convertJoinFilters(
            JoinRelType joinType, List<RexNode> newJoinFilters, List<RexNode> newProjects) {
        List<RexNode> result;
        if ((joinType != JoinRelType.LEFT)) {
            RexNode origJoinFilter = newJoinFilters.get(0);
            RexShuttle shuttle =
                    new RexShuttle() {
                        @Override
                        public RexNode visitInputRef(RexInputRef inputRef) {
                            int index = inputRef.getIndex();
                            return newProjects.get(index);
                        }
                    };

            result = new ArrayList<>(newJoinFilters);
            result.set(0, origJoinFilter.accept(shuttle));
        } else {
            result = newJoinFilters;
        }
        return result;
    }

    /**
     * Creates the row type for a new {@code MultiJoin} node.
     *
     * <p>Sometimes, the original join's row type is not suitable for a {@code MultiJoin}, and a new
     * row type must be constructed.
     *
     * <pre>
     * Example:
     *  A RIGHT JOIN B INNER JOIN C
     *
     * BEFORE:
     *
     *        Inner Join (RowType is a.type, b.type, c.type)
     *                      /           \
     *    Project (A.fields, B.fields)   C
     *        |
     *    MultiJoin
     *     /   \
     *    B     A
     *
     * AFTER:
     *
     *    Project (A.fields, B.fields, C.fields)
     *         |
     *     MultiJoin (RowType is b.type, a.type, c.type)
     *     /   |   \
     *   B     A    C
     *
     * </pre>
     *
     * @param typeFactory type factory
     * @param newProjects list of rex nodes for new projection
     * @param fieldNames list of field names for new projection
     * @return a row type suitable for the new {@code MultiJoin} node
     */
    private RelDataType combineRowTypes(
            RelDataTypeFactory typeFactory, List<RexNode> newProjects, List<String> fieldNames) {
        RelDataType[] types = new RelDataType[newProjects.size()];
        String[] typeFieldNames = new String[newProjects.size()];
        for (int i = 0; i < newProjects.size(); i++) {
            RexInputRef ref = (RexInputRef) newProjects.get(i);
            int idx = ref.getIndex();
            types[idx] = ref.getType();
            typeFieldNames[idx] = fieldNames.get(i);
        }

        return typeFactory.createStructType(Arrays.asList(types), Arrays.asList(typeFieldNames));
    }

    /**
     * If one of original join's inputs is {@code FlinkOrderPreservingProject}, i.e. converted RIGHT
     * JOIN, we need to pull up this project above the MultiJoin node.
     *
     * <pre>
     * Example:
     *  A RIGHT JOIN B INNER JOIN C
     *
     * BEFORE:
     *
     *                        Inner Join
     *                      /           \
     *    Project (A.fields, B.fields)   C
     *        |
     *    MultiJoin
     *     /   \
     *    B     A
     *
     * AFTER:
     *
     *    Project (A.fields, B.fields, C.fields)
     *         |
     *     MultiJoin
     *     /   |   \
     *   B     A    C
     *
     * RowType of newProjects must correspond to original node's type.
     * </pre>
     *
     * @param origJoin the original join node
     * @param rexBuilder rex builder
     * @param newProjects list of rex nodes for new projection
     * @param fieldNames list of field names for new projection
     * @param left left child of original join after projection skip
     * @param right right child of original join after projection skip
     * @param project skipped project
     */
    private void createNewProjects(
            RelNode origJoin,
            RexBuilder rexBuilder,
            List<RexNode> newProjects,
            List<String> fieldNames,
            RelNode left,
            RelNode right,
            Project project,
            boolean isLeft) {
        int rightSize = right.getRowType().getFieldCount();
        int leftSize = left.getRowType().getFieldCount();

        List<RelDataTypeField> origJoinFields = origJoin.getRowType().getFieldList();
        List<RexNode> projectedFields;

        if (isLeft) {
            projectedFields = project.getProjects();
            for (int i = 0; i < leftSize; i++) {
                if (projectedFields.get(i) instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) projectedFields.get(i);
                    RexInputRef ref =
                            rexBuilder.makeInputRef(
                                    origJoinFields.get(i).getType(), inputRef.getIndex());
                    newProjects.add(ref);
                    fieldNames.add(origJoinFields.get(i).getName());
                }
            }
            for (int i = leftSize; i < rightSize + leftSize; i++) {
                RexInputRef ref = rexBuilder.makeInputRef(origJoinFields.get(i).getType(), i);
                newProjects.add(ref);
                fieldNames.add(origJoinFields.get(i).getName());
            }
        } else {
            projectedFields =
                    project.getProjects().stream()
                            .map(expr -> shiftRex(expr, leftSize, rexBuilder))
                            .collect(Collectors.toList());
            for (int i = 0; i < leftSize; i++) {
                RexInputRef ref = rexBuilder.makeInputRef(origJoinFields.get(i).getType(), i);
                newProjects.add(ref);
                fieldNames.add(origJoinFields.get(i).getName());
            }

            for (int i = leftSize; i < leftSize + rightSize; i++) {
                if (projectedFields.get(i - leftSize) instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) projectedFields.get(i - leftSize);
                    RexInputRef ref =
                            rexBuilder.makeInputRef(
                                    origJoinFields.get(i).getType(), inputRef.getIndex());
                    newProjects.add(ref);
                    fieldNames.add(origJoinFields.get(i).getName());
                }
            }
        }
    }

    /**
     * Shifts the index of a {@code RexInputRef} within a given expression.
     *
     * @param expr the expression to process
     * @param offset the offset to apply to the input reference index
     * @param rexBuilder the {@code RexBuilder} used to create modified expressions
     * @return the shifted {@code RexInputRef} if applicable; otherwise, returns the original
     *     expression
     */
    private RexNode shiftRex(RexNode expr, int offset, RexBuilder rexBuilder) {
        if (expr instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) expr;
            return rexBuilder.makeInputRef(ref.getType(), ref.getIndex() + offset);
        }
        return expr;
    }

    /**
     * Converts join condition refs according to the new projection.
     *
     * @param joinConditions conditions of multi join node
     * @param newProjections list of rex nodes for new projection
     * @param origJoinIdx integer value represents original join condition index in the new join
     *     conditions
     * @return a list of converted join conditions
     */
    private List<RexNode> convertJoinConditions(
            List<RexNode> joinConditions, List<RexNode> newProjections, int origJoinIdx) {
        RexNode origJoinCondition = joinConditions.get(origJoinIdx);
        RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int index = inputRef.getIndex();
                        return newProjections.get(index);
                    }
                };

        List<RexNode> result = new ArrayList<>(joinConditions);
        result.set(origJoinIdx, origJoinCondition.accept(shuttle));
        return result;
    }

    private void buildInputNullGenFieldList(
            RelNode left, RelNode right, JoinRelType joinType, List<Boolean> isNullGenFieldList) {
        if (joinType == JoinRelType.INNER) {
            buildNullGenFieldList(left, isNullGenFieldList);
            buildNullGenFieldList(right, isNullGenFieldList);
        } else if (joinType == JoinRelType.LEFT) {
            // If origin joinType is left means join fields from right side must be null generated
            // fields, so we need only judge these join fields in left side and set null generate
            // field is true for all right fields.
            buildNullGenFieldList(left, isNullGenFieldList);

            for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
                isNullGenFieldList.add(true);
            }
        } else {
            // Now, join to multi join rule only support Full outer join, Inner join and Left/Right
            // join.
            throw new TableException(
                    "This is a bug. Now, join to multi join rule only support Full outer "
                            + "join, Inner join and Left/Right join.");
        }
    }

    private void buildNullGenFieldList(RelNode rel, List<Boolean> isNullGenFieldList) {
        MultiJoin multiJoin = rel instanceof MultiJoin ? (MultiJoin) rel : null;
        if (multiJoin == null) {
            // other operators.
            for (int i = 0; i < rel.getRowType().getFieldCount(); i++) {
                isNullGenFieldList.add(false);
            }
        } else {
            List<RelNode> inputs = multiJoin.getInputs();
            List<JoinRelType> joinTypes = multiJoin.getJoinTypes();
            for (int i = 0; i < inputs.size() - 1; i++) {
                // In list joinTypes, right join node will be added as [RIGHT, INNER], so we need to
                // get the joinType from joinTypes in index i.
                if (joinTypes.get(i) == JoinRelType.RIGHT) {
                    buildInputNullGenFieldList(
                            inputs.get(i), inputs.get(i + 1), joinTypes.get(i), isNullGenFieldList);
                } else {
                    // In list joinTypes, left join node and inner join node will be added as
                    // [INNER, LEFT] and [INNER, INNER] respectively. so we need to get the joinType
                    // from joinTypes in index i + 1.
                    buildInputNullGenFieldList(
                            inputs.get(i),
                            inputs.get(i + 1),
                            joinTypes.get(i + 1),
                            isNullGenFieldList);
                }
            }
        }
    }

    /**
     * Combines the inputs into a LogicalJoin into an array of inputs.
     *
     * @param join original join
     * @param left left input into join
     * @param right right input into join
     * @param projFieldsList returns a list of the new combined projection fields
     * @param joinFieldRefCountsList returns a list of the new combined join field reference counts
     * @return combined left and right inputs in an array
     */
    private List<RelNode> combineInputs(
            Join join,
            RelNode left,
            RelNode right,
            List<ImmutableBitSet> projFieldsList,
            List<int[]> joinFieldRefCountsList) {
        final List<RelNode> newInputs = new ArrayList<>();

        combineInputs(join, left, newInputs, projFieldsList, joinFieldRefCountsList);
        combineInputs(join, right, newInputs, projFieldsList, joinFieldRefCountsList);

        return newInputs;
    }

    private void combineInputs(
            Join join,
            RelNode child,
            List<RelNode> newInputs,
            List<ImmutableBitSet> projFieldsList,
            List<int[]> joinFieldRefCountsList) {
        if (canCombine(child, join)) {
            final MultiJoin childMultiJoin = (MultiJoin) child;
            for (int i = 0; i < childMultiJoin.getInputs().size(); i++) {
                newInputs.add(childMultiJoin.getInput(i));
                projFieldsList.add(childMultiJoin.getProjFields().get(i));
                joinFieldRefCountsList.add(
                        childMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray());
            }

        } else {
            newInputs.add(child);
            projFieldsList.add(null);
            joinFieldRefCountsList.add(new int[child.getRowType().getFieldCount()]);
        }
    }

    /**
     * Combines the join conditions and join types from the left and right join inputs. If the join
     * itself is either a left or right outer join, then the join condition corresponding to the
     * join is also set in the position corresponding to the null-generating input into the join.
     * The join type is also set.
     *
     * @param joinRel join rel
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     * @param joinSpecs the list where the join types and conditions will be copied
     * @param newLevels array of join tree levels
     */
    private int combineJoinInfo(
            Join joinRel,
            RelNode left,
            RelNode right,
            List<Pair<JoinRelType, RexNode>> joinSpecs,
            List<Integer> newLevels) {
        JoinRelType joinType = joinRel.getJoinType();
        final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        int idx = 0;
        boolean leftCombined = canCombine(left, joinRel);
        boolean rightCombined = canCombine(right, joinRel);
        switch (joinType) {
            case LEFT:
            case INNER:
                if (leftCombined) {
                    MultiJoin leftMultiJoin = (MultiJoin) left;
                    copyJoinInfo(
                            leftMultiJoin,
                            leftMultiJoin.getJoinTypes(),
                            leftMultiJoin.getOuterJoinConditions(),
                            joinSpecs,
                            0,
                            null,
                            null);
                    newLevels.addAll(leftMultiJoin.getLevels());
                } else {
                    joinSpecs.add(Pair.of(JoinRelType.INNER, rexBuilder.makeLiteral(true)));
                    newLevels.add(0);
                }

                joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
                newLevels.add(newLevels.size() - 1);
                idx = joinSpecs.size() - 1;

                if (rightCombined) {
                    MultiJoin rightMultiJoin = (MultiJoin) right;
                    joinSpecs.clear();
                    joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
                    idx = 0;
                    List<RexNode> joinConditions = rightMultiJoin.getOuterJoinConditions();
                    List<JoinRelType> joinTypes = rightMultiJoin.getJoinTypes();
                    copyJoinInfo(
                            rightMultiJoin,
                            joinTypes,
                            joinConditions,
                            joinSpecs,
                            left.getRowType().getFieldCount(),
                            right.getRowType().getFieldList(),
                            joinRel.getRowType().getFieldList());
                    newLevels.clear();
                    newLevels.add(rightMultiJoin.getLevels().size() - 1);
                    newLevels.addAll(rightMultiJoin.getLevels());
                }

                break;
            default:
                throw new TableException(
                        "This is a bug. This rule only supports left and inner joins");
        }

        return idx;
    }

    /**
     * Copies join data from a source MultiJoin to a new set of arrays. Also adjusts the conditions
     * to reflect the new position of an input if that input ends up being shifted to the right.
     *
     * @param multiJoin the source MultiJoin
     * @param joinTypes src join types
     * @param joinConditions src join conditions
     * @param destJoinSpecs the list where the join types and conditions will be copied
     */
    private void copyJoinInfo(
            MultiJoin multiJoin,
            List<JoinRelType> joinTypes,
            List<RexNode> joinConditions,
            List<Pair<JoinRelType, RexNode>> destJoinSpecs,
            int adjustmentAmount,
            List<RelDataTypeField> srcFields,
            List<RelDataTypeField> destFields) {

        final List<Pair<JoinRelType, RexNode>> srcJoinSpecs = Pair.zip(joinTypes, joinConditions);

        if (adjustmentAmount == 0) {
            destJoinSpecs.addAll(srcJoinSpecs);
        } else {
            assert srcFields != null;
            assert destFields != null;
            int nFields = srcFields.size();
            int[] adjustments = new int[nFields];
            Arrays.fill(adjustments, adjustmentAmount);
            for (Pair<JoinRelType, RexNode> src : srcJoinSpecs) {
                destJoinSpecs.add(
                        Pair.of(
                                src.left,
                                src.right == null
                                        ? null
                                        : src.right.accept(
                                                new RelOptUtil.RexInputConverter(
                                                        multiJoin.getCluster().getRexBuilder(),
                                                        srcFields,
                                                        destFields,
                                                        adjustments))));
            }
        }
    }

    /**
     * Combines the join filters from the left and right inputs (if they are MultiJoinRels) with the
     * join filter in the joinrel into a single AND'd join filter, unless the inputs correspond to
     * null generating inputs in an outer join.
     *
     * @param join Join
     * @param left Left input of the join
     * @param right Right input of the join
     * @return combined join filters AND-ed together
     */
    private List<RexNode> combineJoinFilters(Join join, RelNode left, RelNode right) {
        JoinRelType joinType = join.getJoinType();

        if (joinType == JoinRelType.RIGHT) {
            throw new TableException("This is a bug. This rule only supports left and inner joins");
        }
        // AND the join condition if this isn't a left join; In those cases, the
        // outer join condition is already tracked separately.
        final List<RexNode> filters = new ArrayList<>();
        if ((joinType != JoinRelType.LEFT)) {
            filters.add(join.getCondition());
        }
        if (canCombine(left, join)) {
            filters.add(((MultiJoin) left).getJoinFilter());
        }

        if (canCombine(right, join)) {
            filters.add(
                    shiftRightFilter(
                            join, left, (MultiJoin) right, ((MultiJoin) right).getJoinFilter()));
        }

        return filters;
    }

    /**
     * Returns whether an input can be merged into a given relational expression without changing
     * semantics.
     *
     * <p>This method should be extended to check for the common join key restriction to support
     * multiple multi joins. See <a
     * href="https://issues.apache.org/jira/browse/FLINK-37890">FLINK-37890</a>.
     *
     * @param input input into a join
     * @return true if the input can be combined into a parent MultiJoin
     */
    private boolean canCombine(RelNode input, Join origJoin) {
        if (input instanceof MultiJoin) {
            MultiJoin join = (MultiJoin) input;

            if (join.isFullOuterJoin()) {
                return false;
            }

            return haveCommonJoinKey(origJoin, join);
        } else {
            return false;
        }
    }

    /**
     * Checks if original join and child multi-join have common join keys to decide if we can merge
     * them into a single MultiJoin with one more input.
     *
     * @param origJoin original Join
     * @param otherJoin child MultiJoin
     * @return true if original Join and child multi-join have at least one common JoinKey
     */
    private boolean haveCommonJoinKey(Join origJoin, MultiJoin otherJoin) {
        Set<String> origJoinKeys = getJoinKeys(origJoin);
        Set<String> otherJoinKeys = getJoinKeys(otherJoin);

        origJoinKeys.retainAll(otherJoinKeys);

        return !origJoinKeys.isEmpty();
    }

    /**
     * Returns a set of join keys as strings following this format [table_name.field_name].
     *
     * @param join Join or MultiJoin node
     * @return set of all the join keys (keys from join conditions)
     */
    public Set<String> getJoinKeys(RelNode join) {
        Set<String> joinKeys = new HashSet<>();
        List<RexCall> conditions = Collections.emptyList();
        List<RelNode> inputs = join.getInputs();

        if (join instanceof Join) {
            conditions = collectConjunctions(((Join) join).getCondition());
        } else if (join instanceof MultiJoin) {
            conditions =
                    ((MultiJoin) join)
                            .getOuterJoinConditions().stream()
                                    .flatMap(cond -> collectConjunctions(cond).stream())
                                    .collect(Collectors.toList());
        }

        RelMetadataQuery mq = join.getCluster().getMetadataQuery();

        for (RexCall condition : conditions) {
            for (RexNode operand : condition.getOperands()) {
                if (operand instanceof RexInputRef) {
                    addJoinKeysByOperand((RexInputRef) operand, inputs, mq, joinKeys);
                }
            }
        }

        return joinKeys;
    }

    /**
     * Retrieves conjunctions from joinCondition.
     *
     * @param joinCondition join condition
     * @return List of RexCalls representing conditions
     */
    private List<RexCall> collectConjunctions(RexNode joinCondition) {
        return RelOptUtil.conjunctions(joinCondition).stream()
                .map(rexNode -> (RexCall) rexNode)
                .collect(Collectors.toList());
    }

    /**
     * Appends join key's string representation to the set of join keys.
     *
     * @param ref input ref to the operand
     * @param inputs List of node's inputs
     * @param mq RelMetadataQuery needed to retrieve column origins
     * @param joinKeys Set of join keys to be added
     */
    private void addJoinKeysByOperand(
            RexInputRef ref, List<RelNode> inputs, RelMetadataQuery mq, Set<String> joinKeys) {
        int inputRefIndex = ref.getIndex();
        Tuple2<RelNode, Integer> targetInputAndIdx = getTargetInputAndIdx(inputRefIndex, inputs);
        RelNode targetInput = targetInputAndIdx.f0;
        int idxInTargetInput = targetInputAndIdx.f1;

        Set<RelColumnOrigin> origins = mq.getColumnOrigins(targetInput, idxInTargetInput);
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

    /**
     * Get real table that contains needed input ref (join key).
     *
     * @param inputRefIndex index of the required field
     * @param inputs inputs of the node
     * @return target input + idx of the required field as target input's
     */
    private Tuple2<RelNode, Integer> getTargetInputAndIdx(int inputRefIndex, List<RelNode> inputs) {
        RelNode targetInput = null;
        int idxInTargetInput = 0;
        int inputFieldEnd = 0;
        for (RelNode input : inputs) {
            inputFieldEnd += input.getRowType().getFieldCount();
            if (inputRefIndex < inputFieldEnd) {
                targetInput = input;
                int targetInputStartIdx = inputFieldEnd - input.getRowType().getFieldCount();
                idxInTargetInput = inputRefIndex - targetInputStartIdx;
                break;
            }
        }

        targetInput = unwrapHepRelVertex(targetInput);

        assert targetInput != null;

        // if target input is a projection we need to project idxInTargetInput
        if (targetInput instanceof Project) {
            Project project = (Project) targetInput;
            RexNode expr = project.getProjects().get(idxInTargetInput);

            if (expr instanceof RexInputRef) {
                idxInTargetInput = ((RexInputRef) expr).getIndex();
                targetInput = unwrapHepRelVertex(project.getInput());
            }
        }

        if (targetInput instanceof TableScan
                || targetInput instanceof Values
                || targetInput instanceof TableFunctionScan
                || targetInput.getInputs().isEmpty()) {
            return new Tuple2<>(targetInput, idxInTargetInput);
        }

        return getTargetInputAndIdx(idxInTargetInput, targetInput.getInputs());
    }

    /**
     * Unwraps HepRelVertex if target is HepRelVertex, does nothing otherwise.
     *
     * @param target target RelNode
     * @return unwrapped RelNode
     */
    private RelNode unwrapHepRelVertex(RelNode target) {
        return (target instanceof HepRelVertex) ? ((HepRelVertex) target).getCurrentRel() : target;
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
            Join joinRel, RelNode left, MultiJoin right, RexNode rightFilter) {
        if (rightFilter == null) {
            return null;
        }

        int nFieldsOnLeft = left.getRowType().getFieldList().size();
        int nFieldsOnRight = right.getRowType().getFieldList().size();
        int[] adjustments = new int[nFieldsOnRight];
        Arrays.fill(adjustments, nFieldsOnLeft);
        rightFilter =
                rightFilter.accept(
                        new RelOptUtil.RexInputConverter(
                                joinRel.getCluster().getRexBuilder(),
                                right.getRowType().getFieldList(),
                                joinRel.getRowType().getFieldList(),
                                adjustments));
        return rightFilter;
    }

    /**
     * Adds on to the existing join condition reference counts the references from the new join
     * condition.
     *
     * @param multiJoinInputs inputs into the new MultiJoin
     * @param nTotalFields total number of fields in the MultiJoin
     * @param joinCondition the new join condition
     * @param origJoinFieldRefCounts existing join condition reference counts
     * @return Map containing the new join condition
     */
    private Map<Integer, ImmutableIntList> addOnJoinFieldRefCounts(
            List<RelNode> multiJoinInputs,
            int nTotalFields,
            RexNode joinCondition,
            List<int[]> origJoinFieldRefCounts) {
        // count the input references in the join condition
        final int[] joinCondRefCounts = new int[nTotalFields];
        joinCondition.accept(new InputReferenceCounter(joinCondRefCounts));

        // first, make a copy of the ref counters
        final Map<Integer, int[]> refCountsMap = new HashMap<>();
        final int nInputs = multiJoinInputs.size();
        int currInput = 0;
        for (int[] origRefCounts : origJoinFieldRefCounts) {
            refCountsMap.put(currInput, origRefCounts.clone());
            currInput++;
        }

        // add on to the counts for each input into the MultiJoin the
        // reference counts computed for the current join condition
        currInput = -1;
        int startField = 0;
        int nFields = 0;
        for (int i = 0; i < nTotalFields; i++) {
            if (joinCondRefCounts[i] == 0) {
                continue;
            }
            while (i >= (startField + nFields)) {
                startField += nFields;
                currInput++;
                assert currInput < nInputs;
                nFields = multiJoinInputs.get(currInput).getRowType().getFieldCount();
            }
            final int[] refCounts = refCountsMap.get(currInput);
            refCounts[i - startField] += joinCondRefCounts[i];
        }

        final Map<Integer, ImmutableIntList> aMap = new HashMap<>();
        for (Map.Entry<Integer, int[]> entry : refCountsMap.entrySet()) {
            aMap.put(entry.getKey(), ImmutableIntList.of(entry.getValue()));
        }
        return Collections.unmodifiableMap(aMap);
    }

    /**
     * Combines the post-join filters from the left and right inputs (if they are MultiJoinRels)
     * into a single AND'd filter.
     *
     * @param joinRel the original LogicalJoin
     * @param left left child of the LogicalJoin
     * @param right right child of the LogicalJoin
     * @return combined post-join filters AND'd together
     */
    private List<RexNode> combinePostJoinFilters(Join joinRel, RelNode left, RelNode right) {
        final List<RexNode> filters = new ArrayList<>();
        if (right instanceof MultiJoin) {
            final MultiJoin multiRight = (MultiJoin) right;
            filters.add(
                    shiftRightFilter(joinRel, left, multiRight, multiRight.getPostJoinFilter()));
        }

        if (left instanceof MultiJoin) {
            filters.add(((MultiJoin) left).getPostJoinFilter());
        }

        return filters;
    }

    /**
     * Checks if a join is an interval join. Interval joins have special time-based semantics and
     * should not be merged into MultiJoin.
     *
     * @param join the join to check
     * @return true if the join condition or outputs access time attributes
     */
    private boolean isIntervalJoin(Join join) {
        if (!(join instanceof LogicalJoin)) {
            return true;
        }

        //        FlinkLogicalJoin flinkLogicalJoin =
        //                (FlinkLogicalJoin) FlinkLogicalJoin.CONVERTER().convert(join);
        return IntervalJoinUtil.satisfyIntervalJoin(join);
    }

    /**
     * Checks if a join is a temporal/lookup join. Interval joins have special time-based semantics
     * (FOR SYSTEM_TIME AS OF) and should not be merged into a MultiJoin.
     *
     * @param call the join call to check
     * @return true if the join condition or outputs access time attributes
     */
    private boolean isTemporalJoin(RelOptRuleCall call) {
        final RelNode left = call.rel(1);
        final RelNode right = call.rel(2);

        return containsSnapshot(left) || containsSnapshot(right);
    }

    /**
     * Checks if a RelNode tree contains FlinkLogicalSnapshot nodes, which indicate temporal/lookup
     * joins. These joins have special semantics and should not be merged into MultiJoin.
     *
     * @param relNode the RelNode to check
     * @return true if the node or its children contain FlinkLogicalSnapshot
     */
    private boolean containsSnapshot(RelNode relNode) {
        RelNode original;
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

    // ~ Inner Classes ----------------------------------------------------------

    /** Visitor that keeps a reference count of the inputs used by an expression. */
    private static class InputReferenceCounter extends RexVisitorImpl<Void> {
        private final int[] refCounts;

        InputReferenceCounter(int[] refCounts) {
            super(true);
            this.refCounts = refCounts;
        }

        public Void visitInputRef(RexInputRef inputRef) {
            refCounts[inputRef.getIndex()]++;
            return null;
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableJoinToMultiJoinRule.Config.builder()
                        .build()
                        .as(Config.class)
                        .withOperandFor(LogicalJoin.class);

        @Override
        default JoinToMultiJoinRule toRule() {
            return new JoinToMultiJoinRule(this);
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
