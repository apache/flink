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

import org.apache.flink.table.api.TableException;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink Planner rule to flatten a tree of {@link LogicalJoin}s into a single {@link MultiJoin} with
 * N inputs.
 *
 * <p>This rule is copied from {@link org.apache.calcite.rel.rules.JoinToMultiJoinRule}. In this
 * rule, we support richer join type to convert to one multi join set, like left outer join and
 * right outer join, by rewrite $canCombine() method.
 *
 * <p>An input is not flattened if the input is a null generating input in an outer join, i.e.,
 * either input in a full outer join, semi join, anti join, the right side of a left outer join, or
 * the lef side of a right outer join.
 *
 * <p>Join conditions are also pulled up from the inputs into the topmost {@link MultiJoin}.
 *
 * <p>Outer join information is also stored in the {@link MultiJoin}. A boolean flag indicates if
 * the join is a full outer join, and in the case of left and right outer joins, the join type and
 * outer join conditions are stored in arrays in the {@link MultiJoin}. This outer join information
 * is associated with the null generating input in the outer join. So, in the case of a left outer
 * join between A and B, the information is associated with B, not A.
 *
 * <p>Here are examples of the {@link MultiJoin}s constructed after this rule has been applied on
 * following join trees.
 *
 * <ul>
 *   <li>A JOIN B &rarr; MJ(A, B)
 *   <li>A JOIN B JOIN C &rarr; MJ(A, B, C)
 *   <li>A LEFT JOIN B &rarr; MJ(A, B)
 *   <li>A RIGHT JOIN B &rarr; MJ(A, B)
 *   <li>A FULL JOIN B &rarr; MJ[full](A, B)
 *   <li>A LEFT JOIN (B JOIN C) &rarr; MJ(A, B, C)
 *   <li>(A JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) JOIN C &rarr; MJ(A, B, C)
 *   <li>(A LEFT JOIN B) LEFT JOIN C &rarr; MJ(A, B, C)
 *   <li>(A RIGHT JOIN B) RIGHT JOIN C &rarr; MJ(MJ(A, B), C)
 *   <li>(A LEFT JOIN B) RIGHT JOIN C &rarr; MJ(MJ(A, B), C)
 *   <li>(A RIGHT JOIN B) LEFT JOIN C &rarr; MJ(MJ(A, B), C)
 *   <li>A LEFT JOIN (B FULL JOIN C) &rarr; MJ(A, MJ[full](B, C))
 *   <li>(A LEFT JOIN B) FULL JOIN (C RIGHT JOIN D) &rarr; MJ[full](MJ(A, B), MJ(C, D))
 *   <li>SEMI JOIN and ANTI JOIN not support now.
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
public class FlinkJoinToMultiJoinRule extends RelRule<FlinkJoinToMultiJoinRule.Config>
        implements TransformationRule {

    public static final FlinkJoinToMultiJoinRule INSTANCE =
            FlinkJoinToMultiJoinRule.Config.DEFAULT.toRule();

    /** Creates a JoinToMultiJoinRule. */
    public FlinkJoinToMultiJoinRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinToMultiJoinRule(Class<? extends Join> clazz) {
        this(Config.DEFAULT.withOperandFor(clazz));
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinToMultiJoinRule(
            Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
        this(
                Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(Config.class)
                        .withOperandFor(joinClass));
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        return origJoin.getJoinType().projectsRight();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        final RelNode left = call.rel(1);
        final RelNode right = call.rel(2);

        // inputNullGenFieldList records whether the field in originJoin is null generate field.
        List<Boolean> inputNullGenFieldList = new ArrayList<>();
        // Build null generate field list.
        buildInputNullGenFieldList(left, right, origJoin.getJoinType(), inputNullGenFieldList);

        // Combine the children MultiJoin inputs into an array of inputs for the new MultiJoin.
        final List<ImmutableBitSet> projFieldsList = new ArrayList<>();
        final List<int[]> joinFieldRefCountsList = new ArrayList<>();
        final List<RelNode> newInputs =
                combineInputs(
                        origJoin,
                        left,
                        right,
                        projFieldsList,
                        joinFieldRefCountsList,
                        inputNullGenFieldList);

        // Combine the outer join information from the left and right inputs, and include the outer
        // join information from the current join, if it's a left/right outer join.
        final List<Pair<JoinRelType, RexNode>> joinSpecs = new ArrayList<>();
        combineOuterJoins(origJoin, newInputs, left, right, joinSpecs, inputNullGenFieldList);

        // Pull up the join filters from the children MultiJoinRels and combine them with the join
        // filter associated with this LogicalJoin to form the join filter for the new MultiJoin.
        List<RexNode> newJoinFilters =
                combineJoinFilters(origJoin, left, right, inputNullGenFieldList);

        // Add on the join field reference counts for the join condition associated with this
        // LogicalJoin.
        final com.google.common.collect.ImmutableMap<Integer, ImmutableIntList>
                newJoinFieldRefCountsMap =
                        addOnJoinFieldRefCounts(
                                newInputs,
                                origJoin.getRowType().getFieldCount(),
                                origJoin.getCondition(),
                                joinFieldRefCountsList);

        List<RexNode> newPostJoinFilters = combinePostJoinFilters(origJoin, left, right);

        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();
        RelNode multiJoin =
                new MultiJoin(
                        origJoin.getCluster(),
                        newInputs,
                        RexUtil.composeConjunction(rexBuilder, newJoinFilters),
                        origJoin.getRowType(),
                        origJoin.getJoinType() == JoinRelType.FULL,
                        Pair.right(joinSpecs),
                        Pair.left(joinSpecs),
                        projFieldsList,
                        newJoinFieldRefCountsMap,
                        RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true));

        call.transformTo(multiJoin);
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
        } else if (joinType == JoinRelType.RIGHT) {
            // If origin joinType is right means join fields from left side must be null generated
            // fields, so we need only judge these join fields in right side and set null generate
            // field is true for all left fields.
            for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
                isNullGenFieldList.add(true);
            }

            buildNullGenFieldList(right, isNullGenFieldList);
        } else if (joinType == JoinRelType.FULL) {
            // For full outer join, both the left side and the right side must be null generated
            // fields, so all join fields will be set as null generated field.
            for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
                isNullGenFieldList.add(true);
            }
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
            List<int[]> joinFieldRefCountsList,
            List<Boolean> inputNullGenFieldList) {
        final List<RelNode> newInputs = new ArrayList<>();
        // Leave the null generating sides of an outer join intact; don't pull up those children
        // inputs into the array we're constructing.
        JoinInfo joinInfo = join.analyzeCondition();
        ImmutableIntList leftKeys = joinInfo.leftKeys;
        ImmutableIntList rightKeys = joinInfo.rightKeys;

        if (canCombine(
                left,
                leftKeys,
                join.getJoinType(),
                join.getJoinType().generatesNullsOnLeft(),
                true,
                inputNullGenFieldList,
                0)) {
            final MultiJoin leftMultiJoin = (MultiJoin) left;
            for (int i = 0; i < leftMultiJoin.getInputs().size(); i++) {
                newInputs.add(leftMultiJoin.getInput(i));
                projFieldsList.add(leftMultiJoin.getProjFields().get(i));
                joinFieldRefCountsList.add(
                        leftMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray());
            }

        } else {
            newInputs.add(left);
            projFieldsList.add(null);
            joinFieldRefCountsList.add(new int[left.getRowType().getFieldCount()]);
        }

        if (canCombine(
                right,
                rightKeys,
                join.getJoinType(),
                join.getJoinType().generatesNullsOnRight(),
                false,
                inputNullGenFieldList,
                left.getRowType().getFieldCount())) {
            final MultiJoin rightMultiJoin = (MultiJoin) right;
            for (int i = 0; i < rightMultiJoin.getInputs().size(); i++) {
                newInputs.add(rightMultiJoin.getInput(i));
                projFieldsList.add(rightMultiJoin.getProjFields().get(i));
                joinFieldRefCountsList.add(
                        rightMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray());
            }
        } else {
            newInputs.add(right);
            projFieldsList.add(null);
            joinFieldRefCountsList.add(new int[right.getRowType().getFieldCount()]);
        }

        return newInputs;
    }

    /**
     * Combines the outer join conditions and join types from the left and right join inputs. If the
     * join itself is either a left or right outer join, then the join condition corresponding to
     * the join is also set in the position corresponding to the null-generating input into the
     * join. The join type is also set.
     *
     * @param joinRel join rel
     * @param combinedInputs the combined inputs to the join
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     * @param joinSpecs the list where the join types and conditions will be copied
     */
    private void combineOuterJoins(
            Join joinRel,
            List<RelNode> combinedInputs,
            RelNode left,
            RelNode right,
            List<Pair<JoinRelType, RexNode>> joinSpecs,
            List<Boolean> inputNullGenFieldList) {
        JoinRelType joinType = joinRel.getJoinType();
        JoinInfo joinInfo = joinRel.analyzeCondition();
        ImmutableIntList leftKeys = joinInfo.leftKeys;
        ImmutableIntList rightKeys = joinInfo.rightKeys;
        boolean leftCombined =
                canCombine(
                        left,
                        leftKeys,
                        joinType,
                        joinType.generatesNullsOnLeft(),
                        true,
                        inputNullGenFieldList,
                        0);
        boolean rightCombined =
                canCombine(
                        right,
                        rightKeys,
                        joinType,
                        joinType.generatesNullsOnRight(),
                        false,
                        inputNullGenFieldList,
                        left.getRowType().getFieldCount());
        switch (joinType) {
            case LEFT:
                if (leftCombined) {
                    copyOuterJoinInfo((MultiJoin) left, joinSpecs, 0, null, null);
                } else {
                    joinSpecs.add(Pair.of(JoinRelType.INNER, null));
                }
                joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
                break;
            case RIGHT:
                joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
                if (rightCombined) {
                    copyOuterJoinInfo(
                            (MultiJoin) right,
                            joinSpecs,
                            left.getRowType().getFieldCount(),
                            right.getRowType().getFieldList(),
                            joinRel.getRowType().getFieldList());
                } else {
                    joinSpecs.add(Pair.of(JoinRelType.INNER, null));
                }
                break;
            default:
                if (leftCombined) {
                    copyOuterJoinInfo((MultiJoin) left, joinSpecs, 0, null, null);
                } else {
                    joinSpecs.add(Pair.of(JoinRelType.INNER, null));
                }
                if (rightCombined) {
                    copyOuterJoinInfo(
                            (MultiJoin) right,
                            joinSpecs,
                            left.getRowType().getFieldCount(),
                            right.getRowType().getFieldList(),
                            joinRel.getRowType().getFieldList());
                } else {
                    joinSpecs.add(Pair.of(JoinRelType.INNER, null));
                }
        }
    }

    /**
     * Copies outer join data from a source MultiJoin to a new set of arrays. Also adjusts the
     * conditions to reflect the new position of an input if that input ends up being shifted to the
     * right.
     *
     * @param multiJoin the source MultiJoin
     * @param destJoinSpecs the list where the join types and conditions will be copied
     * @param adjustmentAmount if &gt; 0, the amount the RexInputRefs in the join conditions need to
     *     be adjusted by
     * @param srcFields the source fields that the original join conditions are referencing
     * @param destFields the destination fields that the new join conditions
     */
    private void copyOuterJoinInfo(
            MultiJoin multiJoin,
            List<Pair<JoinRelType, RexNode>> destJoinSpecs,
            int adjustmentAmount,
            List<RelDataTypeField> srcFields,
            List<RelDataTypeField> destFields) {
        final List<Pair<JoinRelType, RexNode>> srcJoinSpecs =
                Pair.zip(multiJoin.getJoinTypes(), multiJoin.getOuterJoinConditions());

        if (adjustmentAmount == 0) {
            destJoinSpecs.addAll(srcJoinSpecs);
        } else {
            assert srcFields != null;
            assert destFields != null;
            int nFields = srcFields.size();
            int[] adjustments = new int[nFields];
            for (int idx = 0; idx < nFields; idx++) {
                adjustments[idx] = adjustmentAmount;
            }
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
    private List<RexNode> combineJoinFilters(
            Join join, RelNode left, RelNode right, List<Boolean> inputNullGenFieldList) {
        JoinRelType joinType = join.getJoinType();
        JoinInfo joinInfo = join.analyzeCondition();
        ImmutableIntList leftKeys = joinInfo.leftKeys;
        ImmutableIntList rightKeys = joinInfo.rightKeys;

        // AND the join condition if this isn't a left or right outer join; In those cases, the
        // outer join condition is already tracked separately.
        final List<RexNode> filters = new ArrayList<>();
        if ((joinType != JoinRelType.LEFT) && (joinType != JoinRelType.RIGHT)) {
            filters.add(join.getCondition());
        }
        if (canCombine(
                left,
                leftKeys,
                joinType,
                joinType.generatesNullsOnLeft(),
                true,
                inputNullGenFieldList,
                0)) {
            filters.add(((MultiJoin) left).getJoinFilter());
        }
        // Need to adjust the RexInputs of the right child, since those need to shift over to the
        // right.
        if (canCombine(
                right,
                rightKeys,
                joinType,
                joinType.generatesNullsOnRight(),
                false,
                inputNullGenFieldList,
                left.getRowType().getFieldCount())) {
            MultiJoin multiJoin = (MultiJoin) right;
            filters.add(shiftRightFilter(join, left, multiJoin, multiJoin.getJoinFilter()));
        }

        return filters;
    }

    /**
     * Returns whether an input can be merged into a given relational expression without changing
     * semantics.
     *
     * @param input input into a join
     * @param nullGenerating true if the input is null generating
     * @return true if the input can be combined into a parent MultiJoin
     */
    private boolean canCombine(
            RelNode input,
            ImmutableIntList joinKeys,
            JoinRelType joinType,
            boolean nullGenerating,
            boolean isLeft,
            List<Boolean> inputNullGenFieldList,
            int beginIndex) {
        if (input instanceof MultiJoin) {
            MultiJoin join = (MultiJoin) input;
            if (join.isFullOuterJoin() || nullGenerating) {
                return false;
            }

            if (joinType == JoinRelType.LEFT) {
                if (!isLeft) {
                    return false;
                } else {
                    for (int joinKey : joinKeys) {
                        if (inputNullGenFieldList.get(joinKey + beginIndex)) {
                            return false;
                        }
                    }
                }
            } else if (joinType == JoinRelType.RIGHT) {
                if (isLeft) {
                    return false;
                } else {
                    for (int joinKey : joinKeys) {
                        if (inputNullGenFieldList.get(joinKey + beginIndex)) {
                            return false;
                        }
                    }
                }
            } else if (joinType == JoinRelType.INNER) {
                for (int joinKey : joinKeys) {
                    if (inputNullGenFieldList.get(joinKey + beginIndex)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
            return true;
        } else {
            return false;
        }
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
    private com.google.common.collect.ImmutableMap<Integer, ImmutableIntList>
            addOnJoinFieldRefCounts(
                    List<RelNode> multiJoinInputs,
                    int nTotalFields,
                    RexNode joinCondition,
                    List<int[]> origJoinFieldRefCounts) {
        // count the input references in the join condition
        int[] joinCondRefCounts = new int[nTotalFields];
        joinCondition.accept(new InputReferenceCounter(joinCondRefCounts));

        // first, make a copy of the ref counters
        final Map<Integer, int[]> refCountsMap = new HashMap<>();
        int nInputs = multiJoinInputs.size();
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
            int[] refCounts = refCountsMap.get(currInput);
            refCounts[i - startField] += joinCondRefCounts[i];
        }

        final com.google.common.collect.ImmutableMap.Builder<Integer, ImmutableIntList> builder =
                com.google.common.collect.ImmutableMap.builder();
        for (Map.Entry<Integer, int[]> entry : refCountsMap.entrySet()) {
            builder.put(entry.getKey(), ImmutableIntList.of(entry.getValue()));
        }
        return builder.build();
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
                ImmutableFlinkJoinToMultiJoinRule.Config.builder()
                        .build()
                        .as(Config.class)
                        .withOperandFor(LogicalJoin.class);

        @Override
        default FlinkJoinToMultiJoinRule toRule() {
            return new FlinkJoinToMultiJoinRule(this);
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
