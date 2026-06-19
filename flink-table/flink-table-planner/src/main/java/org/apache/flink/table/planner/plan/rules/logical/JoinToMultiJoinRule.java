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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.hint.StateTtlHint;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.NoCommonJoinKeyException;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.TransformationRule;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.hint.StateTtlHint.STATE_TTL;
import static org.apache.flink.table.planner.plan.utils.MultiJoinUtil.createJoinAttributeMap;

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
 *   <li>Is specifically designed for stream processing, as the resulting LogicalMultiJoin will be
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

        if (!origJoin.getJoinType().projectsRight()) {
            return false;
        }

        // Enable multi-join if either config is enabled OR MULTI_JOIN hint is present
        return isEnabledViaConfig(origJoin) || hasMultiJoinHint(origJoin);
    }

    /**
     * Checks if multi-join optimization is enabled via configuration.
     *
     * @param join the join node
     * @return true if TABLE_OPTIMIZER_MULTI_JOIN_ENABLED is set to true
     */
    private boolean isEnabledViaConfig(Join join) {
        final TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(join);
        return tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED);
    }

    /**
     * Checks if the MULTI_JOIN hint is present on the join node.
     *
     * <p>Note: By the time this rule sees the join, the QueryHintsResolver has already validated
     * the hint. If the hint is present with valid options, it means both sides of this join were
     * mentioned in the original hint and have been validated.
     *
     * @param join the join node
     * @return true if MULTI_JOIN hint is present and valid
     */
    private boolean hasMultiJoinHint(Join join) {
        return join.getHints().stream()
                .anyMatch(
                        hint ->
                                JoinStrategy.MULTI_JOIN.getJoinHintName().equals(hint.hintName)
                                        && !hint.listOptions.isEmpty());
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

        // Combine the children LogicalMultiJoin inputs into an array of inputs for the new
        // MultiJoin.
        final List<ImmutableBitSet> projFieldsList = new ArrayList<>();
        final List<int[]> joinFieldRefCountsList = new ArrayList<>();
        final List<RelNode> newInputs =
                combineInputs(origJoin, left, right, projFieldsList, joinFieldRefCountsList);

        // Combine the join information from the left and right inputs, and include the
        // join information from the current join.
        final List<Pair<JoinRelType, RexNode>> joinSpecs = new ArrayList<>();
        combineJoinInfo(origJoin, left, joinSpecs);

        // Pull up the join filters from the children MultiJoinRels and combine them with the join
        // filter associated with this LogicalJoin to form the join filter for the new MultiJoin.
        final List<RexNode> newJoinFilters =
                combineJoinFilters(origJoin, left, right, inputNullGenFieldList);

        // Add on the join field reference counts for the join condition associated with this
        // LogicalJoin.
        final Map<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
                addOnJoinFieldRefCounts(
                        newInputs,
                        origJoin.getRowType().getFieldCount(),
                        origJoin.getCondition(),
                        joinFieldRefCountsList);

        List<RexNode> newPostJoinFilters = combinePostJoinFilters(origJoin, left, right);

        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();

        // Handle hints: if left or right side is a MultiJoin, reuse their hints and add new ones
        final RelHint.Builder builder = RelHint.builder(STATE_TTL.getHintName());
        handleStateTtlHintsForInput(builder, left, origJoin, FlinkHints.LEFT_INPUT);
        handleStateTtlHintsForInput(builder, right, origJoin, FlinkHints.RIGHT_INPUT);

        RelNode multiJoin =
                new MultiJoin(
                        origJoin.getCluster(),
                        List.of(builder.build()),
                        newInputs,
                        RexUtil.composeConjunction(rexBuilder, newJoinFilters),
                        origJoin.getRowType(),
                        origJoin.getJoinType() == JoinRelType.FULL,
                        Pair.right(joinSpecs),
                        Pair.left(joinSpecs),
                        projFieldsList,
                        com.google.common.collect.ImmutableMap.copyOf(newJoinFieldRefCountsMap),
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
     * @return combined left and right inputs in an array
     */
    private List<RelNode> combineInputs(
            Join join,
            RelNode left,
            RelNode right,
            List<ImmutableBitSet> projFieldsList,
            List<int[]> joinFieldRefCountsList) {
        final List<RelNode> newInputs = new ArrayList<>();

        combineIfCan(join, left, newInputs, projFieldsList, joinFieldRefCountsList);
        combineIfCan(join, right, newInputs, projFieldsList, joinFieldRefCountsList);

        return newInputs;
    }

    private void combineIfCan(
            Join join,
            RelNode relNode,
            List<RelNode> newInputs,
            List<ImmutableBitSet> projFieldsList,
            List<int[]> joinFieldRefCountsList) {
        if (canCombine(relNode, join)) {
            final MultiJoin multiJoin = (MultiJoin) relNode;
            for (int i = 0; i < multiJoin.getInputs().size(); i++) {
                newInputs.add(multiJoin.getInput(i));
                projFieldsList.add(multiJoin.getProjFields().get(i));
                joinFieldRefCountsList.add(
                        multiJoin.getJoinFieldRefCountsMap().get(i).toIntArray());
            }
        } else {
            newInputs.add(relNode);
            projFieldsList.add(null);
            joinFieldRefCountsList.add(new int[relNode.getRowType().getFieldCount()]);
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
     * @param joinSpecs the list where the join types and conditions will be copied
     */
    private void combineJoinInfo(
            Join joinRel, RelNode left, List<Pair<JoinRelType, RexNode>> joinSpecs) {
        JoinRelType joinType = joinRel.getJoinType();
        final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        boolean leftCombined = canCombine(left, joinRel);
        switch (joinType) {
            case LEFT:
            case INNER:
                if (leftCombined) {
                    copyJoinInfo((MultiJoin) left, joinSpecs);
                } else {
                    joinSpecs.add(Pair.of(JoinRelType.INNER, rexBuilder.makeLiteral(true)));
                }
                joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
                break;

            default:
                throw new TableException(
                        "This is a bug. This rule only supports left and inner joins");
        }
    }

    /**
     * Copies join data from a source MultiJoin to a new set of arrays. Also adjusts the conditions
     * to reflect the new position of an input if that input ends up being shifted to the right.
     *
     * @param multiJoin the source MultiJoin
     * @param destJoinSpecs the list where the join types and conditions will be copied
     */
    private void copyJoinInfo(MultiJoin multiJoin, List<Pair<JoinRelType, RexNode>> destJoinSpecs) {
        // getOuterJoinConditions are return all join conditions since that's how we use it
        final List<Pair<JoinRelType, RexNode>> srcJoinSpecs =
                Pair.zip(multiJoin.getJoinTypes(), multiJoin.getOuterJoinConditions());

        destJoinSpecs.addAll(srcJoinSpecs);
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
     * them into a single MultiJoin with one more input. The method uses {@link
     * AttributeBasedJoinKeyExtractor} to try to create valid common join key extractors.
     *
     * @param origJoin original Join
     * @param otherJoin child MultiJoin
     * @return true if original Join and child multi-join have at least one common JoinKey
     */
    private boolean haveCommonJoinKey(Join origJoin, MultiJoin otherJoin) {
        final List<RelNode> combinedJoinInputs =
                Stream.concat(otherJoin.getInputs().stream(), Stream.of(origJoin.getRight()))
                        .collect(Collectors.toUnmodifiableList());

        final List<RowType> combinedInputTypes =
                combinedJoinInputs.stream()
                        .map(i -> FlinkTypeFactory.toLogicalRowType(i.getRowType()))
                        .collect(Collectors.toUnmodifiableList());

        final List<RexNode> combinedJoinConditions =
                Stream.concat(
                                otherJoin.getOuterJoinConditions().stream(),
                                List.of(origJoin.getCondition()).stream())
                        .collect(Collectors.toUnmodifiableList());

        final Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
                joinAttributeMap =
                        createJoinAttributeMap(combinedJoinInputs, combinedJoinConditions);

        boolean haveCommonJoinKey = false;
        try {
            // we probe to instantiate AttributeBasedJoinKeyExtractor's constructor to check whether
            // it's possible to initialize common join key structures
            final JoinKeyExtractor keyExtractor =
                    new AttributeBasedJoinKeyExtractor(joinAttributeMap, combinedInputTypes);
            haveCommonJoinKey = keyExtractor.getCommonJoinKeyIndices(0).length > 0;
        } catch (NoCommonJoinKeyException ignored) {
            // failed to instantiate common join key structures => no common join key
        }

        return haveCommonJoinKey;
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

        if (containsSnapshot(left) || containsSnapshot(right)) {
            return true;
        }
        return false;
    }

    /**
     * Checks if a RelNode tree contains FlinkLogicalSnapshot nodes, which indicate temporal/lookup
     * joins. These joins have special semantics and should not be merged into MultiJoin.
     *
     * @param relNode the RelNode to check
     * @return true if the node or its children contain FlinkLogicalSnapshot
     */
    private boolean containsSnapshot(RelNode relNode) {
        final RelNode original;
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

    /**
     * Processes state TTL hints for a given side (left or right) of a join operation.
     *
     * <p>This method handles two scenarios:
     *
     * <ul>
     *   <li>If the input is a LogicalMultiJoin, it copies all existing STATE_TTL hints from that
     *       MultiJoin
     *   <li>If the input is a regular RelNode, it extracts the STATE_TTL hint from the original
     *       join
     * </ul>
     *
     * @param builder the RelHint.Builder to add hints to
     * @param input the input RelNode (either LogicalMultiJoin or regular RelNode)
     * @param origJoin the original join containing the hints to process
     * @param joinSide the expected input property key (LEFT_INPUT or RIGHT_INPUT)
     */
    private void handleStateTtlHintsForInput(
            RelHint.Builder builder, RelNode input, Join origJoin, String joinSide) {

        if (canCombine(input, origJoin)) {
            // Input is a MultiJoin and will be combined with original join,
            // so we copy all existing STATE_TTL hints
            final MultiJoin multiJoin = (MultiJoin) input;
            multiJoin.getHints().stream()
                    .filter(hint -> hint.hintName.equals(STATE_TTL.getHintName()))
                    .forEach(hint -> hint.listOptions.forEach(builder::hintOption));
        } else {
            extractStateTtlHint(builder, origJoin, joinSide);
        }
    }

    /**
     * Extracts STATE_TTL hints from the original join for a specific input side.
     *
     * <p>This method processes all STATE_TTL hints and extracts the TTL value for the expected
     * input property, following Flink's hint precedence rules where multiple hints with the same
     * key use the first occurrence.
     *
     * @param builder the RelHint.Builder to add the hint to
     * @param origJoin the original join containing the hints
     * @param joinSide the expected input property key (LEFT_INPUT or RIGHT_INPUT)
     */
    private void extractStateTtlHint(RelHint.Builder builder, Join origJoin, String joinSide) {

        final List<RelHint> stateTtlHints =
                origJoin.getHints().stream()
                        .filter(hint -> hint.hintName.equals(STATE_TTL.getHintName()))
                        .collect(Collectors.toList());

        // Process all STATE_TTL hints, following Flink's hint precedence rules
        String ttlValue = null;
        for (final RelHint stateTtlHint : stateTtlHints) {
            // For each hint, get the TTL value for the expected input property
            // If the key exists multiple times in the same hint, the last occurrence wins
            final String hintTtlValue = stateTtlHint.kvOptions.get(joinSide);
            if (hintTtlValue != null) {
                ttlValue = hintTtlValue;
            }
        }

        // Use the extracted TTL value or default to NO_STATE_TTL
        builder.hintOption(Objects.requireNonNullElse(ttlValue, StateTtlHint.NO_STATE_TTL));
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
