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
import org.apache.flink.table.planner.plan.cost.FlinkCost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Flink busy join reorder rule, which will convert {@link MultiJoin} to a busy join tree.
 *
 * <p>In this busy join reorder strategy, we will first try to reorder all the inner join type
 * inputs in the multiJoin, and then add all outer join type inputs on the top.
 *
 * <p>First, reordering all the inner join type inputs in the multiJoin. We adopt the concept of
 * level in dynamic programming, and the latter layer will use the results stored in the previous
 * layers. First, we put all inputs (each input in {@link MultiJoin}) into level 0, then we build
 * all two-inputs join at level 1 based on the {@link FlinkCost} of level 0, then we will build
 * three-inputs join based on the previous two levels, then four-inputs joins ... etc, util we
 * reorder all the inner join type inputs in the multiJoin. When building m-inputs join, we only
 * keep the best plan (have the lowest {@link FlinkCost}) for the same set of m inputs. E.g., for
 * three-inputs join, we keep only the best plan for inputs {A, B, C} among plans (A J B) J C, (A J
 * C) J B, (B J C) J A.
 *
 * <p>Second, we will add all outer join type inputs in the MultiJoin on the top.
 */
public class FlinkBusyJoinReorderRule extends RelRule<FlinkBusyJoinReorderRule.Config>
        implements TransformationRule {

    /** Creates an SparkJoinReorderRule. */
    protected FlinkBusyJoinReorderRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkBusyJoinReorderRule(RelBuilderFactory relBuilderFactory) {
        this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory).as(Config.class));
    }

    @Deprecated // to be removed before 2.0
    public FlinkBusyJoinReorderRule(
            RelFactories.JoinFactory joinFactory,
            RelFactories.ProjectFactory projectFactory,
            RelFactories.FilterFactory filterFactory) {
        this(RelBuilder.proto(joinFactory, projectFactory, filterFactory));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final RelBuilder relBuilder = call.builder();
        final MultiJoin multiJoinRel = call.rel(0);
        final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
        RelNode bestOrder = findBestOrder(call.getMetadataQuery(), relBuilder, multiJoin);
        call.transformTo(bestOrder);
    }

    private static RelNode findBestOrder(
            RelMetadataQuery mq, RelBuilder relBuilder, LoptMultiJoin multiJoin) {
        // In our busy join reorder strategy, we will first try to reorder all the inner join type
        // inputs in the multiJoin, and then add all outer join type inputs on the top.
        // First, reorder all the inner join type inputs in the multiJoin.
        List<Map<Set<Integer>, JoinPlan>> foundPlans = reOrderInnerJoin(mq, relBuilder, multiJoin);

        JoinPlan finalPlan;
        // Second, add all outer join type inputs in the multiJoin on the top.
        if (canOuterJoin(multiJoin)) {
            finalPlan =
                    addToTopForOuterJoin(
                            getBestPlan(mq, foundPlans.get(foundPlans.size() - 1)),
                            multiJoin,
                            relBuilder);
        } else {
            if (foundPlans.size() != multiJoin.getNumJoinFactors()) {
                finalPlan =
                        addToTop(
                                getBestPlan(mq, foundPlans.get(foundPlans.size() - 1)),
                                multiJoin,
                                relBuilder);
            } else {
                assert foundPlans.get(foundPlans.size() - 1).size() == 1;
                finalPlan = new ArrayList<>(foundPlans.get(foundPlans.size() - 1).values()).get(0);
            }
        }

        final List<String> fieldNames = multiJoin.getMultiJoinRel().getRowType().getFieldNames();
        return creatToProject(relBuilder, multiJoin, finalPlan, fieldNames);
    }

    private static List<Map<Set<Integer>, JoinPlan>> reOrderInnerJoin(
            RelMetadataQuery mq, RelBuilder relBuilder, LoptMultiJoin multiJoin) {
        List<Map<Set<Integer>, JoinPlan>> foundPlans = new ArrayList<>();

        // First, we put each input in MultiJoin into level 0.
        Map<Set<Integer>, JoinPlan> joinPlanMap = new LinkedHashMap<>();
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            if (!multiJoin.isNullGenerating(i)) {
                HashSet<Integer> set1 = new HashSet<>();
                LinkedHashSet<Integer> set2 = new LinkedHashSet<>();
                set1.add(i);
                set2.add(i);
                RelNode joinFactor = multiJoin.getJoinFactor(i);
                joinPlanMap.put(set1, new JoinPlan(set2, joinFactor));
            }
        }
        foundPlans.add(joinPlanMap);

        // Build plans for next levels until the last level has only one plan. This plan contains
        // all inputs that can be joined, so there's no need to continue
        while (foundPlans.size() < multiJoin.getNumJoinFactors()) {
            Map<Set<Integer>, JoinPlan> levelPlan =
                    searchLevel(mq, relBuilder, new ArrayList<>(foundPlans), multiJoin, false);
            if (levelPlan.size() == 0) {
                break;
            }
            foundPlans.add(levelPlan);
        }

        return foundPlans;
    }

    private static boolean canOuterJoin(LoptMultiJoin multiJoin) {
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            if (multiJoin.getOuterJoinCond(i) != null
                    && RelOptUtil.conjunctions(multiJoin.getOuterJoinCond(i)).size() != 0) {
                return true;
            }
        }
        return false;
    }

    private static JoinPlan getBestPlan(
            RelMetadataQuery mq, Map<Set<Integer>, JoinPlan> levelPlan) {
        JoinPlan bestPlan = null;
        for (Map.Entry<Set<Integer>, JoinPlan> entry : levelPlan.entrySet()) {
            if (bestPlan == null || entry.getValue().betterThan(bestPlan, mq)) {
                bestPlan = entry.getValue();
            }
        }

        return bestPlan;
    }

    private static JoinPlan addToTopForOuterJoin(
            JoinPlan bestPlan, LoptMultiJoin multiJoin, RelBuilder relBuilder) {
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        List<Integer> remainIndexes = new ArrayList<>();
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            if (!bestPlan.factorIds.contains(i)) {
                remainIndexes.add(i);
            }
        }

        RelNode leftNode = bestPlan.relNode;
        LinkedHashSet<Integer> set = new LinkedHashSet<>(bestPlan.factorIds);
        for (int index : remainIndexes) {
            RelNode rightNode = multiJoin.getJoinFactor(index);

            // make new join condition
            Optional<Tuple2<Set<RexCall>, JoinRelType>> joinConds =
                    getConditionsAndJoinType(
                            bestPlan.factorIds, Collections.singleton(index), multiJoin, true);

            if (!joinConds.isPresent()) {
                // join type is always left.
                leftNode =
                        relBuilder
                                .push(leftNode)
                                .push(rightNode)
                                .join(JoinRelType.LEFT, rexBuilder.makeLiteral(true))
                                .build();
            } else {
                Set<RexCall> conditions = joinConds.get().f0;
                List<RexNode> rexCalls = new ArrayList<>(conditions);
                Set<RexCall> newCondition =
                        convertToNewCondition(
                                new ArrayList<>(set),
                                Collections.singletonList(index),
                                rexCalls,
                                multiJoin);
                // all given left join.
                leftNode =
                        relBuilder
                                .push(leftNode)
                                .push(rightNode)
                                .join(JoinRelType.LEFT, newCondition)
                                .build();
            }
            set.add(index);
        }
        return new JoinPlan(set, leftNode);
    }

    private static JoinPlan addToTop(
            JoinPlan bestPlan, LoptMultiJoin multiJoin, RelBuilder relBuilder) {
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        List<Integer> remainIndexes = new ArrayList<>();
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            if (!bestPlan.factorIds.contains(i)) {
                remainIndexes.add(i);
            }
        }

        RelNode leftNode = bestPlan.relNode;
        LinkedHashSet<Integer> set = new LinkedHashSet<>(bestPlan.factorIds);
        for (int index : remainIndexes) {
            set.add(index);
            RelNode rightNode = multiJoin.getJoinFactor(index);
            leftNode =
                    relBuilder
                            .push(leftNode)
                            .push(rightNode)
                            .join(
                                    multiJoin.getMultiJoinRel().getJoinTypes().get(index),
                                    rexBuilder.makeLiteral(true))
                            .build();
        }
        return new JoinPlan(set, leftNode);
    }

    private static RelNode creatToProject(
            RelBuilder relBuilder,
            LoptMultiJoin multiJoin,
            JoinPlan finalPlan,
            List<String> fieldNames) {
        List<RexNode> newProjExprs = new ArrayList<>();
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

        List<Integer> newJoinOrder = new ArrayList<>(finalPlan.factorIds);
        int nJoinFactors = multiJoin.getNumJoinFactors();
        List<RelDataTypeField> fields = multiJoin.getMultiJoinFields();

        // create a mapping from each factor to its field offset in the join
        // ordering
        final Map<Integer, Integer> factorToOffsetMap = new HashMap<>();
        for (int pos = 0, fieldStart = 0; pos < nJoinFactors; pos++) {
            factorToOffsetMap.put(newJoinOrder.get(pos), fieldStart);
            fieldStart += multiJoin.getNumFieldsInJoinFactor(newJoinOrder.get(pos));
        }

        for (int currFactor = 0; currFactor < nJoinFactors; currFactor++) {
            // if the factor is the right factor in a removable self-join,
            // then where possible, remap references to the right factor to
            // the corresponding reference in the left factor
            Integer leftFactor = null;
            if (multiJoin.isRightFactorInRemovableSelfJoin(currFactor)) {
                leftFactor = multiJoin.getOtherSelfJoinFactor(currFactor);
            }
            for (int fieldPos = 0;
                    fieldPos < multiJoin.getNumFieldsInJoinFactor(currFactor);
                    fieldPos++) {
                int newOffset =
                        requireNonNull(
                                        factorToOffsetMap.get(currFactor),
                                        () -> "factorToOffsetMap.get(currFactor)")
                                + fieldPos;
                if (leftFactor != null) {
                    Integer leftOffset = multiJoin.getRightColumnMapping(currFactor, fieldPos);
                    if (leftOffset != null) {
                        newOffset =
                                requireNonNull(
                                                factorToOffsetMap.get(leftFactor),
                                                "factorToOffsetMap.get(leftFactor)")
                                        + leftOffset;
                    }
                }
                newProjExprs.add(
                        rexBuilder.makeInputRef(
                                fields.get(newProjExprs.size()).getType(), newOffset));
            }
        }

        relBuilder.push(finalPlan.relNode);
        relBuilder.project(newProjExprs, fieldNames);

        // Place the post-join filter (if it exists) on top of the final
        // projection.
        RexNode postJoinFilter = multiJoin.getMultiJoinRel().getPostJoinFilter();
        if (postJoinFilter != null) {
            relBuilder.filter(postJoinFilter);
        }
        return relBuilder.build();
    }

    private static Map<Set<Integer>, JoinPlan> searchLevel(
            RelMetadataQuery mq,
            RelBuilder relBuilder,
            List<Map<Set<Integer>, JoinPlan>> existingLevels,
            LoptMultiJoin multiJoin,
            boolean isOuterJoin) {
        Map<Set<Integer>, JoinPlan> nextLevel = new LinkedHashMap<>();
        int k = 0;
        int lev = existingLevels.size() - 1;
        while (k <= lev - k) {
            ArrayList<JoinPlan> oneSideCandidates = new ArrayList<>(existingLevels.get(k).values());
            int oneSideSize = oneSideCandidates.size();
            for (int i = 0; i < oneSideSize; i++) {
                JoinPlan oneSidePlan = oneSideCandidates.get(i);
                ArrayList<JoinPlan> otherSideCandidates;
                if (k == lev - k) {
                    otherSideCandidates = new ArrayList<>(oneSideCandidates);
                    if (i > 0) {
                        otherSideCandidates.subList(0, i).clear();
                    }
                } else {
                    otherSideCandidates = new ArrayList<>(existingLevels.get(lev - k).values());
                }
                for (JoinPlan otherSidePlan : otherSideCandidates) {
                    Optional<JoinPlan> newJoinPlan =
                            buildJoin(
                                    relBuilder, oneSidePlan, otherSidePlan, multiJoin, isOuterJoin);
                    if (newJoinPlan.isPresent()) {
                        JoinPlan existingPlan = nextLevel.get(newJoinPlan.get().factorIds);
                        // check if it's the first plan for the factor set, or it's a better plan
                        // than the existing one due to lower cost.
                        if (existingPlan == null
                                || newJoinPlan.get().betterThan(existingPlan, mq)) {
                            nextLevel.put(newJoinPlan.get().factorIds, newJoinPlan.get());
                        }
                    }
                }
            }
            k += 1;
        }
        return nextLevel;
    }

    private static Optional<JoinPlan> buildJoin(
            RelBuilder relBuilder,
            JoinPlan oneSidePlan,
            JoinPlan otherSidePlan,
            LoptMultiJoin multiJoin,
            boolean isOuterJoin) {
        // intersect, should not join two overlapping factor sets.
        Set<Integer> resSet = new HashSet<>(oneSidePlan.factorIds);
        resSet.retainAll(otherSidePlan.factorIds);
        if (!resSet.isEmpty()) {
            return Optional.empty();
        }

        Optional<Tuple2<Set<RexCall>, JoinRelType>> joinConds =
                getConditionsAndJoinType(
                        oneSidePlan.factorIds, otherSidePlan.factorIds, multiJoin, isOuterJoin);
        if (!joinConds.isPresent()) {
            return Optional.empty();
        }

        Set<RexCall> conditions = joinConds.get().f0;
        JoinRelType joinType = joinConds.get().f1;

        LinkedHashSet<Integer> newFactorIds = new LinkedHashSet<>();
        JoinPlan leftPlan;
        JoinPlan rightPlan;
        // put the deeper side on the left, tend to build a left-deep tree.
        if (oneSidePlan.factorIds.size() >= otherSidePlan.factorIds.size()) {
            leftPlan = oneSidePlan;
            rightPlan = otherSidePlan;
        } else {
            leftPlan = otherSidePlan;
            rightPlan = oneSidePlan;
            if (isOuterJoin) {
                joinType = (joinType == JoinRelType.LEFT) ? JoinRelType.RIGHT : JoinRelType.LEFT;
            }
        }
        newFactorIds.addAll(leftPlan.factorIds);
        newFactorIds.addAll(rightPlan.factorIds);

        List<RexNode> rexCalls = new ArrayList<>(conditions);
        Set<RexCall> newCondition =
                convertToNewCondition(
                        new ArrayList<>(leftPlan.factorIds),
                        new ArrayList<>(rightPlan.factorIds),
                        rexCalls,
                        multiJoin);

        Join newJoin =
                (Join)
                        relBuilder
                                .push(leftPlan.relNode)
                                .push(rightPlan.relNode)
                                .join(joinType, newCondition)
                                .build();

        return Optional.of(new JoinPlan(newFactorIds, newJoin));
    }

    private static Set<RexCall> convertToNewCondition(
            List<Integer> leftFactorIds,
            List<Integer> rightFactorIds,
            List<RexNode> rexNodes,
            LoptMultiJoin multiJoin) {
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        Set<RexCall> newCondition = new HashSet<>();
        for (RexNode cond : rexNodes) {
            RexCall rexCond = (RexCall) cond;
            List<RexNode> resultRexNode = new ArrayList<>();
            for (RexNode rexNode : rexCond.getOperands()) {
                rexNode =
                        rexNode.accept(
                                new RexInputConverterForBusyJoin(
                                        rexBuilder, multiJoin, leftFactorIds, rightFactorIds));
                resultRexNode.add(rexNode);
            }
            RexNode resultRex = rexBuilder.makeCall(rexCond.op, resultRexNode);
            newCondition.add((RexCall) resultRex);
        }

        return newCondition;
    }

    private static Optional<Tuple2<Set<RexCall>, JoinRelType>> getConditionsAndJoinType(
            Set<Integer> oneFactorIds,
            Set<Integer> otherFactorIds,
            LoptMultiJoin multiJoin,
            boolean isOuterJoin) {
        if (oneFactorIds.size() + otherFactorIds.size() < 2) {
            return Optional.empty();
        }
        JoinRelType joinType = JoinRelType.INNER;
        if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            assert multiJoin.getNumJoinFactors() == 2;
            joinType = JoinRelType.FULL;
        }

        Set<RexCall> resultRexCall = new HashSet<>();
        List<RexNode> joinConditions = new ArrayList<>();
        if (isOuterJoin) {
            for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
                joinConditions.addAll(RelOptUtil.conjunctions(multiJoin.getOuterJoinCond(i)));
            }
        } else {
            joinConditions = multiJoin.getJoinFilters();
        }

        for (RexNode joinCond : joinConditions) {
            if (joinCond instanceof RexCall) {
                RexCall callCondition = (RexCall) joinCond;
                ImmutableBitSet factorsRefByJoinFilter =
                        multiJoin.getFactorsRefByJoinFilter(callCondition);
                int oneFactorNumbers = 0;
                int otherFactorNumbers = 0;
                for (int oneFactorId : oneFactorIds) {
                    if (factorsRefByJoinFilter.get(oneFactorId)) {
                        oneFactorNumbers++;
                        if (isOuterJoin && multiJoin.isNullGenerating(oneFactorId)) {
                            joinType = JoinRelType.RIGHT;
                        }
                    }
                }
                for (int otherFactorId : otherFactorIds) {
                    if (factorsRefByJoinFilter.get(otherFactorId)) {
                        otherFactorNumbers++;
                        if (isOuterJoin && multiJoin.isNullGenerating(otherFactorId)) {
                            joinType = JoinRelType.LEFT;
                        }
                    }
                }

                if (oneFactorNumbers > 0
                        && otherFactorNumbers > 0
                        && oneFactorNumbers + otherFactorNumbers
                                == factorsRefByJoinFilter.asSet().size()) {
                    resultRexCall.add(callCondition);
                }
            } else {
                return Optional.empty();
            }
        }

        if (resultRexCall.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Tuple2.of(resultRexCall, joinType));
        }
    }

    // ~ Inner Classes ----------------------------------------------------------
    private static class JoinPlan {
        final LinkedHashSet<Integer> factorIds;
        final RelNode relNode;

        JoinPlan(LinkedHashSet<Integer> factorIds, RelNode relNode) {
            this.factorIds = factorIds;
            this.relNode = relNode;
        }

        private boolean betterThan(JoinPlan otherPlan, RelMetadataQuery mq) {
            RelOptCost thisCost = mq.getCumulativeCost(this.relNode);
            RelOptCost otherCost = mq.getCumulativeCost(otherPlan.relNode);
            if (thisCost == null
                    || otherCost == null
                    || thisCost.getRows() == 0.0
                    || otherCost.getRows() == 0.0) {
                return false;
            } else {
                return thisCost.isLt(otherCost);
            }
        }
    }

    private static class RexInputConverterForBusyJoin extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final LoptMultiJoin multiJoin;
        private final List<Integer> leftFactorIds;
        private final List<Integer> rightFactorIds;

        public RexInputConverterForBusyJoin(
                RexBuilder rexBuilder,
                LoptMultiJoin multiJoin,
                List<Integer> leftFactorIds,
                List<Integer> rightFactorIds) {
            this.rexBuilder = rexBuilder;
            this.multiJoin = multiJoin;
            this.leftFactorIds = leftFactorIds;
            this.rightFactorIds = rightFactorIds;
        }

        @Override
        public RexNode visitInputRef(RexInputRef var) {
            // begin to do it
            int index = var.getIndex();
            int destIndex = 0;
            int factorRef = multiJoin.findRef(index);
            if (leftFactorIds.contains(factorRef)) {
                for (Integer leftFactorId : leftFactorIds) {
                    if (leftFactorId == factorRef) {
                        destIndex += findFactorIndex(index, multiJoin);
                        return new RexInputRef(destIndex, var.getType());
                    } else {
                        destIndex += multiJoin.getNumFieldsInJoinFactor(leftFactorId);
                    }
                }
            } else {
                for (int leftFactor : leftFactorIds) {
                    destIndex += multiJoin.getNumFieldsInJoinFactor(leftFactor);
                }
                for (Integer rightFactorId : rightFactorIds) {
                    if (rightFactorId == factorRef) {
                        destIndex += findFactorIndex(index, multiJoin);
                        return rexBuilder.makeInputRef(var.getType(), destIndex);
                    } else {
                        destIndex += multiJoin.getNumFieldsInJoinFactor(rightFactorId);
                    }
                }
            }

            return var;
        }

        private static int findFactorIndex(int index, LoptMultiJoin multiJoin) {
            int factorId = multiJoin.findRef(index);
            int resultIndex = 0;
            for (int i = 0; i < factorId; i++) {
                resultIndex += multiJoin.getNumFieldsInJoinFactor(i);
            }
            return index - resultIndex;
        }
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                EMPTY.withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
                        .as(Config.class);

        @Override
        default FlinkBusyJoinReorderRule toRule() {
            return new FlinkBusyJoinReorderRule(this);
        }
    }
}
