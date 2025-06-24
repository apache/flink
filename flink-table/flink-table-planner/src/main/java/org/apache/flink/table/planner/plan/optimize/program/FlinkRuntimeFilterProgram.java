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

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.runtimefilter.BatchPhysicalGlobalRuntimeFilterBuilder;
import org.apache.flink.table.planner.plan.nodes.physical.batch.runtimefilter.BatchPhysicalLocalRuntimeFilterBuilder;
import org.apache.flink.table.planner.plan.nodes.physical.batch.runtimefilter.BatchPhysicalRuntimeFilter;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Planner program that tries to inject runtime filter for suitable join to improve join
 * performance.
 *
 * <p>We build the runtime filter in a two-phase manner: First, each subtask on the build side
 * builds a local filter based on its local data, and sends the built filter to a global aggregation
 * node. Then the global aggregation node aggregates the received filters into a global filter, and
 * sends the global filter to all probe side subtasks. Therefore, we will add {@link
 * BatchPhysicalLocalRuntimeFilterBuilder}, {@link BatchPhysicalGlobalRuntimeFilterBuilder} and
 * {@link BatchPhysicalRuntimeFilter} into the physical plan.
 *
 * <p>For example, for the following query:
 *
 * <pre>{@code SELECT * FROM fact, dim WHERE x = a AND z = 2}</pre>
 *
 * <p>The original physical plan:
 *
 * <pre>{@code
 * Calc(select=[a, b, c, x, y, CAST(2 AS BIGINT) AS z])
 * +- HashJoin(joinType=[InnerJoin], where=[=(x, a)], select=[a, b, c, x, y], build=[right])
 *    :- Exchange(distribution=[hash[a]])
 *    :  +- TableSourceScan(table=[[fact]], fields=[a, b, c])
 *    +- Exchange(distribution=[hash[x]])
 *       +- Calc(select=[x, y], where=[=(z, 2)])
 *          +- TableSourceScan(table=[[dim, filter=[]]], fields=[x, y, z])
 * }</pre>
 *
 * <p>This optimized physical plan:
 *
 * <pre>{@code
 * Calc(select=[a, b, c, x, y, CAST(2 AS BIGINT) AS z])
 * +- HashJoin(joinType=[InnerJoin], where=[=(x, a)], select=[a, b, c, x, y], build=[right])
 *    :- Exchange(distribution=[hash[a]])
 *    :  +- RuntimeFilter(select=[a])
 *    :     :- Exchange(distribution=[broadcast])
 *    :     :  +- GlobalRuntimeFilterBuilder
 *    :     :     +- Exchange(distribution=[single])
 *    :     :        +- LocalRuntimeFilterBuilder(select=[x])
 *    :     :           +- Calc(select=[x, y], where=[=(z, 2)])
 *    :     :              +- TableSourceScan(table=[[dim, filter=[]]], fields=[x, y, z])
 *    :     +- TableSourceScan(table=[[fact]], fields=[a, b, c])
 *    +- Exchange(distribution=[hash[x]])
 *       +- Calc(select=[x, y], where=[=(z, 2)])
 *          +- TableSourceScan(table=[[dim, filter=[]]], fields=[x, y, z])
 *
 * }</pre>
 */
public class FlinkRuntimeFilterProgram implements FlinkOptimizeProgram<BatchOptimizeContext> {

    @Override
    public RelNode optimize(RelNode root, BatchOptimizeContext context) {
        if (!isRuntimeFilterEnabled(root)) {
            return root;
        }

        // To avoid that one side can be used both as a build side and as a probe side
        checkState(
                getMinProbeDataSize(root) > getMaxBuildDataSize(root),
                "The min probe data size should be larger than the max build data size.");

        DefaultRelShuttle shuttle =
                new DefaultRelShuttle() {
                    @Override
                    public RelNode visit(RelNode rel) {
                        if (!(rel instanceof Join)) {
                            List<RelNode> newInputs = new ArrayList<>();
                            for (RelNode input : rel.getInputs()) {
                                RelNode newInput = input.accept(this);
                                newInputs.add(newInput);
                            }
                            return rel.copy(rel.getTraitSet(), newInputs);
                        }

                        Join join = (Join) rel;
                        RelNode newLeft = join.getLeft().accept(this);
                        RelNode newRight = join.getRight().accept(this);

                        return tryInjectRuntimeFilter(
                                join.copy(join.getTraitSet(), Arrays.asList(newLeft, newRight)));
                    }
                };
        return shuttle.visit(root);
    }

    /**
     * Judge whether the join is suitable, and try to inject runtime filter for it.
     *
     * @param join the join node
     * @return the new join node with runtime filter.
     */
    private static Join tryInjectRuntimeFilter(Join join) {

        // check supported join type
        if (!(isSuitableJoinType(join.getJoinType()))) {
            return join;
        }

        // check supported join implementation
        if (!(join instanceof BatchPhysicalHashJoin)
                && !(join instanceof BatchPhysicalSortMergeJoin)) {
            return join;
        }

        boolean leftIsBuild;
        if (canBeProbeSide(join.getLeft())) {
            leftIsBuild = false;
        } else if (canBeProbeSide(join.getRight())) {
            leftIsBuild = true;
        } else {
            return join;
        }

        // check left join + left build
        if (join.getJoinType() == JoinRelType.LEFT && !leftIsBuild) {
            return join;
        }

        // check right join + right build
        if (join.getJoinType() == JoinRelType.RIGHT && leftIsBuild) {
            return join;
        }

        JoinInfo joinInfo = join.analyzeCondition();
        RelNode buildSide;
        RelNode probeSide;
        ImmutableIntList buildIndices;
        ImmutableIntList probeIndices;
        if (leftIsBuild) {
            buildSide = join.getLeft();
            probeSide = join.getRight();
            buildIndices = joinInfo.leftKeys;
            probeIndices = joinInfo.rightKeys;
        } else {
            buildSide = join.getRight();
            probeSide = join.getLeft();
            buildIndices = joinInfo.rightKeys;
            probeIndices = joinInfo.leftKeys;
        }

        Optional<BuildSideInfo> suitableBuildOpt =
                findSuitableBuildSide(
                        buildSide,
                        buildIndices,
                        (build, indices) ->
                                isSuitableDataSize(build, probeSide, indices, probeIndices));

        if (suitableBuildOpt.isPresent()) {
            BuildSideInfo suitableBuildInfo = suitableBuildOpt.get();
            RelNode newProbe =
                    tryPushDownProbeAndInjectRuntimeFilter(
                            probeSide, probeIndices, suitableBuildInfo, false);
            if (leftIsBuild) {
                return join.copy(join.getTraitSet(), Arrays.asList(buildSide, newProbe));
            } else {
                return join.copy(join.getTraitSet(), Arrays.asList(newProbe, buildSide));
            }
        }

        return join;
    }

    /**
     * Inject runtime filter and return the new probe side (without exchange).
     *
     * @param buildSide the build side
     * @param probeSide the probe side
     * @param buildIndices the build projection
     * @param probeIndices the probe projection
     * @return the new probe side
     */
    private static RelNode createNewProbeWithRuntimeFilter(
            RelNode buildSide,
            RelNode probeSide,
            ImmutableIntList buildIndices,
            ImmutableIntList probeIndices) {
        Optional<Double> buildRowCountOpt = getEstimatedRowCount(buildSide);
        checkState(buildRowCountOpt.isPresent());
        int buildRowCount = buildRowCountOpt.get().intValue();
        int maxRowCount =
                (int)
                        Math.ceil(
                                getMaxBuildDataSize(buildSide)
                                        / FlinkRelMdUtil.binaryRowAverageSize(buildSide));
        double filterRatio = computeFilterRatio(buildSide, probeSide, buildIndices, probeIndices);

        String[] buildFiledNames =
                buildIndices.stream()
                        .map(buildSide.getRowType().getFieldNames()::get)
                        .toArray(String[]::new);
        RelNode localBuilder =
                new BatchPhysicalLocalRuntimeFilterBuilder(
                        buildSide.getCluster(),
                        buildSide.getTraitSet(),
                        buildSide,
                        buildIndices.toIntArray(),
                        buildFiledNames,
                        buildRowCount,
                        maxRowCount);
        RelNode globalBuilder =
                new BatchPhysicalGlobalRuntimeFilterBuilder(
                        localBuilder.getCluster(),
                        localBuilder.getTraitSet(),
                        createExchange(localBuilder, FlinkRelDistribution.SINGLETON()),
                        buildFiledNames,
                        buildRowCount,
                        maxRowCount);
        RelNode runtimeFilter =
                new BatchPhysicalRuntimeFilter(
                        probeSide.getCluster(),
                        probeSide.getTraitSet(),
                        createExchange(globalBuilder, FlinkRelDistribution.BROADCAST_DISTRIBUTED()),
                        probeSide,
                        probeIndices.toIntArray(),
                        filterRatio);

        return runtimeFilter;
    }

    /**
     * Find a suitable build side. In order not to affect MultiInput, when the original build side
     * of runtime filter is not an {@link Exchange}, we need to push down the builder, until we find
     * an exchange and inject the builder there.
     *
     * @param rel the original build side
     * @param buildIndices build indices
     * @param buildSideChecker check whether current build side is suitable
     * @return An optional info of the suitable build side.It will be empty if we cannot find the
     *     suitable build side.
     */
    private static Optional<BuildSideInfo> findSuitableBuildSide(
            RelNode rel,
            ImmutableIntList buildIndices,
            BiFunction<RelNode, ImmutableIntList, Boolean> buildSideChecker) {
        if (rel instanceof Exchange) {
            // found the desired exchange, inject builder here
            Exchange exchange = (Exchange) rel;
            if (!(exchange.getInput() instanceof BatchPhysicalRuntimeFilter)
                    && buildSideChecker.apply(exchange.getInput(), buildIndices)) {
                return Optional.of(new BuildSideInfo(exchange.getInput(), buildIndices));
            }
        } else if (rel instanceof BatchPhysicalRuntimeFilter) {
            // runtime filter should not as build side
            return Optional.empty();
        } else if (rel instanceof Calc) {
            // try to push the builder to input of projection
            Calc calc = ((Calc) rel);
            RexProgram program = calc.getProgram();
            List<RexNode> projects =
                    program.getProjectList().stream()
                            .map(program::expandLocalRef)
                            .collect(Collectors.toList());
            ImmutableIntList inputIndices = getInputIndices(projects, buildIndices);
            if (inputIndices.isEmpty()) {
                return Optional.empty();
            }
            return findSuitableBuildSide(calc.getInput(), inputIndices, buildSideChecker);

        } else if (rel instanceof Join) {
            // try to push the builder to one input of join
            Join join = (Join) rel;
            if (!isSuitableJoinType(join.getJoinType())) {
                return Optional.empty();
            }

            Tuple2<ImmutableIntList, ImmutableIntList> tuple2 = getInputIndices(join, buildIndices);
            ImmutableIntList leftIndices = tuple2.f0;
            ImmutableIntList rightIndices = tuple2.f1;

            if (join.getJoinType() == JoinRelType.LEFT) {
                rightIndices = ImmutableIntList.of();
            } else if (join.getJoinType() == JoinRelType.RIGHT) {
                leftIndices = ImmutableIntList.of();
            }

            if (leftIndices.isEmpty() && rightIndices.isEmpty()) {
                return Optional.empty();
            }

            boolean firstCheckLeft = !leftIndices.isEmpty() && join.getLeft() instanceof Exchange;
            Optional<BuildSideInfo> buildSideInfoOpt = Optional.empty();
            if (firstCheckLeft) {
                buildSideInfoOpt =
                        findSuitableBuildSide(join.getLeft(), leftIndices, buildSideChecker);
                if (!buildSideInfoOpt.isPresent() && !rightIndices.isEmpty()) {
                    buildSideInfoOpt =
                            findSuitableBuildSide(join.getRight(), rightIndices, buildSideChecker);
                }
                return buildSideInfoOpt;
            } else {
                if (!rightIndices.isEmpty()) {
                    buildSideInfoOpt =
                            findSuitableBuildSide(join.getRight(), rightIndices, buildSideChecker);
                    if (!buildSideInfoOpt.isPresent() && !leftIndices.isEmpty()) {
                        buildSideInfoOpt =
                                findSuitableBuildSide(
                                        join.getLeft(), leftIndices, buildSideChecker);
                    }
                }
            }
            return buildSideInfoOpt;
        } else if (rel instanceof BatchPhysicalGroupAggregateBase) {
            // try to push the builder to input of agg, if the indices are all in grouping keys.
            BatchPhysicalGroupAggregateBase agg = (BatchPhysicalGroupAggregateBase) rel;
            int[] grouping = agg.grouping();

            for (int k : buildIndices) {
                if (k >= grouping.length) {
                    return Optional.empty();
                }
            }

            return findSuitableBuildSide(
                    agg.getInput(),
                    ImmutableIntList.copyOf(
                            buildIndices.stream()
                                    .map(index -> agg.grouping()[index])
                                    .collect(Collectors.toList())),
                    buildSideChecker);

        } else {
            // the above cases can cover all cases of TPC-DS test
            // we may find more cases later
        }

        return Optional.empty();
    }

    /**
     * Try to push down the probe side of runtime filter, and inject the runtime filter.
     *
     * @param rel the original probe side
     * @param probeIndices the probe indices
     * @param buildSideInfo the build side info
     * @param filterHasBenefit Whether it has benefit to inject the filter at the current position.
     *     We believe that only if the filter goes through an Exchange, Join or Agg is beneficial,
     *     otherwise there is no benefit. We should inject the filter only if it has benefit.
     * @return the new probe side wit runtime filter
     */
    private static RelNode tryPushDownProbeAndInjectRuntimeFilter(
            RelNode rel,
            ImmutableIntList probeIndices,
            BuildSideInfo buildSideInfo,
            boolean filterHasBenefit) {
        if (rel instanceof BatchPhysicalRuntimeFilter) {
            // do nothing, return current probe side directly. Because we don't inject more than
            // once runtime filter at the same place
            return rel;
        } else if (rel instanceof Exchange) {
            // try to push the probe side to the input of exchange
            Exchange exchange = (Exchange) rel;
            return exchange.copy(
                    exchange.getTraitSet(),
                    Collections.singletonList(
                            tryPushDownProbeAndInjectRuntimeFilter(
                                    exchange.getInput(), probeIndices, buildSideInfo, true)));
        } else if (rel instanceof Calc) {
            // try to push the probe side to the input of projection
            Calc calc = ((Calc) rel);
            RexProgram program = calc.getProgram();
            List<RexNode> projects =
                    program.getProjectList().stream()
                            .map(program::expandLocalRef)
                            .collect(Collectors.toList());
            ImmutableIntList inputIndices = getInputIndices(projects, probeIndices);
            if (!inputIndices.isEmpty()) {
                return calc.copy(
                        calc.getTraitSet(),
                        Collections.singletonList(
                                tryPushDownProbeAndInjectRuntimeFilter(
                                        calc.getInput(),
                                        inputIndices,
                                        buildSideInfo,
                                        filterHasBenefit)));
            }
        } else if (rel instanceof Join) {
            // try to push the probe side to the all inputs of join
            Join join = (Join) rel;
            Tuple2<ImmutableIntList, ImmutableIntList> tuple2 = getInputIndices(join, probeIndices);
            ImmutableIntList leftIndices = tuple2.f0;
            ImmutableIntList rightIndices = tuple2.f1;

            if (!leftIndices.isEmpty() || !rightIndices.isEmpty()) {
                RelNode leftSide = join.getLeft();
                RelNode rightSide = join.getRight();

                if (!leftIndices.isEmpty()) {
                    leftSide =
                            tryPushDownProbeAndInjectRuntimeFilter(
                                    leftSide, leftIndices, buildSideInfo, true);
                }

                if (!rightIndices.isEmpty()) {
                    rightSide =
                            tryPushDownProbeAndInjectRuntimeFilter(
                                    rightSide, rightIndices, buildSideInfo, true);
                }

                return join.copy(join.getTraitSet(), Arrays.asList(leftSide, rightSide));
            }
        } else if (rel instanceof BatchPhysicalGroupAggregateBase) {
            // try to push the probe side to input of agg, if the indices are all in grouping keys.
            BatchPhysicalGroupAggregateBase agg = (BatchPhysicalGroupAggregateBase) rel;
            int[] grouping = agg.grouping();
            if (probeIndices.stream().allMatch(index -> (index < grouping.length))) {
                return agg.copy(
                        agg.getTraitSet(),
                        Collections.singletonList(
                                tryPushDownProbeAndInjectRuntimeFilter(
                                        agg.getInput(),
                                        ImmutableIntList.copyOf(
                                                probeIndices.stream()
                                                        .map(index -> agg.grouping()[index])
                                                        .collect(Collectors.toList())),
                                        buildSideInfo,
                                        true)));
            }
        } else if (rel instanceof Union) {
            // try to push the probe side to all inputs of union
            Union union = (Union) rel;
            List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : union.getInputs()) {
                newInputs.add(
                        tryPushDownProbeAndInjectRuntimeFilter(
                                input, probeIndices, buildSideInfo, filterHasBenefit));
            }
            return union.copy(union.getTraitSet(), newInputs, union.all);
        } else if (rel instanceof BatchPhysicalDynamicFilteringTableSourceScan) {
            BatchPhysicalDynamicFilteringTableSourceScan tableScan =
                    (BatchPhysicalDynamicFilteringTableSourceScan) rel;
            final Set<Integer> dynamicFilteringIndices =
                    new HashSet<>(tableScan.dynamicFilteringIndices());
            if (dynamicFilteringIndices.containsAll(probeIndices)) {
                // do nothing, return current probe side directly. Because the fields have already
                // filtered by DPP.
                return rel;
            }
        } else {
            // the above cases can cover all cases of TPC-DS test
            // we may find more cases later
        }

        if (filterHasBenefit) {
            return createNewProbeWithRuntimeFilter(
                    ignoreExchange(buildSideInfo.buildSide),
                    ignoreExchange(rel),
                    buildSideInfo.buildIndices,
                    probeIndices);
        } else {
            // If the probe side is a direct table source, or only simple Calc/Union, no other
            // operations, we will not inject runtime filter, because we believe the benefit to be
            // small or even negative
            return rel;
        }
    }

    private static BatchPhysicalExchange createExchange(
            RelNode input, FlinkRelDistribution newDistribution) {
        RelTraitSet newTraitSet =
                input.getCluster()
                        .getPlanner()
                        .emptyTraitSet()
                        .replace(FlinkConventions.BATCH_PHYSICAL())
                        .replace(newDistribution);

        return new BatchPhysicalExchange(input.getCluster(), newTraitSet, input, newDistribution);
    }

    /**
     * Try to map project output indices to it's input indices.If the output indices can't be fully
     * mapped to input, return empty.
     */
    private static ImmutableIntList getInputIndices(
            List<RexNode> projects, ImmutableIntList outputIndices) {
        List<Integer> inputIndices = new ArrayList<>();
        for (int k : outputIndices) {
            RexNode rexNode = projects.get(k);
            if (!(rexNode instanceof RexInputRef)) {
                return ImmutableIntList.of();
            }
            inputIndices.add(((RexInputRef) rexNode).getIndex());
        }
        return ImmutableIntList.copyOf(inputIndices);
    }

    /**
     * Try to map join output indices to join inputs' indices(left and right). If the output indices
     * can't be fully mapped to left or right input, return empty.
     *
     * @param join the join rel
     * @param outputIndices the output indices
     * @return the mapped left and right input indices.
     */
    private static Tuple2<ImmutableIntList, ImmutableIntList> getInputIndices(
            Join join, ImmutableIntList outputIndices) {
        JoinInfo joinInfo = join.analyzeCondition();
        Map<Integer, Integer> leftToRightJoinKeysMapping =
                createKeysMapping(joinInfo.leftKeys, joinInfo.rightKeys);
        Map<Integer, Integer> rightToLeftJoinKeysMapping =
                createKeysMapping(joinInfo.rightKeys, joinInfo.leftKeys);

        List<Integer> leftIndices = new ArrayList<>();
        List<Integer> rightIndices = new ArrayList<>();

        int leftFieldCnt = join.getLeft().getRowType().getFieldCount();
        for (int index : outputIndices) {
            if (index < leftFieldCnt) {
                leftIndices.add(index);
                // if it's join key, map to right
                if (leftToRightJoinKeysMapping.containsKey(index)) {
                    rightIndices.add(leftToRightJoinKeysMapping.get(index));
                }
            } else {
                int rightIndex = index - leftFieldCnt;
                rightIndices.add(rightIndex);
                // if it's join key, map to left
                if (rightToLeftJoinKeysMapping.containsKey(rightIndex)) {
                    leftIndices.add(rightToLeftJoinKeysMapping.get(rightIndex));
                }
            }
        }

        ImmutableIntList left =
                leftIndices.size() == outputIndices.size()
                        ? ImmutableIntList.copyOf(leftIndices)
                        : ImmutableIntList.of();
        ImmutableIntList right =
                rightIndices.size() == outputIndices.size()
                        ? ImmutableIntList.copyOf(rightIndices)
                        : ImmutableIntList.of();

        return Tuple2.of(left, right);
    }

    private static Map<Integer, Integer> createKeysMapping(
            ImmutableIntList keyList1, ImmutableIntList keyList2) {
        checkState(keyList1.size() == keyList2.size());
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < keyList1.size(); ++i) {
            mapping.put(keyList1.get(i), keyList2.get(i));
        }
        return mapping;
    }

    private static boolean canBeProbeSide(RelNode rel) {
        Optional<Double> size = getEstimatedDataSize(rel);
        return size.isPresent() && size.get() >= getMinProbeDataSize(rel);
    }

    private static boolean isSuitableDataSize(
            RelNode buildSide,
            RelNode probeSide,
            ImmutableIntList buildIndices,
            ImmutableIntList probeIndices) {
        Optional<Double> buildSize = getEstimatedDataSize(buildSide);
        Optional<Double> probeSize = getEstimatedDataSize(probeSide);

        long maxBuildDataSize = getMaxBuildDataSize(buildSide);
        long minProbeDataSize = getMinProbeDataSize(probeSide);
        double minFilterRatio = getMinFilterRatio(buildSide);

        if (!buildSize.isPresent() || !probeSize.isPresent()) {
            return false;
        }

        if (buildSize.get() > maxBuildDataSize || probeSize.get() < minProbeDataSize) {
            return false;
        }

        return computeFilterRatio(buildSide, probeSide, buildIndices, probeIndices)
                >= minFilterRatio;
    }

    private static double computeFilterRatio(
            RelNode buildSide,
            RelNode probeSide,
            ImmutableIntList buildIndices,
            ImmutableIntList probeIndices) {

        Optional<Double> buildNdv = getEstimatedNdv(buildSide, ImmutableBitSet.of(buildIndices));
        Optional<Double> probeNdv = getEstimatedNdv(probeSide, ImmutableBitSet.of(probeIndices));

        if (buildNdv.isPresent() && probeNdv.isPresent()) {
            return Math.max(0, 1 - buildNdv.get() / probeNdv.get());
        } else {
            Optional<Double> buildRowCount = getEstimatedRowCount(buildSide);
            Optional<Double> probeRowCount = getEstimatedRowCount(probeSide);
            checkState(buildRowCount.isPresent() && probeRowCount.isPresent());
            return Math.max(0, 1 - buildRowCount.get() / probeRowCount.get());
        }
    }

    private static RelNode ignoreExchange(RelNode relNode) {
        if (relNode instanceof Exchange) {
            return relNode.getInput(0);
        } else {
            return relNode;
        }
    }

    private static Optional<Double> getEstimatedDataSize(RelNode relNode) {
        return Optional.ofNullable(JoinUtil.binaryRowRelNodeSize(relNode));
    }

    private static Optional<Double> getEstimatedRowCount(RelNode relNode) {
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        return Optional.ofNullable(mq.getRowCount(relNode));
    }

    private static Optional<Double> getEstimatedNdv(RelNode relNode, ImmutableBitSet keys) {
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        return Optional.ofNullable(mq.getDistinctRowCount(relNode, keys, null));
    }

    private static class BuildSideInfo {
        private final RelNode buildSide;
        private final ImmutableIntList buildIndices;

        public BuildSideInfo(RelNode buildSide, ImmutableIntList buildIndices) {
            this.buildSide = checkNotNull(buildSide);
            this.buildIndices = checkNotNull(buildIndices);
        }
    }

    private static boolean isRuntimeFilterEnabled(RelNode relNode) {
        return unwrapTableConfig(relNode)
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_ENABLED);
    }

    private static long getMaxBuildDataSize(RelNode relNode) {
        return unwrapTableConfig(relNode)
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_MAX_BUILD_DATA_SIZE)
                .getBytes();
    }

    private static long getMinProbeDataSize(RelNode relNode) {
        return unwrapTableConfig(relNode)
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_MIN_PROBE_DATA_SIZE)
                .getBytes();
    }

    private static double getMinFilterRatio(RelNode relNode) {
        return unwrapTableConfig(relNode)
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_MIN_FILTER_RATIO);
    }

    public static boolean isSuitableJoinType(JoinRelType joinType) {
        // check supported join type
        return joinType == JoinRelType.INNER
                || joinType == JoinRelType.SEMI
                || joinType == JoinRelType.LEFT
                || joinType == JoinRelType.RIGHT;
    }
}
