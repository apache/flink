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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinAssociation;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinLookupChain;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.JoinTypeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexProgram;
import org.immutables.value.Value;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.buildLookupChainAndUpdateTopJoinAssociation;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getDeltaJoin;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getDeltaJoinSpec;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getRexProgramBetweenJoinAndDeltaJoin;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getRexProgramBetweenJoinAndTableScan;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getTableScan;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.swapJoinType;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * A rule that converts a {@link StreamPhysicalJoin} to a {@link StreamPhysicalDeltaJoin} if all
 * conditions are matched.
 *
 * <p>Currently, only {@link StreamPhysicalJoin} satisfied the following requirements can be
 * converted to {@link StreamPhysicalDeltaJoin}.
 *
 * <ol>
 *   <li>The join is INNER join.
 *   <li>There is at least one join key pair in the join.
 *   <li>The downstream nodes of this join can accept duplicate changes.
 *   <li>All join inputs are with changelog "I" or "I, UA".
 *   <li>If this join outputs update records, the non-equiv conditions must be applied on upsert
 *       keys of this join.
 *   <li>All upstream nodes of this join are in {@code
 *       DeltaJoinUtil#ALL_SUPPORTED_DELTA_JOIN_UPSTREAM_NODES}
 *   <li>The join keys include at least one complete index in each source table of the join input.
 *   <li>All table sources of this join inputs support async {@link LookupTableSource}.
 * </ol>
 *
 * <p>See more at {@link DeltaJoinUtil#canConvertToDeltaJoin}.
 */
@Value.Enclosing
public class DeltaJoinRewriteRule extends RelRule<DeltaJoinRewriteRule.Config> {

    public static final DeltaJoinRewriteRule INSTANCE =
            DeltaJoinRewriteRule.Config.DEFAULT.toRule();

    private DeltaJoinRewriteRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        OptimizerConfigOptions.DeltaJoinStrategy deltaJoinStrategy =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY);
        if (OptimizerConfigOptions.DeltaJoinStrategy.NONE == deltaJoinStrategy) {
            return false;
        }

        StreamPhysicalJoin join = call.rel(0);
        return DeltaJoinUtil.canConvertToDeltaJoin(join);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalJoin join = call.rel(0);

        Optional<StreamPhysicalDeltaJoin> leftBottomDeltaJoin = getDeltaJoin(join.getLeft());
        Optional<StreamPhysicalDeltaJoin> rightBottomDeltaJoin = getDeltaJoin(join.getRight());

        final StreamPhysicalDeltaJoin deltaJoin;
        if (leftBottomDeltaJoin.isEmpty() && rightBottomDeltaJoin.isEmpty()) {
            deltaJoin = convertToBinaryDeltaJoin(join);
        } else if (leftBottomDeltaJoin.isPresent() && rightBottomDeltaJoin.isPresent()) {
            deltaJoin =
                    convertToBushyDeltaJoin(
                            join, leftBottomDeltaJoin.get(), rightBottomDeltaJoin.get());
        } else {
            deltaJoin =
                    leftBottomDeltaJoin
                            .map(
                                    streamPhysicalDeltaJoin ->
                                            convertToLeftRightHandDeltaJoin(
                                                    join, streamPhysicalDeltaJoin, true))
                            .orElseGet(
                                    () ->
                                            convertToLeftRightHandDeltaJoin(
                                                    join, rightBottomDeltaJoin.get(), false));
        }

        call.transformTo(deltaJoin);
    }

    /**
     * Convert single join to binary delta join.
     *
     * <pre>{@code
     *     Join               DeltaJoin
     *   /     \     =>       /      \
     * #0      #1            #0      #1
     *
     * }</pre>
     */
    private StreamPhysicalDeltaJoin convertToBinaryDeltaJoin(StreamPhysicalJoin join) {
        RelOptCluster cluster = join.getCluster();
        JoinInfo joinInfo = join.analyzeCondition();
        int[] leftJoinKeys = joinInfo.leftKeys.toIntArray();
        int[] rightJoinKeys = joinInfo.rightKeys.toIntArray();
        FlinkJoinType left2RightJoinType = JoinTypeUtil.getFlinkJoinType(join.getJoinType());
        FlinkJoinType right2LeftJoinType = swapJoinType(left2RightJoinType);

        // treat right as lookup side
        StreamPhysicalTableSourceScan rightTableScan = getTableScan(join.getRight());
        TableSourceTable rightTable = rightTableScan.tableSourceTable();
        RexProgram calcOnRightTable =
                getRexProgramBetweenJoinAndTableScan(join.getRight()).orElse(null);
        DeltaJoinSpec left2RightDeltaJoinSpec =
                getDeltaJoinSpec(joinInfo, rightTable, calcOnRightTable, cluster, true);
        DeltaJoinAssociation.Association left2RightAssociation =
                DeltaJoinAssociation.Association.of(left2RightJoinType, left2RightDeltaJoinSpec);

        DeltaJoinLookupChain left2RightLookupChain = DeltaJoinLookupChain.newInstance();
        left2RightLookupChain.addNode(
                DeltaJoinLookupChain.Node.of(
                        0, // inputTableBinaryInputOrdinal
                        1, // lookupTableBinaryInputOrdinal
                        left2RightDeltaJoinSpec,
                        left2RightJoinType));

        // treat left as lookup side
        StreamPhysicalTableSourceScan leftTableScan = getTableScan(join.getLeft());
        TableSourceTable leftTable = leftTableScan.tableSourceTable();
        RexProgram calcOnLeftTable =
                getRexProgramBetweenJoinAndTableScan(join.getLeft()).orElse(null);
        DeltaJoinSpec right2LeftDeltaJoinSpec =
                getDeltaJoinSpec(joinInfo, leftTable, calcOnLeftTable, cluster, false);
        DeltaJoinAssociation.Association right2LeftAssociation =
                DeltaJoinAssociation.Association.of(right2LeftJoinType, right2LeftDeltaJoinSpec);
        DeltaJoinLookupChain right2LeftLookupChain = DeltaJoinLookupChain.newInstance();
        right2LeftLookupChain.addNode(
                DeltaJoinLookupChain.Node.of(
                        1, // inputTableBinaryInputOrdinal
                        0, // lookupTableBinaryInputOrdinal
                        right2LeftDeltaJoinSpec,
                        right2LeftJoinType));

        DeltaJoinAssociation deltaJoinAssociation =
                DeltaJoinAssociation.create(
                        left2RightJoinType,
                        join.getCondition(),
                        leftJoinKeys,
                        rightJoinKeys,
                        leftTableScan,
                        calcOnLeftTable,
                        rightTableScan,
                        calcOnRightTable,
                        left2RightAssociation,
                        right2LeftAssociation);

        return new StreamPhysicalDeltaJoin(
                join.getCluster(),
                join.getTraitSet(),
                join.getHints(),
                join.getLeft(),
                join.getRight(),
                join.getJoinType(),
                join.getCondition(),
                Collections.singletonList(0), // leftAllBinaryInputOrdinals
                Collections.singletonList(0), // rightAllBinaryInputOrdinals
                left2RightLookupChain,
                right2LeftLookupChain,
                deltaJoinAssociation,
                join.getRowType());
    }

    /**
     * Convert cascaded top join to delta join with bushy tree.
     *
     * <pre>{@code
     *             Join                              DeltaJoin
     *      /               \                    /               \
     *   DeltaJoin      DeltaJoin    =>        DeltaJoin      DeltaJoin
     *    /      \       /      \             /      \       /      \
     * #0 A    #1 B   #2 C     #3 D        #0 A    #1 B   #2 C     #3 D
     *
     * }</pre>
     */
    private StreamPhysicalDeltaJoin convertToBushyDeltaJoin(
            StreamPhysicalJoin topJoin,
            StreamPhysicalDeltaJoin leftBottomDeltaJoin,
            StreamPhysicalDeltaJoin rightBottomDeltaJoin) {
        FlinkTypeFactory typeFactory = unwrapTypeFactory(topJoin);

        RexProgram calcOnLeftBottomDeltaJoin =
                getRexProgramBetweenJoinAndDeltaJoin(topJoin.getLeft()).orElse(null);
        RexProgram calcOnRightBottomDeltaJoin =
                getRexProgramBetweenJoinAndDeltaJoin(topJoin.getRight()).orElse(null);

        // Take the following sql as example for bushy tree.
        // Each table has columns: A(a0, a1), B(b0, b1, b2), C(c0, c1), D(d0, d1, d2).
        // SQL: (A join B on a0 = b0) join (C join D on c0 = d0) on a1 = c1 and b2 = d2
        //
        //                            Top
        //                   (a1 = c1 and b2 = d2)
        //          /                                    \
        //       Bottom1                                Bottom2
        //      (a0 = b0)                              (c0 = d0)
        //       /       \                              /     \
        //   A(a0,a1)  B(b0,b1,b2)                C(c0,c1)   D(d0,d1,d2)
        DeltaJoinAssociation joinAssociationOnLeft = leftBottomDeltaJoin.getDeltaJoinAssociation();
        DeltaJoinAssociation joinAssociationOnRight =
                rightBottomDeltaJoin.getDeltaJoinAssociation();

        // 1. get all binary input in left and right bottom delta join
        int leftAllBinaryInputCount = joinAssociationOnLeft.getBinaryInputCount();
        int rightAllBinaryInputCount = joinAssociationOnRight.getBinaryInputCount();
        // get [A, B]
        List<Integer> leftAllBinaryInputOrdinals =
                IntStream.range(0, leftAllBinaryInputCount).boxed().collect(Collectors.toList());
        // get [C, D]
        List<Integer> rightAllBinaryInputOrdinals =
                IntStream.range(0, rightAllBinaryInputCount).boxed().collect(Collectors.toList());

        JoinInfo topJoinInfo =
                JoinInfo.of(topJoin.getLeft(), topJoin.getRight(), topJoin.getCondition());
        DeltaJoinAssociation topDeltaJoinAssociation =
                joinAssociationOnLeft.merge(
                        joinAssociationOnRight,
                        JoinTypeUtil.getFlinkJoinType(topJoin.getJoinType()),
                        topJoin.getCondition(),
                        topJoinInfo.leftKeys.toIntArray(),
                        topJoinInfo.rightKeys.toIntArray(),
                        calcOnLeftBottomDeltaJoin,
                        calcOnRightBottomDeltaJoin);

        JoinSpec topJoinSpec = JoinUtil.createJoinSpec(topJoin);

        DeltaJoinLookupChain left2RightLookupChain =
                buildLookupChainAndUpdateTopJoinAssociation(
                        topJoinSpec,
                        topJoinInfo.pairs(),
                        joinAssociationOnLeft,
                        joinAssociationOnRight,
                        topJoin.getLeft(),
                        topJoin.getRight(),
                        true,
                        topDeltaJoinAssociation,
                        calcOnRightBottomDeltaJoin,
                        typeFactory);

        DeltaJoinLookupChain right2LeftLookupChain =
                buildLookupChainAndUpdateTopJoinAssociation(
                        topJoinSpec,
                        topJoinInfo.pairs(),
                        joinAssociationOnLeft,
                        joinAssociationOnRight,
                        topJoin.getLeft(),
                        topJoin.getRight(),
                        false,
                        topDeltaJoinAssociation,
                        calcOnLeftBottomDeltaJoin,
                        typeFactory);

        return new StreamPhysicalDeltaJoin(
                topJoin.getCluster(),
                topJoin.getTraitSet(),
                topJoin.getHints(),
                topJoin.getLeft(),
                topJoin.getRight(),
                topJoin.getJoinType(),
                topJoin.getCondition(),
                leftAllBinaryInputOrdinals,
                rightAllBinaryInputOrdinals,
                left2RightLookupChain,
                right2LeftLookupChain,
                topDeltaJoinAssociation,
                topJoin.getRowType());
    }

    private StreamPhysicalDeltaJoin convertToLeftRightHandDeltaJoin(
            StreamPhysicalJoin topJoin, StreamPhysicalDeltaJoin bottomDeltaJoin, boolean isLHS) {
        FlinkTypeFactory typeFactory = unwrapTypeFactory(topJoin);

        // Take the following sql as example for LHS and RHS tree
        // Each table has columns: A(a0, a1), B(b0, b1, b2), C(c0, c1), D(d0, d1, d2).
        // LHS SQL: (A join B on a0 = b0) join C on a1 = c1 and b2 = c2
        //
        //                      Top
        //              (a1 = c1 and b2 = c2)
        //            /                        \
        //          Bottom                      C
        //         (a0 = b0)               C(c0,c1,c2)
        //        /        \
        //       A          B
        //    A(a0,a1)  B(b0,b1,b2)
        //
        // RHS SQL: C join (A join B on a0 = b0) on c1 = a1 and c2 = b2
        //
        //                      Top
        //       (C.c1 = Bottom.a1 and C.c2 = Bottom.b2)
        //          /                        \
        //          C                       Bottom
        //      C(c0,c1,c2)                (a0 = b0)
        //                                  /     \
        //                                 A       B
        //                             A(a0,a1)  B(b0,b1,b2)
        RexProgram calcOnBottomDeltaJoin =
                isLHS
                        ? getRexProgramBetweenJoinAndDeltaJoin(topJoin.getLeft()).orElse(null)
                        : getRexProgramBetweenJoinAndDeltaJoin(topJoin.getRight()).orElse(null);
        RelNode inputC = isLHS ? topJoin.getRight() : topJoin.getLeft();
        StreamPhysicalTableSourceScan tableScanC = getTableScan(inputC);
        RexProgram calcOnC = getRexProgramBetweenJoinAndTableScan(inputC).orElse(null);

        JoinInfo topJoinInfo = topJoin.analyzeCondition();
        DeltaJoinAssociation joinAssociationOnC = DeltaJoinAssociation.create(tableScanC, calcOnC);
        DeltaJoinAssociation joinAssociationOnLeft =
                isLHS ? bottomDeltaJoin.getDeltaJoinAssociation() : joinAssociationOnC;
        DeltaJoinAssociation joinAssociationOnRight =
                isLHS ? joinAssociationOnC : bottomDeltaJoin.getDeltaJoinAssociation();

        DeltaJoinAssociation topJoinAssociation =
                joinAssociationOnLeft.merge(
                        joinAssociationOnRight,
                        JoinTypeUtil.getFlinkJoinType(topJoin.getJoinType()),
                        topJoin.getCondition(),
                        topJoinInfo.leftKeys.toIntArray(),
                        topJoinInfo.rightKeys.toIntArray(),
                        isLHS ? calcOnBottomDeltaJoin : null,
                        isLHS ? null : calcOnBottomDeltaJoin);

        RexProgram calcOnLeftSide = isLHS ? calcOnBottomDeltaJoin : calcOnC;
        RexProgram calcOnRightSide = isLHS ? calcOnC : calcOnBottomDeltaJoin;
        JoinSpec topJoinSpec = JoinUtil.createJoinSpec(topJoin);

        DeltaJoinLookupChain left2RightLookupChain =
                buildLookupChainAndUpdateTopJoinAssociation(
                        topJoinSpec,
                        topJoinInfo.pairs(),
                        joinAssociationOnLeft,
                        joinAssociationOnRight,
                        topJoin.getLeft(),
                        topJoin.getRight(),
                        true,
                        topJoinAssociation,
                        calcOnRightSide,
                        typeFactory);

        DeltaJoinLookupChain right2LeftLookupChain =
                buildLookupChainAndUpdateTopJoinAssociation(
                        topJoinSpec,
                        topJoinInfo.pairs(),
                        joinAssociationOnLeft,
                        joinAssociationOnRight,
                        topJoin.getLeft(),
                        topJoin.getRight(),
                        false,
                        topJoinAssociation,
                        calcOnLeftSide,
                        typeFactory);

        List<Integer> allBinaryInputOrdinalsOnC = Collections.singletonList(0);
        List<Integer> allBinaryInputOrdinalsOnBottomDeltaJoin =
                IntStream.range(0, bottomDeltaJoin.getDeltaJoinAssociation().getBinaryInputCount())
                        .boxed()
                        .collect(Collectors.toList());

        // optimize top join to delta join
        return new StreamPhysicalDeltaJoin(
                topJoin.getCluster(),
                topJoin.getTraitSet(),
                topJoin.getHints(),
                topJoin.getLeft(),
                topJoin.getRight(),
                topJoin.getJoinType(),
                topJoin.getCondition(),
                isLHS ? allBinaryInputOrdinalsOnBottomDeltaJoin : allBinaryInputOrdinalsOnC,
                isLHS ? allBinaryInputOrdinalsOnC : allBinaryInputOrdinalsOnBottomDeltaJoin,
                left2RightLookupChain,
                right2LeftLookupChain,
                topJoinAssociation,
                topJoin.getRowType());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        DeltaJoinRewriteRule.Config DEFAULT =
                ImmutableDeltaJoinRewriteRule.Config.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalJoin.class).anyInputs())
                        .withDescription("DeltaJoinRewriteRule");

        @Override
        default DeltaJoinRewriteRule toRule() {
            return new DeltaJoinRewriteRule(this);
        }
    }
}
