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

package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;

/**
 * Logical node for the {@code LATERAL SNAPSHOT} processing-time temporal table join.
 *
 * <p>The {@code LATERAL SNAPSHOT} join materializes the build-side row-time attribute, the row-time
 * attribute of the probe-side is forwarded. The arguments of the {@code SNAPSHOT} function are
 * persisted in fields of the logical node.
 */
public class FlinkLogicalLateralSnapshotJoin extends Join implements FlinkLogicalRel {

    private final String loadCompletedCondition;
    private final Long loadCompletedTime;
    private final @Nullable Long loadCompletedIdleTimeoutMs;
    private final @Nullable Long stateTtlMs;

    public FlinkLogicalLateralSnapshotJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            String loadCompletedCondition,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        super(
                cluster,
                traitSet,
                Collections.emptyList(),
                left,
                right,
                condition,
                Collections.emptySet(),
                joinType);
        Preconditions.checkNotNull(loadCompletedTime, "loadCompletedTime must not be null.");
        this.loadCompletedCondition = loadCompletedCondition;
        this.loadCompletedTime = loadCompletedTime;
        this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
        this.stateTtlMs = stateTtlMs;
    }

    public String getLoadCompletedCondition() {
        return loadCompletedCondition;
    }

    public Long getLoadCompletedTime() {
        return loadCompletedTime;
    }

    public @Nullable Long getLoadCompletedIdleTimeoutMs() {
        return loadCompletedIdleTimeoutMs;
    }

    public @Nullable Long getStateTtlMs() {
        return stateTtlMs;
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new FlinkLogicalLateralSnapshotJoin(
                getCluster(),
                traitSet,
                left,
                right,
                conditionExpr,
                joinType,
                loadCompletedCondition,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs);
    }

    @Override
    protected RelDataType deriveRowType() {
        return LateralSnapshotJoinUtil.deriveRowType(
                getCluster().getTypeFactory(),
                getLeft().getRowType(),
                getRight().getRowType(),
                getJoinType(),
                getSystemFieldList());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter terms = super.explainTerms(pw);
        terms.item("loadCompletedCondition", loadCompletedCondition);
        terms.item("loadCompletedTime", loadCompletedTime);
        if (loadCompletedIdleTimeoutMs != null) {
            terms.item("loadCompletedIdleTimeout", loadCompletedIdleTimeoutMs + " ms");
        }
        if (stateTtlMs != null) {
            terms.item("stateTtl", stateTtlMs + " ms");
        }
        return terms;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRowCnt = mq.getRowCount(getLeft());
        double leftRowSize = mq.getAverageRowSize(getLeft());
        double rightRowCnt = mq.getRowCount(getRight());
        double cpuCost = leftRowCnt + rightRowCnt;
        double ioCost = leftRowCnt * leftRowSize;
        return planner.getCostFactory().makeCost(leftRowCnt, cpuCost, ioCost);
    }

    public static FlinkLogicalLateralSnapshotJoin create(
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            String loadCompletedCondition,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        RelOptCluster cluster = left.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL()).simplify();
        return new FlinkLogicalLateralSnapshotJoin(
                cluster,
                traitSet,
                left,
                right,
                condition,
                joinType,
                loadCompletedCondition,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs);
    }
}
