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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLateralSnapshotJoin;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Stream physical node for the LATERAL SNAPSHOT processing-time temporal table join. The build side
 * is loaded into operator state during a LOAD phase; once the build-side watermark crosses the
 * configured flip point, the operator switches to a JOIN phase and processes probe-side records
 * against the loaded build state.
 */
public class StreamPhysicalLateralSnapshotJoin extends CommonPhysicalJoin
        implements StreamPhysicalRel {

    private final String loadCompletedCondition;
    private final Long loadCompletedTime;
    private final @Nullable Long loadCompletedIdleTimeoutMs;
    private final @Nullable Long stateTtlMs;

    public StreamPhysicalLateralSnapshotJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode leftRel,
            RelNode rightRel,
            RexNode condition,
            JoinRelType joinType,
            String loadCompletedCondition,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        super(cluster, traitSet, leftRel, rightRel, condition, joinType, Collections.emptyList());
        Preconditions.checkNotNull(loadCompletedTime, "loadCompletedTime must not be null.");
        this.loadCompletedCondition = loadCompletedCondition;
        this.loadCompletedTime = loadCompletedTime;
        this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
        this.stateTtlMs = stateTtlMs;
    }

    @Override
    public boolean requireWatermark() {
        return true;
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
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new StreamPhysicalLateralSnapshotJoin(
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
    public RelWriter explainTerms(RelWriter pw) {
        final RelWriter terms = super.explainTerms(pw);
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
    public ExecNode<?> translateToExecNode() {
        // The build (right) side carries a watermark, so it must expose a row-time attribute whose
        // field index drives the event-time-ordered application of buffered build-side changes.
        final List<RelDataTypeField> rightFields = getRight().getRowType().getFieldList();
        int rightTimeAttributeIndex = -1;
        for (int i = 0; i < rightFields.size(); i++) {
            if (FlinkTypeFactory.isRowtimeIndicatorType(rightFields.get(i).getType())) {
                rightTimeAttributeIndex = i;
                break;
            }
        }
        if (rightTimeAttributeIndex < 0) {
            throw new TableException(
                    "The build (right) side of a LATERAL SNAPSHOT join must have a row-time "
                            + "attribute. This is a bug, please file an issue.");
        }

        return new StreamExecLateralSnapshotJoin(
                unwrapTableConfig(this),
                joinSpec(),
                rightTimeAttributeIndex,
                loadCompletedCondition,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }
}
