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

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * {@link LateralSnapshotJoinOperator} that keeps the committed build side as a multi-set (row →
 * reference count) in {@link MapState}. Retractions are matched by full-row equality, so an
 * upstream changelog must carry complete before-images. This is the layout used when the build side
 * has no usable primary/upsert key.
 */
@Internal
public class MultiSetLateralSnapshotJoinOperator extends LateralSnapshotJoinOperator {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting static final String BUILD_TABLE_STATE_NAME = "build-table";

    /**
     * Build-side table as multi-set (row → reference count). Holds the committed snapshot, i.e. all
     * build changes up to the last applied build watermark; probes join against this.
     */
    private transient MapState<RowData, Long> buildTableState;

    public MultiSetLateralSnapshotJoinOperator(
            boolean isLeftOuterJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            int buildRowtimeIndex,
            GeneratedJoinCondition generatedJoinCondition,
            boolean[] filterNullKeys,
            long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        super(
                isLeftOuterJoin,
                leftType,
                rightType,
                buildRowtimeIndex,
                generatedJoinCondition,
                filterNullKeys,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs);
    }

    @Override
    protected void openBuildState() {
        buildTableState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        BUILD_TABLE_STATE_NAME, getRightType(), Types.LONG));
    }

    @Override
    protected void applyBuildChange(RowData change) throws Exception {
        final RowKind changeType = change.getRowKind();
        change.setRowKind(RowKind.INSERT);
        final Long currentCnt = buildTableState.get(change);
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            buildTableState.put(change, currentCnt == null ? 1L : currentCnt + 1L);
        } else {
            if (currentCnt == null || currentCnt <= 0L) {
                incUnmatchedBuildRetractions();
                return;
            }
            if (currentCnt == 1L) {
                buildTableState.remove(change);
            } else {
                buildTableState.put(change, currentCnt - 1L);
            }
        }
    }

    @Override
    protected long joinBuildRows(RowData probe) throws Exception {
        long fanOut = 0;
        for (Map.Entry<RowData, Long> entry : buildTableState.entries()) {
            final RowData buildRow = entry.getKey();
            final long count = entry.getValue();
            if (joinMatches(probe, buildRow)) {
                for (long i = 0; i < count; i++) {
                    emitJoined(probe, buildRow);
                    fanOut++;
                }
            }
        }
        return fanOut;
    }

    @Override
    protected void clearBuildTable() {
        buildTableState.clear();
    }

    @VisibleForTesting
    MapState<RowData, Long> getBuildTableState() {
        return buildTableState;
    }
}
