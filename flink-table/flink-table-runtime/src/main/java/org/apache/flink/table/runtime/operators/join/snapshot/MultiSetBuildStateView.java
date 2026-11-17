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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.Map;

/**
 * {@link BuildStateView} that keeps the committed build side as a multi-set (row → reference count)
 * in {@link MapState}. Retractions are matched by full-row equality, so an upstream changelog must
 * carry complete before-images. This is the layout used when the build side has no usable
 * primary/upsert key.
 */
class MultiSetBuildStateView implements BuildStateView {

    @VisibleForTesting static final String BUILD_TABLE_STATE_NAME = "build-table";

    private final InternalTypeInfo<RowData> buildType;

    /**
     * Build-side table as multi-set (row → reference count). Holds the committed snapshot, i.e. all
     * build changes up to the last applied build watermark; probes join against this.
     */
    private MapState<RowData, Long> buildTableState;

    MultiSetBuildStateView(InternalTypeInfo<RowData> buildType) {
        this.buildType = buildType;
    }

    @Override
    public void open(RuntimeContext ctx) {
        buildTableState =
                ctx.getMapState(
                        new MapStateDescriptor<>(BUILD_TABLE_STATE_NAME, buildType, Types.LONG));
    }

    @Override
    public boolean apply(RowData change) throws Exception {
        final RowKind changeType = change.getRowKind();
        change.setRowKind(RowKind.INSERT);
        final Long currentCnt = buildTableState.get(change);
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            buildTableState.put(change, currentCnt == null ? 1L : currentCnt + 1L);
            return true;
        }
        if (currentCnt == null || currentCnt <= 0L) {
            return false;
        }
        if (currentCnt == 1L) {
            buildTableState.remove(change);
        } else {
            buildTableState.put(change, currentCnt - 1L);
        }
        return true;
    }

    @Override
    public void forEachRow(RowVisitor visitor) throws Exception {
        for (Map.Entry<RowData, Long> entry : buildTableState.entries()) {
            visitor.visit(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        buildTableState.clear();
    }

    @VisibleForTesting
    MapState<RowData, Long> getBuildTableState() {
        return buildTableState;
    }
}
