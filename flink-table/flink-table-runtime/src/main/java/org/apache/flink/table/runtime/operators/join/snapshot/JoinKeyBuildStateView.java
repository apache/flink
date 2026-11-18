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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/**
 * {@link BuildStateView} for a build side whose upsert key is identical to the join key. The build
 * table of a join key holds at most one row, so it is kept in a {@link ValueState}.
 *
 * <p>Insertions and updates ({@code +I}/{@code +U}) overwrite the row, deletes ({@code -D}),
 * including partial deletes that carry only the key columns, clear it. Update-befores ({@code -U})
 * are ignored because the following {@code +U} overwrites the row.
 */
@Internal
public class JoinKeyBuildStateView implements BuildStateView {

    @VisibleForTesting static final String BUILD_ROW_STATE_NAME = "build-row";

    /** The committed build row of the current key. */
    private final ValueState<RowData> buildRowState;

    public JoinKeyBuildStateView(RuntimeContext ctx, InternalTypeInfo<RowData> buildType) {
        buildRowState = ctx.getState(new ValueStateDescriptor<>(BUILD_ROW_STATE_NAME, buildType));
    }

    @Override
    public boolean apply(RowData change) throws Exception {
        final RowKind changeType = change.getRowKind();
        if (changeType == RowKind.UPDATE_BEFORE) {
            return true;
        }
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            change.setRowKind(RowKind.INSERT);
            buildRowState.update(change);
            return true;
        }
        // DELETE, including partial deletes that carry only the key columns.
        if (buildRowState.value() == null) {
            return false;
        }
        buildRowState.clear();
        return true;
    }

    @Override
    public boolean requiresRowtimeOrdering() {
        return false;
    }

    @Override
    public void forEachRow(RowVisitor visitor) throws Exception {
        final RowData buildRow = buildRowState.value();
        if (buildRow != null) {
            visitor.visit(buildRow, 1L);
        }
    }

    @Override
    public void clear() {
        buildRowState.clear();
    }

    @VisibleForTesting
    ValueState<RowData> getBuildRowState() {
        return buildRowState;
    }
}
