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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/**
 * {@link BuildStateView} that keys the committed build side by its upsert key.
 *
 * <p>For each keyed-state key (the join key) the latest full build row is held per upsert key in a
 * {@link MapState} from the upsert-key projection to the row. The projection may omit the join-key
 * columns, which are constant within a keyed-state key. Insertions and updates ({@code +I}/{@code
 * +U}) overwrite the row for the key; deletes ({@code -D}), including partial deletes that carry
 * only the upsert-key columns, remove it by key. Update-befores ({@code -U}) are ignored because
 * the following {@code +U} overwrites the row. Unlike {@link MultiSetBuildStateView} this needs no
 * full before-image, so an upsert changelog can feed the build side directly without a
 * ChangelogNormalize. Changes of a key arrive in order, so they are applied in arrival order.
 *
 * <p>The upsert key is projected by a (generated) {@link RowDataKeySelector}, which yields a binary
 * key that compares equal across state backends and after restore.
 */
@Internal
public class UpsertBuildStateView implements BuildStateView {

    @VisibleForTesting static final String BUILD_TABLE_STATE_NAME = "build-table-upsert";

    /**
     * Projects the upsert-key columns out of a (possibly partial) build row. Join-key columns may
     * be omitted.
     */
    private final RowDataKeySelector upsertKeySelector;

    /** Committed build rows of the current key, keyed by their upsert-key projection. */
    private final MapState<RowData, RowData> buildTableByKey;

    public UpsertBuildStateView(
            RuntimeContext ctx,
            InternalTypeInfo<RowData> buildType,
            RowDataKeySelector upsertKeySelector) {
        this.upsertKeySelector = upsertKeySelector;
        buildTableByKey =
                ctx.getMapState(
                        new MapStateDescriptor<>(
                                BUILD_TABLE_STATE_NAME,
                                upsertKeySelector.getProducedType(),
                                buildType));
    }

    @Override
    public boolean apply(RowData change) throws Exception {
        final RowKind changeType = change.getRowKind();
        if (changeType == RowKind.UPDATE_BEFORE) {
            // The upsert key contains the join key, so the -U is always followed by its +U for the
            // same key, which overwrites the row.
            return true;
        }
        final RowData upsertKey = upsertKeySelector.getKey(change);
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            change.setRowKind(RowKind.INSERT);
            buildTableByKey.put(upsertKey, change);
            return true;
        }
        // DELETE, including partial deletes that carry only the upsert key.
        if (!buildTableByKey.contains(upsertKey)) {
            return false;
        }
        buildTableByKey.remove(upsertKey);
        return true;
    }

    @Override
    public boolean requiresRowtimeOrdering() {
        return false;
    }

    @Override
    public void forEachRow(RowVisitor visitor) throws Exception {
        for (RowData buildRow : buildTableByKey.values()) {
            visitor.visit(buildRow, 1L);
        }
    }

    @Override
    public void clear() {
        buildTableByKey.clear();
    }

    @VisibleForTesting
    MapState<RowData, RowData> getBuildTableByKey() {
        return buildTableByKey;
    }
}
