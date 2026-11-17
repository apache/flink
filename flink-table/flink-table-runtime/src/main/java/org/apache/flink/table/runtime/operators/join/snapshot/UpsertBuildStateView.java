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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/**
 * {@link BuildStateView} that keys the committed build side by its upsert key.
 *
 * <p>For each keyed-state key (the join key) the latest full build row is held per upsert key in a
 * {@link MapState} from the upsert-key projection to the row. Insertions and updates ({@code
 * +I}/{@code +U}) overwrite the row for the key; deletes ({@code -D}), including partial deletes
 * that carry only the upsert-key columns, remove it by key. Update-befores ({@code -U}) are ignored
 * because the following {@code +U} overwrites the row. Unlike {@link MultiSetBuildStateView} this
 * needs no full before-image, so an upsert changelog can feed the build side directly without a
 * ChangelogNormalize. Changes of a key arrive in order, so they are applied in arrival order.
 */
@Internal
public class UpsertBuildStateView implements BuildStateView {

    @VisibleForTesting static final String BUILD_TABLE_STATE_NAME = "build-table-upsert";

    /** Committed build rows of the current key, keyed by their upsert-key projection. */
    private final MapState<RowData, RowData> buildTableByKey;

    /** Field getters to project the upsert key out of a build row. */
    private final RowData.FieldGetter[] upsertKeyGetters;

    /** Serializes the projected upsert key into a {@link RowData}. */
    private final RowDataSerializer upsertKeySerializer;

    public UpsertBuildStateView(
            RuntimeContext ctx, InternalTypeInfo<RowData> buildType, int[] buildUpsertKeyIndices) {
        checkUpsertKeyIndices(buildType, buildUpsertKeyIndices);
        final RowType buildRow = buildType.toRowType();
        final LogicalType[] keyTypes = new LogicalType[buildUpsertKeyIndices.length];
        upsertKeyGetters = new RowData.FieldGetter[buildUpsertKeyIndices.length];
        for (int i = 0; i < buildUpsertKeyIndices.length; i++) {
            final int idx = buildUpsertKeyIndices[i];
            final LogicalType fieldType = buildRow.getTypeAt(idx);
            keyTypes[i] = fieldType;
            upsertKeyGetters[i] = RowData.createFieldGetter(fieldType, idx);
        }
        final InternalTypeInfo<RowData> upsertKeyType = InternalTypeInfo.ofFields(keyTypes);
        upsertKeySerializer = upsertKeyType.toRowSerializer();
        buildTableByKey =
                ctx.getMapState(
                        new MapStateDescriptor<>(BUILD_TABLE_STATE_NAME, upsertKeyType, buildType));
    }

    private static void checkUpsertKeyIndices(InternalTypeInfo<RowData> buildType, int[] indices) {
        Preconditions.checkArgument(
                indices.length > 0,
                "buildUpsertKeyIndices must not be empty for the upsert build-side layout");
        final int fieldCount = buildType.toRowType().getFieldCount();
        for (int idx : indices) {
            Preconditions.checkArgument(
                    idx >= 0 && idx < fieldCount,
                    "buildUpsertKeyIndices out of bounds for build row: %s",
                    idx);
        }
    }

    @Override
    public boolean apply(RowData change) throws Exception {
        final RowKind changeType = change.getRowKind();
        if (changeType == RowKind.UPDATE_BEFORE) {
            // The upsert key contains the join key, so the -U is always followed by its +U for the
            // same key, which overwrites the row.
            return true;
        }
        final RowData upsertKey = projectUpsertKey(change);
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

    /**
     * Projects the upsert key out of a (possibly partial) build row into a fresh {@code
     * BinaryRowData}. A binary key keeps {@link MapState} lookups consistent across state backends
     * and after restore, where heap keys deserialize to {@code BinaryRowData} and would not compare
     * equal to a {@code GenericRowData} probe key.
     */
    private RowData projectUpsertKey(RowData row) {
        final GenericRowData key = new GenericRowData(upsertKeyGetters.length);
        for (int i = 0; i < upsertKeyGetters.length; i++) {
            key.setField(i, upsertKeyGetters[i].getFieldOrNull(row));
        }
        return upsertKeySerializer.toBinaryRow(key).copy();
    }

    @VisibleForTesting
    MapState<RowData, RowData> getBuildTableByKey() {
        return buildTableByKey;
    }
}
