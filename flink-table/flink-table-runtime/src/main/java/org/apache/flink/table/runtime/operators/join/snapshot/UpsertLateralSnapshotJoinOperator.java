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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * {@link LateralSnapshotJoinOperator} that keys the committed build side by its upsert key.
 *
 * <p>For each keyed-state key (the join key) the latest full build row is held per upsert key in a
 * {@link MapState} from the upsert-key projection to the row. Insertions and updates ({@code
 * +I}/{@code +U}) overwrite the row for the key; deletes and update-befores ({@code -D}/{@code
 * -U}), including partial deletes that carry only the upsert-key columns, remove it by key. Unlike
 * {@link MultiSetLateralSnapshotJoinOperator} this needs no full before-image, so an upsert
 * changelog can feed the build side directly without a ChangelogNormalize.
 */
@Internal
public class UpsertLateralSnapshotJoinOperator extends LateralSnapshotJoinOperator {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting static final String BUILD_TABLE_STATE_NAME = "build-table-upsert";

    /** Field indices of the build-side upsert key within the build row. */
    private final int[] buildUpsertKeyIndices;

    /** Committed build rows of the current key, keyed by their upsert-key projection. */
    private transient MapState<RowData, RowData> buildTableByKey;

    /** Field getters to project the upsert key out of a build row. */
    private transient RowData.FieldGetter[] upsertKeyGetters;

    /** Serializes the projected upsert key into a {@link RowData}. */
    private transient RowDataSerializer upsertKeySerializer;

    public UpsertLateralSnapshotJoinOperator(
            boolean isLeftOuterJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            int buildRowtimeIndex,
            GeneratedJoinCondition generatedJoinCondition,
            boolean[] filterNullKeys,
            long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs,
            int[] buildUpsertKeyIndices) {
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
        Preconditions.checkNotNull(buildUpsertKeyIndices);
        Preconditions.checkArgument(
                buildUpsertKeyIndices.length > 0,
                "buildUpsertKeyIndices must not be empty for the upsert build-side layout");
        final int fieldCount = rightType.toRowType().getFieldCount();
        for (int idx : buildUpsertKeyIndices) {
            Preconditions.checkArgument(
                    idx >= 0 && idx < fieldCount,
                    "buildUpsertKeyIndices out of bounds for build row: %s",
                    idx);
        }
        this.buildUpsertKeyIndices = buildUpsertKeyIndices;
    }

    @Override
    protected void openBuildState() {
        final RowType buildRow = getRightType().toRowType();
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
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        BUILD_TABLE_STATE_NAME, upsertKeyType, getRightType()));
    }

    @Override
    protected void applyBuildChange(RowData change) throws Exception {
        final RowKind changeType = change.getRowKind();
        final RowData upsertKey = projectUpsertKey(change);
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            change.setRowKind(RowKind.INSERT);
            buildTableByKey.put(upsertKey, change);
        } else {
            // DELETE / UPDATE_BEFORE, including partial deletes that carry only the upsert key.
            if (!buildTableByKey.contains(upsertKey)) {
                incUnmatchedBuildRetractions();
                return;
            }
            buildTableByKey.remove(upsertKey);
        }
    }

    @Override
    protected long joinBuildRows(RowData probe) throws Exception {
        long fanOut = 0;
        for (RowData buildRow : buildTableByKey.values()) {
            if (joinMatches(probe, buildRow)) {
                emitJoined(probe, buildRow);
                fanOut++;
            }
        }
        return fanOut;
    }

    @Override
    protected void clearBuildTable() {
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
