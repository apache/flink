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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;

/**
 * A sink materializer that buffers records and compacts them on watermark progression.
 *
 * <p>This operator implements the watermark-based compaction algorithm from FLIP-558 for handling
 * changelog disorder when the upsert key differs from the sink's primary key.
 */
public class WatermarkCompactingSinkMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(WatermarkCompactingSinkMaterializer.class);

    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state TTL. This will result in incorrect result. "
                    + "You can increase the state TTL to avoid this.";
    private static final Set<String> ORDERED_STATE_BACKENDS = Set.of("rocksdb", "forst");

    private final InsertConflictStrategy conflictStrategy;
    private final TypeSerializer<RowData> serializer;
    private final GeneratedRecordEqualiser generatedRecordEqualiser;
    private final GeneratedRecordEqualiser generatedUpsertKeyEqualiser;
    private final int[] inputUpsertKey;
    private final boolean hasUpsertKey;

    private transient MapStateDescriptor<Long, List<RowData>> bufferDescriptor;
    private transient MapState<Long, List<RowData>> buffer;
    private transient ValueState<RowData> currentValue;
    private transient RecordEqualiser equaliser;
    private transient RecordEqualiser upsertKeyEqualiser;
    private transient TimestampedCollector<RowData> collector;
    private transient boolean isOrderedStateBackend;

    // Reused ProjectedRowData for comparing upsertKey if hasUpsertKey.
    private transient ProjectedRowData upsertKeyProjectedRow1;
    private transient ProjectedRowData upsertKeyProjectedRow2;

    public WatermarkCompactingSinkMaterializer(
            InsertConflictStrategy conflictStrategy,
            TypeSerializer<RowData> serializer,
            GeneratedRecordEqualiser generatedRecordEqualiser,
            @Nullable GeneratedRecordEqualiser generatedUpsertKeyEqualiser,
            @Nullable int[] inputUpsertKey) {
        validateConflictStrategy(conflictStrategy);
        this.conflictStrategy = conflictStrategy;
        this.serializer = serializer;
        this.generatedRecordEqualiser = generatedRecordEqualiser;
        this.generatedUpsertKeyEqualiser = generatedUpsertKeyEqualiser;
        this.inputUpsertKey = inputUpsertKey;
        this.hasUpsertKey = inputUpsertKey != null && inputUpsertKey.length > 0;
    }

    private static void validateConflictStrategy(InsertConflictStrategy strategy) {
        Preconditions.checkArgument(
                strategy.getBehavior() == ConflictBehavior.ERROR
                        || strategy.getBehavior() == ConflictBehavior.NOTHING,
                "Only ERROR and NOTHING strategies are supported, got: %s",
                strategy);
    }

    @Override
    public void open() throws Exception {
        super.open();
        initializeEqualisers();
        detectOrderedStateBackend();
        initializeState();
        this.collector = new TimestampedCollector<>(output);
    }

    private void initializeEqualisers() {
        if (hasUpsertKey) {
            this.upsertKeyEqualiser =
                    generatedUpsertKeyEqualiser.newInstance(
                            getRuntimeContext().getUserCodeClassLoader());
            upsertKeyProjectedRow1 = ProjectedRowData.from(inputUpsertKey);
            upsertKeyProjectedRow2 = ProjectedRowData.from(inputUpsertKey);
        }
        this.equaliser =
                generatedRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
    }

    private void detectOrderedStateBackend() {
        KeyedStateBackend<?> keyedStateBackend = getKeyedStateBackend();
        String backendType =
                keyedStateBackend != null ? keyedStateBackend.getBackendTypeIdentifier() : "";
        this.isOrderedStateBackend = ORDERED_STATE_BACKENDS.contains(backendType);

        if (isOrderedStateBackend) {
            LOG.info("Using ordered state backend optimization for {} backend", backendType);
        }
    }

    private void initializeState() {
        this.bufferDescriptor =
                new MapStateDescriptor<>(
                        "watermark-buffer",
                        SortedLongSerializer.INSTANCE,
                        new ListSerializer<>(serializer));
        this.buffer = getRuntimeContext().getMapState(bufferDescriptor);

        ValueStateDescriptor<RowData> currentValueDescriptor =
                new ValueStateDescriptor<>("current-value", serializer);
        this.currentValue = getRuntimeContext().getState(currentValueDescriptor);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        long assignedTimestamp = element.getTimestamp();
        bufferRecord(assignedTimestamp, row);
    }

    private void bufferRecord(long timestamp, RowData row) throws Exception {
        List<RowData> records = buffer.get(timestamp);
        if (records == null) {
            records = new ArrayList<>();
        }
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                // Try to cancel out a pending retraction; if none, just append
                if (!tryCancelRetraction(records, row)) {
                    records.add(row);
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                // Try to cancel out an existing addition; if none, keep for cross-bucket
                if (!tryCancelAddition(records, row)) {
                    records.add(row);
                }
                break;
        }
        buffer.put(timestamp, records);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        final long watermarkTimestamp = mark.getTimestamp();

        // Iterate over all keys and compact their buffered records
        this.<RowData>getKeyedStateBackend()
                .applyToAllKeys(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        bufferDescriptor,
                        (key, state) -> compactAndEmit(watermarkTimestamp));

        super.processWatermark(mark);
    }

    private void compactAndEmit(long newWatermark) throws Exception {
        RowData previousValue = currentValue.value();
        List<RowData> pendingRecords = collectPendingRecords(previousValue, newWatermark);

        if (pendingRecords.size() > 1) {
            if (conflictStrategy.getBehavior() == ConflictBehavior.ERROR) {
                throw new TableRuntimeException(
                        "Primary key constraint violation: multiple distinct records with "
                                + "the same primary key detected. Use ON CONFLICT DO NOTHING "
                                + "to keep the first record.");
            } else if (previousValue == null) {
                final RowData newValue = pendingRecords.get(0);
                emit(newValue, INSERT);
                currentValue.update(newValue);
            }
        } else if (pendingRecords.isEmpty()) {
            if (previousValue != null) {
                emit(previousValue, DELETE);
                currentValue.clear();
            }
        } else {
            final RowData newValue = pendingRecords.get(0);
            if (previousValue == null) {
                emit(newValue, INSERT);
                currentValue.update(newValue);
            } else if (!recordEquals(previousValue, newValue)) {
                emit(newValue, UPDATE_AFTER);
                currentValue.update(newValue);
            }
        }
    }

    private List<RowData> collectPendingRecords(RowData previousValue, long newWatermark)
            throws Exception {
        List<RowData> records = new ArrayList<>();
        if (previousValue != null) {
            records.add(previousValue);
        }
        Iterator<Map.Entry<Long, List<RowData>>> iterator = buffer.entries().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Long, List<RowData>> entry = iterator.next();
            if (entry.getKey() <= newWatermark) {
                for (RowData pendingRecord : entry.getValue()) {
                    switch (pendingRecord.getRowKind()) {
                        case INSERT:
                        case UPDATE_AFTER:
                            addRow(records, pendingRecord);
                            break;

                        case UPDATE_BEFORE:
                        case DELETE:
                            retractRow(records, pendingRecord);
                            break;
                    }
                }
                iterator.remove();
            } else if (isOrderedStateBackend) {
                break;
            }
        }
        return records;
    }

    private void addRow(List<RowData> values, RowData add) {
        if (hasUpsertKey) {
            int index = findFirst(values, add);
            if (index == -1) {
                values.add(add);
            } else {
                values.set(index, add);
            }
        } else {
            values.add(add);
        }
    }

    private void retractRow(List<RowData> values, RowData retract) {
        final int index = findFirst(values, retract);
        if (index == -1) {
            LOG.info(STATE_CLEARED_WARN_MSG);
        } else {
            // Remove first found row
            values.remove(index);
        }
    }

    /**
     * Attempts to cancel out a retraction by finding a matching retractive record
     * (DELETE/UPDATE_BEFORE) with identical content.
     *
     * @return true if a matching retraction was found and removed, false otherwise
     */
    private boolean tryCancelRetraction(List<RowData> values, RowData addition) {
        final Iterator<RowData> iterator = values.iterator();
        while (iterator.hasNext()) {
            RowData candidate = iterator.next();
            RowKind kind = candidate.getRowKind();
            if ((kind == DELETE || kind == RowKind.UPDATE_BEFORE)
                    && recordEquals(addition, candidate)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to cancel out an addition by finding a matching additive record
     * (INSERT/UPDATE_AFTER) with identical content.
     *
     * @return true if a matching addition was found and removed, false otherwise
     */
    private boolean tryCancelAddition(List<RowData> values, RowData retraction) {
        final Iterator<RowData> iterator = values.iterator();
        while (iterator.hasNext()) {
            RowData candidate = iterator.next();
            RowKind kind = candidate.getRowKind();
            if ((kind == INSERT || kind == UPDATE_AFTER) && recordEquals(retraction, candidate)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private int findFirst(List<RowData> values, RowData target) {
        final Iterator<RowData> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            if (equalsIgnoreRowKind(target, iterator.next())) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private boolean equalsIgnoreRowKind(RowData newRow, RowData oldRow) {
        newRow.setRowKind(oldRow.getRowKind());
        if (hasUpsertKey) {
            return this.upsertKeyEqualiser.equals(
                    upsertKeyProjectedRow1.replaceRow(newRow),
                    upsertKeyProjectedRow2.replaceRow(oldRow));
        }
        return equaliser.equals(newRow, oldRow);
    }

    private void emit(RowData row, RowKind kind) {
        RowKind originalKind = row.getRowKind();
        row.setRowKind(kind);
        collector.collect(row);
        row.setRowKind(originalKind);
    }

    private boolean recordEquals(RowData row1, RowData row2) {
        RowKind kind1 = row1.getRowKind();
        RowKind kind2 = row2.getRowKind();
        row1.setRowKind(RowKind.INSERT);
        row2.setRowKind(RowKind.INSERT);
        boolean result = equaliser.equals(row1, row2);
        row1.setRowKind(kind1);
        row2.setRowKind(kind2);
        return result;
    }

    /** Factory method to create a new instance. */
    public static WatermarkCompactingSinkMaterializer create(
            InsertConflictStrategy conflictStrategy,
            RowType physicalRowType,
            GeneratedRecordEqualiser rowEqualiser,
            @Nullable GeneratedRecordEqualiser upsertKeyEqualiser,
            @Nullable int[] inputUpsertKey) {
        return new WatermarkCompactingSinkMaterializer(
                conflictStrategy,
                InternalSerializers.create(physicalRowType),
                rowEqualiser,
                upsertKeyEqualiser,
                inputUpsertKey);
    }
}
