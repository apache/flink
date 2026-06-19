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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sink.SortedLongSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Timer-based sorting buffer for handling ORDER BY in {@link ProcessTableFunction} inputs.
 *
 * <p>An instance buffers rows by timestamp and emits them in sorted order when watermark advances.
 * It implements {@link Triggerable} to handle timer events, registering a single timer per key at
 * {@code lastWatermark + 1}. This reduces the timer overhead from O(K × T) to O(K) where K = keys
 * and T = unique timestamps per key.
 *
 * <p>When a timer fires, all buffered data with timestamp ≤ current watermark is processed in
 * sorted order according to the ORDER BY specification.
 */
@Internal
class InputSortBuffer implements Triggerable<RowData, VoidNamespace> {

    private static final Set<String> ORDERED_STATE_BACKENDS = Set.of("rocksdb", "forst");

    /** Functional interface for consuming sorted rows. */
    interface SortedRowConsumer {
        void accept(RowData row) throws Exception;
    }

    private final int timeColumn;
    private final RecordComparator comparator;
    private final SortedRowConsumer rowConsumer;
    private final boolean isOrderedStateBackend;
    private final MapState<Long, List<RowData>> dataState;

    private InternalTimerService<VoidNamespace> timerService;

    private long triggeringWatermark = Long.MIN_VALUE;

    InputSortBuffer(
            int inputIdx,
            LogicalType inputRowType,
            int timeColumn,
            RecordComparator comparator,
            KeyedStateStore keyedStateStore,
            SortedRowConsumer rowConsumer,
            StateTtlConfig ttlConfig,
            KeyedStateBackend<RowData> keyedStateBackend) {
        this.timeColumn = timeColumn;
        this.comparator = comparator;
        this.rowConsumer = rowConsumer;
        this.isOrderedStateBackend = isOrderedStateBackend(keyedStateBackend);

        final MapStateDescriptor<Long, List<RowData>> mapStateDescriptor =
                new MapStateDescriptor<>(
                        "input-sort-buffer-data-" + inputIdx,
                        SortedLongSerializer.INSTANCE,
                        new ListSerializer<>(InternalSerializers.create(inputRowType)));

        if (ttlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }

        this.dataState = keyedStateStore.getMapState(mapStateDescriptor);
    }

    /** Sets the timer service after construction. Must be called before processing any elements. */
    void setTimerService(InternalTimerService<VoidNamespace> timerService) {
        this.timerService = timerService;
    }

    /** Returns the timer service for this sorting service. */
    InternalTimerService<VoidNamespace> getTimerService() {
        return timerService;
    }

    /** Processes an input element, buffering it for later sorted emission. */
    void processElement(RowData input) throws Exception {
        final long rowTime = input.getLong(timeColumn);

        // Drop late events
        if (rowTime <= triggeringWatermark) {
            return;
        }

        // Add row to the buffer for this timestamp
        List<RowData> rows = dataState.get(rowTime);
        final boolean isNewTimestamp = rows == null;
        if (isNewTimestamp) {
            rows = new ArrayList<>(1);
        }
        rows.add(input);
        dataState.put(rowTime, rows);

        // Register timer at triggeringWatermark + 1 (idempotent - only one timer per key)
        // This allows batch processing of all timestamps when watermark advances
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, triggeringWatermark + 1);
    }

    /** Updates the watermark that triggers the current sorting. */
    public void updateWatermark(long watermark) {
        triggeringWatermark = watermark;
    }

    @Override
    public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        // Timer fired for this key (key context set by Flink timer service)
        // Process ALL buffered data with rowTime <= current watermark
        // Must process timestamps in sorted order to maintain ORDER BY semantics

        final boolean hasRemainingData =
                processSortedTimestamps(
                        dataState,
                        triggeringWatermark,
                        comparator,
                        rowConsumer,
                        isOrderedStateBackend);

        // Re-register timer if any data remains (timestamps > watermark)
        if (hasRemainingData) {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, triggeringWatermark + 1);
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) {
        // Not used - only event-time timers are registered
    }

    /**
     * Detects if the given state backend maintains keys in sorted order.
     *
     * @param backend the keyed state backend
     * @return true if the backend is RocksDB or ForSt, false otherwise
     */
    private static boolean isOrderedStateBackend(KeyedStateBackend<?> backend) {
        return ORDERED_STATE_BACKENDS.contains(backend.getBackendTypeIdentifier());
    }

    /**
     * Processes buffered timestamps in sorted order, ensuring ORDER BY correctness.
     *
     * <p>For ordered state backends (RocksDB/ForSt), entries are already sorted by the backend, so
     * we can directly iterate and process them. For unordered backends, we collect all timestamps,
     * sort them manually, and then process each timestamp.
     *
     * @param dataState the MapState containing buffered rows by timestamp
     * @param watermark the current watermark timestamp
     * @param comparator optional comparator for secondary sorting within a timestamp
     * @param rowConsumer consumer to emit sorted rows
     * @param isOrderedBackend whether the state backend maintains sorted key order
     * @return true if there are remaining timestamps > watermark, false otherwise
     * @throws Exception if processing fails
     */
    private static boolean processSortedTimestamps(
            MapState<Long, List<RowData>> dataState,
            long watermark,
            RecordComparator comparator,
            SortedRowConsumer rowConsumer,
            boolean isOrderedBackend)
            throws Exception {

        boolean hasRemainingData = false;

        if (isOrderedBackend) {
            // Ordered backend (RocksDB/ForSt): entries already sorted, iterate directly
            final Iterator<Map.Entry<Long, List<RowData>>> iterator =
                    dataState.entries().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<Long, List<RowData>> entry = iterator.next();
                final long timestamp = entry.getKey();

                if (timestamp <= watermark) {
                    final List<RowData> rows = entry.getValue();
                    if (comparator != null) {
                        rows.sort(comparator);
                    }
                    for (RowData row : rows) {
                        rowConsumer.accept(row);
                    }
                    iterator.remove();
                } else {
                    // Early exit: all remaining entries have timestamp > watermark
                    hasRemainingData = true;
                    break;
                }
            }
        } else {
            // Unordered backend: collect, sort, then process
            final List<Long> readyTimestamps = new ArrayList<>();

            for (Long ts : dataState.keys()) {
                if (ts <= watermark) {
                    readyTimestamps.add(ts);
                } else {
                    hasRemainingData = true;
                }
            }

            Collections.sort(readyTimestamps);

            for (Long timestamp : readyTimestamps) {
                final List<RowData> rows = dataState.get(timestamp);
                if (rows != null) {
                    if (comparator != null) {
                        rows.sort(comparator);
                    }
                    for (RowData row : rows) {
                        rowConsumer.accept(row);
                    }
                    dataState.remove(timestamp);
                }
            }
        }

        return hasRemainingData;
    }
}
