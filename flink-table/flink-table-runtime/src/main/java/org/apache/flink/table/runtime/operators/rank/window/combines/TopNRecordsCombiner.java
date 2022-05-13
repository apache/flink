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

package org.apache.flink.table.runtime.operators.rank.window.combines;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.TopNBuffer;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.StateKeyContext;
import org.apache.flink.table.runtime.operators.window.state.WindowMapState;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.table.runtime.util.WindowKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;

/**
 * An implementation of {@link RecordsCombiner} that save topN records of incremental input records
 * into the window state.
 */
public final class TopNRecordsCombiner implements RecordsCombiner {

    /** The service to register event-time or processing-time timers. */
    private final WindowTimerService<Long> timerService;

    /** Context to switch current key for states. */
    private final StateKeyContext keyContext;

    /** The state stores window accumulators. */
    private final WindowMapState<Long, List<RowData>> dataState;

    /** The util to compare two sortKey equals to each other. */
    private final Comparator<RowData> sortKeyComparator;

    /** The util to get sort key from input record. */
    private final KeySelector<RowData, RowData> sortKeySelector;

    /** TopN size. */
    private final long topN;

    /** Serializer to copy record if required. */
    private final TypeSerializer<RowData> recordSerializer;

    /** Whether the operator works in event-time mode, used to indicate registering which timer. */
    private final boolean isEventTime;

    public TopNRecordsCombiner(
            WindowTimerService<Long> timerService,
            StateKeyContext keyContext,
            WindowMapState<Long, List<RowData>> dataState,
            Comparator<RowData> sortKeyComparator,
            KeySelector<RowData, RowData> sortKeySelector,
            long topN,
            TypeSerializer<RowData> recordSerializer,
            boolean isEventTime) {
        this.timerService = timerService;
        this.keyContext = keyContext;
        this.dataState = dataState;
        this.sortKeyComparator = sortKeyComparator;
        this.sortKeySelector = sortKeySelector;
        this.topN = topN;
        this.recordSerializer = recordSerializer;
        this.isEventTime = isEventTime;
    }

    @Override
    public void combine(WindowKey windowKey, Iterator<RowData> records) throws Exception {
        // step 1: load all incremental records into TopNBuffer
        TopNBuffer buffer = new TopNBuffer(sortKeyComparator, ArrayList::new);
        while (records.hasNext()) {
            RowData record = records.next();
            if (!isAccumulateMsg(record)) {
                throw new UnsupportedOperationException(
                        "Window rank does not support input RowKind: "
                                + record.getRowKind().shortString());
            }

            RowData sortKey = sortKeySelector.getKey(record);
            if (buffer.checkSortKeyInBufferRange(sortKey, topN)) {
                // the incoming record is reused, we should copy it to insert into buffer
                buffer.put(sortKey, recordSerializer.copy(record));
            }
        }

        // step 2: flush data in TopNBuffer into state
        Iterator<Map.Entry<RowData, Collection<RowData>>> bufferItr = buffer.entrySet().iterator();
        keyContext.setCurrentKey(windowKey.getKey());
        Long window = windowKey.getWindow();
        while (bufferItr.hasNext()) {
            Map.Entry<RowData, Collection<RowData>> entry = bufferItr.next();
            RowData sortKey = entry.getKey();
            List<RowData> existsData = dataState.get(window, sortKey);
            if (existsData == null) {
                existsData = new ArrayList<>();
            }
            existsData.addAll(entry.getValue());
            dataState.put(window, sortKey, existsData);
        }
        // step 3: register timer for current window
        if (isEventTime) {
            timerService.registerEventTimeWindowTimer(window);
        }
        // we don't need register processing-time timer, because we already register them
        // per-record in AbstractWindowAggProcessor.processElement()
    }

    @Override
    public void close() throws Exception {}

    // ----------------------------------------------------------------------------------------
    // Factory
    // ----------------------------------------------------------------------------------------

    /** Factory to create {@link TopNRecordsCombiner}. */
    public static final class Factory implements RecordsCombiner.Factory {

        private static final long serialVersionUID = 1L;

        // The util to compare two sortKey equals to each other.
        private final GeneratedRecordComparator generatedSortKeyComparator;
        private final KeySelector<RowData, RowData> sortKeySelector;
        private final TypeSerializer<RowData> recordSerializer;
        private final long topN;

        public Factory(
                GeneratedRecordComparator genSortKeyComparator,
                RowDataKeySelector sortKeySelector,
                TypeSerializer<RowData> recordSerializer,
                long topN) {
            this.generatedSortKeyComparator = genSortKeyComparator;
            this.sortKeySelector = sortKeySelector;
            this.recordSerializer = recordSerializer;
            this.topN = topN;
        }

        @Override
        public RecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime)
                throws Exception {
            final Comparator<RowData> sortKeyComparator =
                    generatedSortKeyComparator.newInstance(runtimeContext.getUserCodeClassLoader());
            WindowMapState<Long, List<RowData>> windowMapState =
                    (WindowMapState<Long, List<RowData>>) windowState;
            return new TopNRecordsCombiner(
                    timerService,
                    stateBackend::setCurrentKey,
                    windowMapState,
                    sortKeyComparator,
                    sortKeySelector,
                    topN,
                    recordSerializer,
                    isEventTime);
        }
    }
}
