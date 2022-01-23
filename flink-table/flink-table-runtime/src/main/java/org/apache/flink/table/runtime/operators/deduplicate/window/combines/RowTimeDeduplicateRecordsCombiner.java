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

package org.apache.flink.table.runtime.operators.deduplicate.window.combines;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.StateKeyContext;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.table.runtime.operators.window.state.WindowValueState;
import org.apache.flink.table.runtime.util.WindowKey;

import java.util.Iterator;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.isDuplicate;

/**
 * An implementation of {@link RecordsCombiner} that stores the first/last records of incremental
 * input records into the window state.
 */
public final class RowTimeDeduplicateRecordsCombiner implements RecordsCombiner {

    /** The service to register event-time or processing-time timers. */
    private final WindowTimerService<Long> timerService;

    /** Context to switch current key for states. */
    private final StateKeyContext keyContext;

    /** The state stores first/last record of each window. */
    private final WindowValueState<Long> dataState;

    private final int rowtimeIndex;

    private final boolean keepLastRow;

    /** Serializer to copy record if required. */
    private final TypeSerializer<RowData> recordSerializer;

    public RowTimeDeduplicateRecordsCombiner(
            WindowTimerService<Long> timerService,
            StateKeyContext keyContext,
            WindowValueState<Long> dataState,
            int rowtimeIndex,
            boolean keepLastRow,
            TypeSerializer<RowData> recordSerializer) {
        this.timerService = timerService;
        this.keyContext = keyContext;
        this.dataState = dataState;
        this.rowtimeIndex = rowtimeIndex;
        this.keepLastRow = keepLastRow;
        this.recordSerializer = recordSerializer;
    }

    @Override
    public void combine(WindowKey windowKey, Iterator<RowData> records) throws Exception {
        // step 1: get first/last record of incremental data
        RowData bufferedResult = null;
        while (records.hasNext()) {
            RowData record = records.next();
            if (!isAccumulateMsg(record)) {
                throw new UnsupportedOperationException(
                        "Window deduplicate does not support input RowKind: "
                                + record.getRowKind().shortString());
            }
            if (isDuplicate(bufferedResult, record, rowtimeIndex, keepLastRow)) {
                // the incoming record is reused, we should copy it
                bufferedResult = recordSerializer.copy(record);
            }
        }
        if (bufferedResult == null) {
            return;
        }
        // step 2: flush data into state
        keyContext.setCurrentKey(windowKey.getKey());
        Long window = windowKey.getWindow();
        RowData preRow = dataState.value(window);
        if (isDuplicate(preRow, bufferedResult, rowtimeIndex, keepLastRow)) {
            dataState.update(window, bufferedResult);
        }
        // step 3: register timer for current window
        timerService.registerEventTimeWindowTimer(window);
    }

    @Override
    public void close() throws Exception {}

    // ----------------------------------------------------------------------------------------
    // Factory
    // ----------------------------------------------------------------------------------------

    /** Factory to create {@link RowTimeDeduplicateRecordsCombiner}. */
    public static final class Factory implements RecordsCombiner.Factory {

        private static final long serialVersionUID = 1L;

        private final TypeSerializer<RowData> recordSerializer;
        private final int rowtimeIndex;
        private final boolean keepLastRow;

        public Factory(
                TypeSerializer<RowData> recordSerializer, int rowtimeIndex, boolean keepLastRow) {
            this.recordSerializer = recordSerializer;
            this.rowtimeIndex = rowtimeIndex;
            this.keepLastRow = keepLastRow;
        }

        @Override
        public RecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime)
                throws Exception {
            WindowValueState<Long> windowMapState = (WindowValueState<Long>) windowState;
            return new RowTimeDeduplicateRecordsCombiner(
                    timerService,
                    stateBackend::setCurrentKey,
                    windowMapState,
                    rowtimeIndex,
                    keepLastRow,
                    recordSerializer);
        }
    }
}
