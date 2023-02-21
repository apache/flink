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

package org.apache.flink.table.runtime.operators.aggregate.window.combines;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.StateKeyContext;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.table.runtime.operators.window.state.WindowValueState;
import org.apache.flink.table.runtime.util.WindowKey;

import java.time.ZoneId;
import java.util.Iterator;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * An implementation of {@link RecordsCombiner} that accumulates input records into the window
 * accumulator state.
 */
public class AggCombiner implements RecordsCombiner {

    /** The service to register event-time or processing-time timers. */
    private final WindowTimerService<Long> timerService;

    /** Context to switch current key for states. */
    private final StateKeyContext keyContext;

    /** The state stores window accumulators. */
    private final WindowValueState<Long> accState;

    /** Function used to handle all aggregates. */
    private final NamespaceAggsHandleFunction<Long> aggregator;

    /** Whether the operator works in event-time mode, used to indicate registering which timer. */
    private final boolean isEventTime;

    public AggCombiner(
            WindowTimerService<Long> timerService,
            StateKeyContext keyContext,
            WindowValueState<Long> accState,
            NamespaceAggsHandleFunction<Long> aggregator,
            boolean isEventTime) {
        this.timerService = timerService;
        this.keyContext = keyContext;
        this.accState = accState;
        this.aggregator = aggregator;
        this.isEventTime = isEventTime;
    }

    @Override
    public void combine(WindowKey windowKey, Iterator<RowData> records) throws Exception {
        // step 0: set current key for states and timers
        keyContext.setCurrentKey(windowKey.getKey());

        // step 1: get the accumulator for the current key and window
        Long window = windowKey.getWindow();
        RowData acc = accState.value(window);
        if (acc == null) {
            acc = aggregator.createAccumulators();
        }

        // step 2: set accumulator to function
        aggregator.setAccumulators(window, acc);

        // step 3: do accumulate
        while (records.hasNext()) {
            RowData record = records.next();
            if (isAccumulateMsg(record)) {
                aggregator.accumulate(record);
            } else {
                aggregator.retract(record);
            }
        }

        // step 4: update accumulator into state
        acc = aggregator.getAccumulators();
        accState.update(window, acc);

        // step 5: register timer for current window
        if (isEventTime) {
            long currentWatermark = timerService.currentWatermark();
            ZoneId shiftTimeZone = timerService.getShiftTimeZone();
            // the registered window timer should hasn't been triggered
            if (!isWindowFired(window, currentWatermark, shiftTimeZone)) {
                timerService.registerEventTimeWindowTimer(window);
            }
        }
        // we don't need register processing-time timer, because we already register them
        // per-record in AbstractWindowAggProcessor.processElement()
    }

    @Override
    public void close() throws Exception {
        aggregator.close();
    }

    // ----------------------------------------------------------------------------------------
    // Factory
    // ----------------------------------------------------------------------------------------

    /** Factory to create {@link AggCombiner}. */
    public static final class Factory implements RecordsCombiner.Factory {
        private static final long serialVersionUID = 1L;

        private final GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler;

        public Factory(GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler) {
            this.genAggsHandler = genAggsHandler;
        }

        @Override
        public RecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime)
                throws Exception {
            final NamespaceAggsHandleFunction<Long> aggregator =
                    genAggsHandler.newInstance(runtimeContext.getUserCodeClassLoader());
            aggregator.open(
                    new PerWindowStateDataViewStore(
                            stateBackend, LongSerializer.INSTANCE, runtimeContext));
            WindowValueState<Long> windowValueState = (WindowValueState<Long>) windowState;
            return new AggCombiner(
                    timerService,
                    stateBackend::setCurrentKey,
                    windowValueState,
                    aggregator,
                    isEventTime);
        }
    }
}
