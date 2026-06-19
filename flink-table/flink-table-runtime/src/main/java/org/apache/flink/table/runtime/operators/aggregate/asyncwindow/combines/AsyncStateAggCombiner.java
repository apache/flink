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

package org.apache.flink.table.runtime.operators.aggregate.asyncwindow.combines;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.UnsupportedStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.async.tvf.combines.AsyncStateRecordsCombiner;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.WindowAsyncState;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.WindowAsyncValueState;
import org.apache.flink.table.runtime.operators.window.tvf.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;

import java.time.ZoneId;
import java.util.Iterator;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * An implementation of {@link RecordsCombiner} that accumulates input records into the window
 * accumulator with async state.
 */
public class AsyncStateAggCombiner implements AsyncStateRecordsCombiner {

    /** The service to register event-time or processing-time timers. */
    private final WindowTimerService<Long> timerService;

    /** The state stores window accumulators. */
    private final WindowAsyncValueState<Long> accState;

    /** Function used to handle all aggregates. */
    private final NamespaceAggsHandleFunction<Long> aggregator;

    /** Whether the operator works in event-time mode, used to indicate registering which timer. */
    private final boolean isEventTime;

    public AsyncStateAggCombiner(
            WindowTimerService<Long> timerService,
            WindowAsyncValueState<Long> accState,
            NamespaceAggsHandleFunction<Long> aggregator,
            boolean isEventTime) {
        this.timerService = timerService;
        this.accState = accState;
        this.aggregator = aggregator;
        this.isEventTime = isEventTime;
    }

    @Override
    public StateFuture<Void> asyncCombine(Long window, Iterator<RowData> records) throws Exception {
        StateFuture<Void> resultFuture =
                // step 1: get the accumulator for the current key and window
                accState.asyncValue(window)
                        .thenCompose(
                                acc -> {
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

                                    return accState.asyncUpdate(window, acc);
                                });

        // step 5: register timer for current window
        if (isEventTime) {
            long currentWatermark = timerService.currentWatermark();
            ZoneId shiftTimeZone = timerService.getShiftTimeZone();
            // the registered window timer shouldn't been triggered
            if (!isWindowFired(window, currentWatermark, shiftTimeZone)) {
                timerService.registerEventTimeWindowTimer(window);
            }
        }
        // we don't need register processing-time timer, because we already register them
        // per-record in AbstractWindowAggProcessor.processElement()
        return resultFuture;
    }

    @Override
    public void close() throws Exception {
        aggregator.close();
    }

    // ----------------------------------------------------------------------------------------
    // Factory
    // ----------------------------------------------------------------------------------------

    /** Factory to create {@link AsyncStateAggCombiner}. */
    public static final class Factory implements AsyncStateRecordsCombiner.Factory {
        private static final long serialVersionUID = 1L;

        private final GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler;

        public Factory(GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler) {
            this.genAggsHandler = genAggsHandler;
        }

        @Override
        public AsyncStateRecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                WindowAsyncState<Long> windowState,
                boolean isEventTime)
                throws Exception {
            final NamespaceAggsHandleFunction<Long> aggregator =
                    genAggsHandler.newInstance(runtimeContext.getUserCodeClassLoader());
            aggregator.open(new UnsupportedStateDataViewStore(runtimeContext));
            WindowAsyncValueState<Long> windowValueState =
                    (WindowAsyncValueState<Long>) windowState;
            return new AsyncStateAggCombiner(
                    timerService, windowValueState, aggregator, isEventTime);
        }
    }
}
