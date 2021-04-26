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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.combines.WindowCombineFunction;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.StateKeyContext;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.table.runtime.operators.window.state.WindowValueState;
import org.apache.flink.table.runtime.util.WindowKey;

import java.time.ZoneId;
import java.util.Iterator;

import static org.apache.flink.table.runtime.util.StateConfigUtil.isStateImmutableInStateBackend;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * An implementation of {@link WindowCombineFunction} that accumulates local accumulators records
 * into the window accumulator state.
 *
 * <p>Note: this only supports event-time window.
 */
public final class GlobalAggAccCombiner implements WindowCombineFunction {

    /** The service to register event-time or processing-time timers. */
    private final WindowTimerService<Long> timerService;

    /** Context to switch current key for states. */
    private final StateKeyContext keyContext;

    /** The state stores window accumulators. */
    private final WindowValueState<Long> accState;

    /** Local aggregate function to handle local combined accumulator rows. */
    private final NamespaceAggsHandleFunction<Long> localAggregator;

    /** Global aggregate function to handle global accumulator rows. */
    private final NamespaceAggsHandleFunction<Long> globalAggregator;

    /** Whether to copy key and input record, because key and record are reused. */
    private final boolean requiresCopy;

    /** Serializer to copy key if required. */
    private final TypeSerializer<RowData> keySerializer;

    public GlobalAggAccCombiner(
            WindowTimerService<Long> timerService,
            StateKeyContext keyContext,
            WindowValueState<Long> accState,
            NamespaceAggsHandleFunction<Long> localAggregator,
            NamespaceAggsHandleFunction<Long> globalAggregator,
            boolean requiresCopy,
            TypeSerializer<RowData> keySerializer) {
        this.timerService = timerService;
        this.keyContext = keyContext;
        this.accState = accState;
        this.localAggregator = localAggregator;
        this.globalAggregator = globalAggregator;
        this.requiresCopy = requiresCopy;
        this.keySerializer = keySerializer;
    }

    @Override
    public void combine(WindowKey windowKey, Iterator<RowData> localAccs) throws Exception {
        // step 0: set current key for states and timers
        final RowData key;
        if (requiresCopy) {
            // the incoming key is reused, we should copy it if state backend doesn't copy it
            key = keySerializer.copy(windowKey.getKey());
        } else {
            key = windowKey.getKey();
        }
        keyContext.setCurrentKey(key);
        Long window = windowKey.getWindow();

        // step 1: merge localAccs into one acc
        RowData acc = localAggregator.createAccumulators();
        localAggregator.setAccumulators(window, acc);
        while (localAccs.hasNext()) {
            RowData localAcc = localAccs.next();
            localAggregator.merge(window, localAcc);
        }
        RowData mergedLocalAcc = localAggregator.getAccumulators();

        // step2: merge acc into state
        RowData stateAcc = accState.value(window);
        if (stateAcc == null) {
            stateAcc = globalAggregator.createAccumulators();
        }
        globalAggregator.setAccumulators(window, stateAcc);
        globalAggregator.merge(window, mergedLocalAcc);
        stateAcc = globalAggregator.getAccumulators();
        accState.update(window, stateAcc);

        // step 3: register timer for current window
        long currentWatermark = timerService.currentWatermark();
        ZoneId shiftTimeZone = timerService.getShiftTimeZone();
        // the registered window timer should hasn't been triggered
        if (!isWindowFired(window, currentWatermark, shiftTimeZone)) {
            timerService.registerEventTimeWindowTimer(window);
        }
    }

    @Override
    public void close() throws Exception {
        localAggregator.close();
        globalAggregator.close();
    }

    // ----------------------------------------------------------------------------------------
    // Factory
    // ----------------------------------------------------------------------------------------

    /** Factory to create {@link GlobalAggAccCombiner}. */
    public static final class Factory implements WindowCombineFunction.Factory {

        private static final long serialVersionUID = 1L;

        private final GeneratedNamespaceAggsHandleFunction<Long> genLocalAggsHandler;
        private final GeneratedNamespaceAggsHandleFunction<Long> genGlobalAggsHandler;
        private final TypeSerializer<RowData> keySerializer;

        public Factory(
                GeneratedNamespaceAggsHandleFunction<Long> genLocalAggsHandler,
                GeneratedNamespaceAggsHandleFunction<Long> genGlobalAggsHandler,
                TypeSerializer<RowData> keySerializer) {
            this.genLocalAggsHandler = genLocalAggsHandler;
            this.genGlobalAggsHandler = genGlobalAggsHandler;
            this.keySerializer = keySerializer;
        }

        @Override
        public WindowCombineFunction create(
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime)
                throws Exception {
            final NamespaceAggsHandleFunction<Long> localAggregator =
                    genLocalAggsHandler.newInstance(runtimeContext.getUserCodeClassLoader());
            final NamespaceAggsHandleFunction<Long> globalAggregator =
                    genGlobalAggsHandler.newInstance(runtimeContext.getUserCodeClassLoader());
            localAggregator.open(
                    new PerWindowStateDataViewStore(
                            stateBackend, LongSerializer.INSTANCE, runtimeContext));
            globalAggregator.open(
                    new PerWindowStateDataViewStore(
                            stateBackend, LongSerializer.INSTANCE, runtimeContext));
            boolean requiresCopy = !isStateImmutableInStateBackend(stateBackend);
            WindowValueState<Long> windowValueState = (WindowValueState<Long>) windowState;
            return new GlobalAggAccCombiner(
                    timerService,
                    stateBackend::setCurrentKey,
                    windowValueState,
                    localAggregator,
                    globalAggregator,
                    requiresCopy,
                    keySerializer);
        }
    }
}
