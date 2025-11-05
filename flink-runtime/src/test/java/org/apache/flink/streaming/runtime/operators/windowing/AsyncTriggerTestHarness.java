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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TestInternalTimerService;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

/** Utility for testing {@link AsyncTrigger} behaviour. */
public class AsyncTriggerTestHarness<T, W extends Window> extends TriggerTestHarness<T, W> {

    private final AsyncTrigger<T, W> trigger;

    // Async adaptor to realStateBackend.
    private final AsyncKeyedStateBackend<Integer> asyncStateBackend;

    /**
     * Initialize test harness for async trigger.
     *
     * <p>The state backend is heap, which does not support async state operation. The tests use
     * async state API, but all state operations execute in sync mode.
     */
    public AsyncTriggerTestHarness(AsyncTrigger<T, W> trigger, TypeSerializer<W> windowSerializer)
            throws Exception {
        super(null, windowSerializer);
        this.trigger = trigger;

        this.asyncStateBackend = new AsyncKeyedStateBackendAdaptor<>(stateBackend);
    }

    // ------------------------------------------------------------------------------
    // Override TriggerTestHarness API
    // ------------------------------------------------------------------------------

    @Override
    public TriggerResult processElement(StreamRecord<T> element, W window) throws Exception {
        return completeStateFuture(asyncProcessElement(element, window));
    }

    @Override
    public TriggerResult advanceProcessingTime(long time, W window) throws Exception {
        return completeStateFuture(asyncAdvanceProcessingTime(time, window));
    }

    @Override
    public TriggerResult advanceWatermark(long time, W window) throws Exception {
        return completeStateFuture(asyncAdvanceWatermark(time, window));
    }

    @Override
    public Collection<Tuple2<W, TriggerResult>> advanceProcessingTime(long time) throws Exception {
        return asyncAdvanceProcessingTime(time).stream()
                .map(f -> Tuple2.of(f.f0, completeStateFuture(f.f1)))
                .collect(Collectors.toList());
    }

    @Override
    public Collection<Tuple2<W, TriggerResult>> advanceWatermark(long time) throws Exception {
        return asyncAdvanceWatermark(time).stream()
                .map(f -> Tuple2.of(f.f0, completeStateFuture(f.f1)))
                .collect(Collectors.toList());
    }

    @Override
    public TriggerResult invokeOnEventTime(long timestamp, W window) throws Exception {
        return completeStateFuture(asyncInvokeOnEventTime(timestamp, window));
    }

    @Override
    public void mergeWindows(W targetWindow, Collection<W> mergedWindows) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearTriggerState(W window) throws Exception {
        completeStateFuture(asyncClearTriggerState(window));
    }

    // ------------------------------------------------------------------------------
    // Using Async State API
    // ------------------------------------------------------------------------------

    StateFuture<TriggerResult> asyncProcessElement(StreamRecord<T> element, W window)
            throws Exception {
        TestTriggerContext<Integer, W> triggerContext =
                new TestTriggerContext<>(
                        KEY, window, internalTimerService, asyncStateBackend, windowSerializer);
        return trigger.onElement(
                element.getValue(), element.getTimestamp(), window, triggerContext);
    }

    StateFuture<TriggerResult> asyncAdvanceProcessingTime(long time, W window) throws Exception {
        Collection<Tuple2<W, StateFuture<TriggerResult>>> firings =
                asyncAdvanceProcessingTime(time);

        if (firings.size() != 1) {
            throw new IllegalStateException(
                    "Must have exactly one timer firing. Fired timers: " + firings);
        }

        Tuple2<W, StateFuture<TriggerResult>> firing = firings.iterator().next();

        if (!firing.f0.equals(window)) {
            throw new IllegalStateException("Trigger fired for another window.");
        }

        return firing.f1;
    }

    StateFuture<TriggerResult> asyncAdvanceWatermark(long time, W window) throws Exception {
        Collection<Tuple2<W, StateFuture<TriggerResult>>> firings = asyncAdvanceWatermark(time);

        if (firings.size() != 1) {
            throw new IllegalStateException(
                    "Must have exactly one timer firing. Fired timers: " + firings);
        }

        Tuple2<W, StateFuture<TriggerResult>> firing = firings.iterator().next();

        if (!firing.f0.equals(window)) {
            throw new IllegalStateException("Trigger fired for another window.");
        }

        return firing.f1;
    }

    Collection<Tuple2<W, StateFuture<TriggerResult>>> asyncAdvanceProcessingTime(long time)
            throws Exception {
        Collection<TestInternalTimerService.Timer<Integer, W>> firedTimers =
                internalTimerService.advanceProcessingTime(time);

        Collection<Tuple2<W, StateFuture<TriggerResult>>> result = new ArrayList<>();

        for (TestInternalTimerService.Timer<Integer, W> timer : firedTimers) {
            TestTriggerContext<Integer, W> triggerContext =
                    new TestTriggerContext<>(
                            KEY,
                            timer.getNamespace(),
                            internalTimerService,
                            asyncStateBackend,
                            windowSerializer);

            StateFuture<TriggerResult> triggerResult =
                    trigger.onProcessingTime(
                            timer.getTimestamp(), timer.getNamespace(), triggerContext);

            result.add(new Tuple2<>(timer.getNamespace(), triggerResult));
        }

        return result;
    }

    Collection<Tuple2<W, StateFuture<TriggerResult>>> asyncAdvanceWatermark(long time)
            throws Exception {
        Collection<TestInternalTimerService.Timer<Integer, W>> firedTimers =
                internalTimerService.advanceWatermark(time);

        Collection<Tuple2<W, StateFuture<TriggerResult>>> result = new ArrayList<>();

        for (TestInternalTimerService.Timer<Integer, W> timer : firedTimers) {
            StateFuture<TriggerResult> triggerResult = asyncInvokeOnEventTime(timer);
            result.add(new Tuple2<>(timer.getNamespace(), triggerResult));
        }

        return result;
    }

    private StateFuture<TriggerResult> asyncInvokeOnEventTime(
            TestInternalTimerService.Timer<Integer, W> timer) throws Exception {
        TestTriggerContext<Integer, W> triggerContext =
                new TestTriggerContext<>(
                        KEY,
                        timer.getNamespace(),
                        internalTimerService,
                        asyncStateBackend,
                        windowSerializer);

        return trigger.onEventTime(timer.getTimestamp(), timer.getNamespace(), triggerContext);
    }

    StateFuture<TriggerResult> asyncInvokeOnEventTime(long timestamp, W window) throws Exception {
        TestInternalTimerService.Timer<Integer, W> timer =
                new TestInternalTimerService.Timer<>(timestamp, KEY, window);

        return asyncInvokeOnEventTime(timer);
    }

    StateFuture<Void> asyncClearTriggerState(W window) throws Exception {
        TestTriggerContext<Integer, W> triggerContext =
                new TestTriggerContext<>(
                        KEY, window, internalTimerService, asyncStateBackend, windowSerializer);
        return trigger.clear(window, triggerContext);
    }

    // ------------------------------------------------------------------------------
    // Context
    // ------------------------------------------------------------------------------

    private static class TestTriggerContext<K, W extends Window>
            implements AsyncTrigger.TriggerContext {

        protected final InternalTimerService<W> timerService;
        protected final AsyncKeyedStateBackend<Integer> stateBackend;
        protected final K key;
        protected final W window;
        protected final TypeSerializer<W> windowSerializer;

        TestTriggerContext(
                K key,
                W window,
                InternalTimerService<W> timerService,
                AsyncKeyedStateBackend<Integer> stateBackend,
                TypeSerializer<W> windowSerializer) {
            this.key = key;
            this.window = window;
            this.timerService = timerService;
            this.stateBackend = stateBackend;
            this.windowSerializer = windowSerializer;
        }

        @Override
        public long getCurrentProcessingTime() {
            return timerService.currentProcessingTime();
        }

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public long getCurrentWatermark() {
            return timerService.currentWatermark();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            timerService.registerProcessingTimeTimer(window, time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            timerService.registerEventTimeTimer(window, time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            timerService.deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            timerService.deleteEventTimeTimer(window, time);
        }

        @Override
        public <T, S extends State> S getPartitionedState(StateDescriptor<T> stateDescriptor) {
            try {
                // single key (KEY), no need declaration.
                return stateBackend.getOrCreateKeyedState(
                        window, windowSerializer, stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }
    }

    static <T> T completeStateFuture(StateFuture<T> future) {
        InternalAsyncFuture<T> internalAsyncFuture = (InternalAsyncFuture<T>) future;

        // The harness executes state operations in sync mode, any StateFuture should be completed.
        Preconditions.checkArgument(internalAsyncFuture.isDone());
        return internalAsyncFuture.get();
    }
}
