/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.asyncprocessing.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.runtime.state.v2.adaptor.CompleteStateIterator;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava33.com.google.common.collect.FluentIterable;
import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link AsyncWindowOperator} that also allows an {@link Evictor} to be used.
 *
 * <p>The {@code Evictor} is used to remove elements from a pane before/after the evaluation of
 * {@link InternalWindowFunction} and after the window evaluation gets triggered by a {@link
 * AsyncTrigger}.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class AsyncEvictingWindowOperator<K, IN, OUT, W extends Window>
        extends AsyncWindowOperator<K, IN, StateIterator<IN>, OUT, W> {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // these fields are set by the API stream graph builder to configure the operator

    private final Evictor<? super IN, ? super W> evictor;

    private final StateDescriptor<StreamRecord<IN>> evictingWindowStateDescriptor;

    // ------------------------------------------------------------------------
    // the fields below are instantiated once the operator runs in the runtime

    private transient EvictorContext evictorContext;

    private transient InternalListState<K, W, StreamRecord<IN>> evictingWindowState;

    // ------------------------------------------------------------------------

    public AsyncEvictingWindowOperator(
            WindowAssigner<? super IN, W> windowAssigner,
            TypeSerializer<W> windowSerializer,
            KeySelector<IN, K> keySelector,
            TypeSerializer<K> keySerializer,
            StateDescriptor<StreamRecord<IN>> windowStateDescriptor,
            InternalAsyncWindowFunction<StateIterator<IN>, OUT, K, W> windowFunction,
            AsyncTrigger<? super IN, ? super W> trigger,
            Evictor<? super IN, ? super W> evictor,
            long allowedLateness,
            OutputTag<IN> lateDataOutputTag) {

        super(
                windowAssigner,
                windowSerializer,
                keySelector,
                keySerializer,
                null,
                windowFunction,
                trigger,
                allowedLateness,
                lateDataOutputTag);

        this.evictor = checkNotNull(evictor);
        this.evictingWindowStateDescriptor = checkNotNull(windowStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        element.getValue(), element.getTimestamp(), windowAssignerContext);

        final K key = (K) getCurrentKey();
        // Note: windowFutures is empty => implicitly isSkippedElement == true.
        Collection<StateFuture<Void>> windowFutures = new ArrayList<>();

        if (windowAssigner instanceof MergingWindowAssigner) {
            throw new UnsupportedOperationException(
                    "Async WindowOperator not support merging window (e.g. session window) yet.");
        } else {
            for (W window : elementWindows) {

                // drop if the window is already late
                if (isWindowLate(window)) {
                    continue;
                }

                AtomicReference<TriggerResult> triggerResult = new AtomicReference<>();
                windowDeclaredVariable.set(window);

                // Implicitly isSkippedElement = false
                windowFutures.add(
                        evictingWindowState
                                .asyncAdd(element)
                                .thenCompose(
                                        ignore ->
                                                triggerContext
                                                        .onElement(element)
                                                        .thenAccept(triggerResult::set))
                                .thenConditionallyCompose(
                                        ignore -> triggerResult.get().isFire(),
                                        ignore ->
                                                evictingWindowState
                                                        .asyncGet()
                                                        .thenConditionallyCompose(
                                                                Objects::nonNull,
                                                                contents ->
                                                                        emitWindowContents(
                                                                                key,
                                                                                window,
                                                                                contents,
                                                                                evictingWindowState)))
                                .thenConditionallyCompose(
                                        ignore -> triggerResult.get().isPurge(),
                                        ignore -> evictingWindowState.asyncClear())
                                .thenAccept(ignore -> registerCleanupTimer(window)));
            }

            // side output input event if
            // element not handled by any window
            // late arriving tag has been set
            // windowAssigner is event time and current timestamp + allowed lateness no less than
            // element timestamp
            if (windowFutures.isEmpty() && isElementLate(element)) {
                if (lateDataOutputTag != null) {
                    sideOutput(element);
                } else {
                    this.numLateRecordsDropped.inc();
                }
            }
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        windowDeclaredVariable.set(timer.getNamespace());
        AtomicReference<TriggerResult> triggerResult = new AtomicReference<>();

        if (windowAssigner instanceof MergingWindowAssigner) {
            throw new UnsupportedOperationException(
                    "Async WindowOperator not support merging window (e.g. session window) yet.");
        } else {
            triggerContext
                    .onEventTime(timer.getTimestamp())
                    .thenAccept(triggerResult::set)
                    .thenConditionallyCompose(
                            ignore -> triggerResult.get().isFire(),
                            ignore ->
                                    evictingWindowState
                                            .asyncGet()
                                            .thenConditionallyCompose(
                                                    Objects::nonNull,
                                                    contents ->
                                                            emitWindowContents(
                                                                    timer.getKey(),
                                                                    timer.getNamespace(),
                                                                    contents,
                                                                    evictingWindowState)))
                    .thenConditionallyCompose(
                            ignore -> triggerResult.get().isPurge(),
                            ignore -> evictingWindowState.asyncClear())
                    .thenConditionallyCompose(
                            ignore ->
                                    windowAssigner.isEventTime()
                                            && isCleanupTime(
                                                    timer.getNamespace(), timer.getTimestamp()),
                            ignore -> clearAllState(timer.getNamespace(), evictingWindowState));
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        windowDeclaredVariable.set(timer.getNamespace());
        AtomicReference<TriggerResult> triggerResult = new AtomicReference<>();

        if (windowAssigner instanceof MergingWindowAssigner) {
            throw new UnsupportedOperationException(
                    "Async WindowOperator not support merging window (e.g. session window) yet.");
        } else {
            triggerContext
                    .onProcessingTime(timer.getTimestamp())
                    .thenAccept(triggerResult::set)
                    .thenConditionallyCompose(
                            ignore -> triggerResult.get().isFire(),
                            ignore ->
                                    evictingWindowState
                                            .asyncGet()
                                            .thenConditionallyCompose(
                                                    Objects::nonNull,
                                                    contents ->
                                                            emitWindowContents(
                                                                    timer.getKey(),
                                                                    timer.getNamespace(),
                                                                    contents,
                                                                    evictingWindowState)))
                    .thenConditionallyCompose(
                            ignore -> triggerResult.get().isPurge(),
                            ignore -> evictingWindowState.asyncClear())
                    .thenConditionallyCompose(
                            ignore ->
                                    !windowAssigner.isEventTime()
                                            && isCleanupTime(
                                                    timer.getNamespace(), timer.getTimestamp()),
                            ignore -> clearAllState(timer.getNamespace(), evictingWindowState));
        }
    }

    private StateFuture<Void> emitWindowContents(
            K key,
            W window,
            StateIterator<StreamRecord<IN>> contents,
            ListState<StreamRecord<IN>> windowState)
            throws Exception {
        timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
        List<StreamRecord<IN>> elements = new ArrayList<>();
        return contents.onNext(
                        (item) -> {
                            elements.add(item);
                        })
                .thenApply(
                        ignore -> {
                            // Work around type system restrictions...
                            FluentIterable<TimestampedValue<IN>> recordsWithTimestamp =
                                    FluentIterable.from(elements).transform(TimestampedValue::from);
                            evictorContext.evictBefore(
                                    recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

                            FluentIterable<IN> projectedContents =
                                    recordsWithTimestamp.transform(TimestampedValue::getValue);
                            StateIterator<IN> projectedContentIter =
                                    new CompleteStateIterator<>(projectedContents);

                            return Tuple2.of(recordsWithTimestamp, projectedContentIter);
                        })
                .thenCompose(
                        tuple ->
                                userFunction
                                        .process(
                                                key,
                                                window,
                                                processContext,
                                                tuple.f1,
                                                timestampedCollector)
                                        .thenCompose(
                                                ignore -> {
                                                    evictorContext.evictAfter(
                                                            tuple.f0, Iterables.size(tuple.f0));

                                                    return windowState.asyncUpdate(
                                                            tuple.f0.stream()
                                                                    .map(
                                                                            TimestampedValue
                                                                                    ::getStreamRecord)
                                                                    .collect(Collectors.toList()));
                                                }));
    }

    private StateFuture<Void> clearAllState(W window, ListState<StreamRecord<IN>> windowState) {
        return windowState
                .asyncClear()
                .thenCompose(ignore -> triggerContext.clear())
                .thenCompose(ignore -> processContext.clear());
    }

    /**
     * {@code EvictorContext} is a utility for handling {@code Evictor} invocations. It can be
     * reused by setting the {@code key} and {@code window} fields. No internal state must be kept
     * in the {@code EvictorContext}.
     */
    class EvictorContext implements Evictor.EvictorContext {

        protected DeclaredVariable<W> window;

        public EvictorContext(DeclaredVariable<W> window) {
            this.window = window;
        }

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long getCurrentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public MetricGroup getMetricGroup() {
            return AsyncEvictingWindowOperator.this.getMetricGroup();
        }

        void evictBefore(Iterable<TimestampedValue<IN>> elements, int size) {
            evictor.evictBefore((Iterable) elements, size, window.get(), this);
        }

        void evictAfter(Iterable<TimestampedValue<IN>> elements, int size) {
            evictor.evictAfter((Iterable) elements, size, window.get(), this);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        evictorContext = new EvictorContext(windowDeclaredVariable);
        evictingWindowState =
                getOrCreateKeyedState(
                        windowSerializer.createInstance(),
                        windowSerializer,
                        evictingWindowStateDescriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
        evictorContext = null;
    }

    // ------------------------------------------------------------------------
    // Getters for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Evictor<? super IN, ? super W> getEvictor() {
        return evictor;
    }

    @Override
    @VisibleForTesting
    @SuppressWarnings("unchecked, rawtypes")
    public StateDescriptor<?> getStateDescriptor() {
        return evictingWindowStateDescriptor;
    }
}
