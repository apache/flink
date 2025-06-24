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

package org.apache.flink.datastream.impl.extension.window.operators;

import org.apache.flink.api.common.state.v2.AppendingState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.WindowProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.datastream.impl.extension.window.context.DefaultOneInputWindowContext;
import org.apache.flink.datastream.impl.extension.window.context.WindowTriggerContext;
import org.apache.flink.datastream.impl.extension.window.function.InternalOneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.utils.WindowUtils;
import org.apache.flink.datastream.impl.operators.BaseKeyedProcessOperator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.v2.internal.InternalAppendingState;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.runtime.state.v2.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Operator for {@link OneInputWindowStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class OneInputWindowProcessOperator<K, IN, OUT, W extends Window>
        extends BaseKeyedProcessOperator<K, IN, OUT> implements Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------

    /** User-defined {@link WindowProcessFunction}. */
    private final OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction;

    /**
     * The allowed lateness for elements. This is used for:
     *
     * <ul>
     *   <li>Deciding if an element should be dropped from a window due to lateness.
     *   <li>Clearing the state of a window if the time out-of the {@code window.maxTimestamp +
     *       allowedLateness} landmark.
     * </ul>
     */
    private final long allowedLateness;

    // ------------------------------------------------------------------------
    // Operator components
    // ------------------------------------------------------------------------

    private transient InternalTimerService<W> internalTimerService;

    /** For serializing the window in checkpoints. */
    private final TypeSerializer<W> windowSerializer;

    // ------------------------------------------------------------------------
    // Window assigner and trigger
    // ------------------------------------------------------------------------

    private final WindowAssigner<? super IN, W> windowAssigner;

    private transient WindowAssigner.WindowAssignerContext windowAssignerContext;

    private final Trigger<? super IN, ? super W> trigger;

    private transient WindowTriggerContext<K, IN, W> triggerContext;

    private transient DefaultOneInputWindowContext<K, IN, W> windowFunctionContext;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    private final StateDescriptor<IN> windowStateDescriptor;

    /** The state in which the window contents is stored. Each window is a namespace */
    private transient InternalAppendingState<K, W, IN, IN, StateIterator<IN>, Iterable<IN>>
            windowState;

    /**
     * The {@link #windowState}, typed to merging state for merging windows. Null if the window
     * state is not mergeable.
     */
    private transient InternalMergingState<K, W, IN, IN, StateIterator<IN>, Iterable<IN>>
            windowMergingState;

    /** The state that holds the merging window metadata (the sets that describe what is merged). */
    private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

    public OneInputWindowProcessOperator(
            InternalOneInputWindowStreamProcessFunction<IN, OUT, W> windowProcessFunction,
            WindowAssigner<? super IN, W> windowAssigner,
            Trigger<? super IN, ? super W> trigger,
            TypeSerializer<W> windowSerializer,
            StateDescriptor<IN> windowStateDescriptor,
            long allowedLateness) {
        super(windowProcessFunction);
        checkArgument(allowedLateness >= 0);
        this.windowProcessFunction = windowProcessFunction.getWindowProcessFunction();
        this.windowAssigner = windowAssigner;
        this.trigger = trigger;
        this.windowSerializer = windowSerializer;
        this.windowStateDescriptor = windowStateDescriptor;
        this.allowedLateness = allowedLateness;
    }

    @Override
    public void open() throws Exception {
        super.open();
        internalTimerService =
                getInternalTimerService("process-window-timers", windowSerializer, this);

        // create (or restore) the state that hold the actual window contents
        // NOTE - the state may be null in the case of the overriding evicting window operator
        if (windowStateDescriptor != null) {
            windowState =
                    getOrCreateKeyedState(
                            windowSerializer.createInstance(),
                            windowSerializer,
                            windowStateDescriptor);
        }

        // create the typed and helper states for merging windows
        if (windowAssigner instanceof MergingWindowAssigner) {

            // store a typed reference for the state of merging windows - sanity check
            if (windowState instanceof InternalMergingState) {
                windowMergingState =
                        (InternalMergingState<K, W, IN, IN, StateIterator<IN>, Iterable<IN>>)
                                windowState;
            } else if (windowState != null) {
                throw new IllegalStateException(
                        "The window uses a merging assigner, but the window state is not mergeable.");
            }

            @SuppressWarnings("unchecked")
            final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

            final TupleSerializer<Tuple2<W, W>> tupleSerializer =
                    new TupleSerializer<>(
                            typedTuple, new TypeSerializer[] {windowSerializer, windowSerializer});

            final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
                    new ListStateDescriptor<>("merging-window-set", tupleSerializer);

            // get the state that stores the merging sets
            mergingSetsState =
                    getOrCreateKeyedState(
                            VoidNamespaceSerializer.INSTANCE.createInstance(),
                            VoidNamespaceSerializer.INSTANCE,
                            mergingSetsStateDescriptor);
            mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);
        }

        triggerContext =
                new WindowTriggerContext<>(
                        null, null, this, internalTimerService, trigger, windowSerializer);
        windowAssignerContext =
                new WindowAssigner.WindowAssignerContext() {
                    @Override
                    public long getCurrentProcessingTime() {
                        return internalTimerService.currentProcessingTime();
                    }
                };
        windowFunctionContext =
                new DefaultOneInputWindowContext<>(
                        null,
                        windowState,
                        windowProcessFunction,
                        this,
                        windowSerializer,
                        windowMergingState != null);
    }

    @Override
    public void close() throws Exception {
        super.close();
        triggerContext = null;
        windowFunctionContext = null;
        windowAssignerContext = null;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        element.getValue(), element.getTimestamp(), windowAssignerContext);

        // if element is handled by none of assigned elementWindows
        boolean isSkippedElement = true;

        final K key = (K) this.getCurrentKey();

        if (windowAssigner instanceof MergingWindowAssigner) {
            MergingWindowSet<W> mergingWindows = getMergingWindowSet();

            for (W window : elementWindows) {

                // adding the new window might result in a merge, in that case the actualWindow
                // is the merged window and we work with that. If we don't merge then
                // actualWindow == window
                W actualWindow =
                        mergingWindows.addWindow(
                                window,
                                new MergingWindowSet.MergeFunction<>() {
                                    @Override
                                    public void merge(
                                            W mergeResult,
                                            Collection<W> mergedWindows,
                                            W stateWindowResult,
                                            Collection<W> mergedStateWindows)
                                            throws Exception {

                                        if ((windowAssigner.isEventTime()
                                                && mergeResult.maxTimestamp() + allowedLateness
                                                        <= internalTimerService
                                                                .currentWatermark())) {
                                            throw new UnsupportedOperationException(
                                                    "The end timestamp of an "
                                                            + "event-time window cannot become earlier than the current watermark "
                                                            + "by merging. Current event time: "
                                                            + internalTimerService
                                                                    .currentWatermark()
                                                            + " window: "
                                                            + mergeResult);
                                        } else if (!windowAssigner.isEventTime()) {
                                            long currentProcessingTime =
                                                    internalTimerService.currentProcessingTime();
                                            if (mergeResult.maxTimestamp()
                                                    <= currentProcessingTime) {
                                                throw new UnsupportedOperationException(
                                                        "The end timestamp of a "
                                                                + "processing-time window cannot become earlier than the current processing time "
                                                                + "by merging. Current processing time: "
                                                                + currentProcessingTime
                                                                + " window: "
                                                                + mergeResult);
                                            }
                                        }

                                        triggerContext.setKey(key);
                                        triggerContext.setWindow(mergeResult);

                                        triggerContext.onMerge(mergedWindows);

                                        for (W m : mergedWindows) {
                                            triggerContext.setWindow(m);
                                            triggerContext.clear();
                                            WindowUtils.deleteCleanupTimer(
                                                    m,
                                                    windowAssigner,
                                                    triggerContext,
                                                    allowedLateness);
                                        }

                                        // merge the merged state windows into the newly resulting
                                        // state window
                                        windowMergingState.mergeNamespaces(
                                                stateWindowResult, mergedStateWindows);
                                    }
                                });

                // drop if the window is already late
                if (WindowUtils.isWindowLate(
                        actualWindow, windowAssigner, internalTimerService, allowedLateness)) {
                    mergingWindows.retireWindow(actualWindow);
                    continue;
                }
                isSkippedElement = false;

                W stateWindow = mergingWindows.getStateWindow(actualWindow);
                if (stateWindow == null) {
                    throw new IllegalStateException(
                            "Window " + window + " is not in in-flight window set.");
                }

                windowState.setCurrentNamespace(stateWindow);
                outputCollector.setTimestamp(window.maxTimestamp());
                windowFunctionContext.setWindow(window);
                windowProcessFunction.onRecord(
                        element.getValue(),
                        outputCollector,
                        partitionedContext,
                        windowFunctionContext);

                triggerContext.setKey(key);
                triggerContext.setWindow(actualWindow);

                TriggerResult triggerResult = triggerContext.onElement(element);

                if (triggerResult.isFire()) {
                    emitWindowContents(actualWindow);
                }

                if (triggerResult.isPurge()) {
                    windowState.clear();
                }

                WindowUtils.registerCleanupTimer(
                        actualWindow, windowAssigner, triggerContext, allowedLateness);
            }

            // need to make sure to update the merging state in state
            mergingWindows.persist();
        } else {
            for (W window : elementWindows) {

                // drop if the window is already late
                if (WindowUtils.isWindowLate(
                        window, windowAssigner, internalTimerService, allowedLateness)) {
                    continue;
                }
                isSkippedElement = false;

                windowState.setCurrentNamespace(window);
                outputCollector.setTimestamp(window.maxTimestamp());
                windowFunctionContext.setWindow(window);
                windowProcessFunction.onRecord(
                        element.getValue(),
                        outputCollector,
                        partitionedContext,
                        windowFunctionContext);

                triggerContext.setKey(key);
                triggerContext.setWindow(window);

                TriggerResult triggerResult = triggerContext.onElement(element);

                if (triggerResult.isFire()) {
                    emitWindowContents(window);
                }

                if (triggerResult.isPurge()) {
                    windowState.clear();
                }

                WindowUtils.registerCleanupTimer(
                        window, windowAssigner, triggerContext, allowedLateness);
            }
        }

        // side output input event if element not handled by any window late arriving tag has been
        // set windowAssigner is event time and current timestamp + allowed lateness no less than
        // element timestamp.
        if (isSkippedElement
                && WindowUtils.isElementLate(
                        element, windowAssigner, allowedLateness, internalTimerService)) {
            windowProcessFunction.onLateRecord(
                    element.getValue(), outputCollector, partitionedContext);
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.setKey(timer.getKey());
        triggerContext.setWindow(timer.getNamespace());

        MergingWindowSet<W> mergingWindows;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.getWindow());
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // trigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                windowState.setCurrentNamespace(stateWindow);
            }
        } else {
            windowState.setCurrentNamespace(triggerContext.getWindow());
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            emitWindowContents(triggerContext.getWindow());
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        if (windowAssigner.isEventTime()
                && WindowUtils.isCleanupTime(
                        triggerContext.getWindow(),
                        timer.getTimestamp(),
                        windowAssigner,
                        allowedLateness)) {
            clearAllState(triggerContext.getWindow(), windowState, mergingWindows);
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.setKey(timer.getKey());
        triggerContext.setWindow(timer.getNamespace());

        MergingWindowSet<W> mergingWindows;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.getWindow());
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // trigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                windowState.setCurrentNamespace(stateWindow);
            }
        } else {
            windowState.setCurrentNamespace(triggerContext.getWindow());
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            emitWindowContents(triggerContext.getWindow());
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        if (!windowAssigner.isEventTime()
                && WindowUtils.isCleanupTime(
                        triggerContext.getWindow(),
                        timer.getTimestamp(),
                        windowAssigner,
                        allowedLateness)) {
            clearAllState(triggerContext.getWindow(), windowState, mergingWindows);
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    @Override
    protected ProcessingTimeManager getProcessingTimeManager() {
        // we don't support user utilize processing time in window operators
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    /**
     * Drops all state for the given window and calls {@link Trigger#clear(Window,
     * Trigger.TriggerContext)}.
     *
     * <p>The caller must ensure that the correct key is set in the state backend and the
     * triggerContext object.
     */
    private void clearAllState(
            W window,
            AppendingState<IN, StateIterator<IN>, Iterable<IN>> windowState,
            MergingWindowSet<W> mergingWindows)
            throws Exception {
        windowState.clear();
        triggerContext.clear();
        windowFunctionContext.setWindow(window);
        windowProcessFunction.onClear(outputCollector, partitionedContext, windowFunctionContext);
        if (mergingWindows != null) {
            mergingWindows.retireWindow(window);
            mergingWindows.persist();
        }
    }

    /** Emits the contents of the given window using the {@link WindowProcessFunction}. */
    private void emitWindowContents(W window) throws Exception {
        // only time window touch the time concept.
        outputCollector.setTimestamp(window.maxTimestamp());
        windowFunctionContext.setWindow(window);
        windowProcessFunction.onTrigger(outputCollector, partitionedContext, windowFunctionContext);
    }

    /**
     * Retrieves the {@link MergingWindowSet} for the currently active key. The caller must ensure
     * that the correct key is set in the state backend.
     *
     * <p>The caller must also ensure to properly persist changes to state using {@link
     * MergingWindowSet#persist()}.
     */
    protected MergingWindowSet<W> getMergingWindowSet() throws Exception {

        MergingWindowAssigner<? super IN, W> mergingAssigner =
                (MergingWindowAssigner<? super IN, W>) windowAssigner;
        return new MergingWindowSet<>(mergingAssigner, mergingSetsState);
    }
}
