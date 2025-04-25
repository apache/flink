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
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonBroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.datastream.impl.extension.window.context.DefaultTwoInputWindowContext;
import org.apache.flink.datastream.impl.extension.window.context.WindowTriggerContext;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.utils.WindowUtils;
import org.apache.flink.datastream.impl.operators.BaseKeyedTwoInputNonBroadcastProcessOperator;
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
import org.apache.flink.util.TaggedUnion;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Operator for {@link TwoInputNonBroadcastWindowStreamProcessFunction} in {@link
 * KeyedPartitionStream}.
 */
public class TwoInputNonBroadcastWindowProcessOperator<K, IN1, IN2, OUT, W extends Window>
        extends BaseKeyedTwoInputNonBroadcastProcessOperator<K, IN1, IN2, OUT>
        implements Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------

    private final TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT>
            windowProcessFunction;

    /**
     * The allowed lateness for elements. This is used for:
     *
     * <ul>
     *   <li>Deciding if an element should be dropped from a window due to lateness.
     *   <li>Clearing the state of a window if the time out-of the {@code window.maxTimestamp +
     *       allowedLateness} landmark.
     * </ul>
     */
    protected final long allowedLateness;

    // ------------------------------------------------------------------------
    // Operator components
    // ------------------------------------------------------------------------

    protected transient InternalTimerService<W> internalTimerService;

    /** For serializing the window in checkpoints. */
    private final TypeSerializer<W> windowSerializer;

    // ------------------------------------------------------------------------
    // Window assigner and trigger
    // ------------------------------------------------------------------------

    private final WindowAssigner<? super TaggedUnion<IN1, IN2>, W> windowAssigner;

    private transient WindowAssigner.WindowAssignerContext windowAssignerContext;

    private final Trigger<? super TaggedUnion<IN1, IN2>, ? super W> trigger;

    private transient WindowTriggerContext<K, ? super TaggedUnion<IN1, IN2>, W> triggerContext;

    private transient DefaultTwoInputWindowContext<K, IN1, IN2, W> windowFunctionContext;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    private final StateDescriptor<IN1> leftWindowStateDescriptor;

    private final StateDescriptor<IN2> rightWindowStateDescriptor;

    /**
     * The state in which the window contents from the left input are stored. Each window is a
     * namespace.
     */
    private transient InternalAppendingState<K, W, IN1, IN1, StateIterator<IN1>, Iterable<IN1>>
            leftWindowState;

    /**
     * The state in which the window contents from the right input are stored. Each window is a
     * namespace
     */
    private transient InternalAppendingState<K, W, IN2, IN2, StateIterator<IN2>, Iterable<IN2>>
            rightWindowState;

    /**
     * The {@link #leftWindowState}, typed to merging state for merging windows. Null if the window
     * state is not mergeable.
     */
    private transient InternalMergingState<K, W, IN1, IN1, StateIterator<IN1>, Iterable<IN1>>
            leftWindowMergingState;

    /**
     * The {@link #rightWindowState}, typed to merging state for merging windows. Null if the window
     * state is not mergeable.
     */
    private transient InternalMergingState<K, W, IN2, IN2, StateIterator<IN2>, Iterable<IN2>>
            rightWindowMergingState;

    /** The state that holds the merging window metadata (the sets that describe what is merged). */
    private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

    public TwoInputNonBroadcastWindowProcessOperator(
            InternalTwoInputWindowStreamProcessFunction<IN1, IN2, OUT, W> windowFunction,
            WindowAssigner<? super TaggedUnion<IN1, IN2>, W> windowAssigner,
            Trigger<? super TaggedUnion<IN1, IN2>, ? super W> trigger,
            TypeSerializer<W> windowSerializer,
            StateDescriptor<IN1> leftWindowStateDescriptor,
            StateDescriptor<IN2> rightWindowStateDescriptor,
            long allowedLateness) {
        super(windowFunction);
        checkArgument(allowedLateness >= 0);
        this.windowProcessFunction = windowFunction.getWindowProcessFunction();
        this.windowAssigner = windowAssigner;
        this.trigger = trigger;
        this.windowSerializer = windowSerializer;
        this.leftWindowStateDescriptor = leftWindowStateDescriptor;
        this.rightWindowStateDescriptor = rightWindowStateDescriptor;
        this.allowedLateness = allowedLateness;
    }

    @Override
    public void open() throws Exception {
        super.open();
        internalTimerService =
                getInternalTimerService("process-window-timers", windowSerializer, this);

        // create (or restore) the state that hold the actual window contents
        // NOTE - the state may be null in the case of the overriding evicting window operator
        if (leftWindowStateDescriptor != null) {
            leftWindowState =
                    getOrCreateKeyedState(
                            windowSerializer.createInstance(),
                            windowSerializer,
                            leftWindowStateDescriptor);
        }

        if (rightWindowStateDescriptor != null) {
            rightWindowState =
                    getOrCreateKeyedState(
                            windowSerializer.createInstance(),
                            windowSerializer,
                            rightWindowStateDescriptor);
        }

        // create the typed and helper states for merging windows
        if (windowAssigner instanceof MergingWindowAssigner) {

            // store a typed reference for the state of merging windows - sanity check
            if (leftWindowState instanceof InternalMergingState) {
                leftWindowMergingState =
                        (InternalMergingState<K, W, IN1, IN1, StateIterator<IN1>, Iterable<IN1>>)
                                leftWindowState;
            } else if (leftWindowState != null) {
                throw new IllegalStateException(
                        "The window uses a merging assigner, but the window state is not mergeable.");
            }

            if (rightWindowState instanceof InternalMergingState) {
                rightWindowMergingState =
                        (InternalMergingState<K, W, IN2, IN2, StateIterator<IN2>, Iterable<IN2>>)
                                rightWindowState;
            } else if (rightWindowState != null) {
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
                new DefaultTwoInputWindowContext<>(
                        null,
                        leftWindowState,
                        rightWindowState,
                        windowProcessFunction,
                        this,
                        windowSerializer,
                        leftWindowMergingState != null);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        TaggedUnion.one(element.getValue()),
                        element.getTimestamp(),
                        windowAssignerContext);

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
                                        leftWindowMergingState.mergeNamespaces(
                                                stateWindowResult, mergedStateWindows);
                                        rightWindowMergingState.mergeNamespaces(
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

                leftWindowState.setCurrentNamespace(stateWindow);
                collector.setTimestamp(window.maxTimestamp());
                windowFunctionContext.setWindow(window);
                windowProcessFunction.onRecord1(
                        element.getValue(), collector, partitionedContext, windowFunctionContext);

                triggerContext.setKey(key);
                triggerContext.setWindow(actualWindow);

                TriggerResult triggerResult =
                        triggerContext.onElement(
                                new StreamRecord<>(
                                        TaggedUnion.one(element.getValue()),
                                        element.getTimestamp()));

                if (triggerResult.isFire()) {
                    emitWindowContents(actualWindow);
                }

                if (triggerResult.isPurge()) {
                    leftWindowState.clear();
                    rightWindowState.clear();
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

                leftWindowState.setCurrentNamespace(window);
                collector.setTimestamp(window.maxTimestamp());
                windowFunctionContext.setWindow(window);
                windowProcessFunction.onRecord1(
                        element.getValue(), collector, partitionedContext, windowFunctionContext);

                triggerContext.setKey(key);
                triggerContext.setWindow(window);

                TriggerResult triggerResult =
                        triggerContext.onElement(
                                new StreamRecord<>(
                                        TaggedUnion.one(element.getValue()),
                                        element.getTimestamp()));

                if (triggerResult.isFire()) {
                    emitWindowContents(window);
                }

                if (triggerResult.isPurge()) {
                    leftWindowState.clear();
                    rightWindowState.clear();
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
            windowProcessFunction.onLateRecord1(element.getValue(), collector, partitionedContext);
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        TaggedUnion.two(element.getValue()),
                        element.getTimestamp(),
                        windowAssignerContext);

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
                                        leftWindowMergingState.mergeNamespaces(
                                                stateWindowResult, mergedStateWindows);
                                        rightWindowMergingState.mergeNamespaces(
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

                rightWindowState.setCurrentNamespace(stateWindow);
                collector.setTimestamp(window.maxTimestamp());
                windowFunctionContext.setWindow(window);
                windowProcessFunction.onRecord2(
                        element.getValue(), collector, partitionedContext, windowFunctionContext);

                triggerContext.setKey(key);
                triggerContext.setWindow(actualWindow);

                TriggerResult triggerResult =
                        triggerContext.onElement(
                                new StreamRecord<>(
                                        TaggedUnion.two(element.getValue()),
                                        element.getTimestamp()));

                if (triggerResult.isFire()) {
                    emitWindowContents(actualWindow);
                }

                if (triggerResult.isPurge()) {
                    leftWindowState.clear();
                    rightWindowState.clear();
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

                rightWindowState.setCurrentNamespace(window);
                collector.setTimestamp(window.maxTimestamp());
                windowFunctionContext.setWindow(window);
                windowProcessFunction.onRecord2(
                        element.getValue(), collector, partitionedContext, windowFunctionContext);

                triggerContext.setKey(key);
                triggerContext.setWindow(window);

                TriggerResult triggerResult =
                        triggerContext.onElement(
                                new StreamRecord<>(
                                        TaggedUnion.two(element.getValue()),
                                        element.getTimestamp()));

                if (triggerResult.isFire()) {
                    emitWindowContents(window);
                }

                if (triggerResult.isPurge()) {
                    leftWindowState.clear();
                    rightWindowState.clear();
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
            windowProcessFunction.onLateRecord2(element.getValue(), collector, partitionedContext);
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
                leftWindowState.setCurrentNamespace(stateWindow);
                rightWindowState.setCurrentNamespace(stateWindow);
            }
        } else {
            leftWindowState.setCurrentNamespace(triggerContext.getWindow());
            rightWindowState.setCurrentNamespace(triggerContext.getWindow());
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            emitWindowContents(triggerContext.getWindow());
        }

        if (triggerResult.isPurge()) {
            leftWindowState.clear();
            rightWindowState.clear();
        }

        if (windowAssigner.isEventTime()
                && WindowUtils.isCleanupTime(
                        triggerContext.getWindow(),
                        timer.getTimestamp(),
                        windowAssigner,
                        allowedLateness)) {
            clearAllState(
                    triggerContext.getWindow(), leftWindowState, rightWindowState, mergingWindows);
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
                leftWindowState.setCurrentNamespace(stateWindow);
                rightWindowState.setCurrentNamespace(stateWindow);
            }
        } else {
            leftWindowState.setCurrentNamespace(triggerContext.getWindow());
            rightWindowState.setCurrentNamespace(triggerContext.getWindow());
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            emitWindowContents(triggerContext.getWindow());
        }

        if (triggerResult.isPurge()) {
            leftWindowState.clear();
            rightWindowState.clear();
        }

        if (!windowAssigner.isEventTime()
                && WindowUtils.isCleanupTime(
                        triggerContext.getWindow(),
                        timer.getTimestamp(),
                        windowAssigner,
                        allowedLateness)) {
            clearAllState(
                    triggerContext.getWindow(), leftWindowState, rightWindowState, mergingWindows);
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
            AppendingState<IN1, StateIterator<IN1>, Iterable<IN1>> leftWindowState,
            AppendingState<IN2, StateIterator<IN2>, Iterable<IN2>> rightWindowState,
            MergingWindowSet<W> mergingWindows)
            throws Exception {
        leftWindowState.clear();
        rightWindowState.clear();
        triggerContext.clear();
        windowFunctionContext.setWindow(window);
        windowProcessFunction.onClear(collector, partitionedContext, windowFunctionContext);
        if (mergingWindows != null) {
            mergingWindows.retireWindow(window);
            mergingWindows.persist();
        }
    }

    /**
     * Emits the contents of the given window using the user-defined {@link
     * TwoInputNonBroadcastWindowStreamProcessFunction}.
     */
    private void emitWindowContents(W window) throws Exception {
        // only time window touch the time concept.
        collector.setTimestamp(window.maxTimestamp());
        windowFunctionContext.setWindow(window);
        windowProcessFunction.onTrigger(collector, partitionedContext, windowFunctionContext);
    }

    /**
     * Retrieves the {@link MergingWindowSet} for the currently active key. The caller must ensure
     * that the correct key is set in the state backend.
     *
     * <p>The caller must also ensure to properly persist changes to state using {@link
     * MergingWindowSet#persist()}.
     */
    protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
        MergingWindowAssigner<? super TaggedUnion<IN1, IN2>, W> mergingAssigner =
                (MergingWindowAssigner<? super TaggedUnion<IN1, IN2>, W>) windowAssigner;
        return new MergingWindowSet<>(mergingAssigner, mergingSetsState);
    }
}
