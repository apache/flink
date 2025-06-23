/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.v2.AppendingState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.declare.state.StateWithDeclaredNamespace;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.runtime.asyncprocessing.operators.TimestampedCollectorWithDeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.internal.InternalAppendingState;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.runtime.state.v2.internal.InternalMergingState;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner.WindowAssignerContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.asyncprocessing.declare.state.StateWithDeclaredNamespace.create;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and {@link
 * AsyncTrigger}. This is the async state access version of WindowOperator.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element is
 * put into panes. A pane is the bucket of elements that have the same key and same {@code Window}.
 * An element can be in multiple panes if it was assigned to multiple windows by the {@code
 * WindowAssigner}.
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires, the given
 * {@link InternalWindowFunction} is invoked to produce the results that are emitted for the pane to
 * which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class AsyncWindowOperator<K, IN, ACC, OUT, W extends Window>
        extends AbstractAsyncStateUdfStreamOperator<
                OUT, InternalAsyncWindowFunction<ACC, OUT, K, W>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncWindowOperator.class);

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------

    protected final WindowAssigner<? super IN, W> windowAssigner;

    private final KeySelector<IN, K> keySelector;

    private final AsyncTrigger<? super IN, ? super W> trigger;

    private final StateDescriptor<?> windowStateDescriptor;

    /** For serializing the key in checkpoints. */
    protected final TypeSerializer<K> keySerializer;

    /** For serializing the window in checkpoints. */
    protected final TypeSerializer<W> windowSerializer;

    /**
     * The allowed lateness for elements. This is used for:
     *
     * <ul>
     *   <li>Deciding if an element should be dropped from a window due to lateness.
     *   <li>Clearing the state of a window if the system time passes the {@code window.maxTimestamp
     *       + allowedLateness} landmark.
     * </ul>
     */
    protected final long allowedLateness;

    /**
     * {@link OutputTag} to use for late arriving events. Elements for which {@code
     * window.maxTimestamp + allowedLateness} is smaller than the current watermark will be emitted
     * to this.
     */
    protected final OutputTag<IN> lateDataOutputTag;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    protected transient Counter numLateRecordsDropped;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    /** The state in which the window contents is stored. Each window is a namespace */
    private transient InternalAppendingState<K, W, IN, ACC, ACC, ACC> windowState;

    /**
     * The {@link #windowState}, typed to merging state for merging windows. Null if the window
     * state is not mergeable. TODO: Not support currently.
     */
    private transient InternalMergingState<K, W, IN, ACC, ACC, ACC> windowMergingState;

    /**
     * The state that holds the merging window metadata (the sets that describe what is merged).
     * TODO: Not support currently.
     */
    private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

    /**
     * This is given to the {@code InternalWindowFunction} for emitting elements with a given
     * timestamp.
     */
    protected transient TimestampedCollectorWithDeclaredVariable<OUT> timestampedCollector;

    protected transient Context triggerContext;

    protected transient WindowContext processContext;

    protected transient DeclaredVariable<W> windowDeclaredVariable;

    protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;

    // ------------------------------------------------------------------------
    // State that needs to be checkpointed
    // ------------------------------------------------------------------------

    protected transient InternalTimerService<W> internalTimerService;

    /** Creates a new {@code WindowOperator} based on the given policies and user functions. */
    public AsyncWindowOperator(
            WindowAssigner<? super IN, W> windowAssigner,
            TypeSerializer<W> windowSerializer,
            KeySelector<IN, K> keySelector,
            TypeSerializer<K> keySerializer,
            StateDescriptor<?> windowStateDescriptor,
            InternalAsyncWindowFunction<ACC, OUT, K, W> windowFunction,
            AsyncTrigger<? super IN, ? super W> trigger,
            long allowedLateness,
            OutputTag<IN> lateDataOutputTag) {

        super(windowFunction);

        checkArgument(allowedLateness >= 0);

        checkArgument(
                windowStateDescriptor == null || windowStateDescriptor.isSerializerInitialized(),
                "window state serializer is not properly initialized");

        this.windowAssigner = checkNotNull(windowAssigner);
        this.windowSerializer = checkNotNull(windowSerializer);
        this.keySelector = checkNotNull(keySelector);
        this.keySerializer = checkNotNull(keySerializer);
        this.windowStateDescriptor = windowStateDescriptor;
        this.trigger = checkNotNull(trigger);
        this.allowedLateness = allowedLateness;
        this.lateDataOutputTag = lateDataOutputTag;

        LOG.info("Initialize async window operator with trigger " + trigger);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void open() throws Exception {
        super.open();
        Preconditions.checkNotNull(
                getAsyncKeyedStateBackend(), "Async state backend is not properly set.");

        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
        timestampedCollector =
                new TimestampedCollectorWithDeclaredVariable<>(output, declarationContext);

        internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);

        windowDeclaredVariable =
                declarationContext.declareVariable(
                        windowSerializer,
                        "_AsyncWindowOperator$window",
                        windowSerializer::createInstance);

        triggerContext = new Context(windowDeclaredVariable);
        processContext = new WindowContext(windowDeclaredVariable);

        windowAssignerContext =
                new WindowAssigner.WindowAssignerContext() {
                    @Override
                    public long getCurrentProcessingTime() {
                        return internalTimerService.currentProcessingTime();
                    }
                };

        // create (or restore) the state that hold the actual window contents
        // NOTE - the state may be null in the case of the overriding evicting window operator
        if (windowStateDescriptor != null) {
            windowState =
                    create(
                            getAsyncKeyedStateBackend()
                                    .getOrCreateKeyedState(
                                            windowSerializer.createInstance(),
                                            windowSerializer,
                                            windowStateDescriptor),
                            windowDeclaredVariable);
        }

        // create the typed and helper states for merging windows
        if (windowAssigner instanceof MergingWindowAssigner) {
            throw new UnsupportedOperationException(
                    "Async WindowOperator not support merging window (e.g. session window) yet.");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        timestampedCollector = null;
        triggerContext = null;
        processContext = null;
        windowAssignerContext = null;
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
                        windowState
                                .asyncAdd(element.getValue())
                                .thenCompose(
                                        ignore ->
                                                triggerContext
                                                        .onElement(element)
                                                        .thenAccept(triggerResult::set))
                                .thenConditionallyCompose(
                                        ignore -> triggerResult.get().isFire(),
                                        ignore ->
                                                windowState
                                                        .asyncGet()
                                                        .thenConditionallyCompose(
                                                                Objects::nonNull,
                                                                contents ->
                                                                        emitWindowContents(
                                                                                key, window,
                                                                                contents)))
                                .thenConditionallyCompose(
                                        ignore -> triggerResult.get().isPurge(),
                                        ignore -> windowState.asyncClear())
                                .thenAccept(
                                        empty -> {
                                            registerCleanupTimer(window);
                                        }));
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
                                    windowState
                                            .asyncGet()
                                            .thenConditionallyCompose(
                                                    Objects::nonNull,
                                                    contents ->
                                                            emitWindowContents(
                                                                    timer.getKey(),
                                                                    timer.getNamespace(),
                                                                    contents)))
                    .thenConditionallyCompose(
                            ignore -> triggerResult.get().isPurge(),
                            ignore -> windowState.asyncClear())
                    .thenConditionallyCompose(
                            ignore ->
                                    windowAssigner.isEventTime()
                                            && isCleanupTime(
                                                    timer.getNamespace(), timer.getTimestamp()),
                            ignore -> clearAllState(timer.getNamespace(), windowState));
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
                                    windowState
                                            .asyncGet()
                                            .thenConditionallyCompose(
                                                    Objects::nonNull,
                                                    contents ->
                                                            emitWindowContents(
                                                                    timer.getKey(),
                                                                    timer.getNamespace(),
                                                                    contents)))
                    .thenConditionallyCompose(
                            ignore -> triggerResult.get().isPurge(),
                            ignore -> windowState.asyncClear())
                    .thenConditionallyCompose(
                            ignore ->
                                    !windowAssigner.isEventTime()
                                            && isCleanupTime(
                                                    timer.getNamespace(), timer.getTimestamp()),
                            ignore -> clearAllState(timer.getNamespace(), windowState));
        }
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        boolean isOutputOnlyAfterEndOfStream =
                windowAssigner instanceof GlobalWindows && trigger.isEndOfStreamTrigger();
        return new OperatorAttributesBuilder()
                .setOutputOnlyAfterEndOfStream(isOutputOnlyAfterEndOfStream)
                .build();
    }

    /**
     * Drops all state for the given window and calls {@link AsyncTrigger#clear(Window,
     * AsyncTrigger.TriggerContext)}.
     *
     * <p>The caller must ensure that the correct key is set in the state backend and the
     * triggerContext object.
     */
    private StateFuture<Void> clearAllState(W window, AppendingState<IN, ACC, ACC> windowState) {
        return windowState
                .asyncClear()
                .thenCompose(ignore -> triggerContext.clear())
                .thenCompose(
                        ignore -> {
                            windowDeclaredVariable.set(window);
                            return processContext.clear();
                        });
    }

    /** Emits the contents of the given window using the {@link InternalWindowFunction}. */
    @SuppressWarnings("unchecked")
    private StateFuture<Void> emitWindowContents(K key, W window, ACC contents) throws Exception {
        timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
        windowDeclaredVariable.set(window);
        return userFunction.process(key, window, processContext, contents, timestampedCollector);
    }

    /**
     * Write skipped late arriving element to SideOutput.
     *
     * @param element skipped late arriving element to side output
     */
    protected void sideOutput(StreamRecord<IN> element) {
        output.collect(lateDataOutputTag, element);
    }

    /**
     * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
     * the given window.
     */
    protected boolean isWindowLate(W window) {
        return (windowAssigner.isEventTime()
                && (cleanupTime(window) <= internalTimerService.currentWatermark()));
    }

    /**
     * Decide if a record is currently late, based on current watermark and allowed lateness.
     *
     * @param element The element to check
     * @return The element for which should be considered when sideoutputs
     */
    protected boolean isElementLate(StreamRecord<IN> element) {
        return (windowAssigner.isEventTime())
                && (element.getTimestamp() + allowedLateness
                        <= internalTimerService.currentWatermark());
    }

    /**
     * Registers a timer to cleanup the content of the window.
     *
     * @param window the window whose state to discard
     */
    protected void registerCleanupTimer(W window) {
        long cleanupTime = cleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // don't set a GC timer for "end of time"
            return;
        }

        if (windowAssigner.isEventTime()) {
            triggerContext.registerEventTimeTimer(cleanupTime);
        } else {
            triggerContext.registerProcessingTimeTimer(cleanupTime);
        }
    }

    /**
     * Deletes the cleanup timer set for the contents of the provided window.
     *
     * @param window the window whose state to discard
     */
    protected void deleteCleanupTimer(W window) {
        long cleanupTime = cleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // no need to clean up because we didn't set one
            return;
        }
        if (windowAssigner.isEventTime()) {
            triggerContext.deleteEventTimeTimer(cleanupTime);
        } else {
            triggerContext.deleteProcessingTimeTimer(cleanupTime);
        }
    }

    /**
     * Returns the cleanup time for a window, which is {@code window.maxTimestamp +
     * allowedLateness}. In case this leads to a value greater than {@link Long#MAX_VALUE} then a
     * cleanup time of {@link Long#MAX_VALUE} is returned.
     *
     * @param window the window whose cleanup time we are computing.
     */
    private long cleanupTime(W window) {
        if (windowAssigner.isEventTime()) {
            long cleanupTime = window.maxTimestamp() + allowedLateness;
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return window.maxTimestamp();
        }
    }

    /** Returns {@code true} if the given time is the cleanup time for the given window. */
    protected final boolean isCleanupTime(W window, long time) {
        return time == cleanupTime(window);
    }

    /**
     * Base class for per-window {@link KeyedStateStore KeyedStateStores}. Used to allow per-window
     * state access for {@link
     * org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
     */
    public abstract class AbstractPerWindowStateStore extends DefaultKeyedStateStore {

        // we have this in the base class even though it's not used in MergingKeyStore so that
        // we can always set it and ignore what actual implementation we have
        protected final DeclaredVariable<W> window;

        public AbstractPerWindowStateStore(
                AsyncKeyedStateBackend<?> keyedStateBackend,
                ExecutionConfig executionConfig,
                DeclaredVariable<W> window) {
            super(
                    keyedStateBackend,
                    new SerializerFactory() {
                        @Override
                        public <T> TypeSerializer<T> createSerializer(
                                TypeInformation<T> typeInformation) {
                            return typeInformation.createSerializer(
                                    executionConfig.getSerializerConfig());
                        }
                    });
            this.window = window;
        }
    }

    /**
     * Regular per-window state store for use with {@link
     * org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
     */
    public class PerWindowStateStore extends AbstractPerWindowStateStore {

        public PerWindowStateStore(
                AsyncKeyedStateBackend<?> keyedStateBackend,
                ExecutionConfig executionConfig,
                DeclaredVariable<W> window) {
            super(keyedStateBackend, executionConfig, window);
        }

        @Override
        protected <S extends State, SV> S getPartitionedState(StateDescriptor<SV> stateDescriptor)
                throws Exception {
            return create(
                    asyncKeyedStateBackend.getOrCreateKeyedState(
                            window.get(), windowSerializer, stateDescriptor),
                    window);
        }
    }

    /**
     * A utility class for handling {@code ProcessWindowFunction} invocations. This can be reused by
     * setting the {@code key} and {@code window} fields. No internal state must be kept in the
     * {@code WindowContext}.
     */
    public class WindowContext implements InternalAsyncWindowFunction.InternalWindowContext {
        protected final DeclaredVariable<W> window;

        protected AbstractPerWindowStateStore windowState;

        public WindowContext(DeclaredVariable<W> window) {
            this.window = window;
            // TODO: support merging window.
            this.windowState =
                    new PerWindowStateStore(
                            getAsyncKeyedStateBackend(), getExecutionConfig(), window);
        }

        @Override
        public String toString() {
            return "WindowContext{Window = " + window.toString() + "}";
        }

        public StateFuture<Void> clear() throws Exception {
            return userFunction.clear(window.get(), this);
        }

        @Override
        public long currentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public KeyedStateStore windowState() {
            return this.windowState;
        }

        @Override
        public KeyedStateStore globalState() {
            return AsyncWindowOperator.this.getKeyedStateStore();
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }
            output.collect(outputTag, new StreamRecord<>(value, window.get().maxTimestamp()));
        }
    }

    /**
     * {@code Context} is a utility for handling {@code AsyncTrigger} invocations. It can be reused
     * by setting the {@code key} and {@code window} fields. No internal state must be kept in the
     * {@code Context}
     */
    public class Context implements AsyncTrigger.OnMergeContext {
        protected DeclaredVariable<W> window;

        public Context(DeclaredVariable<W> window) {
            this.window = window;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return AsyncWindowOperator.this.getMetricGroup();
        }

        public long getCurrentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @SuppressWarnings("unchecked")
        public <T, S extends State> S getPartitionedState(StateDescriptor<T> stateDescriptor) {
            try {
                return StateWithDeclaredNamespace.create(
                        getAsyncKeyedStateBackend()
                                .getOrCreateKeyedState(
                                        window.get(), windowSerializer, stateDescriptor),
                        window);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }

        @Override
        public <T> void mergePartitionedState(StateDescriptor<T> stateDescriptor) {
            throw new UnsupportedOperationException("Merging window not supported yet");
        }

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            internalTimerService.registerProcessingTimeTimer(window.get(), time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            internalTimerService.registerEventTimeTimer(window.get(), time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            internalTimerService.deleteProcessingTimeTimer(window.get(), time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            internalTimerService.deleteEventTimeTimer(window.get(), time);
        }

        public StateFuture<TriggerResult> onElement(StreamRecord<IN> element) throws Exception {
            return trigger.onElement(
                    element.getValue(), element.getTimestamp(), window.get(), this);
        }

        public StateFuture<TriggerResult> onProcessingTime(long time) throws Exception {
            return trigger.onProcessingTime(time, window.get(), this);
        }

        public StateFuture<TriggerResult> onEventTime(long time) throws Exception {
            return trigger.onEventTime(time, window.get(), this);
        }

        public void onMerge(Collection<W> mergedWindows) throws Exception {
            throw new UnsupportedOperationException("Merging window not support yet");
        }

        public StateFuture<Void> clear() throws Exception {
            return trigger.clear(window.get(), this);
        }

        @Override
        public String toString() {
            return "Context{window=" + window + '}';
        }
    }

    // ------------------------------------------------------------------------
    // Getters for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    public AsyncTrigger<? super IN, ? super W> getTrigger() {
        return trigger;
    }

    @VisibleForTesting
    public KeySelector<IN, K> getKeySelector() {
        return keySelector;
    }

    @VisibleForTesting
    public WindowAssigner<? super IN, W> getWindowAssigner() {
        return windowAssigner;
    }

    @VisibleForTesting
    public StateDescriptor<?> getStateDescriptor() {
        return windowStateDescriptor;
    }

    @VisibleForTesting
    public WindowContext getProcessContext() {
        return processContext;
    }

    @VisibleForTesting
    public WindowAssignerContext getWindowAssignerContext() {
        return windowAssignerContext;
    }
}
