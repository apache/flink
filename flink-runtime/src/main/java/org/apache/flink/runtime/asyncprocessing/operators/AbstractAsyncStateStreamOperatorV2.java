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

package org.apache.flink.runtime.asyncprocessing.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.AsyncException;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateExecutionController;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimerServiceAsyncImpl;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeServiceWithAsyncState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessing;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessingOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.ElementOrder;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This operator is an abstract class that give the {@link AbstractStreamOperatorV2} the ability to
 * perform {@link AsyncKeyOrderedProcessing}. The aim is to make any subclass of {@link
 * AbstractStreamOperatorV2} could manipulate async state with only a change of base class.
 */
@Internal
@SuppressWarnings("rawtypes")
public abstract class AbstractAsyncStateStreamOperatorV2<OUT> extends AbstractStreamOperatorV2<OUT>
        implements AsyncKeyOrderedProcessingOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractAsyncStateStreamOperatorV2.class);

    private final Environment environment;
    private final StreamTask<?, ?> streamTask;
    private StateExecutionController asyncExecutionController;

    /** Act as a cache for {@link #setAsyncKeyedContextElement} and {@link #postProcessElement}. */
    private RecordContext currentProcessingContext;

    protected DeclarationManager declarationManager;

    public AbstractAsyncStateStreamOperatorV2(
            StreamOperatorParameters<OUT> parameters, int numberOfInputs) {
        super(parameters, numberOfInputs);
        this.environment = parameters.getContainingTask().getEnvironment();
        this.streamTask = parameters.getContainingTask();
    }

    /** Initialize necessary state components for {@link AbstractStreamOperatorV2}. */
    @Override
    public final void beforeInitializeStateHandler() {
        KeyedStateStore stateStore = stateHandler.getKeyedStateStore().orElse(null);
        if (stateStore instanceof DefaultKeyedStateStore) {
            ((DefaultKeyedStateStore) stateStore).setSupportKeyedStateApiSetV2();
        }

        final int inFlightRecordsLimit = getExecutionConfig().getAsyncStateTotalBufferSize();
        final int asyncBufferSize = getExecutionConfig().getAsyncStateActiveBufferSize();
        final long asyncBufferTimeout = getExecutionConfig().getAsyncStateActiveBufferTimeout();
        int maxParallelism = getExecutionConfig().getMaxParallelism();

        this.declarationManager = new DeclarationManager();
        if (isAsyncKeyOrderedProcessingEnabled()) {
            AsyncKeyedStateBackend asyncKeyedStateBackend =
                    stateHandler.getAsyncKeyedStateBackend();
            if (asyncKeyedStateBackend != null) {
                this.asyncExecutionController =
                        new StateExecutionController(
                                streamTask
                                        .getMailboxExecutorFactory()
                                        .createExecutor(getOperatorConfig().getChainIndex()),
                                this::handleAsyncException,
                                asyncKeyedStateBackend.createStateExecutor(),
                                declarationManager,
                                EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                                maxParallelism,
                                asyncBufferSize,
                                asyncBufferTimeout,
                                inFlightRecordsLimit,
                                asyncKeyedStateBackend,
                                getMetricGroup().addGroup("asyncStateProcessing"));
                asyncKeyedStateBackend.setup(asyncExecutionController);
                if (asyncKeyedStateBackend instanceof AsyncKeyedStateBackendAdaptor) {
                    LOG.warn(
                            "A normal KeyedStateBackend({}) is used when enabling the async state "
                                    + "processing. Parallel asynchronous processing does not work. "
                                    + "All state access will be processed synchronously.",
                            stateHandler.getKeyedStateBackend());
                }
            } else if (stateHandler.getKeyedStateBackend() != null) {
                throw new UnsupportedOperationException(
                        "Current State Backend doesn't support async access, AsyncExecutionController could not work");
            }
        }
    }

    private void handleAsyncException(String message, Throwable exception) {
        environment.failExternally(new AsyncException(message, exception));
    }

    @Override
    public boolean isAsyncKeyOrderedProcessingEnabled() {
        return true;
    }

    @Override
    public ElementOrder getElementOrder() {
        return ElementOrder.RECORD_ORDER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T> void setAsyncKeyedContextElement(
            StreamRecord<T> record, KeySelector<T, ?> keySelector) throws Exception {
        currentProcessingContext =
                asyncExecutionController.buildContext(
                        record.getValue(), keySelector.getKey(record.getValue()));
        // The processElement will be treated as a callback for dummy request. So reference
        // counting should be maintained.
        // When state request submitted, ref count +1, as described in FLIP-425:
        // To cover the statements without a callback, in addition to the reference count marked
        // in Fig.5, each state request itself is also protected by a paired reference count.
        currentProcessingContext.retain();
        asyncExecutionController.setCurrentContext(currentProcessingContext);
        newKeySelected(currentProcessingContext.getKey());
    }

    /**
     * A hook that will be invoked after a new key is selected. It is not recommended to perform
     * async state here. Only some synchronous logic is suggested.
     *
     * @param newKey the new key selected.
     */
    public void newKeySelected(Object newKey) {
        // By default, do nothing.
        // Subclass could override this method to do some operations when a new key is selected.
    }

    @Override
    protected <T> void internalSetKeyContextElement(
            StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
        // This method is invoked only when isAsyncKeyOrderedProcessingEnabled() is false.
        super.internalSetKeyContextElement(record, selector);
        if (selector != null) {
            newKeySelected(getCurrentKey());
        }
    }

    @Override
    public Object getCurrentKey() {
        if (isAsyncKeyOrderedProcessingEnabled()) {
            RecordContext currentContext = asyncExecutionController.getCurrentContext();
            if (currentContext == null) {
                throw new UnsupportedOperationException(
                        "Have not set the current key yet, this may because the operator has not "
                                + "started to run, or you are invoking this under a non-keyed context.");
            }
            return currentContext.getKey();
        } else {
            return super.getCurrentKey();
        }
    }

    @Override
    public final void postProcessElement() {
        // The processElement will be treated as a callback for dummy request. So reference
        // counting should be maintained.
        // When a state request completes, ref count -1, as described in FLIP-425:
        // To cover the statements without a callback, in addition to the reference count marked
        // in Fig.5, each state request itself is also protected by a paired reference count.
        currentProcessingContext.release();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void preserveRecordOrderAndProcess(ThrowingRunnable<Exception> processing) {
        asyncExecutionController.syncPointRequestWithCallback(processing, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> void asyncProcessWithKey(K key, ThrowingRunnable<Exception> processing) {
        RecordContext<K> oldContext = asyncExecutionController.getCurrentContext();
        // build a context and switch to the new context
        RecordContext<K> newContext = asyncExecutionController.buildContext(null, key, true);
        newContext.retain();
        asyncExecutionController.setCurrentContext(newContext);
        // Same logic as RECORD_ORDER, since FIRST_REQUEST_ORDER is problematic when the call's key
        // pass the same key in.
        asyncExecutionController.syncPointRequestWithCallback(processing, true);
        newContext.release();

        // switch to original context
        asyncExecutionController.setCurrentContext(oldContext);
    }

    @Override
    public final DeclarationManager getDeclarationManager() {
        return declarationManager;
    }

    @Override
    public final <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(int inputId) {
        // The real logic should be in First/SecondInputOfTwoInput#getRecordProcessor.
        throw new UnsupportedOperationException(
                "Never getRecordProcessor from AbstractAsyncStateStreamOperatorV2,"
                        + " since this part is handled by the Input.");
    }

    /**
     * Process a non-record event. This method is used to process events that are not related to
     * records, such as watermarks or latency markers. It is used to ensure that the async state
     * processing is performed in the correct order. Subclasses could override this method to inject
     * some async state processing logic.
     *
     * @param triggerAction the action that will be performed when the event is triggered.
     * @param finalAction the action that will be performed when the event is finished considering
     *     the epoch control.
     */
    protected void processNonRecord(
            @Nullable ThrowingRunnable<? extends Exception> triggerAction,
            @Nullable ThrowingRunnable<? extends Exception> finalAction) {
        asyncExecutionController.processNonRecord(triggerAction, finalAction);
    }

    /** Create new state (v2) based on new state descriptor. */
    protected <N, S extends State, T> S getOrCreateKeyedState(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<T> stateDescriptor)
            throws Exception {
        return stateHandler.getOrCreateKeyedState(
                defaultNamespace, namespaceSerializer, stateDescriptor);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (isAsyncKeyOrderedProcessingEnabled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
    }

    @SuppressWarnings("unchecked")
    public <K, N> InternalTimerService<N> getInternalTimerService(
            String name, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {
        if (timeServiceManager == null) {
            throw new RuntimeException("The timer service has not been initialized.");
        }

        if (!isAsyncKeyOrderedProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            return super.getInternalTimerService(name, namespaceSerializer, triggerable);
        }

        InternalTimeServiceManager<K> keyedTimeServiceHandler =
                (InternalTimeServiceManager<K>) timeServiceManager;
        TypeSerializer<K> keySerializer = stateHandler.getKeySerializer();
        checkState(keySerializer != null, "Timers can only be used on keyed operators.");
        // A {@link RecordContext} will be set as the current processing context to preserve record
        // order when the given {@link Triggerable} is invoked.
        InternalTimerService<N> service =
                keyedTimeServiceHandler.getInternalTimerService(
                        name, keySerializer, namespaceSerializer, triggerable);
        if (service instanceof InternalTimerServiceAsyncImpl) {
            ((InternalTimerServiceAsyncImpl<K, N>) service).setup(asyncExecutionController);
        } else if (service instanceof BatchExecutionInternalTimeServiceWithAsyncState) {
            ((BatchExecutionInternalTimeServiceWithAsyncState<K, N>) service)
                    .setup(asyncExecutionController);
        }
        return service;
    }

    // ------------------------------------------------------------------------
    //  Metrics
    // ------------------------------------------------------------------------
    @Override
    protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
        if (!isAsyncKeyOrderedProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            super.reportOrForwardLatencyMarker(marker);
            return;
        }
        processNonRecord(null, () -> super.reportOrForwardLatencyMarker(marker));
    }

    // ------------------------------------------------------------------------
    //  Watermark handling
    // ------------------------------------------------------------------------

    /**
     * A hook that will be triggered when receiving a watermark. Some async state can safely go
     * within this method. Return the watermark that should be normally processed.
     *
     * @param watermark the receiving watermark.
     * @return the watermark that should be processed. Null if there is no need for following
     *     processing.
     */
    public Watermark preProcessWatermark(Watermark watermark) throws Exception {
        return watermark;
    }

    /**
     * A hook that will be invoked after finishing advancing the watermark. It is not recommended to
     * perform async state here. Only some synchronous logic is suggested.
     *
     * @param watermark the advanced watermark.
     */
    public void postProcessWatermark(Watermark watermark) throws Exception {}

    /**
     * Process a watermark when receiving it. Do not override this method since the async processing
     * is difficult to write. Please override the hooks, see {@link #preProcessWatermark(Watermark)}
     * and {@link #postProcessWatermark(Watermark)}. The basic logic of processWatermark with hooks
     * in sync form would be:
     *
     * <pre>
     *             Watermark watermark = preProcessWatermark(mark);
     *             if (watermark != null) {
     *                 super.processWatermark(watermark);
     *                 postProcessWatermark(watermark);
     *             }
     * </pre>
     */
    @Override
    public final void processWatermark(Watermark mark) throws Exception {
        if (!isAsyncKeyOrderedProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            Watermark watermark = preProcessWatermark(mark);
            if (watermark != null) {
                super.processWatermark(watermark);
                postProcessWatermark(watermark);
            }
            return;
        }
        AtomicReference<Watermark> watermarkRef = new AtomicReference<>(null);
        processNonRecord(
                () -> {
                    watermarkRef.set(preProcessWatermark(mark));
                    if (timeServiceManager != null && watermarkRef.get() != null) {
                        timeServiceManager.advanceWatermark(watermarkRef.get());
                    }
                },
                () -> {
                    if (watermarkRef.get() != null) {
                        output.emitWatermark(watermarkRef.get());
                        postProcessWatermark(watermarkRef.get());
                    }
                });
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus, int inputId)
            throws Exception {
        if (!isAsyncKeyOrderedProcessingEnabled()) {
            super.processWatermarkStatus(watermarkStatus, inputId);
            return;
        }
        final AtomicBoolean wasIdle = new AtomicBoolean(false);
        final AtomicReference<Watermark> watermarkRef = new AtomicReference<>(null);
        processNonRecord(
                () -> {
                    wasIdle.set(combinedWatermark.isIdle());
                    if (combinedWatermark.updateStatus(inputId - 1, watermarkStatus.isIdle())) {
                        watermarkRef.set(
                                preProcessWatermark(
                                        new Watermark(combinedWatermark.getCombinedWatermark())));
                        if (timeServiceManager != null && watermarkRef.get() != null) {
                            timeServiceManager.advanceWatermark(watermarkRef.get());
                        }
                    }
                },
                () -> {
                    if (watermarkRef.get() != null) {
                        output.emitWatermark(watermarkRef.get());
                    }
                    if (wasIdle.get() != combinedWatermark.isIdle()) {
                        output.emitWatermarkStatus(watermarkStatus);
                    }
                });
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes, int inputId)
            throws Exception {
        if (!isAsyncKeyOrderedProcessingEnabled()) {
            super.processRecordAttributes(recordAttributes, inputId);
            return;
        }
        processNonRecord(null, () -> super.processRecordAttributes(recordAttributes, inputId));
    }

    @VisibleForTesting
    public StateExecutionController<?> getStateExecutionController() {
        return asyncExecutionController;
    }

    @VisibleForTesting
    public RecordContext getCurrentProcessingContext() {
        return currentProcessingContext;
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        closeIfNeeded();
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeIfNeeded();
    }

    private void closeIfNeeded() {
        if (isAsyncKeyOrderedProcessingEnabled()
                && !streamTask.isFailing()
                && !streamTask.isCanceled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
    }
}
