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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.AsyncStateException;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimerServiceAsyncImpl;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeServiceWithAsyncState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessing;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessingOperator;
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This operator is an abstract class that give the {@link AbstractStreamOperator} the ability to
 * perform {@link AsyncStateProcessing}. The aim is to make any subclass of {@link
 * AbstractStreamOperator} could manipulate async state with only a change of base class.
 */
@Internal
@SuppressWarnings("rawtypes")
public abstract class AbstractAsyncStateStreamOperator<OUT> extends AbstractStreamOperator<OUT>
        implements AsyncStateProcessingOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractAsyncStateStreamOperator.class);

    private AsyncExecutionController asyncExecutionController;

    /** Act as a cache for {@link #setAsyncKeyedContextElement} and {@link #postProcessElement}. */
    private RecordContext currentProcessingContext;

    private Environment environment;

    protected DeclarationManager declarationManager;

    /** Initialize necessary state components for {@link AbstractStreamOperator}. */
    @Override
    public final void beforeInitializeStateHandler() {
        KeyedStateStore stateStore = stateHandler.getKeyedStateStore().orElse(null);
        if (stateStore instanceof DefaultKeyedStateStore) {
            ((DefaultKeyedStateStore) stateStore).setSupportKeyedStateApiSetV2();
        }

        final StreamTask<?, ?> containingTask = checkNotNull(getContainingTask());
        environment = containingTask.getEnvironment();
        final MailboxExecutor mailboxExecutor =
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(getOperatorConfig().getChainIndex());
        final int maxParallelism = environment.getTaskInfo().getMaxNumberOfParallelSubtasks();
        final int inFlightRecordsLimit =
                environment.getExecutionConfig().getAsyncStateTotalBufferSize();
        final int asyncBufferSize =
                environment.getExecutionConfig().getAsyncStateActiveBufferSize();
        final long asyncBufferTimeout =
                environment.getExecutionConfig().getAsyncStateActiveBufferTimeout();

        this.declarationManager = new DeclarationManager();
        if (isAsyncStateProcessingEnabled()) {
            AsyncKeyedStateBackend asyncKeyedStateBackend =
                    stateHandler.getAsyncKeyedStateBackend();
            if (asyncKeyedStateBackend != null) {
                this.asyncExecutionController =
                        new AsyncExecutionController(
                                mailboxExecutor,
                                this::handleAsyncStateException,
                                asyncKeyedStateBackend.createStateExecutor(),
                                declarationManager,
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

    private void handleAsyncStateException(String message, Throwable exception) {
        environment.failExternally(new AsyncStateException(message, exception));
    }

    @Override
    public boolean isAsyncStateProcessingEnabled() {
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
        // Same logic as RECORD_ORDER, since FIRST_STATE_ORDER is problematic when the call's key
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
    @SuppressWarnings("unchecked")
    public final <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(int inputId) {
        // Ideally, only TwoStreamInputOperator/OneInputStreamOperator(Input) will invoke here.
        // Only those operators have the definition of processElement(1/2).
        if (this instanceof TwoInputStreamOperator) {
            switch (inputId) {
                case 1:
                    return AsyncStateProcessing.<T>makeRecordProcessor(
                            this,
                            (KeySelector) stateKeySelector1,
                            ((TwoInputStreamOperator) this)::processElement1);
                case 2:
                    return AsyncStateProcessing.<T>makeRecordProcessor(
                            this,
                            (KeySelector) stateKeySelector2,
                            ((TwoInputStreamOperator) this)::processElement2);
                default:
                    break;
            }
        } else if (this instanceof Input && inputId == 1) {
            return AsyncStateProcessing.<T>makeRecordProcessor(
                    this, (KeySelector) stateKeySelector1, ((Input) this)::processElement);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unsupported operator type %s with input id %d",
                        getClass().getName(), inputId));
    }

    /** Create new state (v2) based on new state descriptor. */
    public <N, S extends State, T> S getOrCreateKeyedState(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<T> stateDescriptor)
            throws Exception {
        return stateHandler.getOrCreateKeyedState(
                defaultNamespace, namespaceSerializer, stateDescriptor);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (isAsyncStateProcessingEnabled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
    }

    /**
     * Returns a {@link InternalTimerService} that can be used to query current processing time and
     * event time and to set timers. An operator can have several timer services, where each has its
     * own namespace serializer. Timer services are differentiated by the string key that is given
     * when requesting them, if you call this method with the same key multiple times you will get
     * the same timer service instance in subsequent requests.
     *
     * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
     * When a timer fires, this key will also be set as the currently active key.
     *
     * <p>Each timer has attached metadata, the namespace. Different timer services can have a
     * different namespace type. If you don't need namespace differentiation you can use {@link
     * org.apache.flink.runtime.state.VoidNamespaceSerializer} as the namespace serializer.
     *
     * @param name The name of the requested timer service. If no service exists under the given
     *     name a new one will be created and returned.
     * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
     * @param triggerable The {@link Triggerable} that should be invoked when timers fire
     * @param <N> The type of the timer namespace.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, N> InternalTimerService<N> getInternalTimerService(
            String name, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {
        if (timeServiceManager == null) {
            throw new RuntimeException("The timer service has not been initialized.");
        }

        if (!isAsyncStateProcessingEnabled()) {
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

    @Override
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        // This method is invoked only when isAsyncStateProcessingEnabled() is false.
        super.setKeyContextElement1(record);
        if (stateKeySelector1 != null) {
            newKeySelected(getCurrentKey());
        }
    }

    @Override
    public void setKeyContextElement2(StreamRecord record) throws Exception {
        // This method is invoked only when isAsyncStateProcessingEnabled() is false.
        super.setKeyContextElement2(record);
        if (stateKeySelector2 != null) {
            newKeySelected(getCurrentKey());
        }
    }

    @Override
    public Object getCurrentKey() {
        if (isAsyncStateProcessingEnabled()) {
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

    // ------------------------------------------------------------------------
    //  Metrics
    // ------------------------------------------------------------------------

    @Override
    protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
        if (!isAsyncStateProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            super.reportOrForwardLatencyMarker(marker);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> super.reportOrForwardLatencyMarker(marker));
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
     * A hook that will be invoked after finishing advancing the watermark and right before the
     * watermark being emitting downstream. Here is a chance for customization of the emitting
     * watermark. It is not recommended to perform async state here. Only some synchronous logic is
     * suggested.
     *
     * @param watermark the advanced watermark.
     * @return the watermark that should be emitted to downstream. Null if there is no need for
     *     following emitting.
     */
    public Watermark postProcessWatermark(Watermark watermark) throws Exception {
        return watermark;
    }

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
        if (!isAsyncStateProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            Watermark watermark = preProcessWatermark(mark);
            if (watermark != null) {
                super.processWatermark(watermark);
                postProcessWatermark(watermark);
            }
            return;
        }
        AtomicReference<Watermark> watermarkRef = new AtomicReference<>(null);
        asyncExecutionController.processNonRecord(
                () -> {
                    watermarkRef.set(preProcessWatermark(mark));
                    if (timeServiceManager != null && watermarkRef.get() != null) {
                        timeServiceManager.advanceWatermark(watermarkRef.get());
                    }
                },
                () -> {
                    if (watermarkRef.get() != null) {
                        Watermark postProcessWatermark = postProcessWatermark(watermarkRef.get());
                        if (postProcessWatermark != null) {
                            output.emitWatermark(postProcessWatermark);
                        }
                    }
                });
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            super.processWatermarkStatus(watermarkStatus);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> super.processWatermarkStatus(watermarkStatus));
    }

    @Override
    protected void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
            throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            super.processWatermarkStatus(watermarkStatus, index);
            return;
        }
        final AtomicBoolean wasIdle = new AtomicBoolean(false);
        final AtomicReference<Watermark> watermarkRef = new AtomicReference<>(null);
        asyncExecutionController.processNonRecord(
                () -> {
                    wasIdle.set(combinedWatermark.isIdle());
                    // index is 0-based
                    if (combinedWatermark.updateStatus(index, watermarkStatus.isIdle())) {
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
                        postProcessWatermark(watermarkRef.get());
                    }
                    if (wasIdle.get() != combinedWatermark.isIdle()) {
                        output.emitWatermarkStatus(watermarkStatus);
                    }
                });
    }

    @Experimental
    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            super.processRecordAttributes(recordAttributes);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> super.processRecordAttributes(recordAttributes));
    }

    @Experimental
    @Override
    public void processRecordAttributes1(RecordAttributes recordAttributes) {
        if (!isAsyncStateProcessingEnabled()) {
            super.processRecordAttributes1(recordAttributes);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> super.processRecordAttributes1(recordAttributes));
    }

    @Experimental
    @Override
    public void processRecordAttributes2(RecordAttributes recordAttributes) {
        if (!isAsyncStateProcessingEnabled()) {
            super.processRecordAttributes2(recordAttributes);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> super.processRecordAttributes2(recordAttributes));
    }

    public void processWatermarkInternal(WatermarkEvent watermark) throws Exception {
        super.processWatermark(watermark);
    }

    public void processWatermark1Internal(WatermarkEvent watermark) throws Exception {
        super.processWatermark1(watermark);
    }

    public void processWatermark2Internal(WatermarkEvent watermark) throws Exception {
        super.processWatermark2(watermark);
    }

    @Override
    public void processWatermark(WatermarkEvent watermark) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            this.processWatermarkInternal(watermark);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> this.processWatermarkInternal(watermark));
    }

    @Override
    public void processWatermark1(WatermarkEvent watermark) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            this.processWatermark1Internal(watermark);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> this.processWatermark1Internal(watermark));
    }

    @Override
    public void processWatermark2(WatermarkEvent watermark) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            this.processWatermark2Internal(watermark);
            return;
        }
        asyncExecutionController.processNonRecord(
                null, () -> this.processWatermark2Internal(watermark));
    }

    @VisibleForTesting
    AsyncExecutionController<?> getAsyncExecutionController() {
        return asyncExecutionController;
    }

    @VisibleForTesting
    RecordContext getCurrentProcessingContext() {
        return currentProcessingContext;
    }

    public <K> AsyncKeyedStateBackend<K> getAsyncKeyedStateBackend() {
        return stateHandler.getAsyncKeyedStateBackend();
    }

    public void drainStateRequests() {
        if (isAsyncStateProcessingEnabled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
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
        if (isAsyncStateProcessingEnabled()
                && !getContainingTask().isFailing()
                && !getContainingTask().isCanceled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
    }
}
