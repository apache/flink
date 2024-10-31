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
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.AsyncStateException;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessing;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessingOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.ElementOrder;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This operator is an abstract class that give the {@link AbstractStreamOperatorV2} the ability to
 * perform {@link AsyncStateProcessing}. The aim is to make any subclass of {@link
 * AbstractStreamOperatorV2} could manipulate async state with only a change of base class.
 */
@Internal
@SuppressWarnings("rawtypes")
public abstract class AbstractAsyncStateStreamOperatorV2<OUT> extends AbstractStreamOperatorV2<OUT>
        implements AsyncStateProcessingOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractAsyncStateStreamOperatorV2.class);

    private final Environment environment;
    private AsyncExecutionController asyncExecutionController;

    private RecordContext currentProcessingContext;

    public AbstractAsyncStateStreamOperatorV2(
            StreamOperatorParameters<OUT> parameters, int numberOfInputs) {
        super(parameters, numberOfInputs);
        this.environment = parameters.getContainingTask().getEnvironment();
    }

    /** Initialize necessary state components for {@link AbstractStreamOperatorV2}. */
    @Override
    public final void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        super.initializeState(streamTaskStateManager);
        getRuntimeContext().setKeyedStateStoreV2(stateHandler.getKeyedStateStoreV2().orElse(null));

        final int inFlightRecordsLimit = getExecutionConfig().getAsyncInflightRecordsLimit();
        final int asyncBufferSize = getExecutionConfig().getAsyncStateBufferSize();
        final long asyncBufferTimeout = getExecutionConfig().getAsyncStateBufferTimeout();
        int maxParallelism = getExecutionConfig().getMaxParallelism();

        AsyncKeyedStateBackend asyncKeyedStateBackend = stateHandler.getAsyncKeyedStateBackend();
        if (asyncKeyedStateBackend != null) {
            this.asyncExecutionController =
                    new AsyncExecutionController(
                            environment.getMainMailboxExecutor(),
                            this::handleAsyncStateException,
                            asyncKeyedStateBackend.createStateExecutor(),
                            maxParallelism,
                            asyncBufferSize,
                            asyncBufferTimeout,
                            inFlightRecordsLimit,
                            asyncKeyedStateBackend);
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
        asyncExecutionController.syncPointRequestWithCallback(processing);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> void asyncProcessWithKey(K key, ThrowingRunnable<Exception> processing) {
        RecordContext<K> previousContext = currentProcessingContext;

        // build a context and switch to the new context
        currentProcessingContext = asyncExecutionController.buildContext(null, key);
        currentProcessingContext.retain();
        asyncExecutionController.setCurrentContext(currentProcessingContext);
        // Same logic as RECORD_ORDER, since FIRST_STATE_ORDER is problematic when the call's key
        // pass the same key in.
        preserveRecordOrderAndProcess(processing);
        postProcessElement();

        // switch to original context
        asyncExecutionController.setCurrentContext(previousContext);
        currentProcessingContext = previousContext;
    }

    @Override
    public final <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(int inputId) {
        // The real logic should be in First/SecondInputOfTwoInput#getRecordProcessor.
        throw new UnsupportedOperationException(
                "Never getRecordProcessor from AbstractAsyncStateStreamOperatorV2,"
                        + " since this part is handled by the Input.");
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
        if (isAsyncStateProcessingEnabled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
    }

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
        return keyedTimeServiceHandler.getAsyncInternalTimerService(
                name,
                keySerializer,
                namespaceSerializer,
                triggerable,
                (AsyncExecutionController<K>) asyncExecutionController);
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
        asyncExecutionController.processNonRecord(() -> super.reportOrForwardLatencyMarker(marker));
    }

    // ------------------------------------------------------------------------
    //  Watermark handling
    // ------------------------------------------------------------------------
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            super.processWatermark(mark);
            return;
        }
        asyncExecutionController.processNonRecord(() -> super.processWatermark(mark));
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus, int inputId)
            throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            super.processWatermarkStatus(watermarkStatus, inputId);
            return;
        }
        asyncExecutionController.processNonRecord(
                () -> {
                    boolean wasIdle = combinedWatermark.isIdle();
                    if (combinedWatermark.updateStatus(inputId - 1, watermarkStatus.isIdle())) {
                        super.processWatermark(
                                new Watermark(combinedWatermark.getCombinedWatermark()));
                    }
                    if (wasIdle != combinedWatermark.isIdle()) {
                        output.emitWatermarkStatus(watermarkStatus);
                    }
                });
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes, int inputId)
            throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            super.processRecordAttributes(recordAttributes, inputId);
            return;
        }
        asyncExecutionController.processNonRecord(
                () -> super.processRecordAttributes(recordAttributes, inputId));
    }

    @VisibleForTesting
    public AsyncExecutionController<?> getAsyncExecutionController() {
        return asyncExecutionController;
    }

    @VisibleForTesting
    public RecordContext getCurrentProcessingContext() {
        return currentProcessingContext;
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        asyncExecutionController.drainInflightRecords(0);
    }

    @Override
    public void close() throws Exception {
        super.close();
        asyncExecutionController.close();
    }
}
