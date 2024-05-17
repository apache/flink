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

package org.apache.flink.streaming.runtime.operators.asyncprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.AsyncStateException;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

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
                            inFlightRecordsLimit);
            asyncKeyedStateBackend.setup(asyncExecutionController);
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
    public final <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(int inputId) {
        // The real logic should be in First/SecondInputOfTwoInput#getRecordProcessor.
        throw new UnsupportedOperationException(
                "Never getRecordProcessor from AbstractAsyncStateStreamOperatorV2,"
                        + " since this part is handled by the Input.");
    }

    @Override
    public final OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        if (isAsyncStateProcessingEnabled()) {
            asyncExecutionController.drainInflightRecords(0);
        }
        // {@link #snapshotState(StateSnapshotContext)} will be called in
        // stateHandler#snapshotState, so the {@link #snapshotState(StateSnapshotContext)} is not
        // needed to override.
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, factory);
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
        KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
        checkState(keyedStateBackend != null, "Timers can only be used on keyed operators.");
        return keyedTimeServiceHandler.getAsyncInternalTimerService(
                name,
                keyedStateBackend.getKeySerializer(),
                namespaceSerializer,
                triggerable,
                (AsyncExecutionController<K>) asyncExecutionController);
    }

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

    @VisibleForTesting
    AsyncExecutionController<?> getAsyncExecutionController() {
        return asyncExecutionController;
    }

    @VisibleForTesting
    RecordContext getCurrentProcessingContext() {
        return currentProcessingContext;
    }
}
