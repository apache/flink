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
import org.apache.flink.api.common.operators.MailboxExecutor;
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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

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

    private AsyncExecutionController asyncExecutionController;

    private RecordContext currentProcessingContext;

    private Environment environment;

    /** Initialize necessary state components for {@link AbstractStreamOperator}. */
    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        super.initializeState(streamTaskStateManager);
        getRuntimeContext().setKeyedStateStoreV2(stateHandler.getKeyedStateStoreV2().orElse(null));
        final StreamTask<?, ?> containingTask = checkNotNull(getContainingTask());
        environment = containingTask.getEnvironment();
        final MailboxExecutor mailboxExecutor = environment.getMainMailboxExecutor();
        final int maxParallelism = environment.getTaskInfo().getMaxNumberOfParallelSubtasks();
        final int inFlightRecordsLimit =
                environment.getExecutionConfig().getAsyncInflightRecordsLimit();
        final int asyncBufferSize = environment.getExecutionConfig().getAsyncStateBufferSize();
        final long asyncBufferTimeout =
                environment.getExecutionConfig().getAsyncStateBufferTimeout();

        AsyncKeyedStateBackend asyncKeyedStateBackend = stateHandler.getAsyncKeyedStateBackend();
        if (asyncKeyedStateBackend != null) {
            this.asyncExecutionController =
                    new AsyncExecutionController(
                            mailboxExecutor,
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
        KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
        checkState(keyedStateBackend != null, "Timers can only be used on keyed operators.");
        // A {@link RecordContext} will be set as the current processing context to preserve record
        // order when the given {@link Triggerable} is invoked.
        return keyedTimeServiceHandler.getAsyncInternalTimerService(
                name,
                keyedStateBackend.getKeySerializer(),
                namespaceSerializer,
                triggerable,
                (AsyncExecutionController<K>) asyncExecutionController);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        super.setKeyContextElement1(record);
        if (stateKeySelector1 != null) {
            setAsyncKeyedContextElement(record, stateKeySelector1);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setKeyContextElement2(StreamRecord record) throws Exception {
        super.setKeyContextElement2(record);
        if (stateKeySelector2 != null) {
            setAsyncKeyedContextElement(record, stateKeySelector2);
        }
    }

    @Override
    public Object getCurrentKey() {
        return currentProcessingContext.getKey();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            super.processWatermark(mark);
            return;
        }
        asyncExecutionController.processNonRecord(() -> super.processWatermark(mark));
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        if (!isAsyncStateProcessingEnabled()) {
            // If async state processing is disabled, fallback to the super class.
            super.processWatermarkStatus(watermarkStatus);
            return;
        }
        asyncExecutionController.processNonRecord(
                () -> super.processWatermarkStatus(watermarkStatus));
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
