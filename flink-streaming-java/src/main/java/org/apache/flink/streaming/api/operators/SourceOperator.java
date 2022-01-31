/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It
 * implements the interface of {@link PushingAsyncDataInput} for naturally compatible with one input
 * processing in runtime stack.
 *
 * <p><b>Important Note on Serialization:</b> The SourceOperator inherits the {@link
 * java.io.Serializable} interface from the StreamOperator, but is in fact NOT serializable. The
 * operator must only be instantiates in the StreamTask from its factory.
 *
 * @param <OUT> The output type of the operator.
 */
@Internal
public class SourceOperator<OUT, SplitT extends SourceSplit> extends AbstractStreamOperator<OUT>
        implements OperatorEventHandler, PushingAsyncDataInput<OUT> {
    private static final long serialVersionUID = 1405537676017904695L;

    // Package private for unit test.
    static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
            new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);

    /**
     * The factory for the source reader. This is a workaround, because currently the SourceReader
     * must be lazily initialized, which is mainly because the metrics groups that the reader relies
     * on is lazily initialized.
     */
    private final FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception>
            readerFactory;

    /**
     * The serializer for the splits, applied to the split types before storing them in the reader
     * state.
     */
    private final SimpleVersionedSerializer<SplitT> splitSerializer;

    /** The event gateway through which this operator talks to its coordinator. */
    private final OperatorEventGateway operatorEventGateway;

    /** The factory for timestamps and watermark generators. */
    private final WatermarkStrategy<OUT> watermarkStrategy;

    /** The Flink configuration. */
    private final Configuration configuration;

    /**
     * Host name of the machine where the operator runs, to support locality aware work assignment.
     */
    private final String localHostname;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    // ---- lazily initialized fields (these fields are the "hot" fields) ----

    /** The source reader that does most of the work. */
    private SourceReader<OUT, SplitT> sourceReader;

    private ReaderOutput<OUT> currentMainOutput;

    private DataOutput<OUT> lastInvokedOutput;

    /** The state that holds the currently assigned splits. */
    private ListState<SplitT> readerState;

    /**
     * The event time and watermarking logic. Ideally this would be eagerly passed into this
     * operator, but we currently need to instantiate this lazily, because the metric groups exist
     * only later.
     */
    private TimestampsAndWatermarks<OUT> eventTimeLogic;

    /** A mode to control the behaviour of the {@link #emitNext(DataOutput)} method. */
    private OperatingMode operatingMode;

    private final CompletableFuture<Void> finished = new CompletableFuture<>();
    private final SourceOperatorAvailabilityHelper availabilityHelper =
            new SourceOperatorAvailabilityHelper();

    private enum OperatingMode {
        READING,
        OUTPUT_NOT_INITIALIZED,
        SOURCE_STOPPED,
        DATA_FINISHED
    }

    private InternalSourceReaderMetricGroup sourceMetricGroup;

    private @Nullable LatencyMarkerEmitter<OUT> latencyMarerEmitter;

    public SourceOperator(
            FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception>
                    readerFactory,
            OperatorEventGateway operatorEventGateway,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            WatermarkStrategy<OUT> watermarkStrategy,
            ProcessingTimeService timeService,
            Configuration configuration,
            String localHostname,
            boolean emitProgressiveWatermarks) {

        this.readerFactory = checkNotNull(readerFactory);
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
        this.splitSerializer = checkNotNull(splitSerializer);
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.processingTimeService = timeService;
        this.configuration = checkNotNull(configuration);
        this.localHostname = checkNotNull(localHostname);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.operatingMode = OperatingMode.OUTPUT_NOT_INITIALIZED;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        initSourceMetricGroup();
    }

    @VisibleForTesting
    protected void initSourceMetricGroup() {
        sourceMetricGroup = InternalSourceReaderMetricGroup.wrap(getMetricGroup());
    }

    /**
     * Initializes the reader. The code from this method should ideally happen in the constructor or
     * in the operator factory even. It has to happen here at a slightly later stage, because of the
     * lazy metric initialization.
     *
     * <p>Calling this method explicitly is an optional way to have the reader initialization a bit
     * earlier than in open(), as needed by the {@link
     * org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask}
     *
     * <p>This code should move to the constructor once the metric groups are available at task
     * setup time.
     */
    public void initReader() throws Exception {
        if (sourceReader != null) {
            return;
        }

        final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        final SourceReaderContext context =
                new SourceReaderContext() {
                    @Override
                    public SourceReaderMetricGroup metricGroup() {
                        return sourceMetricGroup;
                    }

                    @Override
                    public Configuration getConfiguration() {
                        return configuration;
                    }

                    @Override
                    public String getLocalHostName() {
                        return localHostname;
                    }

                    @Override
                    public int getIndexOfSubtask() {
                        return subtaskIndex;
                    }

                    @Override
                    public void sendSplitRequest() {
                        operatorEventGateway.sendEventToCoordinator(
                                new RequestSplitEvent(getLocalHostName()));
                    }

                    @Override
                    public void sendSourceEventToCoordinator(SourceEvent event) {
                        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return new UserCodeClassLoader() {
                            @Override
                            public ClassLoader asClassLoader() {
                                return getRuntimeContext().getUserCodeClassLoader();
                            }

                            @Override
                            public void registerReleaseHookIfAbsent(
                                    String releaseHookName, Runnable releaseHook) {
                                getRuntimeContext()
                                        .registerUserCodeClassLoaderReleaseHookIfAbsent(
                                                releaseHookName, releaseHook);
                            }
                        };
                    }
                };

        sourceReader = readerFactory.apply(context);
    }

    public InternalSourceReaderMetricGroup getSourceMetricGroup() {
        return sourceMetricGroup;
    }

    @Override
    public void open() throws Exception {
        initReader();

        // in the future when we this one is migrated to the "eager initialization" operator
        // (StreamOperatorV2), then we should evaluate this during operator construction.
        if (emitProgressiveWatermarks) {
            eventTimeLogic =
                    TimestampsAndWatermarks.createProgressiveEventTimeLogic(
                            watermarkStrategy,
                            sourceMetricGroup,
                            getProcessingTimeService(),
                            getExecutionConfig().getAutoWatermarkInterval());
        } else {
            eventTimeLogic =
                    TimestampsAndWatermarks.createNoOpEventTimeLogic(
                            watermarkStrategy, sourceMetricGroup);
        }

        // restore the state if necessary.
        final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
        if (!splits.isEmpty()) {
            sourceReader.addSplits(splits);
        }

        // Register the reader to the coordinator.
        registerReader();

        sourceMetricGroup.idlingStarted();
        // Start the reader after registration, sending messages in start is allowed.
        sourceReader.start();

        eventTimeLogic.startPeriodicWatermarkEmits();
    }

    @Override
    public void finish() throws Exception {
        if (eventTimeLogic != null) {
            eventTimeLogic.stopPeriodicWatermarkEmits();
        }
        if (latencyMarerEmitter != null) {
            latencyMarerEmitter.close();
        }
        super.finish();

        finished.complete(null);
    }

    public CompletableFuture<Void> stop() {
        switch (operatingMode) {
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                this.operatingMode = OperatingMode.SOURCE_STOPPED;
                availabilityHelper.forceStop();
                break;
        }
        return finished;
    }

    @Override
    public void close() throws Exception {
        if (sourceReader != null) {
            sourceReader.close();
        }
        super.close();
    }

    @Override
    public DataInputStatus emitNext(DataOutput<OUT> output) throws Exception {
        // guarding an assumptions we currently make due to the fact that certain classes
        // assume a constant output, this assumption does not need to stand if we emitted all
        // records. In that case the output will change to FinishedDataOutput
        assert lastInvokedOutput == output
                || lastInvokedOutput == null
                || this.operatingMode == OperatingMode.DATA_FINISHED;

        // short circuit the hot path. Without this short circuit (READING handled in the
        // switch/case) InputBenchmark.mapSink was showing a performance regression.
        if (operatingMode == OperatingMode.READING) {
            return convertToInternalStatus(sourceReader.pollNext(currentMainOutput));
        }
        return emitNextNotReading(output);
    }

    private DataInputStatus emitNextNotReading(DataOutput<OUT> output) throws Exception {
        switch (operatingMode) {
            case OUTPUT_NOT_INITIALIZED:
                currentMainOutput = eventTimeLogic.createMainOutput(output);
                initializeLatencyMarkerEmitter(output);
                lastInvokedOutput = output;
                this.operatingMode = OperatingMode.READING;
                return convertToInternalStatus(sourceReader.pollNext(currentMainOutput));
            case SOURCE_STOPPED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            case DATA_FINISHED:
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_INPUT;
            case READING:
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    private void initializeLatencyMarkerEmitter(DataOutput<OUT> output) {
        long latencyTrackingInterval =
                getExecutionConfig().isLatencyTrackingConfigured()
                        ? getExecutionConfig().getLatencyTrackingInterval()
                        : getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration()
                                .getLong(MetricOptions.LATENCY_INTERVAL);
        if (latencyTrackingInterval > 0) {
            latencyMarerEmitter =
                    new LatencyMarkerEmitter<>(
                            getProcessingTimeService(),
                            output::emitLatencyMarker,
                            latencyTrackingInterval,
                            getOperatorID(),
                            getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    private DataInputStatus convertToInternalStatus(InputStatus inputStatus) {
        switch (inputStatus) {
            case MORE_AVAILABLE:
                return DataInputStatus.MORE_AVAILABLE;
            case NOTHING_AVAILABLE:
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.NOTHING_AVAILABLE;
            case END_OF_INPUT:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            default:
                throw new IllegalArgumentException("Unknown input status: " + inputStatus);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();
        LOG.debug("Taking a snapshot for checkpoint {}", checkpointId);
        readerState.update(sourceReader.snapshotState(checkpointId));
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        switch (operatingMode) {
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                return availabilityHelper.update(sourceReader.isAvailable());
            case SOURCE_STOPPED:
            case DATA_FINISHED:
                return AvailabilityProvider.AVAILABLE;
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        final ListState<byte[]> rawState =
                context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);
        readerState = new SimpleVersionedListState<>(rawState, splitSerializer);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        sourceReader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        sourceReader.notifyCheckpointAborted(checkpointId);
    }

    @SuppressWarnings("unchecked")
    public void handleOperatorEvent(OperatorEvent event) {
        if (event instanceof AddSplitEvent) {
            try {
                sourceReader.addSplits(((AddSplitEvent<SplitT>) event).splits(splitSerializer));
            } catch (IOException e) {
                throw new FlinkRuntimeException("Failed to deserialize the splits.", e);
            }
        } else if (event instanceof SourceEventWrapper) {
            sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
        } else if (event instanceof NoMoreSplitsEvent) {
            sourceReader.notifyNoMoreSplits();
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
    }

    private void registerReader() {
        operatorEventGateway.sendEventToCoordinator(
                new ReaderRegistrationEvent(
                        getRuntimeContext().getIndexOfThisSubtask(), localHostname));
    }

    // --------------- methods for unit tests ------------

    @VisibleForTesting
    public SourceReader<OUT, SplitT> getSourceReader() {
        return sourceReader;
    }

    @VisibleForTesting
    ListState<SplitT> getReaderState() {
        return readerState;
    }

    private static class SourceOperatorAvailabilityHelper {
        private final CompletableFuture<Void> forcedStopFuture = new CompletableFuture<>();
        private final MultipleFuturesAvailabilityHelper availabilityHelper;

        private SourceOperatorAvailabilityHelper() {
            availabilityHelper = new MultipleFuturesAvailabilityHelper(2);
            availabilityHelper.anyOf(0, forcedStopFuture);
        }

        public CompletableFuture<?> update(CompletableFuture<Void> sourceReaderFuture) {
            if (sourceReaderFuture == AvailabilityProvider.AVAILABLE
                    || sourceReaderFuture.isDone()) {
                return AvailabilityProvider.AVAILABLE;
            }
            availabilityHelper.resetToUnAvailable();
            availabilityHelper.anyOf(0, forcedStopFuture);
            availabilityHelper.anyOf(1, sourceReaderFuture);
            return availabilityHelper.getAvailableFuture();
        }

        public void forceStop() {
            forcedStopFuture.complete(null);
        }
    }
}
