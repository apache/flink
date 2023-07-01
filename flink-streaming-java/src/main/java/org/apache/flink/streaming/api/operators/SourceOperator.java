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
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
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
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.PipelineOptions.ALLOW_UNALIGNED_SOURCE_SPLITS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It
 * implements the interface of {@link PushingAsyncDataInput} which is naturally compatible with one
 * input processing in runtime stack.
 *
 * <p><b>Important Note on Serialization:</b> The SourceOperator inherits the {@link
 * java.io.Serializable} interface from the StreamOperator, but is in fact NOT serializable. The
 * operator must only be instantiated in the StreamTask from its factory.
 *
 * @param <OUT> The output type of the operator.
 */
@Internal
public class SourceOperator<OUT, SplitT extends SourceSplit> extends AbstractStreamOperator<OUT>
        implements OperatorEventHandler,
                PushingAsyncDataInput<OUT>,
                TimestampsAndWatermarks.WatermarkUpdateListener {
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

    private final WatermarkAlignmentParams watermarkAlignmentParams;

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

    private long latestWatermark = Watermark.UNINITIALIZED.getTimestamp();

    private boolean idle = false;

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

    private final List<SplitT> outputPendingSplits = new ArrayList<>();

    private int numSplits;
    private final Map<String, Long> splitCurrentWatermarks = new HashMap<>();
    private final Set<String> currentlyPausedSplits = new HashSet<>();

    private enum OperatingMode {
        READING,
        WAITING_FOR_ALIGNMENT,
        OUTPUT_NOT_INITIALIZED,
        SOURCE_DRAINED,
        SOURCE_STOPPED,
        DATA_FINISHED
    }

    private InternalSourceReaderMetricGroup sourceMetricGroup;

    private long currentMaxDesiredWatermark = Watermark.MAX_WATERMARK.getTimestamp();
    /** Can be not completed only in {@link OperatingMode#WAITING_FOR_ALIGNMENT} mode. */
    private CompletableFuture<Void> waitingForAlignmentFuture =
            CompletableFuture.completedFuture(null);

    private @Nullable LatencyMarkerEmitter<OUT> latencyMarkerEmitter;

    private final boolean allowUnalignedSourceSplits;

    private final CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;

    public SourceOperator(
            FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception>
                    readerFactory,
            OperatorEventGateway operatorEventGateway,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            WatermarkStrategy<OUT> watermarkStrategy,
            ProcessingTimeService timeService,
            Configuration configuration,
            String localHostname,
            boolean emitProgressiveWatermarks,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {

        this.readerFactory = checkNotNull(readerFactory);
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
        this.splitSerializer = checkNotNull(splitSerializer);
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.processingTimeService = timeService;
        this.configuration = checkNotNull(configuration);
        this.localHostname = checkNotNull(localHostname);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.operatingMode = OperatingMode.OUTPUT_NOT_INITIALIZED;
        this.watermarkAlignmentParams = watermarkStrategy.getAlignmentParameters();
        this.allowUnalignedSourceSplits = configuration.get(ALLOW_UNALIGNED_SOURCE_SPLITS);
        this.canEmitBatchOfRecords = checkNotNull(canEmitBatchOfRecords);
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

                    @Override
                    public int currentParallelism() {
                        return getRuntimeContext().getNumberOfParallelSubtasks();
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
            LOG.info("Restoring state for {} split(s) to reader.", splits.size());
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
        stopInternalServices();
        super.finish();

        finished.complete(null);
    }

    private void stopInternalServices() {
        if (eventTimeLogic != null) {
            eventTimeLogic.stopPeriodicWatermarkEmits();
        }
        if (latencyMarkerEmitter != null) {
            latencyMarkerEmitter.close();
        }
    }

    public CompletableFuture<Void> stop(StopMode mode) {
        switch (operatingMode) {
            case WAITING_FOR_ALIGNMENT:
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                this.operatingMode =
                        mode == StopMode.DRAIN
                                ? OperatingMode.SOURCE_DRAINED
                                : OperatingMode.SOURCE_STOPPED;
                availabilityHelper.forceStop();
                if (this.operatingMode == OperatingMode.SOURCE_STOPPED) {
                    stopInternalServices();
                    finished.complete(null);
                    return finished;
                }
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
        if (operatingMode != OperatingMode.READING) {
            return emitNextNotReading(output);
        }

        InputStatus status;
        do {
            status = sourceReader.pollNext(currentMainOutput);
        } while (status == InputStatus.MORE_AVAILABLE
                && canEmitBatchOfRecords.check()
                && !shouldWaitForAlignment());
        return convertToInternalStatus(status);
    }

    private DataInputStatus emitNextNotReading(DataOutput<OUT> output) throws Exception {
        switch (operatingMode) {
            case OUTPUT_NOT_INITIALIZED:
                if (watermarkAlignmentParams.isEnabled()) {
                    // Only wrap the output when watermark alignment is enabled, as otherwise this
                    // introduces a small performance regression (probably because of an extra
                    // virtual call)
                    processingTimeService.scheduleWithFixedDelay(
                            this::emitLatestWatermark,
                            watermarkAlignmentParams.getUpdateInterval(),
                            watermarkAlignmentParams.getUpdateInterval());
                }
                initializeMainOutput(output);
                return convertToInternalStatus(sourceReader.pollNext(currentMainOutput));
            case SOURCE_STOPPED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.STOPPED;
            case SOURCE_DRAINED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            case DATA_FINISHED:
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_INPUT;
            case WAITING_FOR_ALIGNMENT:
                checkState(!waitingForAlignmentFuture.isDone());
                checkState(shouldWaitForAlignment());
                return convertToInternalStatus(InputStatus.NOTHING_AVAILABLE);
            case READING:
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    private void initializeMainOutput(DataOutput<OUT> output) {
        currentMainOutput = eventTimeLogic.createMainOutput(output, this);
        initializeLatencyMarkerEmitter(output);
        lastInvokedOutput = output;
        // Create per-split output for pending splits added before main output is initialized
        createOutputForSplits(outputPendingSplits);
        this.operatingMode = OperatingMode.READING;
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
            latencyMarkerEmitter =
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

    private void emitLatestWatermark(long time) {
        checkState(currentMainOutput != null);
        if (latestWatermark == Watermark.UNINITIALIZED.getTimestamp()) {
            return;
        }
        operatorEventGateway.sendEventToCoordinator(
                new ReportedWatermarkEvent(
                        idle ? Watermark.MAX_WATERMARK.getTimestamp() : latestWatermark));
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
            case WAITING_FOR_ALIGNMENT:
                return availabilityHelper.update(waitingForAlignmentFuture);
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                return availabilityHelper.update(sourceReader.isAvailable());
            case SOURCE_STOPPED:
            case SOURCE_DRAINED:
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
        if (event instanceof WatermarkAlignmentEvent) {
            updateMaxDesiredWatermark((WatermarkAlignmentEvent) event);
            checkWatermarkAlignment();
            checkSplitWatermarkAlignment();
        } else if (event instanceof AddSplitEvent) {
            handleAddSplitsEvent(((AddSplitEvent<SplitT>) event));
        } else if (event instanceof SourceEventWrapper) {
            sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
        } else if (event instanceof NoMoreSplitsEvent) {
            sourceReader.notifyNoMoreSplits();
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
    }

    private void handleAddSplitsEvent(AddSplitEvent<SplitT> event) {
        try {
            List<SplitT> newSplits = event.splits(splitSerializer);
            numSplits += newSplits.size();
            if (operatingMode == OperatingMode.OUTPUT_NOT_INITIALIZED) {
                // For splits arrived before the main output is initialized, store them into the
                // pending list. Outputs of these splits will be created once the main output is
                // ready.
                outputPendingSplits.addAll(newSplits);
            } else {
                // Create output directly for new splits if the main output is already initialized.
                createOutputForSplits(newSplits);
            }
            sourceReader.addSplits(newSplits);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to deserialize the splits.", e);
        }
    }

    private void createOutputForSplits(List<SplitT> newSplits) {
        for (SplitT split : newSplits) {
            currentMainOutput.createOutputForSplit(split.splitId());
        }
    }

    private void updateMaxDesiredWatermark(WatermarkAlignmentEvent event) {
        currentMaxDesiredWatermark = event.getMaxWatermark();
        sourceMetricGroup.updateMaxDesiredWatermark(currentMaxDesiredWatermark);
    }

    @Override
    public void updateIdle(boolean isIdle) {
        this.idle = isIdle;
    }

    @Override
    public void updateCurrentEffectiveWatermark(long watermark) {
        latestWatermark = watermark;
        checkWatermarkAlignment();
    }

    @Override
    public void updateCurrentSplitWatermark(String splitId, long watermark) {
        splitCurrentWatermarks.put(splitId, watermark);
        if (numSplits > 1
                && watermark > currentMaxDesiredWatermark
                && !currentlyPausedSplits.contains(splitId)) {
            pauseOrResumeSplits(Collections.singletonList(splitId), Collections.emptyList());
            currentlyPausedSplits.add(splitId);
        }
    }

    /**
     * Finds the splits that are beyond the current max watermark and pauses them. At the same time,
     * splits that have been paused and where the global watermark caught up are resumed.
     *
     * <p>Note: This takes effect only if there are multiple splits, otherwise it does nothing.
     */
    private void checkSplitWatermarkAlignment() {
        if (numSplits <= 1) {
            // A single split can't overtake any other splits assigned to this operator instance.
            // It is sufficient for the source to stop processing.
            return;
        }
        Collection<String> splitsToPause = new ArrayList<>();
        Collection<String> splitsToResume = new ArrayList<>();
        splitCurrentWatermarks.forEach(
                (splitId, splitWatermark) -> {
                    if (splitWatermark > currentMaxDesiredWatermark) {
                        splitsToPause.add(splitId);
                    } else if (currentlyPausedSplits.contains(splitId)) {
                        splitsToResume.add(splitId);
                    }
                });
        splitsToPause.removeAll(currentlyPausedSplits);
        if (!splitsToPause.isEmpty() || !splitsToResume.isEmpty()) {
            pauseOrResumeSplits(splitsToPause, splitsToResume);
            currentlyPausedSplits.addAll(splitsToPause);
            splitsToResume.forEach(currentlyPausedSplits::remove);
        }
    }

    private void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        try {
            sourceReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        } catch (UnsupportedOperationException e) {
            if (!allowUnalignedSourceSplits) {
                throw e;
            }
        }
    }

    private void checkWatermarkAlignment() {
        if (operatingMode == OperatingMode.READING) {
            checkState(waitingForAlignmentFuture.isDone());
            if (shouldWaitForAlignment()) {
                operatingMode = OperatingMode.WAITING_FOR_ALIGNMENT;
                waitingForAlignmentFuture = new CompletableFuture<>();
            }
        } else if (operatingMode == OperatingMode.WAITING_FOR_ALIGNMENT) {
            checkState(!waitingForAlignmentFuture.isDone());
            if (!shouldWaitForAlignment()) {
                operatingMode = OperatingMode.READING;
                waitingForAlignmentFuture.complete(null);
            }
        }
    }

    private boolean shouldWaitForAlignment() {
        return currentMaxDesiredWatermark < latestWatermark;
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
