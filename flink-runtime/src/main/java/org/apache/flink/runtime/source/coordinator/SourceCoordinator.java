/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.DynamicFilteringInfo;
import org.apache.flink.api.connector.source.DynamicParallelismInference;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SupportsBatchSnapshot;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.apache.flink.api.connector.source.util.ratelimit.DynamicRateLimitParams;
import org.apache.flink.api.connector.source.util.ratelimit.RateAllocations;
import org.apache.flink.api.connector.source.util.ratelimit.SubtaskRateInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.DynamicRateLimitEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedRecordsEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.AbstractHeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readAndVerifyCoordinatorSerdeVersion;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readBytes;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.writeCoordinatorSerdeVersion;
import static org.apache.flink.util.IOUtils.closeQuietly;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The default implementation of the {@link OperatorCoordinator} for the {@link Source}.
 *
 * <p>The <code>SourceCoordinator</code> provides an event loop style thread model to interact with
 * the Flink runtime. The coordinator ensures that all the state manipulations are made by its event
 * loop thread. It also helps keep track of the necessary split assignments history per subtask to
 * simplify the {@link SplitEnumerator} implementation.
 *
 * <p>The coordinator maintains a {@link
 * org.apache.flink.api.connector.source.SplitEnumeratorContext SplitEnumeratorContxt} and shares it
 * with the enumerator. When the coordinator receives an action request from the Flink runtime, it
 * sets up the context, and calls corresponding method of the SplitEnumerator to take actions.
 */
@Internal
public class SourceCoordinator<SplitT extends SourceSplit, EnumChkT>
        implements OperatorCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinator.class);

    private final WatermarkAggregator<Integer> combinedWatermark = new WatermarkAggregator<>();

    private final WatermarkAlignmentParams watermarkAlignmentParams;

    private final DynamicRateLimitParams dynamicRateLimitParams;
    private final CheckpointDurationTracker checkpointDurationTracker;
    private final Clock clock;
    private final Map<Integer, Double> reportedThroughputs = new HashMap<>();

    /** Throughput reported per checkpoint id and subtask, until that checkpoint completes. */
    private final Map<Long, Map<Integer, Double>> reportedRecordRates = new HashMap<>();

    private final Map<Integer, Double> currentAllocations = new HashMap<>();
    private double currentTargetRecordsPerSecond;

    private final JobID jobID;

    /** The name of the operator this SourceCoordinator is associated with. */
    private final String operatorName;

    /** The Source that is associated with this SourceCoordinator. */
    private final Source<?, SplitT, EnumChkT> source;

    /** The serializer that handles the serde of the split. */
    private final SimpleVersionedSerializer<SplitT> splitSerializer;

    /** The serializer that handles the serde of the SplitEnumerator checkpoints. */
    private final SimpleVersionedSerializer<EnumChkT> enumCheckpointSerializer;

    /** The context containing the states of the coordinator. */
    private final SourceCoordinatorContext<SplitT> context;

    private final CoordinatorStore coordinatorStore;

    /**
     * The split enumerator created from the associated Source. This one is created either during
     * resetting the coordinator to a checkpoint, or when the coordinator is started.
     */
    private SplitEnumerator<SplitT, EnumChkT> enumerator;

    /** A flag marking whether the coordinator has started. */
    private boolean started;

    /**
     * An ID that the coordinator will register self in the coordinator store with. Other
     * coordinators may send events to this coordinator by the ID.
     */
    @Nullable private final String coordinatorListeningID;

    public SourceCoordinator(
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore) {
        this(
                new JobID(),
                operatorName,
                source,
                context,
                coordinatorStore,
                WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                DynamicRateLimitParams.DISABLED,
                null);
    }

    public SourceCoordinator(
            JobID jobID,
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore,
            WatermarkAlignmentParams watermarkAlignmentParams,
            @Nullable String coordinatorListeningID) {
        this(
                jobID,
                operatorName,
                source,
                context,
                coordinatorStore,
                watermarkAlignmentParams,
                DynamicRateLimitParams.DISABLED,
                coordinatorListeningID);
    }

    public SourceCoordinator(
            JobID jobID,
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore,
            WatermarkAlignmentParams watermarkAlignmentParams,
            DynamicRateLimitParams dynamicRateLimitParams,
            @Nullable String coordinatorListeningID) {
        this(
                jobID,
                operatorName,
                source,
                context,
                coordinatorStore,
                watermarkAlignmentParams,
                dynamicRateLimitParams,
                coordinatorListeningID,
                SystemClock.getInstance());
    }

    @VisibleForTesting
    SourceCoordinator(
            JobID jobID,
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore,
            WatermarkAlignmentParams watermarkAlignmentParams,
            DynamicRateLimitParams dynamicRateLimitParams,
            @Nullable String coordinatorListeningID,
            Clock clock) {
        this.clock = clock;
        this.jobID = jobID;
        this.operatorName = operatorName;
        this.source = source;
        this.enumCheckpointSerializer = source.getEnumeratorCheckpointSerializer();
        this.splitSerializer = source.getSplitSerializer();
        this.context = context;
        this.coordinatorStore = coordinatorStore;
        this.watermarkAlignmentParams = watermarkAlignmentParams;
        this.dynamicRateLimitParams = dynamicRateLimitParams;
        this.checkpointDurationTracker =
                new CheckpointDurationTracker(dynamicRateLimitParams.getRollingWindowSize());
        this.currentTargetRecordsPerSecond = dynamicRateLimitParams.getMaxRecordsPerSecond();
        if (dynamicRateLimitParams.isEnabled()) {
            LOG.info(
                    "Dynamic rate limit enabled for source {}: maxRecordsPerSecond={}, "
                            + "rollingWindowSize={}, throttleDownFactor={}, rampUpFactor={}, "
                            + "durationFractions(min/target/max)={}/{}/{}, calculator={}",
                    operatorName,
                    dynamicRateLimitParams.getMaxRecordsPerSecond(),
                    dynamicRateLimitParams.getRollingWindowSize(),
                    dynamicRateLimitParams.getThrottleDownFactor(),
                    dynamicRateLimitParams.getRampUpFactor(),
                    dynamicRateLimitParams.getMinDurationFraction(),
                    dynamicRateLimitParams.getTargetDurationFraction(),
                    dynamicRateLimitParams.getMaxDurationFraction(),
                    dynamicRateLimitParams.getRateLimitCalculator().getClass().getName());
        }
        this.coordinatorListeningID = coordinatorListeningID;

        if (watermarkAlignmentParams.isEnabled()
                && context.isConcurrentExecutionAttemptsSupported()) {
            throw new IllegalArgumentException(
                    "Watermark alignment is not supported in concurrent execution attempts "
                            + "scenario (e.g. if speculative execution is enabled)");
        }
    }

    @VisibleForTesting
    void announceCombinedWatermark() {
        Set<Integer> subTaskIds = combinedWatermark.keySet();
        if (subTaskIds.isEmpty()) {
            LOG.debug(
                    "Skip distributing maxAllowedWatermark of group={} for source {} - no subtasks.",
                    watermarkAlignmentParams.getWatermarkGroup(),
                    operatorName);
            return;
        }
        checkState(
                watermarkAlignmentParams != WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED);

        Watermark globalCombinedWatermark =
                coordinatorStore.apply(
                        watermarkAlignmentParams.getWatermarkGroup(),
                        (value) -> {
                            WatermarkAggregator aggregator = (WatermarkAggregator) value;
                            return new Watermark(
                                    aggregator.getAggregatedWatermark().getTimestamp());
                        });

        long maxAllowedWatermark;
        try {
            maxAllowedWatermark =
                    Math.addExact(
                            globalCombinedWatermark.getTimestamp(),
                            watermarkAlignmentParams.getMaxAllowedWatermarkDrift());
        } catch (ArithmeticException e) {
            // when the source is idle, globalCombinedWatermark.getTimestamp() is Long.MAX_VALUE,
            // and maxAllowedWatermark arithmetic overflow
            maxAllowedWatermark = Watermark.MAX_WATERMARK.getTimestamp();
        }

        LOG.info(
                "Distributing maxAllowedWatermark={} of group={} to subTaskIds={} for source {}.",
                maxAllowedWatermark,
                watermarkAlignmentParams.getWatermarkGroup(),
                subTaskIds,
                operatorName);

        for (Integer subtaskId : subTaskIds) {
            // Subtask maybe during deploying or restarting, so we only send
            // WatermarkAlignmentEvent to ready task to avoid period task fail
            // (Java-ThreadPoolExecutor will not schedule the period task if it throws an
            // exception).
            context.sendEventToSourceOperatorIfTaskReady(
                    subtaskId, new WatermarkAlignmentEvent(maxAllowedWatermark));
        }
    }

    @VisibleForTesting
    void announceDynamicRateLimit() {
        updateAllocations();
        LOG.info(
                "Distributing dynamic rate limit={} for source {}: {}",
                currentTargetRecordsPerSecond,
                operatorName,
                currentAllocations);
        for (Integer subtaskId : context.registeredReaders().keySet()) {
            sendRateLimitToSubtask(subtaskId);
        }
    }

    private void sendRateLimitToSubtask(int subtaskId) {
        final double rate =
                currentAllocations.getOrDefault(
                        subtaskId, currentTargetRecordsPerSecond / context.currentParallelism());
        context.sendEventToSourceOperatorIfTaskReady(subtaskId, new DynamicRateLimitEvent(rate));
    }

    /**
     * Splits {@link #currentTargetRecordsPerSecond} across all subtasks with {@link
     * RateAllocations}, falling back to an even share for any subtask that gets no valid rate.
     */
    private void updateAllocations() {
        final int parallelism = context.currentParallelism();
        final double evenShare = currentTargetRecordsPerSecond / parallelism;

        final List<SubtaskRateInfo> subtasks = new ArrayList<>(parallelism);
        for (int subtask = 0; subtask < parallelism; subtask++) {
            subtasks.add(
                    new SubtaskRateInfo(
                            subtask,
                            currentAllocations.getOrDefault(subtask, Double.NaN),
                            reportedThroughputs.getOrDefault(subtask, Double.NaN)));
        }

        final Map<Integer, Double> allocated =
                RateAllocations.allocate(currentTargetRecordsPerSecond, subtasks);

        final Map<Integer, Double> validated = new HashMap<>();
        double sum = 0;
        for (int subtask = 0; subtask < parallelism; subtask++) {
            final Double rate = allocated.get(subtask);
            final double valid =
                    rate != null && Double.isFinite(rate) && rate > 0 ? rate : evenShare;
            validated.put(subtask, valid);
            sum += valid;
        }
        if (sum > currentTargetRecordsPerSecond * (1 + 1e-6)) {
            final double scale = currentTargetRecordsPerSecond / sum;
            validated.replaceAll((subtask, rate) -> rate * scale);
        }

        currentAllocations.clear();
        currentAllocations.putAll(validated);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting split enumerator for source {}.", operatorName);

        // we mark this as started first, so that we can later distinguish the cases where
        // 'start()' wasn't called and where 'start()' failed.
        started = true;

        // there are two ways the SplitEnumerator can get created:
        //  (1) Source.restoreEnumerator(), in which case the 'resetToCheckpoint()' method creates
        // it
        //  (2) Source.createEnumerator, in which case it has not been created, yet, and we create
        // it here
        if (enumerator == null) {
            final ClassLoader userCodeClassLoader =
                    context.getCoordinatorContext().getUserCodeClassloader();
            try (TemporaryClassLoaderContext ignored =
                    TemporaryClassLoaderContext.of(userCodeClassLoader)) {
                enumerator = source.createEnumerator(context);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                LOG.error("Failed to create Source Enumerator for source {}", operatorName, t);
                context.failJob(t);
                return;
            }
        }

        // The start sequence is the first task in the coordinator executor.
        // We rely on the single-threaded coordinator executor to guarantee
        // the other methods are invoked after the enumerator has started.
        runInEventLoop(() -> enumerator.start(), "starting the SplitEnumerator.");

        if (coordinatorListeningID != null) {
            coordinatorStore.compute(
                    coordinatorListeningID,
                    (key, oldValue) -> {
                        // The value for a listener ID can be a source coordinator listening to an
                        // event, or an event waiting to be retrieved
                        if (oldValue == null || oldValue instanceof OperatorCoordinator) {
                            // The coordinator has not registered or needs to be recreated after
                            // global failover.
                            return this;
                        } else {
                            checkState(
                                    oldValue instanceof OperatorEvent,
                                    "The existing value for "
                                            + coordinatorStore
                                            + "is expected to be an operator event, but it is in fact "
                                            + oldValue);
                            LOG.info(
                                    "Handling event {} received before the source coordinator with ID {} is registered",
                                    oldValue,
                                    coordinatorListeningID);
                            handleEventFromOperator(0, 0, (OperatorEvent) oldValue);

                            // Since for non-global failover the coordinator will not be recreated
                            // and for global failover both the sender and receiver need to restart,
                            // the coordinator will receive the event only once.
                            // As the event has been processed, it can be removed safely and there's
                            // no need to register the coordinator for further events as well.
                            return null;
                        }
                    });
        }

        if (watermarkAlignmentParams.isEnabled()) {
            LOG.info("Starting schedule the period announceCombinedWatermark task");
            coordinatorStore.putIfAbsent(
                    watermarkAlignmentParams.getWatermarkGroup(), new WatermarkAggregator<>());
            context.schedulePeriodTask(
                    this::announceCombinedWatermark,
                    watermarkAlignmentParams.getUpdateInterval(),
                    watermarkAlignmentParams.getUpdateInterval(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() throws Exception {
        try (MdcUtils.MdcCloseable mdcCloseable =
                MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
            LOG.info("Closing SourceCoordinator for source {}.", operatorName);
            if (started) {
                closeQuietly(enumerator);
            }
            closeQuietly(context);
            LOG.info("Source coordinator for source {} closed.", operatorName);
        }
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    if (event instanceof RequestSplitEvent) {
                        handleRequestSplitEvent(subtask, attemptNumber, (RequestSplitEvent) event);
                    } else if (event instanceof SourceEventWrapper) {
                        handleSourceEvent(
                                subtask,
                                attemptNumber,
                                ((SourceEventWrapper) event).getSourceEvent());
                    } else if (event instanceof ReaderRegistrationEvent) {
                        handleReaderRegistrationEvent(
                                subtask, attemptNumber, (ReaderRegistrationEvent) event);
                    } else if (event instanceof ReportedWatermarkEvent) {
                        handleReportedWatermark(
                                subtask,
                                new Watermark(((ReportedWatermarkEvent) event).getWatermark()));
                    } else if (event instanceof ReportedRecordsEvent) {
                        handleReportedRecords(subtask, (ReportedRecordsEvent) event);
                    } else {
                        throw new FlinkException("Unrecognized Operator Event: " + event);
                    }
                },
                "handling operator event %s from subtask %d (#%d)",
                event,
                subtask,
                attemptNumber);
    }

    @Override
    public void executionAttemptFailed(
            int subtaskId, int attemptNumber, @Nullable Throwable reason) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Removing registered reader after failure for subtask {} (#{}) of source {}.",
                            subtaskId,
                            attemptNumber,
                            operatorName);

                    context.unregisterSourceReader(subtaskId, attemptNumber);
                    context.attemptFailed(subtaskId, attemptNumber);
                    forgetReportedThroughput(subtaskId);
                },
                "handling subtask %d (#%d) failure",
                subtaskId,
                attemptNumber);
    }

    @Override
    public void subtaskReset(int subtaskId, long checkpointId) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Recovering subtask {} to checkpoint {} for source {} to checkpoint.",
                            subtaskId,
                            checkpointId,
                            operatorName);

                    context.subtaskReset(subtaskId);
                    forgetReportedThroughput(subtaskId);

                    final List<SplitT> splitsToAddBack =
                            context.getAndRemoveUncheckpointedAssignment(subtaskId, checkpointId);
                    LOG.debug(
                            "Adding splits back to the split enumerator of source {}: {}",
                            operatorName,
                            splitsToAddBack);
                    enumerator.addSplitsBack(splitsToAddBack, subtaskId);
                },
                "handling subtask %d recovery to checkpoint %d",
                subtaskId,
                checkpointId);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        checkArgument(subtask == gateway.getSubtask());
        checkArgument(attemptNumber == gateway.getExecution().getAttemptNumber());

        runInEventLoop(
                () -> context.attemptReady(gateway),
                "making event gateway to subtask %d (#%d) available",
                subtask,
                attemptNumber);
    }

    /**
     * Whether the enumerator supports batch snapshot. Note the enumerator is created either during
     * resetting the coordinator to a checkpoint, or when the coordinator is started.
     */
    @Override
    public boolean supportsBatchSnapshot() {
        checkNotNull(enumerator);
        return enumerator instanceof SupportsBatchSnapshot;
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Taking a state snapshot on operator {} for checkpoint {}",
                            operatorName,
                            checkpointId);
                    try {
                        if (checkpointId == BATCH_CHECKPOINT_ID) {
                            checkState(supportsBatchSnapshot());
                            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                    DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {
                                // assignments
                                byte[] assignmentData =
                                        context.getAssignmentTracker()
                                                .snapshotState(splitSerializer);
                                out.writeInt(assignmentData.length);
                                out.write(assignmentData);

                                // enumerator
                                byte[] enumeratorData = toBytes(checkpointId);
                                out.writeInt(enumeratorData.length);
                                out.write(enumeratorData);
                                out.flush();
                                result.complete(baos.toByteArray());
                            }
                        } else {
                            if (dynamicRateLimitParams.isEnabled()) {
                                checkpointDurationTracker.onCheckpointStart(
                                        checkpointId, clock.absoluteTimeMillis());
                            }
                            context.onCheckpoint(checkpointId);
                            result.complete(toBytes(checkpointId));
                        }
                    } catch (Throwable e) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                        result.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint SplitEnumerator for source %s",
                                                operatorName),
                                        e));
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Marking checkpoint {} as completed for source {}.",
                            checkpointId,
                            operatorName);
                    context.onCheckpointComplete(checkpointId);
                    enumerator.notifyCheckpointComplete(checkpointId);

                    if (dynamicRateLimitParams.isEnabled()) {
                        adjustDynamicRateLimit(checkpointId);
                    }
                },
                "notifying the enumerator of completion of checkpoint %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Marking checkpoint {} as aborted for source {}.",
                            checkpointId,
                            operatorName);
                    enumerator.notifyCheckpointAborted(checkpointId);
                    reportedRecordRates.remove(checkpointId);
                },
                "calling notifyCheckpointAborted()");
    }

    @Override
    public void resetToCheckpoint(final long checkpointId, @Nullable final byte[] checkpointData)
            throws Exception {

        checkState(!started, "The coordinator can only be reset if it was not yet started");
        assert enumerator == null;

        // the checkpoint data is null if there was no completed checkpoint before
        // in that case we don't restore here, but let a fresh SplitEnumerator be created
        // when "start()" is called.
        if (checkpointData == null) {
            return;
        }

        LOG.info("Restoring SplitEnumerator of source {} from checkpoint.", operatorName);

        final ClassLoader userCodeClassLoader =
                context.getCoordinatorContext().getUserCodeClassloader();

        if (checkpointId == BATCH_CHECKPOINT_ID) {
            try (TemporaryClassLoaderContext ignored =
                    TemporaryClassLoaderContext.of(userCodeClassLoader)) {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                        DataInputStream in = new DataInputViewStreamWrapper(bais)) {
                    int assignmentDataLength = in.readInt();
                    byte[] assignmentData =
                            SourceCoordinatorSerdeUtils.readBytes(in, assignmentDataLength);

                    int enumeratorDataLength = in.readInt();
                    byte[] enumeratorData =
                            SourceCoordinatorSerdeUtils.readBytes(in, enumeratorDataLength);

                    final EnumChkT enumeratorCheckpoint = deserializeCheckpoint(enumeratorData);
                    enumerator = source.restoreEnumerator(context, enumeratorCheckpoint);

                    context.getAssignmentTracker()
                            .restoreState(source.getSplitSerializer(), assignmentData);
                }
            }
        } else {
            try (TemporaryClassLoaderContext ignored =
                    TemporaryClassLoaderContext.of(userCodeClassLoader)) {
                final EnumChkT enumeratorCheckpoint = deserializeCheckpoint(checkpointData);
                enumerator = source.restoreEnumerator(context, enumeratorCheckpoint);
            }
        }
    }

    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {

        ensureStarted();

        // we may end up here even for a non-started enumerator, in case the instantiation
        // failed, and we get the 'subtaskFailed()' notification during the failover.
        // we need to ignore those.
        if (enumerator == null) {
            return;
        }

        context.runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // if we have a JVM critical error, promote it immediately, there is a good
                        // chance the
                        // logging or job failing will not succeed any more
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the SplitEnumerator for Source {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    // ---------------------------------------------------
    @VisibleForTesting
    public SplitEnumerator<SplitT, EnumChkT> getEnumerator() {
        return enumerator;
    }

    @VisibleForTesting
    SourceCoordinatorContext<SplitT> getContext() {
        return context;
    }

    @VisibleForTesting
    public ExecutorService getCoordinatorExecutor() {
        return context.getCoordinatorExecutor();
    }

    // --------------------- Serde -----------------------

    /**
     * Serialize the coordinator state. The current implementation may not be super efficient, but
     * it should not matter that much because most of the state should be rather small. Large states
     * themselves may already be a problem regardless of how the serialization is implemented.
     *
     * @return A byte array containing the serialized state of the source coordinator.
     * @throws Exception When something goes wrong in serialization.
     */
    private byte[] toBytes(long checkpointId) throws Exception {
        return writeCheckpointBytes(
                enumerator.snapshotState(checkpointId), enumCheckpointSerializer);
    }

    static <EnumChkT> byte[] writeCheckpointBytes(
            final EnumChkT enumeratorCheckpoint,
            final SimpleVersionedSerializer<EnumChkT> enumeratorCheckpointSerializer)
            throws Exception {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {

            writeCoordinatorSerdeVersion(out);
            out.writeInt(enumeratorCheckpointSerializer.getVersion());
            byte[] serialziedEnumChkpt =
                    enumeratorCheckpointSerializer.serialize(enumeratorCheckpoint);
            out.writeInt(serialziedEnumChkpt.length);
            out.write(serialziedEnumChkpt);
            out.flush();
            return baos.toByteArray();
        }
    }

    /**
     * Restore the state of this source coordinator from the state bytes.
     *
     * @param bytes The checkpoint bytes that was returned from {@link #toBytes(long)}
     * @throws Exception When the deserialization failed.
     */
    private EnumChkT deserializeCheckpoint(byte[] bytes) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputViewStreamWrapper(bais)) {
            final int coordinatorSerdeVersion = readAndVerifyCoordinatorSerdeVersion(in);
            int enumSerializerVersion = in.readInt();
            int serializedEnumChkptSize = in.readInt();
            byte[] serializedEnumChkpt = readBytes(in, serializedEnumChkptSize);

            if (coordinatorSerdeVersion != SourceCoordinatorSerdeUtils.VERSION_0
                    && bais.available() > 0) {
                throw new IOException("Unexpected trailing bytes in enumerator checkpoint data");
            }

            return enumCheckpointSerializer.deserialize(enumSerializerVersion, serializedEnumChkpt);
        }
    }

    // --------------------- private methods -------------

    private void handleRequestSplitEvent(int subtask, int attemptNumber, RequestSplitEvent event) {
        LOG.info(
                "Source {} received split request from parallel task {} (#{})",
                operatorName,
                subtask,
                attemptNumber);

        // request splits from the enumerator only if the enumerator has un-assigned splits
        // this helps to reduce unnecessary split requests to the enumerator
        if (!context.hasNoMoreSplits(subtask)) {
            enumerator.handleSplitRequest(subtask, event.hostName());
        }
    }

    private void handleSourceEvent(int subtask, int attemptNumber, SourceEvent event) {
        LOG.debug(
                "Source {} received custom event from parallel task {} (#{}): {}",
                operatorName,
                subtask,
                attemptNumber,
                event);

        if (context.isConcurrentExecutionAttemptsSupported()) {
            checkState(
                    enumerator instanceof SupportsHandleExecutionAttemptSourceEvent,
                    "The split enumerator %s must implement SupportsHandleExecutionAttemptSourceEvent "
                            + "to be used in concurrent execution attempts scenario (e.g. if "
                            + "speculative execution is enabled).",
                    enumerator.getClass().getCanonicalName());
            ((SupportsHandleExecutionAttemptSourceEvent) enumerator)
                    .handleSourceEvent(subtask, attemptNumber, event);
        } else {
            enumerator.handleSourceEvent(subtask, event);
        }
    }

    private void handleReaderRegistrationEvent(
            int subtask, int attemptNumber, ReaderRegistrationEvent event) throws Exception {
        checkArgument(subtask == event.subtaskId());

        LOG.info(
                "Source {} registering reader for parallel task {} (#{}) @ {}",
                operatorName,
                subtask,
                attemptNumber,
                event.location());

        final boolean subtaskReaderExisted =
                context.registeredReadersOfAttempts().containsKey(subtask);
        context.registerSourceReader(
                subtask, attemptNumber, event.location(), event.splits(splitSerializer));
        if (!subtaskReaderExisted) {
            enumerator.addReader(event.subtaskId());

            final Boolean isBacklog = context.isBacklog().getAsBoolean();
            if (isBacklog != null) {
                context.sendEventToSourceOperatorIfTaskReady(
                        subtask, new IsProcessingBacklogEvent(isBacklog));
            }

            if (dynamicRateLimitParams.isEnabled()) {
                if (currentAllocations.isEmpty()) {
                    updateAllocations();
                }
                sendRateLimitToSubtask(subtask);
            }
        }
    }

    private void handleReportedWatermark(int subtask, Watermark watermark) throws FlinkException {
        if (context.isConcurrentExecutionAttemptsSupported()) {
            throw new FlinkException(
                    "ReportedWatermarkEvent is not supported in concurrent execution attempts "
                            + "scenario (e.g. if speculative execution is enabled)");
        }

        LOG.debug(
                "New reported watermark={} from subTaskId={} of source {}.",
                watermark,
                subtask,
                operatorName);

        checkState(watermarkAlignmentParams.isEnabled());

        combinedWatermark
                .aggregate(subtask, watermark)
                .ifPresent(
                        newCombinedWatermark ->
                                coordinatorStore.computeIfPresent(
                                        watermarkAlignmentParams.getWatermarkGroup(),
                                        (key, oldValue) -> {
                                            WatermarkAggregator<String> watermarkAggregator =
                                                    (WatermarkAggregator<String>) oldValue;
                                            watermarkAggregator.aggregate(
                                                    operatorName, newCombinedWatermark);
                                            return watermarkAggregator;
                                        }));
    }

    private void adjustDynamicRateLimit(long checkpointId) {
        final OptionalLong checkpointDuration =
                checkpointDurationTracker.onCheckpointComplete(
                        checkpointId, clock.absoluteTimeMillis());
        updateReportedThroughputs(checkpointId);
        if (!checkpointDuration.isPresent()) {
            return;
        }
        // Scale off the observed throughput rather than the last broadcast target, which may
        // already lag behind it. Fall back to the target if nothing has been observed yet.
        final double observedThroughput = getCurrentTotalThroughput();
        final double baseline =
                Double.isNaN(observedThroughput)
                        ? currentTargetRecordsPerSecond
                        : observedThroughput;
        currentTargetRecordsPerSecond =
                dynamicRateLimitParams
                        .getRateLimitCalculator()
                        .computeNewRate(
                                checkpointDurationTracker.getRecentDurations(),
                                baseline,
                                context.getCheckpointIntervalMillis(),
                                dynamicRateLimitParams);
        announceDynamicRateLimit();
    }

    private void handleReportedRecords(int subtask, ReportedRecordsEvent event) {
        LOG.debug(
                "New reported records={} over {}ms for checkpoint {} from subTaskId={} of source {}.",
                event.getNumRecords(),
                event.getElapsedMillis(),
                event.getCheckpointId(),
                subtask,
                operatorName);
        if (event.getElapsedMillis() <= 0) {
            return;
        }
        reportedRecordRates
                .computeIfAbsent(event.getCheckpointId(), id -> new HashMap<>())
                .put(subtask, event.getNumRecords() * 1000.0 / event.getElapsedMillis());
    }

    /**
     * Makes the throughputs reported for the completed checkpoint the current ones. Subtasks that
     * did not report keep their previous throughput.
     */
    private void updateReportedThroughputs(long completedCheckpointId) {
        final Map<Integer, Double> rates = reportedRecordRates.remove(completedCheckpointId);
        reportedRecordRates.keySet().removeIf(id -> id <= completedCheckpointId);
        if (rates != null) {
            reportedThroughputs.putAll(rates);
        }
        final int parallelism = context.currentParallelism();
        reportedThroughputs.keySet().removeIf(subtask -> subtask >= parallelism);
    }

    private void forgetReportedThroughput(int subtaskId) {
        reportedThroughputs.remove(subtaskId);
        reportedRecordRates.values().forEach(rates -> rates.remove(subtaskId));
    }

    /**
     * Returns the sum of the most recently reported per-subtask throughputs, or {@code Double.NaN}
     * if no subtask has reported yet or nothing was emitted. An idle source must not be taken as a
     * baseline, otherwise the rate would collapse to its minimum and could not recover.
     */
    private double getCurrentTotalThroughput() {
        final double total =
                reportedThroughputs.values().stream().mapToDouble(Double::doubleValue).sum();
        return total > 0 ? total : Double.NaN;
    }

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }

    private Optional<DynamicFilteringInfo> getSourceDynamicFilteringInfo() {
        if (coordinatorListeningID != null
                && coordinatorStore.containsKey(coordinatorListeningID)) {
            Object event = coordinatorStore.get(coordinatorListeningID);
            if (event instanceof SourceEventWrapper) {
                SourceEvent sourceEvent = ((SourceEventWrapper) event).getSourceEvent();
                if (sourceEvent instanceof DynamicFilteringInfo) {
                    return Optional.of((DynamicFilteringInfo) sourceEvent);
                }
            }
        }

        return Optional.empty();
    }

    public CompletableFuture<Integer> inferSourceParallelismAsync(
            int parallelismInferenceUpperBound, long dataVolumePerTask) {
        return context.supplyAsync(
                        () -> {
                            if (!(source instanceof DynamicParallelismInference)) {
                                return ExecutionConfig.PARALLELISM_DEFAULT;
                            }

                            DynamicParallelismInference parallelismInference =
                                    (DynamicParallelismInference) source;
                            try {
                                return parallelismInference.inferParallelism(
                                        new DynamicParallelismInference.Context() {
                                            @Override
                                            public Optional<DynamicFilteringInfo>
                                                    getDynamicFilteringInfo() {
                                                return getSourceDynamicFilteringInfo();
                                            }

                                            @Override
                                            public int getParallelismInferenceUpperBound() {
                                                return parallelismInferenceUpperBound;
                                            }

                                            @Override
                                            public long getDataVolumePerTask() {
                                                return dataVolumePerTask;
                                            }
                                        });
                            } catch (Throwable e) {
                                LOG.error(
                                        "Unexpected error occurred when dynamically inferring source parallelism.",
                                        e);
                                return ExecutionConfig.PARALLELISM_DEFAULT;
                            }
                        })
                .thenApply(future -> (Integer) future);
    }

    /** The watermark element for {@link HeapPriorityQueue}. */
    public static class WatermarkElement extends AbstractHeapPriorityQueueElement
            implements PriorityComparable<WatermarkElement> {

        private final Watermark watermark;

        public WatermarkElement(Watermark watermark) {
            this.watermark = watermark;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof WatermarkElement) {
                return watermark.equals(((WatermarkElement) o).watermark);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return watermark.hashCode();
        }

        @Override
        public int comparePriorityTo(@Nonnull WatermarkElement other) {
            return Long.compare(watermark.getTimestamp(), other.watermark.getTimestamp());
        }
    }

    /** The aggregated watermark is the smallest watermark of all keys. */
    static class WatermarkAggregator<T> {

        private final Map<T, WatermarkElement> watermarks = new HashMap<>();

        private final HeapPriorityQueue<WatermarkElement> orderedWatermarks =
                new HeapPriorityQueue<>(PriorityComparator.forPriorityComparableObjects(), 10);

        private static final Watermark DEFAULT_WATERMARK = new Watermark(Long.MIN_VALUE);

        /**
         * Update the {@link Watermark} for the given {@code key)}.
         *
         * @return the new updated combined {@link Watermark} if the value has changed. {@code
         *     Optional.empty()} otherwise.
         */
        public Optional<Watermark> aggregate(T key, Watermark watermark) {
            Watermark oldAggregatedWatermark = getAggregatedWatermark();

            WatermarkElement watermarkElement = new WatermarkElement(watermark);
            WatermarkElement oldWatermarkElement = watermarks.put(key, watermarkElement);
            if (oldWatermarkElement != null) {
                orderedWatermarks.remove(oldWatermarkElement);
            }
            orderedWatermarks.add(watermarkElement);

            Watermark newAggregatedWatermark = getAggregatedWatermark();
            if (newAggregatedWatermark.equals(oldAggregatedWatermark)) {
                return Optional.empty();
            }
            return Optional.of(newAggregatedWatermark);
        }

        public Set<T> keySet() {
            return watermarks.keySet();
        }

        public Watermark getAggregatedWatermark() {
            WatermarkElement aggregatedWatermarkElement = orderedWatermarks.peek();
            return aggregatedWatermarkElement == null
                    ? DEFAULT_WATERMARK
                    : aggregatedWatermarkElement.watermark;
        }
    }
}
