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
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
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
import org.apache.flink.util.TemporaryClassLoaderContext;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readAndVerifyCoordinatorSerdeVersion;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readBytes;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.writeCoordinatorSerdeVersion;
import static org.apache.flink.util.IOUtils.closeQuietly;
import static org.apache.flink.util.Preconditions.checkArgument;
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

    /** The name of the operator this SourceCoordinator is associated with. */
    private final String operatorName;
    /** The Source that is associated with this SourceCoordinator. */
    private final Source<?, SplitT, EnumChkT> source;
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
                operatorName,
                source,
                context,
                coordinatorStore,
                WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                null);
    }

    public SourceCoordinator(
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore,
            WatermarkAlignmentParams watermarkAlignmentParams,
            @Nullable String coordinatorListeningID) {
        this.operatorName = operatorName;
        this.source = source;
        this.enumCheckpointSerializer = source.getEnumeratorCheckpointSerializer();
        this.context = context;
        this.coordinatorStore = coordinatorStore;
        this.watermarkAlignmentParams = watermarkAlignmentParams;
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

        Set<Integer> subTaskIds = combinedWatermark.keySet();
        LOG.info(
                "Distributing maxAllowedWatermark={} of group={} to subTaskIds={} for source {}.",
                maxAllowedWatermark,
                watermarkAlignmentParams.getWatermarkGroup(),
                subTaskIds,
                operatorName);

        // Subtask maybe during deploying or restarting, so we only send WatermarkAlignmentEvent
        // to ready task to avoid period task fail (Java-ThreadPoolExecutor will not schedule
        // the period task if it throws an exception).
        for (Integer subtaskId : subTaskIds) {
            context.sendEventToSourceOperatorIfTaskReady(
                    subtaskId, new WatermarkAlignmentEvent(maxAllowedWatermark));
        }
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
        LOG.info("Closing SourceCoordinator for source {}.", operatorName);
        if (started) {
            closeQuietly(enumerator);
        }
        closeQuietly(context);
        LOG.info("Source coordinator for source {} closed.", operatorName);
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

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Taking a state snapshot on operator {} for checkpoint {}",
                            operatorName,
                            checkpointId);
                    try {
                        context.onCheckpoint(checkpointId);
                        result.complete(toBytes(checkpointId));
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
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userCodeClassLoader)) {
            final EnumChkT enumeratorCheckpoint = deserializeCheckpoint(checkpointData);
            enumerator = source.restoreEnumerator(context, enumeratorCheckpoint);
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
    SplitEnumerator<SplitT, EnumChkT> getEnumerator() {
        return enumerator;
    }

    @VisibleForTesting
    SourceCoordinatorContext<SplitT> getContext() {
        return context;
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
            int subtask, int attemptNumber, ReaderRegistrationEvent event) {
        checkArgument(subtask == event.subtaskId());

        LOG.info(
                "Source {} registering reader for parallel task {} (#{}) @ {}",
                operatorName,
                subtask,
                attemptNumber,
                event.location());

        final boolean subtaskReaderExisted =
                context.registeredReadersOfAttempts().containsKey(subtask);
        context.registerSourceReader(subtask, attemptNumber, event.location());
        if (!subtaskReaderExisted) {
            enumerator.addReader(event.subtaskId());
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

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
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
