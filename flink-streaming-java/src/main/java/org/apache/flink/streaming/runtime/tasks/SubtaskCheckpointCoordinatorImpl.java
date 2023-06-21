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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.Cancellable;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.DelayableTimer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.IOUtils.closeQuietly;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class SubtaskCheckpointCoordinatorImpl implements SubtaskCheckpointCoordinator {

    private static final Logger LOG =
            LoggerFactory.getLogger(SubtaskCheckpointCoordinatorImpl.class);

    private static final int CHECKPOINT_EXECUTION_DELAY_LOG_THRESHOLD_MS = 30_000;

    private final boolean enableCheckpointAfterTasksFinished;

    private final CachingCheckpointStorageWorkerView checkpointStorage;
    private final String taskName;
    private final ExecutorService asyncOperationsThreadPool;
    private final Environment env;
    private final AsyncExceptionHandler asyncExceptionHandler;
    private final ChannelStateWriter channelStateWriter;
    private final StreamTaskActionExecutor actionExecutor;
    private final BiFunctionWithException<
                    ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
            prepareInputSnapshot;
    /** The IDs of the checkpoint for which we are notified aborted. */
    private final Set<Long> abortedCheckpointIds;

    private final int maxRecordAbortedCheckpoints;

    private long maxAbortedCheckpointId = 0;

    private long lastCheckpointId;

    /** Lock that guards state of AsyncCheckpointRunnable registry. * */
    private final Object lock;

    @GuardedBy("lock")
    private final Map<Long, AsyncCheckpointRunnable> checkpoints;

    /** Indicates if this registry is closed. */
    @GuardedBy("lock")
    private boolean closed;

    private final DelayableTimer registerTimer;

    private final Clock clock;

    /** It always be called in Task Thread. */
    private Cancellable alignmentTimer;

    /**
     * It is the checkpointId corresponding to alignmentTimer. And It should be always updated with
     * {@link #alignmentTimer}.
     */
    private long alignmentCheckpointId;

    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorage checkpointStorage,
            CheckpointStorageWorkerView checkpointStorageView,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            boolean unalignedCheckpointEnabled,
            boolean enableCheckpointAfterTasksFinished,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            DelayableTimer registerTimer,
            int maxSubtasksPerChannelStateFile) {
        this(
                checkpointStorageView,
                taskName,
                actionExecutor,
                asyncOperationsThreadPool,
                env,
                asyncExceptionHandler,
                prepareInputSnapshot,
                maxRecordAbortedCheckpoints,
                unalignedCheckpointEnabled
                        ? openChannelStateWriter(
                                taskName, checkpointStorage, env, maxSubtasksPerChannelStateFile)
                        : ChannelStateWriter.NO_OP,
                enableCheckpointAfterTasksFinished,
                registerTimer);
    }

    @VisibleForTesting
    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            ChannelStateWriter channelStateWriter,
            boolean enableCheckpointAfterTasksFinished,
            DelayableTimer registerTimer) {
        this.checkpointStorage =
                new CachingCheckpointStorageWorkerView(checkNotNull(checkpointStorage));
        this.taskName = checkNotNull(taskName);
        this.checkpoints = new HashMap<>();
        this.lock = new Object();
        this.asyncOperationsThreadPool = checkNotNull(asyncOperationsThreadPool);
        this.env = checkNotNull(env);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.actionExecutor = checkNotNull(actionExecutor);
        this.channelStateWriter = checkNotNull(channelStateWriter);
        this.prepareInputSnapshot = prepareInputSnapshot;
        this.abortedCheckpointIds =
                createAbortedCheckpointSetWithLimitSize(maxRecordAbortedCheckpoints);
        this.maxRecordAbortedCheckpoints = maxRecordAbortedCheckpoints;
        this.lastCheckpointId = -1L;
        this.closed = false;
        this.enableCheckpointAfterTasksFinished = enableCheckpointAfterTasksFinished;
        this.registerTimer = registerTimer;
        this.clock = SystemClock.getInstance();
    }

    private static ChannelStateWriter openChannelStateWriter(
            String taskName,
            CheckpointStorage checkpointStorage,
            Environment env,
            int maxSubtasksPerChannelStateFile) {
        return new ChannelStateWriterImpl(
                env.getJobVertexId(),
                taskName,
                env.getTaskInfo().getIndexOfThisSubtask(),
                checkpointStorage,
                env.getChannelStateExecutorFactory(),
                maxSubtasksPerChannelStateFile);
    }

    @Override
    public void abortCheckpointOnBarrier(
            long checkpointId, CheckpointException cause, OperatorChain<?, ?> operatorChain)
            throws IOException {
        LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, taskName);
        lastCheckpointId = Math.max(lastCheckpointId, checkpointId);
        Iterator<Long> iterator = abortedCheckpointIds.iterator();
        while (iterator.hasNext()) {
            long next = iterator.next();
            if (next < lastCheckpointId) {
                iterator.remove();
            } else {
                break;
            }
        }

        checkpointStorage.clearCacheFor(checkpointId);

        channelStateWriter.abort(checkpointId, cause, true);

        // notify the coordinator that we decline this checkpoint
        env.declineCheckpoint(checkpointId, cause);

        actionExecutor.runThrowing(
                () -> {
                    if (checkpointId == alignmentCheckpointId) {
                        cancelAlignmentTimer();
                    }
                    // notify all downstream operators that they should not wait for a barrier from
                    // us and abort checkpoint.
                    operatorChain.abortCheckpoint(checkpointId, cause);
                    operatorChain.broadcastEvent(new CancelCheckpointMarker(checkpointId));
                });
    }

    private void cancelAlignmentTimer() {
        if (alignmentTimer == null) {
            return;
        }
        alignmentTimer.cancel();
        alignmentTimer = null;
    }

    @Override
    public CheckpointStorageWorkerView getCheckpointStorage() {
        return checkpointStorage;
    }

    @Override
    public ChannelStateWriter getChannelStateWriter() {
        return channelStateWriter;
    }

    @Override
    public void checkpointState(
            CheckpointMetaData metadata,
            CheckpointOptions options,
            CheckpointMetricsBuilder metrics,
            OperatorChain<?, ?> operatorChain,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws Exception {

        checkNotNull(options);
        checkNotNull(metrics);

        // All of the following steps happen as an atomic step from the perspective of barriers and
        // records/watermarks/timers/callbacks.
        // We generally try to emit the checkpoint barrier as soon as possible to not affect
        // downstream
        // checkpoint alignments

        if (lastCheckpointId >= metadata.getCheckpointId()) {
            LOG.info(
                    "Out of order checkpoint barrier (aborted previously?): {} >= {}",
                    lastCheckpointId,
                    metadata.getCheckpointId());
            channelStateWriter.abort(metadata.getCheckpointId(), new CancellationException(), true);
            checkAndClearAbortedStatus(metadata.getCheckpointId());
            return;
        }

        logCheckpointProcessingDelay(metadata);

        // Step (0): Record the last triggered checkpointId and abort the sync phase of checkpoint
        // if necessary.
        lastCheckpointId = metadata.getCheckpointId();
        if (checkAndClearAbortedStatus(metadata.getCheckpointId())) {
            // broadcast cancel checkpoint marker to avoid downstream back-pressure due to
            // checkpoint barrier align.
            operatorChain.broadcastEvent(new CancelCheckpointMarker(metadata.getCheckpointId()));
            channelStateWriter.abort(
                    metadata.getCheckpointId(),
                    new CancellationException("checkpoint aborted via notification"),
                    true);
            LOG.info(
                    "Checkpoint {} has been notified as aborted, would not trigger any checkpoint.",
                    metadata.getCheckpointId());
            return;
        }

        // if checkpoint has been previously unaligned, but was forced to be aligned (pointwise
        // connection), revert it here so that it can jump over output data
        if (options.getAlignment() == CheckpointOptions.AlignmentType.FORCED_ALIGNED) {
            options = options.withUnalignedSupported();
            initInputsCheckpoint(metadata.getCheckpointId(), options);
        }

        // Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
        //           The pre-barrier work should be nothing or minimal in the common case.
        operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());

        // Step (2): Send the checkpoint barrier downstream
        LOG.debug(
                "Task {} broadcastEvent at {}, triggerTime {}, passed time {}",
                taskName,
                System.currentTimeMillis(),
                metadata.getTimestamp(),
                System.currentTimeMillis() - metadata.getTimestamp());
        CheckpointBarrier checkpointBarrier =
                new CheckpointBarrier(metadata.getCheckpointId(), metadata.getTimestamp(), options);
        operatorChain.broadcastEvent(checkpointBarrier, options.isUnalignedCheckpoint());

        // Step (3): Register alignment timer to timeout aligned barrier to unaligned barrier
        registerAlignmentTimer(metadata.getCheckpointId(), operatorChain, checkpointBarrier);

        // Step (4): Prepare to spill the in-flight buffers for input and output
        if (options.needsChannelState()) {
            // output data already written while broadcasting event
            channelStateWriter.finishOutput(metadata.getCheckpointId());
        }

        // Step (5): Take the state snapshot. This should be largely asynchronous, to not impact
        // progress of the
        // streaming topology

        Map<OperatorID, OperatorSnapshotFutures> snapshotFutures =
                CollectionUtil.newHashMapWithExpectedSize(operatorChain.getNumberOfOperators());
        try {
            if (takeSnapshotSync(
                    snapshotFutures, metadata, metrics, options, operatorChain, isRunning)) {
                finishAndReportAsync(
                        snapshotFutures,
                        metadata,
                        metrics,
                        operatorChain.isTaskDeployedAsFinished(),
                        isTaskFinished,
                        isRunning);
            } else {
                cleanup(snapshotFutures, metadata, metrics, new Exception("Checkpoint declined"));
            }
        } catch (Exception ex) {
            cleanup(snapshotFutures, metadata, metrics, ex);
            throw ex;
        }
    }

    private void registerAlignmentTimer(
            long checkpointId,
            OperatorChain<?, ?> operatorChain,
            CheckpointBarrier checkpointBarrier) {
        // The timer isn't triggered when the checkpoint completes quickly, so cancel timer here.
        cancelAlignmentTimer();
        if (!checkpointBarrier.getCheckpointOptions().isTimeoutable()) {
            return;
        }

        long timerDelay = BarrierAlignmentUtil.getTimerDelay(clock, checkpointBarrier);

        alignmentTimer =
                registerTimer.registerTask(
                        () -> {
                            try {
                                operatorChain.alignedBarrierTimeout(checkpointId);
                            } catch (Exception e) {
                                ExceptionUtils.rethrowIOException(e);
                            }
                            alignmentTimer = null;
                            return null;
                        },
                        Duration.ofMillis(timerDelay));
        alignmentCheckpointId = checkpointId;
    }

    @Override
    public void notifyCheckpointComplete(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.COMPLETE);
    }

    @Override
    public void notifyCheckpointAborted(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.ABORT);
    }

    @Override
    public void notifyCheckpointSubsumed(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning,
            Task.NotifyCheckpointOperation notifyCheckpointOperation)
            throws Exception {

        Exception previousException = null;
        try {
            if (!isRunning.get()) {
                LOG.debug(
                        "Ignoring notification of checkpoint {} {} for not-running task {}",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskName);
            } else {
                LOG.debug(
                        "Notification of checkpoint {} {} for task {}",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskName);

                if (notifyCheckpointOperation.equals(Task.NotifyCheckpointOperation.ABORT)) {
                    boolean canceled = cancelAsyncCheckpointRunnable(checkpointId);

                    if (!canceled) {
                        if (checkpointId > lastCheckpointId) {
                            // only record checkpoints that have not triggered on task side.
                            abortedCheckpointIds.add(checkpointId);
                            maxAbortedCheckpointId = Math.max(maxAbortedCheckpointId, checkpointId);
                        }
                    }

                    channelStateWriter.abort(
                            checkpointId,
                            new CancellationException("checkpoint aborted via notification"),
                            false);
                }

                try {
                    switch (notifyCheckpointOperation) {
                        case ABORT:
                            operatorChain.notifyCheckpointAborted(checkpointId);
                            break;
                        case COMPLETE:
                            operatorChain.notifyCheckpointComplete(checkpointId);
                            break;
                        case SUBSUME:
                            operatorChain.notifyCheckpointSubsumed(checkpointId);
                    }
                } catch (Exception e) {
                    previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
                }
            }
        } finally {
            try {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        env.getTaskStateManager().notifyCheckpointAborted(checkpointId);
                        break;
                    case COMPLETE:
                        env.getTaskStateManager().notifyCheckpointComplete(checkpointId);
                }
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }

        ExceptionUtils.tryRethrowException(previousException);
    }

    @Override
    public void initInputsCheckpoint(long id, CheckpointOptions checkpointOptions)
            throws CheckpointException {
        if (checkpointOptions.isUnalignedCheckpoint()) {
            channelStateWriter.start(id, checkpointOptions);

            prepareInflightDataSnapshot(id);
        } else if (checkpointOptions.isTimeoutable()) {
            // The output buffer may need to be snapshotted, so start the channelStateWriter here.
            channelStateWriter.start(id, checkpointOptions);
            channelStateWriter.finishInput(id);
        }
    }

    public void waitForPendingCheckpoints() throws Exception {
        if (!enableCheckpointAfterTasksFinished) {
            return;
        }

        List<AsyncCheckpointRunnable> asyncCheckpointRunnables;
        synchronized (lock) {
            asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
        }

        // Waits for each checkpoint independently.
        asyncCheckpointRunnables.forEach(
                ar -> {
                    try {
                        ar.getFinishedFuture().get();
                    } catch (Exception e) {
                        LOG.debug(
                                "Async runnable for checkpoint "
                                        + ar.getCheckpointId()
                                        + " throws exception and exit",
                                e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        cancelAlignmentTimer();
        cancel();
    }

    public void cancel() throws IOException {
        List<AsyncCheckpointRunnable> asyncCheckpointRunnables = null;
        synchronized (lock) {
            if (!closed) {
                closed = true;
                asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
                checkpoints.clear();
            }
        }
        IOUtils.closeAllQuietly(asyncCheckpointRunnables);
        channelStateWriter.close();
    }

    @VisibleForTesting
    int getAsyncCheckpointRunnableSize() {
        synchronized (lock) {
            return checkpoints.size();
        }
    }

    @VisibleForTesting
    int getAbortedCheckpointSize() {
        return abortedCheckpointIds.size();
    }

    private boolean checkAndClearAbortedStatus(long checkpointId) {
        return abortedCheckpointIds.remove(checkpointId)
                || checkpointId + maxRecordAbortedCheckpoints < maxAbortedCheckpointId;
    }

    private void registerAsyncCheckpointRunnable(
            long checkpointId, AsyncCheckpointRunnable asyncCheckpointRunnable) throws IOException {
        synchronized (lock) {
            if (closed) {
                LOG.debug(
                        "Cannot register Closeable, this subtaskCheckpointCoordinator is already closed. Closing argument.");
                closeQuietly(asyncCheckpointRunnable);
                checkState(
                        !checkpoints.containsKey(checkpointId),
                        "SubtaskCheckpointCoordinator was closed without releasing asyncCheckpointRunnable for checkpoint %s",
                        checkpointId);
            } else if (checkpoints.containsKey(checkpointId)) {
                closeQuietly(asyncCheckpointRunnable);
                throw new IOException(
                        String.format(
                                "Cannot register Closeable, async checkpoint %d runnable has been register. Closing argument.",
                                checkpointId));
            } else {
                checkpoints.put(checkpointId, asyncCheckpointRunnable);
            }
        }
    }

    private boolean unregisterAsyncCheckpointRunnable(long checkpointId) {
        synchronized (lock) {
            return checkpoints.remove(checkpointId) != null;
        }
    }

    /**
     * Cancel the async checkpoint runnable with given checkpoint id. If given checkpoint id is not
     * registered, return false, otherwise return true.
     */
    private boolean cancelAsyncCheckpointRunnable(long checkpointId) {
        AsyncCheckpointRunnable asyncCheckpointRunnable;
        synchronized (lock) {
            asyncCheckpointRunnable = checkpoints.remove(checkpointId);
        }
        if (asyncCheckpointRunnable != null) {
            asyncOperationsThreadPool.execute(() -> closeQuietly(asyncCheckpointRunnable));
        }
        return asyncCheckpointRunnable != null;
    }

    private void cleanup(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData metadata,
            CheckpointMetricsBuilder metrics,
            Exception ex) {

        channelStateWriter.abort(metadata.getCheckpointId(), ex, true);
        for (OperatorSnapshotFutures operatorSnapshotResult :
                operatorSnapshotsInProgress.values()) {
            if (operatorSnapshotResult != null) {
                try {
                    operatorSnapshotResult.cancel();
                } catch (Exception e) {
                    LOG.warn("Could not properly cancel an operator snapshot result.", e);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} - did NOT finish synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
                    taskName,
                    metadata.getCheckpointId(),
                    metrics.getAlignmentDurationNanosOrDefault() / 1_000_000,
                    metrics.getSyncDurationMillis());
        }
    }

    private void prepareInflightDataSnapshot(long checkpointId) throws CheckpointException {
        prepareInputSnapshot
                .apply(channelStateWriter, checkpointId)
                .whenComplete(
                        (unused, ex) -> {
                            if (ex != null) {
                                channelStateWriter.abort(
                                        checkpointId,
                                        ex,
                                        false /* result is needed and cleaned by getWriteResult */);
                            } else {
                                channelStateWriter.finishInput(checkpointId);
                            }
                        });
    }

    private void finishAndReportAsync(
            Map<OperatorID, OperatorSnapshotFutures> snapshotFutures,
            CheckpointMetaData metadata,
            CheckpointMetricsBuilder metrics,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws IOException {
        AsyncCheckpointRunnable asyncCheckpointRunnable =
                new AsyncCheckpointRunnable(
                        snapshotFutures,
                        metadata,
                        metrics,
                        System.nanoTime(),
                        taskName,
                        unregisterConsumer(),
                        env,
                        asyncExceptionHandler,
                        isTaskDeployedAsFinished,
                        isTaskFinished,
                        isRunning);

        registerAsyncCheckpointRunnable(
                asyncCheckpointRunnable.getCheckpointId(), asyncCheckpointRunnable);

        // we are transferring ownership over snapshotInProgressList for cleanup to the thread,
        // active on submit
        asyncOperationsThreadPool.execute(asyncCheckpointRunnable);
    }

    private Consumer<AsyncCheckpointRunnable> unregisterConsumer() {
        return asyncCheckpointRunnable ->
                unregisterAsyncCheckpointRunnable(asyncCheckpointRunnable.getCheckpointId());
    }

    private boolean takeSnapshotSync(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointMetricsBuilder checkpointMetrics,
            CheckpointOptions checkpointOptions,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning)
            throws Exception {

        checkState(
                !operatorChain.isClosed(),
                "OperatorChain and Task should never be closed at this point");

        long checkpointId = checkpointMetaData.getCheckpointId();
        long started = System.nanoTime();

        ChannelStateWriteResult channelStateWriteResult =
                checkpointOptions.needsChannelState()
                        ? channelStateWriter.getAndRemoveWriteResult(checkpointId)
                        : ChannelStateWriteResult.EMPTY;

        CheckpointStreamFactory storage =
                checkpointStorage.resolveCheckpointStorageLocation(
                        checkpointId, checkpointOptions.getTargetLocation());

        try {
            operatorChain.snapshotState(
                    operatorSnapshotsInProgress,
                    checkpointMetaData,
                    checkpointOptions,
                    isRunning,
                    channelStateWriteResult,
                    storage);

        } finally {
            checkpointStorage.clearCacheFor(checkpointId);
        }

        LOG.debug(
                "{} - finished synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms, is unaligned checkpoint : {}",
                taskName,
                checkpointId,
                checkpointMetrics.getAlignmentDurationNanosOrDefault() / 1_000_000,
                checkpointMetrics.getSyncDurationMillis(),
                checkpointOptions.isUnalignedCheckpoint());

        checkpointMetrics.setSyncDurationMillis((System.nanoTime() - started) / 1_000_000);
        return true;
    }

    private Set<Long> createAbortedCheckpointSetWithLimitSize(int maxRecordAbortedCheckpoints) {
        return Collections.newSetFromMap(
                new LinkedHashMap<Long, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                        return size() > maxRecordAbortedCheckpoints;
                    }
                });
    }

    // Caches checkpoint output stream factories to prevent multiple output stream per checkpoint.
    // This could result from requesting output stream by different entities (this and
    // channelStateWriter)
    // We can't just pass a stream to the channelStateWriter because it can receive checkpoint call
    // earlier than this class
    // in some unaligned checkpoints scenarios
    private static class CachingCheckpointStorageWorkerView implements CheckpointStorageWorkerView {
        private final Map<Long, CheckpointStreamFactory> cache = new ConcurrentHashMap<>();
        private final CheckpointStorageWorkerView delegate;

        private CachingCheckpointStorageWorkerView(CheckpointStorageWorkerView delegate) {
            this.delegate = delegate;
        }

        void clearCacheFor(long checkpointId) {
            cache.remove(checkpointId);
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId, CheckpointStorageLocationReference reference) {
            return cache.computeIfAbsent(
                    checkpointId,
                    id -> {
                        try {
                            return delegate.resolveCheckpointStorageLocation(
                                    checkpointId, reference);
                        } catch (IOException e) {
                            throw new FlinkRuntimeException(e);
                        }
                    });
        }

        @Override
        public CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
            return delegate.createTaskOwnedStateStream();
        }

        @Override
        public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
            return delegate.createTaskOwnedCheckpointStateToolset();
        }
    }

    private static void logCheckpointProcessingDelay(CheckpointMetaData checkpointMetaData) {
        long delay = System.currentTimeMillis() - checkpointMetaData.getReceiveTimestamp();
        if (delay >= CHECKPOINT_EXECUTION_DELAY_LOG_THRESHOLD_MS) {
            LOG.warn(
                    "Time from receiving all checkpoint barriers/RPC to executing it exceeded threshold: {}ms",
                    delay);
        }
    }
}
