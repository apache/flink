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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
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
    private static final int DEFAULT_MAX_RECORD_ABORTED_CHECKPOINTS = 128;

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

    private long lastCheckpointId;

    /** Lock that guards state of AsyncCheckpointRunnable registry. * */
    private final Object lock;

    @GuardedBy("lock")
    private final Map<Long, AsyncCheckpointRunnable> checkpoints;

    /** Indicates if this registry is closed. */
    @GuardedBy("lock")
    private boolean closed;

    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            CloseableRegistry closeableRegistry,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            boolean unalignedCheckpointEnabled,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot)
            throws IOException {
        this(
                checkpointStorage,
                taskName,
                actionExecutor,
                closeableRegistry,
                asyncOperationsThreadPool,
                env,
                asyncExceptionHandler,
                unalignedCheckpointEnabled,
                prepareInputSnapshot,
                DEFAULT_MAX_RECORD_ABORTED_CHECKPOINTS);
    }

    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            CloseableRegistry closeableRegistry,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            boolean unalignedCheckpointEnabled,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints)
            throws IOException {
        this(
                checkpointStorage,
                taskName,
                actionExecutor,
                closeableRegistry,
                asyncOperationsThreadPool,
                env,
                asyncExceptionHandler,
                prepareInputSnapshot,
                maxRecordAbortedCheckpoints,
                unalignedCheckpointEnabled
                        ? openChannelStateWriter(taskName, checkpointStorage, env)
                        : ChannelStateWriter.NO_OP);
    }

    @VisibleForTesting
    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            CloseableRegistry closeableRegistry,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            ChannelStateWriter channelStateWriter)
            throws IOException {
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
        this.lastCheckpointId = -1L;
        closeableRegistry.registerCloseable(this);
        this.closed = false;
    }

    private static ChannelStateWriter openChannelStateWriter(
            String taskName, CheckpointStorageWorkerView checkpointStorage, Environment env) {
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        taskName, env.getTaskInfo().getIndexOfThisSubtask(), checkpointStorage);
        writer.open();
        return writer;
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

        // notify all downstream operators that they should not wait for a barrier from us
        actionExecutor.runThrowing(
                () -> operatorChain.broadcastEvent(new CancelCheckpointMarker(checkpointId)));
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

        // Step (0): Record the last triggered checkpointId and abort the sync phase of checkpoint
        // if necessary.
        lastCheckpointId = metadata.getCheckpointId();
        if (checkAndClearAbortedStatus(metadata.getCheckpointId())) {
            // broadcast cancel checkpoint marker to avoid downstream back-pressure due to
            // checkpoint barrier align.
            operatorChain.broadcastEvent(new CancelCheckpointMarker(metadata.getCheckpointId()));
            LOG.info(
                    "Checkpoint {} has been notified as aborted, would not trigger any checkpoint.",
                    metadata.getCheckpointId());
            return;
        }

        // Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
        //           The pre-barrier work should be nothing or minimal in the common case.
        operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());

        // Step (2): Send the checkpoint barrier downstream
        operatorChain.broadcastEvent(
                new CheckpointBarrier(metadata.getCheckpointId(), metadata.getTimestamp(), options),
                options.isUnalignedCheckpoint());

        // Step (3): Prepare to spill the in-flight buffers for input and output
        if (options.isUnalignedCheckpoint()) {
            // output data already written while broadcasting event
            channelStateWriter.finishOutput(metadata.getCheckpointId());
        }

        // Step (4): Take the state snapshot. This should be largely asynchronous, to not impact
        // progress of the
        // streaming topology

        Map<OperatorID, OperatorSnapshotFutures> snapshotFutures =
                new HashMap<>(operatorChain.getNumberOfOperators());
        try {
            if (takeSnapshotSync(
                    snapshotFutures, metadata, metrics, options, operatorChain, isRunning)) {
                finishAndReportAsync(snapshotFutures, metadata, metrics, isRunning);
            } else {
                cleanup(snapshotFutures, metadata, metrics, new Exception("Checkpoint declined"));
            }
        } catch (Exception ex) {
            cleanup(snapshotFutures, metadata, metrics, ex);
            throw ex;
        }
    }

    @Override
    public void notifyCheckpointComplete(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {
        if (isRunning.get()) {
            LOG.debug(
                    "Notification of completed checkpoint {} for task {}", checkpointId, taskName);

            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                operatorWrapper.notifyCheckpointComplete(checkpointId);
            }
        } else {
            LOG.debug(
                    "Ignoring notification of complete checkpoint {} for not-running task {}",
                    checkpointId,
                    taskName);
        }
        env.getTaskStateManager().notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        Exception previousException = null;
        if (isRunning.get()) {
            LOG.debug("Notification of aborted checkpoint {} for task {}", checkpointId, taskName);

            boolean canceled = cancelAsyncCheckpointRunnable(checkpointId);

            if (!canceled) {
                if (checkpointId > lastCheckpointId) {
                    // only record checkpoints that have not triggered on task side.
                    abortedCheckpointIds.add(checkpointId);
                }
            }

            channelStateWriter.abort(
                    checkpointId,
                    new CancellationException("checkpoint aborted via notification"),
                    false);

            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                try {
                    operatorWrapper.getStreamOperator().notifyCheckpointAborted(checkpointId);
                } catch (Exception e) {
                    previousException = e;
                }
            }

        } else {
            LOG.debug(
                    "Ignoring notification of aborted checkpoint {} for not-running task {}",
                    checkpointId,
                    taskName);
        }

        env.getTaskStateManager().notifyCheckpointAborted(checkpointId);
        ExceptionUtils.tryRethrowException(previousException);
    }

    @Override
    public void initCheckpoint(long id, CheckpointOptions checkpointOptions)
            throws CheckpointException {
        if (checkpointOptions.isUnalignedCheckpoint()) {
            channelStateWriter.start(id, checkpointOptions);

            prepareInflightDataSnapshot(id);
        }
    }

    @Override
    public void close() throws IOException {
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
        return abortedCheckpointIds.remove(checkpointId);
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
        closeQuietly(asyncCheckpointRunnable);
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
            Supplier<Boolean> isRunning) {
        // we are transferring ownership over snapshotInProgressList for cleanup to the thread,
        // active on submit
        asyncOperationsThreadPool.execute(
                new AsyncCheckpointRunnable(
                        snapshotFutures,
                        metadata,
                        metrics,
                        System.nanoTime(),
                        taskName,
                        registerConsumer(),
                        unregisterConsumer(),
                        env,
                        asyncExceptionHandler,
                        isRunning));
    }

    private Consumer<AsyncCheckpointRunnable> registerConsumer() {
        return asyncCheckpointRunnable -> {
            try {
                registerAsyncCheckpointRunnable(
                        asyncCheckpointRunnable.getCheckpointId(), asyncCheckpointRunnable);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
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

        for (final StreamOperatorWrapper<?, ?> operatorWrapper :
                operatorChain.getAllOperators(true)) {
            if (operatorWrapper.isClosed()) {
                env.declineCheckpoint(
                        checkpointMetaData.getCheckpointId(),
                        new CheckpointException(
                                "Task Name" + taskName,
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING));
                return false;
            }
        }

        long checkpointId = checkpointMetaData.getCheckpointId();
        long started = System.nanoTime();

        ChannelStateWriteResult channelStateWriteResult =
                checkpointOptions.isUnalignedCheckpoint()
                        ? channelStateWriter.getAndRemoveWriteResult(checkpointId)
                        : ChannelStateWriteResult.EMPTY;

        CheckpointStreamFactory storage =
                checkpointStorage.resolveCheckpointStorageLocation(
                        checkpointId, checkpointOptions.getTargetLocation());

        try {
            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                if (!operatorWrapper.isClosed()) {
                    operatorSnapshotsInProgress.put(
                            operatorWrapper.getStreamOperator().getOperatorID(),
                            buildOperatorSnapshotFutures(
                                    checkpointMetaData,
                                    checkpointOptions,
                                    operatorChain,
                                    operatorWrapper.getStreamOperator(),
                                    isRunning,
                                    channelStateWriteResult,
                                    storage));
                }
            }
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
        checkpointMetrics.setUnalignedCheckpoint(checkpointOptions.isUnalignedCheckpoint());
        return true;
    }

    private OperatorSnapshotFutures buildOperatorSnapshotFutures(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            OperatorChain<?, ?> operatorChain,
            StreamOperator<?> op,
            Supplier<Boolean> isRunning,
            ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception {
        OperatorSnapshotFutures snapshotInProgress =
                checkpointStreamOperator(
                        op, checkpointMetaData, checkpointOptions, storage, isRunning);
        if (op == operatorChain.getMainOperator()) {
            snapshotInProgress.setInputChannelStateFuture(
                    channelStateWriteResult
                            .getInputChannelStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
        if (op == operatorChain.getTailOperator()) {
            snapshotInProgress.setResultSubpartitionStateFuture(
                    channelStateWriteResult
                            .getResultSubpartitionStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
        return snapshotInProgress;
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
        public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream()
                throws IOException {
            return delegate.createTaskOwnedStateStream();
        }
    }

    private static OperatorSnapshotFutures checkpointStreamOperator(
            StreamOperator<?> op,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation,
            Supplier<Boolean> isRunning)
            throws Exception {
        try {
            return op.snapshotState(
                    checkpointMetaData.getCheckpointId(),
                    checkpointMetaData.getTimestamp(),
                    checkpointOptions,
                    storageLocation);
        } catch (Exception ex) {
            if (isRunning.get()) {
                LOG.info(ex.getMessage(), ex);
            }
            throw ex;
        }
    }
}
