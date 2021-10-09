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

import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This runnable executes the asynchronous parts of all involved backend snapshots for the subtask.
 */
final class AsyncCheckpointRunnable implements Runnable, Closeable {

    public static final Logger LOG = LoggerFactory.getLogger(AsyncCheckpointRunnable.class);
    private final String taskName;
    private final Consumer<AsyncCheckpointRunnable> unregisterConsumer;
    private final boolean isTaskDeployedAsFinished;
    private final boolean isTaskFinished;
    private final Supplier<Boolean> isTaskRunning;
    private final Environment taskEnvironment;
    private final CompletableFuture<Void> finishedFuture = new CompletableFuture<>();

    public boolean isRunning() {
        return asyncCheckpointState.get() == AsyncCheckpointState.RUNNING;
    }

    enum AsyncCheckpointState {
        RUNNING,
        DISCARDED,
        COMPLETED
    }

    private final AsyncExceptionHandler asyncExceptionHandler;
    private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;
    private final CheckpointMetaData checkpointMetaData;
    private final CheckpointMetricsBuilder checkpointMetrics;
    private final long asyncConstructionNanos;
    private final AtomicReference<AsyncCheckpointState> asyncCheckpointState =
            new AtomicReference<>(AsyncCheckpointState.RUNNING);

    AsyncCheckpointRunnable(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointMetricsBuilder checkpointMetrics,
            long asyncConstructionNanos,
            String taskName,
            Consumer<AsyncCheckpointRunnable> unregister,
            Environment taskEnvironment,
            AsyncExceptionHandler asyncExceptionHandler,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished,
            Supplier<Boolean> isTaskRunning) {

        this.operatorSnapshotsInProgress = checkNotNull(operatorSnapshotsInProgress);
        this.checkpointMetaData = checkNotNull(checkpointMetaData);
        this.checkpointMetrics = checkNotNull(checkpointMetrics);
        this.asyncConstructionNanos = asyncConstructionNanos;
        this.taskName = checkNotNull(taskName);
        this.unregisterConsumer = unregister;
        this.taskEnvironment = checkNotNull(taskEnvironment);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.isTaskDeployedAsFinished = isTaskDeployedAsFinished;
        this.isTaskFinished = isTaskFinished;
        this.isTaskRunning = isTaskRunning;
    }

    @Override
    public void run() {
        final long asyncStartNanos = System.nanoTime();
        final long asyncStartDelayMillis = (asyncStartNanos - asyncConstructionNanos) / 1_000_000L;
        LOG.debug(
                "{} - started executing asynchronous part of checkpoint {}. Asynchronous start delay: {} ms",
                taskName,
                checkpointMetaData.getCheckpointId(),
                asyncStartDelayMillis);

        FileSystemSafetyNet.initializeSafetyNetForThread();
        try {

            SnapshotsFinalizeResult snapshotsFinalizeResult =
                    isTaskDeployedAsFinished
                            ? new SnapshotsFinalizeResult(
                                    TaskStateSnapshot.FINISHED_ON_RESTORE,
                                    TaskStateSnapshot.FINISHED_ON_RESTORE,
                                    0L)
                            : finalizeNonFinishedSnapshots();

            final long asyncEndNanos = System.nanoTime();
            final long asyncDurationMillis = (asyncEndNanos - asyncConstructionNanos) / 1_000_000L;

            checkpointMetrics.setBytesPersistedDuringAlignment(
                    snapshotsFinalizeResult.bytesPersistedDuringAlignment);
            checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

            if (asyncCheckpointState.compareAndSet(
                    AsyncCheckpointState.RUNNING, AsyncCheckpointState.COMPLETED)) {

                reportCompletedSnapshotStates(
                        snapshotsFinalizeResult.jobManagerTaskOperatorSubtaskStates,
                        snapshotsFinalizeResult.localTaskOperatorSubtaskStates,
                        asyncDurationMillis);

            } else {
                LOG.debug(
                        "{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
                        taskName,
                        checkpointMetaData.getCheckpointId());
            }

            finishedFuture.complete(null);
        } catch (Exception e) {
            LOG.info(
                    "{} - asynchronous part of checkpoint {} could not be completed.",
                    taskName,
                    checkpointMetaData.getCheckpointId(),
                    e);
            handleExecutionException(e);
            finishedFuture.completeExceptionally(e);
        } finally {
            unregisterConsumer.accept(this);
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
    }

    private SnapshotsFinalizeResult finalizeNonFinishedSnapshots() throws Exception {
        TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
                new TaskStateSnapshot(operatorSnapshotsInProgress.size(), isTaskFinished);
        TaskStateSnapshot localTaskOperatorSubtaskStates =
                new TaskStateSnapshot(operatorSnapshotsInProgress.size(), isTaskFinished);

        long bytesPersistedDuringAlignment = 0;
        for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry :
                operatorSnapshotsInProgress.entrySet()) {

            OperatorID operatorID = entry.getKey();
            OperatorSnapshotFutures snapshotInProgress = entry.getValue();

            // finalize the async part of all by executing all snapshot runnables
            OperatorSnapshotFinalizer finalizedSnapshots =
                    new OperatorSnapshotFinalizer(snapshotInProgress);

            jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    operatorID, finalizedSnapshots.getJobManagerOwnedState());

            localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    operatorID, finalizedSnapshots.getTaskLocalState());

            bytesPersistedDuringAlignment +=
                    finalizedSnapshots
                            .getJobManagerOwnedState()
                            .getResultSubpartitionState()
                            .getStateSize();
            bytesPersistedDuringAlignment +=
                    finalizedSnapshots
                            .getJobManagerOwnedState()
                            .getInputChannelState()
                            .getStateSize();
        }

        return new SnapshotsFinalizeResult(
                jobManagerTaskOperatorSubtaskStates,
                localTaskOperatorSubtaskStates,
                bytesPersistedDuringAlignment);
    }

    private void reportCompletedSnapshotStates(
            TaskStateSnapshot acknowledgedTaskStateSnapshot,
            TaskStateSnapshot localTaskStateSnapshot,
            long asyncDurationMillis) {

        boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
        boolean hasLocalState = localTaskStateSnapshot.hasState();

        checkState(
                hasAckState || !hasLocalState,
                "Found cached state but no corresponding primary state is reported to the job "
                        + "manager. This indicates a problem.");

        // we signal stateless tasks by reporting null, so that there are no attempts to assign
        // empty state
        // to stateless tasks on restore. This enables simple job modifications that only concern
        // stateless without the need to assign them uids to match their (always empty) states.
        taskEnvironment
                .getTaskStateManager()
                .reportTaskStateSnapshots(
                        checkpointMetaData,
                        checkpointMetrics
                                .setTotalBytesPersisted(
                                        acknowledgedTaskStateSnapshot.getStateSize())
                                .build(),
                        hasAckState ? acknowledgedTaskStateSnapshot : null,
                        hasLocalState ? localTaskStateSnapshot : null);

        LOG.debug(
                "{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
                taskName,
                checkpointMetaData.getCheckpointId(),
                asyncDurationMillis);

        LOG.trace(
                "{} - reported the following states in snapshot for checkpoint {}: {}.",
                taskName,
                checkpointMetaData.getCheckpointId(),
                acknowledgedTaskStateSnapshot);
    }

    private void reportAbortedSnapshotStats(long stateSize) {
        CheckpointMetrics metrics =
                checkpointMetrics.setTotalBytesPersisted(stateSize).buildIncomplete();
        LOG.trace(
                "{} - report failed checkpoint stats: {} {}",
                taskName,
                checkpointMetaData.getCheckpointId(),
                metrics);

        taskEnvironment
                .getTaskStateManager()
                .reportIncompleteTaskStateSnapshots(checkpointMetaData, metrics);
    }

    private void handleExecutionException(Exception e) {

        boolean didCleanup = false;
        AsyncCheckpointState currentState = asyncCheckpointState.get();

        while (AsyncCheckpointState.DISCARDED != currentState) {

            if (asyncCheckpointState.compareAndSet(currentState, AsyncCheckpointState.DISCARDED)) {

                didCleanup = true;

                try {
                    cleanup();
                } catch (Exception cleanupException) {
                    e.addSuppressed(cleanupException);
                }

                Exception checkpointException =
                        new Exception(
                                "Could not materialize checkpoint "
                                        + checkpointMetaData.getCheckpointId()
                                        + " for operator "
                                        + taskName
                                        + '.',
                                e);

                if (isTaskRunning.get()) {
                    // We only report the exception for the original cause of fail and cleanup.
                    // Otherwise this followup exception could race the original exception in
                    // failing the task.
                    try {
                        Optional<CheckpointException> underlyingCheckpointException =
                                ExceptionUtils.findThrowable(
                                        checkpointException, CheckpointException.class);

                        // If this failure is already a CheckpointException, do not overwrite the
                        // original CheckpointFailureReason
                        CheckpointFailureReason reportedFailureReason =
                                underlyingCheckpointException
                                        .map(exception -> exception.getCheckpointFailureReason())
                                        .orElse(CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION);
                        taskEnvironment.declineCheckpoint(
                                checkpointMetaData.getCheckpointId(),
                                new CheckpointException(
                                        reportedFailureReason, checkpointException));
                    } catch (Exception unhandled) {
                        AsynchronousException asyncException = new AsynchronousException(unhandled);
                        asyncExceptionHandler.handleAsyncException(
                                "Failure in asynchronous checkpoint materialization",
                                asyncException);
                    }
                } else {
                    // We never decline checkpoint after task is not running to avoid unexpected job
                    // failover, which caused by exceeding checkpoint tolerable failure threshold.
                    LOG.info(
                            "Ignore decline of checkpoint {} as task is not running anymore.",
                            checkpointMetaData.getCheckpointId());
                }

                currentState = AsyncCheckpointState.DISCARDED;
            } else {
                currentState = asyncCheckpointState.get();
            }
        }

        if (!didCleanup) {
            LOG.trace(
                    "Caught followup exception from a failed checkpoint thread. This can be ignored.",
                    e);
        }
    }

    @Override
    public void close() {
        if (asyncCheckpointState.compareAndSet(
                AsyncCheckpointState.RUNNING, AsyncCheckpointState.DISCARDED)) {

            try {
                final long stateSize = cleanup();
                reportAbortedSnapshotStats(stateSize);
            } catch (Exception cleanupException) {
                LOG.warn(
                        "Could not properly clean up the async checkpoint runnable.",
                        cleanupException);
            }
        } else {
            logFailedCleanupAttempt();
        }
    }

    long getCheckpointId() {
        return checkpointMetaData.getCheckpointId();
    }

    public CompletableFuture<Void> getFinishedFuture() {
        return finishedFuture;
    }

    /** @return discarded state size (if available). */
    private long cleanup() throws Exception {
        LOG.debug(
                "Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.",
                checkpointMetaData.getCheckpointId(),
                taskName);

        Exception exception = null;

        // clean up ongoing operator snapshot results and non partitioned state handles
        long stateSize = 0;
        for (OperatorSnapshotFutures operatorSnapshotResult :
                operatorSnapshotsInProgress.values()) {
            if (operatorSnapshotResult != null) {
                try {
                    stateSize += operatorSnapshotResult.cancel();
                } catch (Exception cancelException) {
                    exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
                }
            }
        }

        if (null != exception) {
            throw exception;
        }
        return stateSize;
    }

    private void logFailedCleanupAttempt() {
        LOG.debug(
                "{} - asynchronous checkpointing operation for checkpoint {} has "
                        + "already been completed. Thus, the state handles are not cleaned up.",
                taskName,
                checkpointMetaData.getCheckpointId());
    }

    private static class SnapshotsFinalizeResult {
        final TaskStateSnapshot jobManagerTaskOperatorSubtaskStates;
        final TaskStateSnapshot localTaskOperatorSubtaskStates;
        final long bytesPersistedDuringAlignment;

        public SnapshotsFinalizeResult(
                TaskStateSnapshot jobManagerTaskOperatorSubtaskStates,
                TaskStateSnapshot localTaskOperatorSubtaskStates,
                long bytesPersistedDuringAlignment) {
            this.jobManagerTaskOperatorSubtaskStates = jobManagerTaskOperatorSubtaskStates;
            this.localTaskOperatorSubtaskStates = localTaskOperatorSubtaskStates;
            this.bytesPersistedDuringAlignment = bytesPersistedDuringAlignment;
        }
    }
}
