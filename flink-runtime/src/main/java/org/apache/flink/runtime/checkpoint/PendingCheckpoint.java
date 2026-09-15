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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been acknowledged by all
 * tasks that need to acknowledge it. Once all tasks have acknowledged it, it becomes a {@link
 * CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the state handles
 * always as serialized values, never as actual values.
 */
@NotThreadSafe
public class PendingCheckpoint implements Checkpoint {

    /** Result of the {@link PendingCheckpoint#acknowledgedTasks} method. */
    public enum TaskAcknowledgeResult {
        SUCCESS, // successful acknowledge of the task
        DUPLICATE, // acknowledge message is a duplicate
        UNKNOWN, // unknown task acknowledged
        DISCARDED // pending checkpoint has been discarded
    }

    // ------------------------------------------------------------------------

    /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final long checkpointId;

    private final long checkpointTimestamp;

    private final Map<OperatorID, OperatorState> operatorStates;

    private final CheckpointPlan checkpointPlan;

    private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

    private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

    private final List<MasterState> masterStates;

    private final Set<String> notYetAcknowledgedMasterStates;

    /** Set of acknowledged tasks. */
    private final Set<ExecutionAttemptID> acknowledgedTasks;

    /**
     * Map of declined tasks to their failure causes (used for deferred abort in regional
     * checkpoint).
     */
    private final Map<ExecutionAttemptID, CheckpointException> declinedTasks;

    /** The checkpoint properties. */
    private final CheckpointProperties props;

    /**
     * The promise to fulfill once the checkpoint has been completed. Note that it will be completed
     * only after the checkpoint is successfully added to CompletedCheckpointStore.
     */
    private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

    @Nullable private final PendingCheckpointStats pendingCheckpointStats;

    private final CompletableFuture<Void> masterTriggerCompletionPromise;

    /** Target storage location to persist the checkpoint metadata to. */
    @Nullable private CheckpointStorageLocation targetLocation;

    private int numAcknowledgedTasks;

    private boolean disposed;

    private boolean discarded;

    private volatile ScheduledFuture<?> cancellerHandle;

    private CheckpointException failureCause;

    // --------------------------------------------------------------------------------------------

    public PendingCheckpoint(
            JobID jobId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointPlan checkpointPlan,
            Collection<OperatorID> operatorCoordinatorsToConfirm,
            Collection<String> masterStateIdentifiers,
            CheckpointProperties props,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            CompletableFuture<Void> masterTriggerCompletionPromise) {
        checkArgument(
                checkpointPlan.getTasksToWaitFor().size() > 0,
                "Checkpoint needs at least one vertex that commits the checkpoint");

        this.jobId = checkNotNull(jobId);
        this.checkpointId = checkpointId;
        this.checkpointTimestamp = checkpointTimestamp;
        this.checkpointPlan = checkNotNull(checkpointPlan);

        this.notYetAcknowledgedTasks =
                CollectionUtil.newHashMapWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        for (Execution execution : checkpointPlan.getTasksToWaitFor()) {
            notYetAcknowledgedTasks.put(execution.getAttemptId(), execution.getVertex());
        }

        this.props = checkNotNull(props);

        this.operatorStates = new HashMap<>();
        this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
        this.notYetAcknowledgedMasterStates =
                masterStateIdentifiers.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(masterStateIdentifiers);
        this.notYetAcknowledgedOperatorCoordinators =
                operatorCoordinatorsToConfirm.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(operatorCoordinatorsToConfirm);
        this.acknowledgedTasks =
                CollectionUtil.newHashSetWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        this.declinedTasks = new HashMap<>();
        this.onCompletionPromise = checkNotNull(onCompletionPromise);
        this.pendingCheckpointStats = pendingCheckpointStats;
        this.masterTriggerCompletionPromise = checkNotNull(masterTriggerCompletionPromise);
    }

    // --------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return jobId;
    }

    @Override
    public long getCheckpointID() {
        return checkpointId;
    }

    public void setCheckpointTargetLocation(CheckpointStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    public CheckpointStorageLocation getCheckpointStorageLocation() {
        return targetLocation;
    }

    public long getCheckpointTimestamp() {
        return checkpointTimestamp;
    }

    public int getNumberOfNonAcknowledgedTasks() {
        return notYetAcknowledgedTasks.size();
    }

    public int getNumberOfNonAcknowledgedOperatorCoordinators() {
        return notYetAcknowledgedOperatorCoordinators.size();
    }

    public CheckpointPlan getCheckpointPlan() {
        return checkpointPlan;
    }

    public int getNumberOfAcknowledgedTasks() {
        return numAcknowledgedTasks;
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public List<MasterState> getMasterStates() {
        return masterStates;
    }

    public boolean isFullyAcknowledged() {
        return areTasksFullyAcknowledged()
                && areCoordinatorsFullyAcknowledged()
                && areMasterStatesFullyAcknowledged();
    }

    boolean areMasterStatesFullyAcknowledged() {
        return notYetAcknowledgedMasterStates.isEmpty() && !disposed;
    }

    boolean areCoordinatorsFullyAcknowledged() {
        return notYetAcknowledgedOperatorCoordinators.isEmpty() && !disposed;
    }

    boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty() && !disposed;
    }

    /**
     * Records a decline from a task for deferred abort in regional checkpoint mode. The decline is
     * buffered instead of immediately aborting the checkpoint.
     */
    public void recordDecline(ExecutionAttemptID attemptId, CheckpointException cause) {
        synchronized (lock) {
            declinedTasks.put(attemptId, cause);
        }
    }

    /** Returns the map of declined tasks and their failure causes. */
    public Map<ExecutionAttemptID, CheckpointException> getDeclinedTasks() {
        synchronized (lock) {
            return Collections.unmodifiableMap(new HashMap<>(declinedTasks));
        }
    }

    /** Returns whether any tasks have declined this checkpoint. */
    public boolean hasDeclines() {
        synchronized (lock) {
            return !declinedTasks.isEmpty();
        }
    }

    /**
     * Marks all tasks that have neither acknowledged nor declined as declined with the given cause.
     * This is used in regional checkpoint mode when a checkpoint timeout fires: unacknowledged
     * tasks are treated as failed (their regions become failed regions) so that {@link
     * CheckpointCoordinator#tryCompleteRegionalCheckpoint} can evaluate whether a regional
     * checkpoint is still possible per FLIP-600 Per-Region Timeout Handling.
     *
     * @param cause the exception to associate with each newly-declined task
     * @return the number of tasks that were marked as declined by this call
     */
    public int markUnacknowledgedTasksAsDeclined(CheckpointException cause) {
        synchronized (lock) {
            int marked = 0;
            for (ExecutionAttemptID remaining : notYetAcknowledgedTasks.keySet()) {
                if (!declinedTasks.containsKey(remaining)) {
                    declinedTasks.put(remaining, cause);
                    marked++;
                }
            }
            return marked;
        }
    }

    /**
     * Returns true if every task in this checkpoint has either acknowledged or declined. This is
     * used in regional checkpoint mode to determine when to evaluate the checkpoint.
     */
    public boolean areAllTasksResponded() {
        synchronized (lock) {
            if (disposed) {
                return false;
            }
            // All tasks responded if every remaining not-yet-acknowledged task has declined
            for (ExecutionAttemptID remaining : notYetAcknowledgedTasks.keySet()) {
                if (!declinedTasks.containsKey(remaining)) {
                    return false;
                }
            }
            return true;
        }
    }

    public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
        return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
    }

    public boolean isDisposed() {
        return disposed;
    }

    /**
     * Checks whether this checkpoint can be subsumed or whether it should always continue,
     * regardless of newer checkpoints in progress.
     *
     * @return True if the checkpoint can be subsumed, false otherwise.
     */
    public boolean canBeSubsumed() {
        // If the checkpoint is forced, it cannot be subsumed.
        return !props.isSavepoint();
    }

    CheckpointProperties getProps() {
        return props;
    }

    /**
     * Sets the handle for the canceller to this pending checkpoint. This method fails with an
     * exception if a handle has already been set.
     *
     * @return true, if the handle was set, false, if the checkpoint is already disposed;
     */
    public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
        synchronized (lock) {
            if (this.cancellerHandle == null) {
                if (!disposed) {
                    this.cancellerHandle = cancellerHandle;
                    return true;
                } else {
                    return false;
                }
            } else {
                throw new IllegalStateException("A canceller handle was already set");
            }
        }
    }

    public CheckpointException getFailureCause() {
        return failureCause;
    }

    // ------------------------------------------------------------------------
    //  Progress and Completion
    // ------------------------------------------------------------------------

    /**
     * Returns the completion future.
     *
     * @return A future to the completed checkpoint
     */
    public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
        return onCompletionPromise;
    }

    public CompletedCheckpoint finalizeCheckpoint(
            CheckpointsCleaner checkpointsCleaner, Runnable postCleanup, Executor executor)
            throws IOException {

        synchronized (lock) {
            checkState(!isDisposed(), "checkpoint is discarded");
            checkState(
                    isFullyAcknowledged(),
                    "Pending checkpoint has not been fully acknowledged yet");

            // make sure we fulfill the promise with an exception if something fails
            try {
                checkpointPlan.fulfillFinishedTaskStatus(operatorStates);

                // write out the metadata
                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(
                                checkpointId, operatorStates.values(), masterStates, props);
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }

                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                checkpointTimestamp,
                                System.currentTimeMillis(),
                                operatorStates,
                                masterStates,
                                props,
                                finalizedLocation,
                                toCompletedCheckpointStats(finalizedLocation));

                // mark this pending checkpoint as disposed, but do NOT drop the state
                dispose(false, checkpointsCleaner, postCleanup, executor);

                return completed;
            } catch (Throwable t) {
                onCompletionPromise.completeExceptionally(t);
                ExceptionUtils.rethrowIOException(t);
                return null; // silence the compiler
            }
        }
    }

    /**
     * Finalizes a regional checkpoint where some tasks have declined. Unlike {@link
     * #finalizeCheckpoint}, this does not require all tasks to have acknowledged. The caller is
     * responsible for ensuring that the operator states have been properly assembled (healthy
     * subtasks from current checkpoint, failed subtasks from a fallback checkpoint).
     */
    public CompletedCheckpoint finalizeRegionalCheckpoint(
            CheckpointsCleaner checkpointsCleaner, Runnable postCleanup, Executor executor)
            throws IOException {

        synchronized (lock) {
            checkState(!isDisposed(), "checkpoint is discarded");

            try {
                checkpointPlan.fulfillFinishedTaskStatus(operatorStates);

                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(
                                checkpointId, operatorStates.values(), masterStates, props);
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }

                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                checkpointTimestamp,
                                System.currentTimeMillis(),
                                operatorStates,
                                masterStates,
                                props,
                                finalizedLocation,
                                toCompletedCheckpointStats(finalizedLocation));

                dispose(false, checkpointsCleaner, postCleanup, executor);

                return completed;
            } catch (Throwable t) {
                onCompletionPromise.completeExceptionally(t);
                ExceptionUtils.rethrowIOException(t);
                return null;
            }
        }
    }

    @Nullable
    private CompletedCheckpointStats toCompletedCheckpointStats(
            CompletedCheckpointStorageLocation finalizedLocation) {
        return pendingCheckpointStats != null
                ? pendingCheckpointStats.toCompletedCheckpointStats(
                        finalizedLocation.getExternalPointer(),
                        finalizedLocation.getMetadataHandle().getStateSize())
                : null;
    }

    /**
     * Acknowledges the task with the given execution attempt id and the given subtask state.
     *
     * @param executionAttemptId of the acknowledged task
     * @param operatorSubtaskStates of the acknowledged task
     * @param metrics Checkpoint metrics for the stats
     * @return TaskAcknowledgeResult of the operation
     */
    public TaskAcknowledgeResult acknowledgeTask(
            ExecutionAttemptID executionAttemptId,
            TaskStateSnapshot operatorSubtaskStates,
            CheckpointMetrics metrics) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

            if (vertex == null) {
                if (acknowledgedTasks.contains(executionAttemptId)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
            } else {
                acknowledgedTasks.add(executionAttemptId);
            }

            long ackTimestamp = System.currentTimeMillis();
            if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskDeployedAsFinished()) {
                checkpointPlan.reportTaskFinishedOnRestore(vertex);
            } else {
                for (OperatorIDPair operatorIDPair : vertex.getJobVertex().getOperatorIDs()) {
                    updateOperatorState(vertex, operatorSubtaskStates, operatorIDPair);
                }

                if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskFinished()) {
                    checkpointPlan.reportTaskHasFinishedOperators(vertex);
                }
            }

            ++numAcknowledgedTasks;

            // publish the checkpoint statistics
            // to prevent null-pointers from concurrent modification, copy reference onto stack
            if (pendingCheckpointStats != null) {
                // Do this in millis because the web frontend works with them
                long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
                long checkpointStartDelayMillis =
                        metrics.getCheckpointStartDelayNanos() / 1_000_000;

                // Extract the regional-checkpoint reference id if this acknowledged state was
                // reused from a historical checkpoint (normally empty for a regular acknowledge).
                Long refCheckpointId = extractRefCheckpointId(operatorSubtaskStates);

                SubtaskStateStats subtaskStateStats =
                        new SubtaskStateStats(
                                vertex.getParallelSubtaskIndex(),
                                ackTimestamp,
                                metrics.getBytesPersistedOfThisCheckpoint(),
                                metrics.getTotalBytesPersisted(),
                                metrics.getSyncDurationMillis(),
                                metrics.getAsyncDurationMillis(),
                                metrics.getBytesProcessedDuringAlignment(),
                                metrics.getBytesPersistedDuringAlignment(),
                                alignmentDurationMillis,
                                checkpointStartDelayMillis,
                                metrics.getUnalignedCheckpoint(),
                                true,
                                refCheckpointId);

                LOG.trace(
                        "Checkpoint {} stats for {}: size={}Kb, duration={}ms, sync part={}ms, async part={}ms",
                        checkpointId,
                        vertex.getTaskNameWithSubtaskIndex(),
                        subtaskStateStats.getStateSize() == 0
                                ? 0
                                : subtaskStateStats.getStateSize() / 1024,
                        subtaskStateStats.getEndToEndDuration(
                                pendingCheckpointStats.getTriggerTimestamp()),
                        subtaskStateStats.getSyncCheckpointDuration(),
                        subtaskStateStats.getAsyncCheckpointDuration());
                pendingCheckpointStats.reportSubtaskStats(
                        vertex.getJobvertexId(), subtaskStateStats);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    /**
     * Reports statistics for a subtask in a failed region of a regional checkpoint, whose state was
     * reused from the historical checkpoint {@code refCheckpointId}. Such subtasks neither
     * acknowledge nor decline into the stats, so their stats must be reported explicitly when the
     * regional checkpoint completes, in order to surface the reference id through the REST API.
     *
     * @param jobVertexId the job vertex the subtask belongs to
     * @param subtaskIndex the parallel subtask index
     * @param ackTimestamp the completion timestamp of the regional checkpoint
     * @param refCheckpointId the historical checkpoint id the subtask's state originates from
     */
    public void reportFallbackSubtaskStats(
            JobVertexID jobVertexId, int subtaskIndex, long ackTimestamp, long refCheckpointId) {
        synchronized (lock) {
            if (pendingCheckpointStats == null) {
                return;
            }
            SubtaskStateStats subtaskStateStats =
                    new SubtaskStateStats(
                            subtaskIndex,
                            ackTimestamp,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            false,
                            true,
                            refCheckpointId);
            pendingCheckpointStats.reportSubtaskStats(jobVertexId, subtaskStateStats);
        }
    }

    /**
     * Extracts the regional-checkpoint reference id from the given task state snapshot, i.e. the
     * historical checkpoint id this state was reused from. Returns {@code null} if no operator
     * subtask state carries a reference id (the regular case).
     */
    private static Long extractRefCheckpointId(TaskStateSnapshot operatorSubtaskStates) {
        if (operatorSubtaskStates == null) {
            return null;
        }
        Long oldest = null;
        for (Map.Entry<OperatorID, OperatorSubtaskState> entry :
                operatorSubtaskStates.getSubtaskStateMappings()) {
            OptionalLong refId = entry.getValue().getRefCheckpointId();
            if (refId.isPresent()) {
                oldest = oldest == null ? refId.getAsLong() : Math.min(oldest, refId.getAsLong());
            }
        }
        return oldest;
    }

    private void updateOperatorState(
            ExecutionVertex vertex,
            TaskStateSnapshot operatorSubtaskStates,
            OperatorIDPair operatorIDPair) {
        OperatorState operatorState = operatorStates.get(operatorIDPair.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new OperatorState(
                            operatorIDPair.getUserDefinedOperatorName(),
                            operatorIDPair.getUserDefinedOperatorUid(),
                            operatorIDPair.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
        } else {
            operatorState.setOperatorName(operatorIDPair.getUserDefinedOperatorName());
            operatorState.setOperatorUid(operatorIDPair.getUserDefinedOperatorUid());
        }
        OperatorSubtaskState operatorSubtaskState =
                operatorSubtaskStates == null
                        ? null
                        : operatorSubtaskStates.getSubtaskStateByOperatorID(
                                operatorIDPair.getGeneratedOperatorID());

        if (operatorSubtaskState != null) {
            operatorState.putState(vertex.getParallelSubtaskIndex(), operatorSubtaskState);
        }
    }

    public TaskAcknowledgeResult acknowledgeCoordinatorState(
            OperatorInfo coordinatorInfo, @Nullable ByteStreamStateHandle stateHandle) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final OperatorID operatorId = coordinatorInfo.operatorId();
            OperatorState operatorState = operatorStates.get(operatorId);

            // sanity check for better error reporting
            if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
                return operatorState != null && operatorState.getCoordinatorState() != null
                        ? TaskAcknowledgeResult.DUPLICATE
                        : TaskAcknowledgeResult.UNKNOWN;
            }

            if (operatorState == null) {
                operatorState =
                        new OperatorState(
                                null,
                                null,
                                operatorId,
                                coordinatorInfo.currentParallelism(),
                                coordinatorInfo.maxParallelism());
                operatorStates.put(operatorId, operatorState);
            }
            if (stateHandle != null) {
                operatorState.setCoordinatorState(stateHandle);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    /**
     * Acknowledges a master state (state generated on the checkpoint coordinator) to the pending
     * checkpoint.
     *
     * @param identifier The identifier of the master state
     * @param state The state to acknowledge
     */
    public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

        synchronized (lock) {
            if (!disposed) {
                if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
                    masterStates.add(state);
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Cancellation
    // ------------------------------------------------------------------------

    /** Aborts a checkpoint with reason and cause. */
    public void abort(
            CheckpointFailureReason reason,
            @Nullable Throwable cause,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor,
            CheckpointStatsTracker statsTracker) {
        try {
            failureCause = new CheckpointException(reason, cause);
            onCompletionPromise.completeExceptionally(failureCause);
            masterTriggerCompletionPromise.completeExceptionally(failureCause);
            assertAbortSubsumedForced(reason);
        } finally {
            dispose(true, checkpointsCleaner, postCleanup, executor);
        }
    }

    private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
        if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
            throw new IllegalStateException(
                    "Bug: savepoints must never be subsumed, "
                            + "the abort reason is : "
                            + reason.message());
        }
    }

    private void dispose(
            boolean releaseState,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {

        synchronized (lock) {
            try {
                numAcknowledgedTasks = -1;
                checkpointsCleaner.cleanCheckpoint(this, releaseState, postCleanup, executor);
            } finally {
                disposed = true;
                notYetAcknowledgedTasks.clear();
                acknowledgedTasks.clear();
                declinedTasks.clear();
                cancelCanceller();
            }
        }
    }

    @Override
    public DiscardObject markAsDiscarded() {
        return new PendingCheckpointDiscardObject();
    }

    private void cancelCanceller() {
        try {
            final ScheduledFuture<?> canceller = this.cancellerHandle;
            if (canceller != null) {
                canceller.cancel(false);
            }
        } catch (Exception e) {
            // this code should not throw exceptions
            LOG.warn("Error while cancelling checkpoint timeout task", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
                checkpointId,
                checkpointTimestamp,
                getNumberOfAcknowledgedTasks(),
                getNumberOfNonAcknowledgedTasks());
    }

    /**
     * Implementation of {@link org.apache.flink.runtime.checkpoint.Checkpoint.DiscardObject} for
     * {@link PendingCheckpoint}.
     */
    public class PendingCheckpointDiscardObject implements DiscardObject {
        /**
         * Discard state. Must be called after {@link #dispose(boolean, CheckpointsCleaner,
         * Runnable, Executor) dispose}.
         */
        @Override
        public void discard() {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                    return;
                } else {
                    discarded = true;
                }
            }
            // discard the private states.
            // unregistered shared states are still considered private at this point.
            try {
                StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
                if (targetLocation != null) {
                    targetLocation.disposeOnFailure();
                }
            } catch (Throwable t) {
                LOG.warn(
                        "Could not properly dispose the private states in the pending checkpoint {} of job {}.",
                        checkpointId,
                        jobId,
                        t);
            } finally {
                operatorStates.clear();
            }
        }

        @Override
        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                } else {
                    discarded = true;
                }
            }
            List<StateObject> discardables =
                    operatorStates.values().stream()
                            .flatMap(op -> op.getDiscardables().stream())
                            .collect(Collectors.toList());

            ConjunctFuture<Void> discardStates =
                    FutureUtils.completeAll(
                            discardables.stream()
                                    .map(
                                            item ->
                                                    FutureUtils.runAsync(
                                                            item::discardState, ioExecutor))
                                    .collect(Collectors.toList()));

            return FutureUtils.runAfterwards(
                    discardStates,
                    () -> {
                        operatorStates.clear();
                        if (targetLocation != null) {
                            targetLocation.disposeOnFailure();
                        }
                    });
        }
    }
}
