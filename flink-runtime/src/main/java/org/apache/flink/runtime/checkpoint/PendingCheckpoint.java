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
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it. Once all tasks have
 * acknowledged it, it becomes a {@link CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the
 * state handles always as serialized values, never as actual values.
 */
public class PendingCheckpoint {

	/**
	 * Result of the {@link PendingCheckpoint#acknowledgedTasks} method.
	 */
	public enum TaskAcknowledgeResult {
		SUCCESS, // successful acknowledge of the task
		DUPLICATE, // acknowledge message is a duplicate
		UNKNOWN, // unknown task acknowledged
		DISCARDED // pending checkpoint has been discarded
	}

	// ------------------------------------------------------------------------

	/** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	private final JobID jobId;

	private final long checkpointId;

	private final long checkpointTimestamp;

	private final Map<OperatorID, OperatorState> operatorStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

	private final List<MasterState> masterStates;

	private final Set<String> notYetAcknowledgedMasterStates;

	/** Set of acknowledged tasks. */
	private final Set<ExecutionAttemptID> acknowledgedTasks;

	/** The checkpoint properties. */
	private final CheckpointProperties props;

	/** Target storage location to persist the checkpoint metadata to. */
	private final CheckpointStorageLocation targetLocation;

	/** The promise to fulfill once the checkpoint has been completed. */
	private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

	/** The executor for potentially blocking I/O operations, like state disposal. */
	private final Executor executor;

	/** The executor for non-blocking operations. */
	private final Executor mainThreadExecutor;

	private final CompletedCheckpointStore completedCheckpointStore;

	/** The lock for avoiding conflict between I/O operations. */
	private final Object operationLock = new Object();

	private int numAcknowledgedTasks;

	private boolean discarded;

	/** Optional stats tracker callback. */
	@Nullable
	private PendingCheckpointStats statsCallback;

	private volatile ScheduledFuture<?> cancellerHandle;

	private CheckpointException failureCause;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			Collection<OperatorID> operatorCoordinatorsToConfirm,
			Collection<String> masterStateIdentifiers,
			CheckpointProperties props,
			CheckpointStorageLocation targetLocation,
			Executor executor,
			Executor mainThreadExecutor,
			CompletableFuture<CompletedCheckpoint> onCompletionPromise,
			CompletedCheckpointStore completedCheckpointStore) {

		checkArgument(verticesToConfirm.size() > 0,
				"Checkpoint needs at least one vertex that commits the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.props = checkNotNull(props);
		this.targetLocation = checkNotNull(targetLocation);
		this.executor = Preconditions.checkNotNull(executor);
		this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);
		this.completedCheckpointStore = Preconditions.checkNotNull(completedCheckpointStore);

		this.operatorStates = new HashMap<>();
		this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
		this.notYetAcknowledgedMasterStates = masterStateIdentifiers.isEmpty()
				? Collections.emptySet() : new HashSet<>(masterStateIdentifiers);
		this.notYetAcknowledgedOperatorCoordinators = operatorCoordinatorsToConfirm.isEmpty()
				? Collections.emptySet() : new HashSet<>(operatorCoordinatorsToConfirm);
		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = checkNotNull(onCompletionPromise);
	}

	// --------------------------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return jobId;
	}

	public long getCheckpointId() {
		return checkpointId;
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
		return areTasksFullyAcknowledged() &&
			areCoordinatorsFullyAcknowledged() &&
			areMasterStatesFullyAcknowledged();
	}

	boolean areMasterStatesFullyAcknowledged() {
		return notYetAcknowledgedMasterStates.isEmpty() && !discarded;
	}

	boolean areCoordinatorsFullyAcknowledged() {
		return notYetAcknowledgedOperatorCoordinators.isEmpty() && !discarded;
	}

	boolean areTasksFullyAcknowledged() {
		return notYetAcknowledgedTasks.isEmpty() && !discarded;
	}

	public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
		return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
	}

	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Checks whether this checkpoint can be subsumed or whether it should always continue, regardless
	 * of newer checkpoints in progress.
	 *
	 * @return True if the checkpoint can be subsumed, false otherwise.
	 */
	public boolean canBeSubsumed() {
		// If the checkpoint is forced, it cannot be subsumed.
		return !props.forceCheckpoint();
	}

	CheckpointProperties getProps() {
		return props;
	}

	/**
	 * Sets the callback for tracking this pending checkpoint.
	 *
	 * @param trackerCallback Callback for collecting subtask stats.
	 */
	void setStatsCallback(@Nullable PendingCheckpointStats trackerCallback) {
		this.statsCallback = trackerCallback;
	}

	/**
	 * Sets the handle for the canceller to this pending checkpoint. This method fails
	 * with an exception if a handle has already been set.
	 *
	 * @return true, if the handle was set, false, if the checkpoint is already disposed;
	 */
	public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
		if (this.cancellerHandle == null) {
			if (!discarded) {
				this.cancellerHandle = cancellerHandle;
				return true;
			} else {
				return false;
			}
		}
		else {
			throw new IllegalStateException("A canceller handle was already set");
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

	public CompletableFuture<CompletedCheckpoint> finalizeCheckpoint() {

		if (isDiscarded()) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				"checkpoint is discarded"));
		}
		if (!isFullyAcknowledged()) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				"Pending checkpoint has not been fully acknowledged yet"));
		}

		// now we stop the canceller before finalization
		// it simplifies the concurrent conflict issue here
		cancelCanceller();

		// make sure we fulfill the promise with an exception if something fails
		final CompletableFuture<CompletedCheckpoint> finalizingFuture =
			CompletableFuture.supplyAsync(() -> {
			try {
				synchronized (operationLock) {
					checkState(!isDiscarded(), "The checkpoint has been discarded");
					// write out the metadata
					final CheckpointMetadata savepoint = new CheckpointMetadata(checkpointId, operatorStates.values(), masterStates);
					final CompletedCheckpointStorageLocation finalizedLocation;

					try (CheckpointMetadataOutputStream out = targetLocation.createMetadataOutputStream()) {
						Checkpoints.storeCheckpointMetadata(savepoint, out);
						finalizedLocation = out.closeAndFinalizeCheckpoint();
					}

					CompletedCheckpoint completed = new CompletedCheckpoint(
						jobId,
						checkpointId,
						checkpointTimestamp,
						System.currentTimeMillis(),
						operatorStates,
						masterStates,
						props,
						finalizedLocation);

					try {
						completedCheckpointStore.addCheckpoint(completed);
					} catch (Throwable t) {
						completed.discardOnFailedStoring();
					}
					return completed;
				}
			} catch (Throwable t) {
				LOG.warn("Could not finalize checkpoint {}.", checkpointId, t);
				onCompletionPromise.completeExceptionally(t);
				throw new CompletionException(t);
			}
		}, executor);

		return finalizingFuture.thenApplyAsync((completed) -> {

			// since canceller has been already cancelled, discarding means the coordinator must be shut down
			// all the resources should be released properly when it's shutting down the coordinator
			checkState(!isDiscarded(), "The checkpoint has been discarded");

			onCompletionPromise.complete(completed);

			// to prevent null-pointers from concurrent modification, copy reference onto stack
			PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Finalize the statsCallback and give the completed checkpoint a
				// callback for discards.
				CompletedCheckpointStats.DiscardCallback discardCallback =
					statsCallback.reportCompletedCheckpoint(completed.getExternalPointer());
				completed.setDiscardCallback(discardCallback);

			}

			// mark this pending checkpoint as disposed, but do NOT drop the state
			dispose(false);

			return completed;
		}, mainThreadExecutor);
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

		if (discarded) {
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

		List<OperatorID> operatorIDs = vertex.getJobVertex().getOperatorIDs();
		int subtaskIndex = vertex.getParallelSubtaskIndex();
		long ackTimestamp = System.currentTimeMillis();

		long stateSize = 0L;

		if (operatorSubtaskStates != null) {
			for (OperatorID operatorID : operatorIDs) {

				OperatorSubtaskState operatorSubtaskState =
					operatorSubtaskStates.getSubtaskStateByOperatorID(operatorID);

				// if no real operatorSubtaskState was reported, we insert an empty state
				if (operatorSubtaskState == null) {
					operatorSubtaskState = new OperatorSubtaskState();
				}

				OperatorState operatorState = operatorStates.get(operatorID);

				if (operatorState == null) {
					operatorState = new OperatorState(
						operatorID,
						vertex.getTotalNumberOfParallelSubtasks(),
						vertex.getMaxParallelism());
					operatorStates.put(operatorID, operatorState);
				}

				operatorState.putState(subtaskIndex, operatorSubtaskState);
				stateSize += operatorSubtaskState.getStateSize();
			}
		}

		++numAcknowledgedTasks;

		// publish the checkpoint statistics
		// to prevent null-pointers from concurrent modification, copy reference onto stack
		final PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			// Do this in millis because the web frontend works with them
			long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
			long checkpointStartDelayMillis = metrics.getCheckpointStartDelayNanos() / 1_000_000;

			SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
				subtaskIndex,
				ackTimestamp,
				stateSize,
				metrics.getSyncDurationMillis(),
				metrics.getAsyncDurationMillis(),
				metrics.getBytesBufferedInAlignment(),
				alignmentDurationMillis,
				checkpointStartDelayMillis);

			statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
		}

		return TaskAcknowledgeResult.SUCCESS;
	}

	public TaskAcknowledgeResult acknowledgeCoordinatorState(
			OperatorCoordinatorCheckpointContext coordinatorInfo,
			@Nullable StreamStateHandle stateHandle) {

		if (discarded) {
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

		if (stateHandle != null) {
			if (operatorState == null) {
				operatorState = new OperatorState(
					operatorId, coordinatorInfo.currentParallelism(), coordinatorInfo.maxParallelism());
				operatorStates.put(operatorId, operatorState);
			}
			operatorState.setCoordinatorState(stateHandle);
		}

		return TaskAcknowledgeResult.SUCCESS;
	}

	/**
	 * Acknowledges a master state (state generated on the checkpoint coordinator) to
	 * the pending checkpoint.
	 *
	 * @param identifier The identifier of the master state
	 * @param state The state to acknowledge
	 */
	public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {
		if (!discarded) {
			if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
				masterStates.add(state);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Cancellation
	// ------------------------------------------------------------------------

	/**
	 * Aborts a checkpoint with reason and cause.
	 */
	public void abort(CheckpointFailureReason reason, @Nullable Throwable cause) {
		try {
			failureCause = new CheckpointException(reason, cause);
			onCompletionPromise.completeExceptionally(failureCause);
			reportFailedCheckpoint(failureCause);
			assertAbortSubsumedForced(reason);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts a checkpoint with reason and cause.
	 */
	public void abort(CheckpointFailureReason reason) {
		abort(reason, null);
	}

	private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
		if (props.forceCheckpoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
			throw new IllegalStateException("Bug: forced checkpoints must never be subsumed, " +
				"the abort reason is : " + reason.message());
		}
	}

	private void dispose(boolean releaseState) {

		try {
			numAcknowledgedTasks = -1;
			if (!discarded && releaseState) {
				executor.execute(new Runnable() {
					@Override
					public void run() {

						synchronized (operationLock) {
							// discard the private states.
							// unregistered shared states are still considered private at this point.
							try {
								StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
								targetLocation.disposeOnFailure();
							} catch (Throwable t) {
								LOG.warn("Could not properly dispose the private states in the pending checkpoint {} of job {}.",
									checkpointId, jobId, t);
							} finally {
								operatorStates.clear();
							}
						}
					}
				});

			}
		} finally {
			discarded = true;
			notYetAcknowledgedTasks.clear();
			acknowledgedTasks.clear();
			cancelCanceller();
		}
	}

	private void cancelCanceller() {
		try {
			final ScheduledFuture<?> canceller = this.cancellerHandle;
			if (canceller != null) {
				canceller.cancel(false);
			}
		}
		catch (Exception e) {
			// this code should not throw exceptions
			LOG.warn("Error while cancelling checkpoint timeout task", e);
		}
	}

	/**
	 * Reports a failed checkpoint with the given optional cause.
	 *
	 * @param cause The failure cause or <code>null</code>.
	 */
	private void reportFailedCheckpoint(Exception cause) {
		// to prevent null-pointers from concurrent modification, copy reference onto stack
		final PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			long failureTimestamp = System.currentTimeMillis();
			statsCallback.reportFailedCheckpoint(failureTimestamp, cause);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
				checkpointId, checkpointTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
