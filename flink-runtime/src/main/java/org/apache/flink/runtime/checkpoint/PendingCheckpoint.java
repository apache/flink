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
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

	/** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator */
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	private final Object lock = new Object();

	private final JobID jobId;

	private final long checkpointId;

	private final long checkpointTimestamp;

	private final Map<OperatorID, OperatorState> operatorStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final List<MasterState> masterState;

	/** Set of acknowledged tasks */
	private final Set<ExecutionAttemptID> acknowledgedTasks;

	/** The checkpoint properties. If the checkpoint should be persisted
	 * externally, it happens in {@link #finalizeCheckpointExternalized()}. */
	private final CheckpointProperties props;

	/** Target directory to potentially persist checkpoint to; <code>null</code> if none configured. */
	private final String targetDirectory;

	/** The promise to fulfill once the checkpoint has been completed. */
	private final FlinkCompletableFuture<CompletedCheckpoint> onCompletionPromise;

	/** The executor for potentially blocking I/O operations, like state disposal */
	private final Executor executor;

	private int numAcknowledgedTasks;

	private boolean discarded;

	/** Optional stats tracker callback. */
	@Nullable
	private PendingCheckpointStats statsCallback;

	private volatile ScheduledFuture<?> cancellerHandle;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			CheckpointProperties props,
			String targetDirectory,
			Executor executor) {

		// Sanity check
		if (props.externalizeCheckpoint() && targetDirectory == null) {
			throw new NullPointerException("No target directory specified to persist checkpoint to.");
		}

		checkArgument(verticesToConfirm.size() > 0,
				"Checkpoint needs at least one vertex that commits the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.props = checkNotNull(props);
		this.targetDirectory = targetDirectory;
		this.executor = Preconditions.checkNotNull(executor);

		this.operatorStates = new HashMap<>();
		this.masterState = new ArrayList<>();
		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = new FlinkCompletableFuture<>();
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

	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	public int getNumberOfNonAcknowledgedTasks() {
		return notYetAcknowledgedTasks.size();
	}

	public int getNumberOfAcknowledgedTasks() {
		return numAcknowledgedTasks;
	}

	public Map<OperatorID, OperatorState> getOperatorStates() {
		return operatorStates;
	}

	public boolean isFullyAcknowledged() {
		return this.notYetAcknowledgedTasks.isEmpty() && !discarded;
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

	String getTargetDirectory() {
		return targetDirectory;
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
		synchronized (lock) {
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
	}

	// ------------------------------------------------------------------------
	//  Progress and Completion
	// ------------------------------------------------------------------------

	/**
	 * Returns the completion future.
	 *
	 * @return A future to the completed checkpoint
	 */
	public Future<CompletedCheckpoint> getCompletionFuture() {
		return onCompletionPromise;
	}

	public CompletedCheckpoint finalizeCheckpointExternalized() throws IOException {

		synchronized (lock) {
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

			// make sure we fulfill the promise with an exception if something fails
			try {
				// externalize the metadata
				final Savepoint savepoint = new SavepointV2(checkpointId, operatorStates.values(), masterState);

				// TEMP FIX - The savepoint store is strictly typed to file systems currently
				//            but the checkpoints think more generic. we need to work with file handles
				//            here until the savepoint serializer accepts a generic stream factory

				// We have this branch here, because savepoints and externalized checkpoints
				// currently behave differently.
				// Savepoints:
				//   - Metadata file in unique directory
				//   - External pointer can be the directory
				// Externalized checkpoints:
				//   - Multiple metadata files per directory possible (need to be unique)
				//   - External pointer needs to be the file itself
				//
				// This should be unified as part of the JobManager metadata stream factories.
				if (props.isSavepoint()) {
					final FileStateHandle metadataHandle = SavepointStore.storeSavepointToHandle(targetDirectory, savepoint);
					final String externalPointer = metadataHandle.getFilePath().getParent().toString();
	
					return finalizeInternal(metadataHandle, externalPointer);
				} else {
					final FileStateHandle metadataHandle = SavepointStore.storeExternalizedCheckpointToHandle(targetDirectory, savepoint);
					final String externalPointer = metadataHandle.getFilePath().toString();

					return finalizeInternal(metadataHandle, externalPointer);
				}
			}
			catch (Throwable t) {
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}
	}

	public CompletedCheckpoint finalizeCheckpointNonExternalized() {
		synchronized (lock) {
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

			// make sure we fulfill the promise with an exception if something fails
			try {
				// finalize without external metadata
				return finalizeInternal(null, null);
			}
			catch (Throwable t) {
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrow(t);
				return null; // silence the compiler
			}
		}
	}

	@GuardedBy("lock")
	private CompletedCheckpoint finalizeInternal(
			@Nullable StreamStateHandle externalMetadata,
			@Nullable String externalPointer) {

		assert(Thread.holdsLock(lock));

		CompletedCheckpoint completed = new CompletedCheckpoint(
				jobId,
				checkpointId,
				checkpointTimestamp,
				System.currentTimeMillis(),
				operatorStates,
				masterState,
				props,
				externalMetadata,
				externalPointer);

		onCompletionPromise.complete(completed);

		// to prevent null-pointers from concurrent modification, copy reference onto stack
		PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			// Finalize the statsCallback and give the completed checkpoint a
			// callback for discards.
			CompletedCheckpointStats.DiscardCallback discardCallback = 
					statsCallback.reportCompletedCheckpoint(externalPointer);
			completed.setDiscardCallback(discardCallback);
		}

		// mark this pending checkpoint as disposed, but do NOT drop the state
		dispose(false);

		return completed;
	}

	/**
	 * Acknowledges the task with the given execution attempt id and the given subtask state.
	 *
	 * @param executionAttemptId of the acknowledged task
	 * @param subtaskState of the acknowledged task
	 * @param metrics Checkpoint metrics for the stats
	 * @return TaskAcknowledgeResult of the operation
	 */
	public TaskAcknowledgeResult acknowledgeTask(
			ExecutionAttemptID executionAttemptId,
			SubtaskState subtaskState,
			CheckpointMetrics metrics) {

		synchronized (lock) {
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

			long stateSize = 0;
			if (subtaskState != null) {
				stateSize = subtaskState.getStateSize();

				@SuppressWarnings("deprecation")
				ChainedStateHandle<StreamStateHandle> nonPartitionedState =
					subtaskState.getLegacyOperatorState();
				ChainedStateHandle<OperatorStateHandle> partitioneableState =
					subtaskState.getManagedOperatorState();
				ChainedStateHandle<OperatorStateHandle> rawOperatorState =
					subtaskState.getRawOperatorState();

				// break task state apart into separate operator states
				for (int x = 0; x < operatorIDs.size(); x++) {
					OperatorID operatorID = operatorIDs.get(x);
					OperatorState operatorState = operatorStates.get(operatorID);

					if (operatorState == null) {
						operatorState = new OperatorState(
							operatorID,
							vertex.getTotalNumberOfParallelSubtasks(),
							vertex.getMaxParallelism());
						operatorStates.put(operatorID, operatorState);
					}

					KeyedStateHandle managedKeyedState = null;
					KeyedStateHandle rawKeyedState = null;

					// only the head operator retains the keyed state
					if (x == operatorIDs.size() - 1) {
						managedKeyedState = subtaskState.getManagedKeyedState();
						rawKeyedState = subtaskState.getRawKeyedState();
					}

					OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(
							nonPartitionedState != null ? nonPartitionedState.get(x) : null,
							partitioneableState != null ? partitioneableState.get(x) : null,
							rawOperatorState != null ? rawOperatorState.get(x) : null,
							managedKeyedState,
							rawKeyedState);

					operatorState.putState(subtaskIndex, operatorSubtaskState);
				}
			}

			++numAcknowledgedTasks;

			// publish the checkpoint statistics
			// to prevent null-pointers from concurrent modification, copy reference onto stack
			final PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Do this in millis because the web frontend works with them
				long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;

				SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
					subtaskIndex,
					ackTimestamp,
					stateSize,
					metrics.getSyncDurationMillis(),
					metrics.getAsyncDurationMillis(),
					metrics.getBytesBufferedInAlignment(),
					alignmentDurationMillis);

				statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Adds a master state (state generated on the checkpoint coordinator) to
	 * the pending checkpoint.
	 *
	 * @param state The state to add
	 */
	public void addMasterState(MasterState state) {
		checkNotNull(state);

		synchronized (lock) {
			if (!discarded) {
				masterState.add(state);
			}
		}
	}


	// ------------------------------------------------------------------------
	//  Cancellation
	// ------------------------------------------------------------------------

	/**
	 * Aborts a checkpoint because it expired (took too long).
	 */
	public void abortExpired() {
		try {
			Exception cause = new Exception("Checkpoint expired before completing");
			onCompletionPromise.completeExceptionally(cause);
			reportFailedCheckpoint(cause);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts the pending checkpoint because a newer completed checkpoint subsumed it.
	 */
	public void abortSubsumed() {
		try {
			Exception cause = new Exception("Checkpoints has been subsumed");
			onCompletionPromise.completeExceptionally(cause);
			reportFailedCheckpoint(cause);

			if (props.forceCheckpoint()) {
				throw new IllegalStateException("Bug: forced checkpoints must never be subsumed");
			}
		} finally {
			dispose(true);
		}
	}

	public void abortDeclined() {
		try {
			Exception cause = new Exception("Checkpoint was declined (tasks not ready)");
			onCompletionPromise.completeExceptionally(cause);
			reportFailedCheckpoint(cause);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts the pending checkpoint due to an error.
	 * @param cause The error's exception.
	 */
	public void abortError(Throwable cause) {
		try {
			Exception failure = new Exception("Checkpoint failed: " + cause.getMessage(), cause);
			onCompletionPromise.completeExceptionally(failure);
			reportFailedCheckpoint(failure);
		} finally {
			dispose(true);
		}
	}

	private void dispose(boolean releaseState) {

		synchronized (lock) {
			try {
				numAcknowledgedTasks = -1;
				if (!discarded && releaseState) {
					executor.execute(new Runnable() {
						@Override
						public void run() {

							// discard the private states.
							// unregistered shared states are still considered private at this point.
							try {
								StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
							} catch (Throwable t) {
								LOG.warn("Could not properly dispose the private states in the pending checkpoint {} of job {}.",
									checkpointId, jobId, t);
							} finally {
								operatorStates.clear();
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
