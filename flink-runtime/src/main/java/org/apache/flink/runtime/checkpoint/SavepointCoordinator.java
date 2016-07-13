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

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.Promise;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The savepoint coordinator is a slightly modified variant of the regular
 * checkpoint coordinator. Checkpoints are not triggered periodically, but
 * manually. The actual checkpointing mechanism is the same as for periodic
 * checkpoints, only the control flow is modified.
 *
 * <p>The savepoint coordinator is meant to be used as a separate coordinator
 * instance. Otherwise, there can be unwanted queueing effects like discarding
 * savepoints, because of in-progress periodic checkpoints.
 *
 * <p>The savepoint coordinator registers callbacks on the regular checkpoint
 * life-cycle and manages a map of promises, which are completed/failed as soon
 * as the trigged checkpoint is done.
 *
 * <p><strong>Important</strong>: it's necessary that both the periodic
 * checkpoint coordinator and the savepoint coordinator <em>share</em> the same
 * instance of the {@link CheckpointIDCounter} to ensure that all task managers
 * see ascending checkpoints IDs.
 */
public class SavepointCoordinator extends CheckpointCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointCoordinator.class);

	/** Store for savepoints. */
	private StateStore<CompletedCheckpoint> savepointStore;

	/** Mapping from checkpoint ID to promises for savepoints. */
	private final Map<Long, Promise<String>> savepointPromises;

	// TODO(uce) Temporary work around to restore initial state on
	// failure during recovery. Will be superseded by FLINK-3397.
	private volatile String savepointRestorePath;

	public SavepointCoordinator(
			JobID jobId,
			long baseInterval,
			long checkpointTimeout,
			int numberKeyGroups,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			ClassLoader userClassLoader,
			CheckpointIDCounter checkpointIDCounter,
			StateStore<CompletedCheckpoint> savepointStore,
			CheckpointStatsTracker statsTracker) throws Exception {

		super(jobId,
				baseInterval,
				checkpointTimeout,
				0L,
				Integer.MAX_VALUE,
				numberKeyGroups,
				tasksToTrigger,
				tasksToWaitFor,
				tasksToCommitTo,
				userClassLoader,
				checkpointIDCounter,
				IgnoreCompletedCheckpointsStore.INSTANCE,
				RecoveryMode.STANDALONE,
				statsTracker);

		this.savepointStore = checkNotNull(savepointStore);
		this.savepointPromises = new ConcurrentHashMap<>();
	}

	public String getSavepointRestorePath() {
		return savepointRestorePath;
	}

	// ------------------------------------------------------------------------
	// Savepoint trigger and reset
	// ------------------------------------------------------------------------

	/**
	 * Triggers a new savepoint using the current system time as the checkpoint timestamp.
	 */
	public Future<String> triggerSavepoint(long timestamp) throws Exception {
		final Promise<String> promise = new scala.concurrent.impl.Promise.DefaultPromise<>();

		try {
			// Get the checkpoint ID up front. If we fail to trigger the checkpoint,
			// the ID will have changed, but this is OK as long as the checkpoint ID
			// generates ascending IDs.
			final long checkpointId = getAndIncrementCheckpointId();

			if (checkpointId == -1) {
				throw new IllegalStateException("Failed to get checkpoint Id");
			}

			// Important: make sure to add the promise to the map before calling
			// any methods that might trigger callbacks, which require the promise.
			// Otherwise, the might be race conditions.
			if (savepointPromises.put(checkpointId, promise) == null) {
				boolean success = false;

				try {
					// All good. The future will be completed as soon as the
					// triggered checkpoint is done.
					success = triggerCheckpoint(timestamp, checkpointId);
				}
				finally {
					if (!success) {
						savepointPromises.remove(checkpointId);
						promise.failure(new Exception("Failed to trigger savepoint"));
					}
				}
			}
			else {
				throw new IllegalStateException("Duplicate checkpoint ID");
			}
		}
		catch (Throwable t) {
			promise.failure(new Exception("Failed to trigger savepoint", t));
		}

		return promise.future();
	}

	/**
	 * Resets the state of {@link Execution} instances back to the state of a savepoint.
	 *
	 * <p>The execution vertices need to be in state {@link ExecutionState#CREATED} when calling
	 * this method. The operation might block. Make sure that calls don't block the job manager
	 * actor.
	 *
	 * @param tasks         Tasks that will possibly be reset
	 * @param savepointPath The path of the savepoint to rollback to
	 * @throws IllegalStateException If coordinator is shut down
	 * @throws IllegalStateException If mismatch between program and savepoint state
	 * @throws Exception             If savepoint store failure
	 */
	public void restoreSavepoint(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			String savepointPath) throws Exception {

		checkNotNull(savepointPath, "Savepoint path");

		synchronized (lock) {
			if (isShutdown()) {
				throw new IllegalStateException("CheckpointCoordinator is shut down");
			}

			LOG.info("Rolling back to savepoint '{}'.", savepointPath);

			CompletedCheckpoint checkpoint = savepointStore.getState(savepointPath);

			LOG.info("Savepoint: {}@{}", checkpoint.getCheckpointID(), checkpoint.getTimestamp());

			// Set the initial state of all tasks
			LOG.debug("Rolling back individual operators.");

			for (Map.Entry<JobVertexID, TaskState> taskStateEntry: checkpoint.getTaskStates().entrySet()) {
				TaskState taskState = taskStateEntry.getValue();
				ExecutionJobVertex executionJobVertex = tasks.get(taskStateEntry.getKey());

				if (executionJobVertex != null) {
					if (executionJobVertex.getParallelism() != taskState.getParallelism()) {
						String msg = String.format("Failed to rollback to savepoint %s. " +
								"Parallelism mismatch between savepoint state and new program. " +
								"Cannot map operator %s with parallelism %d to new program with " +
								"parallelism %d. This indicates that the program has been changed " +
								"in a non-compatible way after the savepoint.",
							checkpoint,
							taskStateEntry.getKey(),
							taskState.getParallelism(),
							executionJobVertex.getParallelism());

						throw new IllegalStateException(msg);
					}

					List<Set<Integer>> keyGroupPartitions = createKeyGroupPartitions(
						numberKeyGroups,
						executionJobVertex.getParallelism());

					for (int i = 0; i < executionJobVertex.getTaskVertices().length; i++) {
						SubtaskState subtaskState = taskState.getState(i);
						SerializedValue<StateHandle<?>> state = null;

						if (subtaskState != null) {
							state = subtaskState.getState();
						}

						Map<Integer, SerializedValue<StateHandle<?>>> kvStateForTaskMap = taskState
							.getUnwrappedKvStates(keyGroupPartitions.get(i));

						Execution currentExecutionAttempt = executionJobVertex
							.getTaskVertices()[i]
							.getCurrentExecutionAttempt();

						currentExecutionAttempt.setInitialState(state, kvStateForTaskMap);
					}
				} else {
					String msg = String.format("Failed to rollback to savepoint %s. " +
							"Cannot map old state for task %s to the new program. " +
							"This indicates that the program has been changed in a " +
							"non-compatible way  after the savepoint.", checkpoint,
						taskStateEntry.getKey());
					throw new IllegalStateException(msg);
				}
			}

			// Reset the checkpoint ID counter
			long nextCheckpointId = checkpoint.getCheckpointID();
			checkpointIdCounter.start();
			checkpointIdCounter.setCount(nextCheckpointId + 1);
			LOG.info("Reset the checkpoint ID to {}", nextCheckpointId);

			if (savepointRestorePath == null) {
				savepointRestorePath = savepointPath;
			}
		}
	}

	// ------------------------------------------------------------------------
	// Checkpoint coordinator callbacks
	// ------------------------------------------------------------------------

	@Override
	protected void onShutdown() {
		// Fail all outstanding savepoint futures
		for (Promise<String> promise : savepointPromises.values()) {
			promise.failure(new Exception("Checkpoint coordinator shutdown"));
		}
		savepointPromises.clear();
	}

	@Override
	protected void onCancelCheckpoint(long canceledCheckpointId) {
		Promise<String> promise = savepointPromises.remove(canceledCheckpointId);

		if (promise != null) {
			promise.failure(new Exception("Savepoint expired before completing"));
		}
	}

	@Override
	protected void onFullyAcknowledgedCheckpoint(CompletedCheckpoint checkpoint) {
		// Sanity check
		Promise<String> promise = checkNotNull(savepointPromises
				.remove(checkpoint.getCheckpointID()));

		// Sanity check
		if (promise.isCompleted()) {
			throw new IllegalStateException("Savepoint promise completed");
		}

		try {
			// Save the checkpoint
			String savepointPath = savepointStore.putState(checkpoint);
			promise.success(savepointPath);
		}
		catch (Exception e) {
			LOG.warn("Failed to store savepoint.", e);
			promise.failure(e);
		}
	}

	// ------------------------------------------------------------------------
	// Job status listener
	// ------------------------------------------------------------------------

	@Override
	public ActorGateway createActivatorDeactivator(
			ActorSystem actorSystem,
			UUID leaderSessionID) {

		synchronized (lock) {
			if (isShutdown()) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (getJobStatusListener() == null) {
				Props props = Props.create(
						SavepointCoordinatorDeActivator.class,
						this,
						leaderSessionID);

				// wrap the ActorRef in a AkkaActorGateway to support message decoration
				setJobStatusListener(new AkkaActorGateway(
						actorSystem.actorOf(props),
						leaderSessionID));
			}

			return getJobStatusListener();
		}
	}

	// ------------------------------------------------------------------------
	// Completed checkpoints
	// ------------------------------------------------------------------------

	private static class IgnoreCompletedCheckpointsStore implements CompletedCheckpointStore {

		private static final CompletedCheckpointStore INSTANCE = new IgnoreCompletedCheckpointsStore();

		@Override
		public void recover() throws Exception {
		}

		@Override
		public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		}

		@Override
		public CompletedCheckpoint getLatestCheckpoint() throws Exception {
			return null;
		}

		@Override
		public void discardAllCheckpoints() throws Exception {
		}

		@Override
		public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
			return Collections.emptyList();
		}

		@Override
		public int getNumberOfRetainedCheckpoints() {
			return 0;
		}
	}

}
