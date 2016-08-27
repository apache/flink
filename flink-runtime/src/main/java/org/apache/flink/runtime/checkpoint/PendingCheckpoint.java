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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it. Once all tasks have
 * acknowledged it, it becomes a {@link CompletedCheckpoint}.
 * 
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the
 * state handles always as serialized values, never as actual values.
 */
public class PendingCheckpoint {

	private final Object lock = new Object();

	private final JobID jobId;

	private final long checkpointId;

	private final long checkpointTimestamp;

	private final Map<JobVertexID, TaskState> taskStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final ClassLoader userCodeClassLoader;

	private final boolean disposeWhenSubsumed;

	private int numAcknowledgedTasks;

	private boolean discarded;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			ClassLoader userCodeClassLoader) {
		this(jobId, checkpointId, checkpointTimestamp, verticesToConfirm, userCodeClassLoader, true);
	}

	PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			ClassLoader userCodeClassLoader,
			boolean disposeWhenSubsumed)
	{
		this.jobId = checkNotNull(jobId);
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.disposeWhenSubsumed = disposeWhenSubsumed;
		this.taskStates = new HashMap<>();

		checkArgument(verticesToConfirm.size() > 0,
				"Checkpoint needs at least one vertex that commits the checkpoint");
	}

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

	public Map<JobVertexID, TaskState> getTaskStates() {
		return taskStates;
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
		return true;
	}

	// ------------------------------------------------------------------------
	//  Progress and Completion
	// ------------------------------------------------------------------------

	public CompletedCheckpoint finalizeCheckpoint() throws Exception {
		synchronized (lock) {
			if (discarded) {
				throw new IllegalStateException("pending checkpoint is discarded");
			}
			if (notYetAcknowledgedTasks.isEmpty()) {
				CompletedCheckpoint completed =  new CompletedCheckpoint(
					jobId,
					checkpointId,
					checkpointTimestamp,
					System.currentTimeMillis(),
					new HashMap<>(taskStates),
					disposeWhenSubsumed);

				dispose(false);

				return completed;
			}
			else {
				throw new IllegalStateException("Cannot complete checkpoint while not all tasks are acknowledged");
			}
		}
	}
	
	public boolean acknowledgeTask(
			ExecutionAttemptID attemptID,
			SerializedValue<StateHandle<?>> state,
			long stateSize,
			Map<Integer, SerializedValue<StateHandle<?>>> kvState) {

		synchronized (lock) {
			if (discarded) {
				return false;
			}
			
			ExecutionVertex vertex = notYetAcknowledgedTasks.remove(attemptID);
			if (vertex != null) {
				if (state != null || kvState != null) {

					JobVertexID jobVertexID = vertex.getJobvertexId();

					TaskState taskState;

					if (taskStates.containsKey(jobVertexID)) {
						taskState = taskStates.get(jobVertexID);
					} else {
						taskState = new TaskState(jobVertexID, vertex.getTotalNumberOfParallelSubtasks());
						taskStates.put(jobVertexID, taskState);
					}

					long timestamp = System.currentTimeMillis() - checkpointTimestamp;

					if (state != null) {
						taskState.putState(
							vertex.getParallelSubtaskIndex(),
							new SubtaskState(
								state,
								stateSize,
								timestamp
							)
						);
					}

					if (kvState != null) {
						for (Map.Entry<Integer, SerializedValue<StateHandle<?>>> entry : kvState.entrySet()) {
							taskState.putKvState(
								entry.getKey(),
								new KeyGroupState(
									entry.getValue(),
									0L,
									timestamp
								));
						}
					}
				}
				numAcknowledgedTasks++;
				return true;
			}
			else {
				return false;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Cancellation
	// ------------------------------------------------------------------------

	/**
	 * Aborts a checkpoint because it expired (took too long).
	 */
	public void abortExpired() throws Exception {
		dispose(true);
	}

	/**
	 * Aborts the pending checkpoint because a newer completed checkpoint subsumed it.
	 */
	public void abortSubsumed() throws Exception {
		dispose(true);
	}

	public void abortDeclined() throws Exception {
		dispose(true);
	}

	/**
	 * Aborts the pending checkpoint due to an error.
	 * @param cause The error's exception.
	 */
	public void abortError(Throwable cause) throws Exception {
		dispose(true);
	}

	protected void dispose(boolean releaseState) throws Exception {
		synchronized (lock) {
			discarded = true;
			numAcknowledgedTasks = -1;
			try {
				if (releaseState) {
					for (TaskState taskState : taskStates.values()) {
						taskState.discard(userCodeClassLoader);
					}
				}
			} finally {
				taskStates.clear();
				notYetAcknowledgedTasks.clear();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
				checkpointId, checkpointTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
