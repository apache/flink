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
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it. Once all tasks have
 * acknowledged it, it becomes a {@link CompletedCheckpoint}.
 * 
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the
 * state handles always as serialized values, never as actual values.</p>
 */
public class PendingCheckpoint {

	private static final Logger LOG = LoggerFactory.getLogger(PendingCheckpoint.class);
		
	private final Object lock = new Object();
	
	private final JobID jobId;
	
	private final long checkpointId;
	
	private final long checkpointTimestamp;

	private final Map<JobVertexID, TaskState> taskStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;
	
	/** Set of acknowledged tasks */
	private final Set<ExecutionAttemptID> acknowledgedTasks;

	private final Executor executor;

	private int numAcknowledgedTasks;
	
	private boolean discarded;
	
	// --------------------------------------------------------------------------------------------
	
	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			Executor executor) {
		if (jobId == null || verticesToConfirm == null) {
			throw new NullPointerException();
		}
		if (verticesToConfirm.size() == 0) {
			throw new IllegalArgumentException("Checkpoint needs at least one vertex that commits the checkpoint");
		}
		
		this.jobId = jobId;
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.executor = Preconditions.checkNotNull(executor);
		
		this.notYetAcknowledgedTasks = verticesToConfirm;
		this.taskStates = new HashMap<>();

		acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
	}
	
	// --------------------------------------------------------------------------------------------


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
	
	public CompletedCheckpoint finalizeCheckpoint() {
		synchronized (lock) {
			Preconditions.checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

				CompletedCheckpoint completed =  new CompletedCheckpoint(
					jobId,
					checkpointId,
					checkpointTimestamp,
					System.currentTimeMillis(),
					new HashMap<>(taskStates));
				dispose(null, false);
				
				return completed;
		}
	}
	
	public TaskAcknowledgeResult acknowledgeTask(
			ExecutionAttemptID executionAttemptId,
			SerializedValue<StateHandle<?>> state,
			long stateSize,
			Map<Integer, SerializedValue<StateHandle<?>>> kvState) {

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
			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Result of the {@link PendingCheckpoint#acknowledgedTasks} method.
	 */
	public enum TaskAcknowledgeResult {
		SUCCESS, // successful acknowledge of the task
		DUPLICATE, // acknowledge message is a duplicate
		UNKNOWN, // unknown task acknowledged
		DISCARDED // pending checkpoint has been discarded
	}
	
	/**
	 * Discards the pending checkpoint, releasing all held resources.
	 */
	public void discard(ClassLoader userClassLoader) {
		dispose(userClassLoader, true);
	}

	private void dispose(final ClassLoader userClassLoader, boolean releaseState) {
		synchronized (lock) {
			numAcknowledgedTasks = -1;
			try {
				if (!discarded && releaseState) {
					executor.execute(new Runnable() {
						@Override
						public void run() {
							for (TaskState taskState: taskStates.values()) {
								try {
									taskState.discard(userClassLoader);
								} catch (Exception e) {
									LOG.warn("Could not properly dispose the task state " +
										"belonging to vertex {} of checkpoint {} and job {}.",
										taskState.getJobVertexID(), checkpointId, jobId, e);
								}
							}

							taskStates.clear();
						}
					});

				}
			} finally {
				discarded = true;
				notYetAcknowledgedTasks.clear();
				acknowledgedTasks.clear();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("PendingCheckpoint %d @ %d - confirmed=%d, pending=%d",
				checkpointId, checkpointTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
