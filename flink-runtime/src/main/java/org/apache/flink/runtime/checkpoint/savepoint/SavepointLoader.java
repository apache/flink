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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Map;

/**
 * The SavepointLoader is a utility to load and verify a Savepoint, and to create a checkpoint from it. 
 */
public class SavepointLoader {

	/**
	 * Loads a savepoint back as a {@link CompletedCheckpoint}.
	 *
	 * <p>This method verifies that tasks and parallelism still match the savepoint parameters.
	 *
	 * @param jobId          The JobID of the job to load the savepoint for.
	 * @param tasks          Tasks that will possibly be reset
	 * @param savepointStore The store that holds the savepoint.
	 * @param savepointPath  The path of the savepoint to rollback to
	 *
	 * @throws IllegalStateException If mismatch between program and savepoint state
	 * @throws Exception             If savepoint store failure
	 */
	public static CompletedCheckpoint loadAndValidateSavepoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			SavepointStore savepointStore,
			String savepointPath) throws Exception {

		// (1) load the savepoint
		Savepoint savepoint = savepointStore.loadSavepoint(savepointPath);
		final Map<JobVertexID, TaskState> taskStates = new HashMap<>(savepoint.getTaskStates().size());
		
		// (2) validate it (parallelism, etc)
		for (TaskState taskState : savepoint.getTaskStates()) {
			ExecutionJobVertex executionJobVertex = tasks.get(taskState.getJobVertexID());

			if (executionJobVertex != null) {
				if (executionJobVertex.getParallelism() == taskState.getParallelism()) {
					taskStates.put(taskState.getJobVertexID(), taskState);
				}
				else {
					String msg = String.format("Failed to rollback to savepoint %s. " +
									"Parallelism mismatch between savepoint state and new program. " +
									"Cannot map operator %s with parallelism %d to new program with " +
									"parallelism %d. This indicates that the program has been changed " +
									"in a non-compatible way after the savepoint.",
							savepoint,
							taskState.getJobVertexID(),
							taskState.getParallelism(),
							executionJobVertex.getParallelism());

					throw new IllegalStateException(msg);
				}
			} else {
				String msg = String.format("Failed to rollback to savepoint %s. " +
								"Cannot map old state for task %s to the new program. " +
								"This indicates that the program has been changed in a " +
								"non-compatible way  after the savepoint.",
						savepointPath, taskState.getJobVertexID());
				throw new IllegalStateException(msg);
			}
		}

		// (3) convert to checkpoint so the system can fall back to it
		return new CompletedCheckpoint(jobId, savepoint.getCheckpointId(), 0L, 0L, taskStates, false);
	} 

	// ------------------------------------------------------------------------

	private SavepointLoader() {}
}
