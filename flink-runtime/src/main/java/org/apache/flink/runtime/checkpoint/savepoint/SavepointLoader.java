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
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The SavepointLoader is a utility to load and verify a Savepoint, and to create a checkpoint from it.
 */
public class SavepointLoader {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointLoader.class);

	/**
	 * Loads a savepoint back as a {@link CompletedCheckpoint}.
	 *
	 * <p>This method verifies that tasks and parallelism still match the savepoint parameters.
	 *
	 * @param jobId          The JobID of the job to load the savepoint for.
	 * @param tasks          Tasks that will possibly be reset
	 * @param savepointPath  The path of the savepoint to rollback to
	 * @param userClassLoader The user code classloader
	 * @param allowNonRestoredState Allow to skip checkpoint state that cannot be mapped
	 * to any job vertex in tasks.
	 *
	 * @throws IllegalStateException If mismatch between program and savepoint state
	 * @throws Exception             If savepoint store failure
	 */
	public static CompletedCheckpoint loadAndValidateSavepoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			String savepointPath,
			ClassLoader userClassLoader,
			boolean allowNonRestoredState) throws IOException {

		// (1) load the savepoint
		Savepoint savepoint = SavepointStore.loadSavepoint(savepointPath, userClassLoader);
		final Map<JobVertexID, TaskState> taskStates = new HashMap<>(savepoint.getTaskStates().size());

		boolean expandedToLegacyIds = false;

		// (2) validate it (parallelism, etc)
		for (TaskState taskState : savepoint.getTaskStates()) {

			ExecutionJobVertex executionJobVertex = tasks.get(taskState.getJobVertexID());

			// on the first time we can not find the execution job vertex for an id, we also consider alternative ids,
			// for example as generated from older flink versions, to provide backwards compatibility.
			if (executionJobVertex == null && !expandedToLegacyIds) {
				tasks = ExecutionJobVertex.includeLegacyJobVertexIDs(tasks);
				executionJobVertex = tasks.get(taskState.getJobVertexID());
				expandedToLegacyIds = true;
				LOG.info("Could not find ExecutionJobVertex. Including legacy JobVertexIDs in search.");
			}

			if (executionJobVertex != null) {
				if (executionJobVertex.getMaxParallelism() == taskState.getMaxParallelism()) {
					taskStates.put(taskState.getJobVertexID(), taskState);
				}
				else {
					String msg = String.format("Failed to rollback to savepoint %s. " +
									"Max parallelism mismatch between savepoint state and new program. " +
									"Cannot map operator %s with max parallelism %d to new program with " +
									"max parallelism %d. This indicates that the program has been changed " +
									"in a non-compatible way after the savepoint.",
							savepoint,
							taskState.getJobVertexID(),
							taskState.getMaxParallelism(),
							executionJobVertex.getMaxParallelism());

					throw new IllegalStateException(msg);
				}
			} else if (allowNonRestoredState) {
				LOG.info("Skipping savepoint state for operator {}.", taskState.getJobVertexID());
			} else {
				String msg = String.format("Failed to rollback to savepoint %s. " +
								"Cannot map savepoint state for operator %s to the new program, " +
								"because the operator is not available in the new program. If " +
								"you want to allow to skip this, you can set the --allowNonRestoredState " +
								"option on the CLI.",
						savepointPath, taskState.getJobVertexID());
				throw new IllegalStateException(msg);
			}
		}

		// (3) convert to checkpoint so the system can fall back to it
		CheckpointProperties props = CheckpointProperties.forStandardSavepoint();
		return new CompletedCheckpoint(jobId, savepoint.getCheckpointId(), 0L, 0L, taskStates, props, savepointPath);
	}

	// ------------------------------------------------------------------------

	private SavepointLoader() {}
}
