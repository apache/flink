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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StreamStateHandle;

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
	 * @param classLoader    The class loader to resolve serialized classes in legacy savepoint versions.
	 * @param allowNonRestoredState Allow to skip checkpoint state that cannot be mapped
	 * to any job vertex in tasks.
	 *
	 * @throws IllegalStateException If mismatch between program and savepoint state
	 * @throws IOException             If savepoint store failure
	 */
	public static CompletedCheckpoint loadAndValidateSavepoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			String savepointPath,
			ClassLoader classLoader,
			boolean allowNonRestoredState) throws IOException {

		// (1) load the savepoint
		final Tuple2<Savepoint, StreamStateHandle> savepointAndHandle = 
				SavepointStore.loadSavepointWithHandle(savepointPath, classLoader);

		Savepoint savepoint = savepointAndHandle.f0;
		final StreamStateHandle metadataHandle = savepointAndHandle.f1;

		if (savepoint.getTaskStates() != null) {
			savepoint = SavepointV2.convertToOperatorStateSavepointV2(tasks, savepoint);
		}
		// generate mapping from operator to task
		Map<OperatorID, ExecutionJobVertex> operatorToJobVertexMapping = new HashMap<>();
		for (ExecutionJobVertex task : tasks.values()) {
			for (OperatorID operatorID : task.getOperatorIDs()) {
				operatorToJobVertexMapping.put(operatorID, task);
			}
		}

		// (2) validate it (parallelism, etc)
		boolean expandedToLegacyIds = false;

		HashMap<OperatorID, OperatorState> operatorStates = new HashMap<>(savepoint.getOperatorStates().size());
		for (OperatorState operatorState : savepoint.getOperatorStates()) {

			ExecutionJobVertex executionJobVertex = operatorToJobVertexMapping.get(operatorState.getOperatorID());

			// on the first time we can not find the execution job vertex for an id, we also consider alternative ids,
			// for example as generated from older flink versions, to provide backwards compatibility.
			if (executionJobVertex == null && !expandedToLegacyIds) {
				operatorToJobVertexMapping = ExecutionJobVertex.includeAlternativeOperatorIDs(operatorToJobVertexMapping);
				executionJobVertex = operatorToJobVertexMapping.get(operatorState.getOperatorID());
				expandedToLegacyIds = true;
				LOG.info("Could not find ExecutionJobVertex. Including user-defined OperatorIDs in search.");
			}

			if (executionJobVertex != null) {

				if (executionJobVertex.getMaxParallelism() == operatorState.getMaxParallelism()
						|| !executionJobVertex.isMaxParallelismConfigured()) {
					operatorStates.put(operatorState.getOperatorID(), operatorState);
				} else {
					String msg = String.format("Failed to rollback to savepoint %s. " +
									"Max parallelism mismatch between savepoint state and new program. " +
									"Cannot map operator %s with max parallelism %d to new program with " +
									"max parallelism %d. This indicates that the program has been changed " +
									"in a non-compatible way after the savepoint.",
							savepoint,
							operatorState.getOperatorID(),
							operatorState.getMaxParallelism(),
							executionJobVertex.getMaxParallelism());

					throw new IllegalStateException(msg);
				}
			} else if (allowNonRestoredState) {
				LOG.info("Skipping savepoint state for operator {}.", operatorState.getOperatorID());
			} else {
				for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
					if (operatorSubtaskState.hasState()) {
						String msg = String.format("Failed to rollback to savepoint %s. " +
								"Cannot map savepoint state for operator %s to the new program, " +
								"because the operator is not available in the new program. If " +
								"you want to allow to skip this, you can set the --allowNonRestoredState " +
								"option on the CLI.",
							savepointPath, operatorState.getOperatorID());

						throw new IllegalStateException(msg);
					}
				}
				LOG.info("Skipping empty savepoint state for operator {}.", operatorState.getOperatorID());
			}
		}

		// (3) convert to checkpoint so the system can fall back to it
		CheckpointProperties props = CheckpointProperties.forStandardSavepoint();

		return new CompletedCheckpoint(
			jobId,
			savepoint.getCheckpointId(),
			0L,
			0L,
			operatorStates,
			savepoint.getMasterStates(),
			props,
			metadataHandle,
			savepointPath);
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private SavepointLoader() {}
}
