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
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.TestExecutors;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Tests concerning the restoring of state from a savepoint to the task executions.
 */
public class SavepointCoordinatorRestoreTest {

	/**
	 * Tests that the unmapped state flag is correctly handled.
	 *
	 * The flag should only apply for state that is part of the checkpoint.
	 */
	@Test
	public void testRestoreUnmappedCheckpointState() throws Exception {
		// --- (1) Create tasks to restore checkpoint with ---
		JobVertexID jobVertexId1 = new JobVertexID();
		JobVertexID jobVertexId2 = new JobVertexID();

		// 1st JobVertex
		ExecutionVertex vertex11 = mockExecutionVertex(mockExecution(), jobVertexId1, 0, 3);
		ExecutionVertex vertex12 = mockExecutionVertex(mockExecution(), jobVertexId1, 1, 3);
		ExecutionVertex vertex13 = mockExecutionVertex(mockExecution(), jobVertexId1, 2, 3);
		// 2nd JobVertex
		ExecutionVertex vertex21 = mockExecutionVertex(mockExecution(), jobVertexId2, 0, 2);
		ExecutionVertex vertex22 = mockExecutionVertex(mockExecution(), jobVertexId2, 1, 2);

		ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(jobVertexId1, new ExecutionVertex[] { vertex11, vertex12, vertex13 });
		ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(jobVertexId2, new ExecutionVertex[] { vertex21, vertex22 });

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexId1, jobVertex1);
		tasks.put(jobVertexId2, jobVertex2);

		SavepointStore store = new HeapSavepointStore();

		SavepointCoordinator coord = new SavepointCoordinator(
			new JobID(),
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			0,
			new ExecutionVertex[] {},
			new ExecutionVertex[] {},
			new ExecutionVertex[] {},
			getClass().getClassLoader(),
			new StandaloneCheckpointIDCounter(),
			store,
			new DisabledCheckpointStatsTracker(),
			TestExecutors.directExecutor());

		// --- (2) Checkpoint misses state for a jobVertex (should work) ---
		Map<JobVertexID, TaskState> checkpointTaskStates = new HashMap<>();
		checkpointTaskStates.put(jobVertexId1, new TaskState(jobVertexId1, 3));

		CompletedCheckpoint checkpoint = new CompletedCheckpoint(new JobID(), 0, 1, 2, new HashMap<>(checkpointTaskStates));

		Savepoint savepoint = new SavepointV0(checkpoint.getCheckpointID(), checkpointTaskStates.values());
		String savepointPath = store.storeSavepoint(savepoint);

		coord.restoreSavepoint(tasks, savepointPath, false);
		coord.restoreSavepoint(tasks, savepointPath, true);

		// --- (3) JobVertex missing for task state that is part of the checkpoint ---
		JobVertexID newJobVertexID = new JobVertexID();

		// There is no task for this
		checkpointTaskStates.put(newJobVertexID, new TaskState(newJobVertexID, 1));

		checkpoint = new CompletedCheckpoint(new JobID(), 1, 2, 3, new HashMap<>(checkpointTaskStates));
		savepoint = new SavepointV0(checkpoint.getCheckpointID(), checkpointTaskStates.values());
		savepointPath = store.storeSavepoint(savepoint);

		// (i) Ignore unmapped state (should succeed)
		coord.restoreSavepoint(tasks, savepointPath, true);

		// (ii) Don't ignore unmapped state (should fail)
		try {
			coord.restoreSavepoint(tasks, savepointPath, false);
			fail("Did not throw the expected Exception.");
		} catch (IllegalStateException ignored) {
		}
	}

	// ------------------------------------------------------------------------

	private Execution mockExecution() {
		return mockExecution(ExecutionState.RUNNING);
	}

	private Execution mockExecution(ExecutionState state) {
		Execution mock = Mockito.mock(Execution.class);
		when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(mock.getState()).thenReturn(state);
		return mock;
	}

	private ExecutionVertex mockExecutionVertex(Execution execution, JobVertexID vertexId, int subtask, int parallelism) {
		ExecutionVertex mock = Mockito.mock(ExecutionVertex.class);
		when(mock.getJobvertexId()).thenReturn(vertexId);
		when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
		when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
		when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		return mock;
	}

	private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
		ExecutionJobVertex vertex = Mockito.mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(vertices.length);
		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getTaskVertices()).thenReturn(vertices);
		return vertex;
	}

}
