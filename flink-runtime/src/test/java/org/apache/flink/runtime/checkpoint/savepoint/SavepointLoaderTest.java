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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SavepointLoaderTest {

	/**
	 * Tests loading and validation of savepoints with correct setup,
	 * parallelism mismatch, and a missing task.
	 */
	@Test
	public void testLoadAndValidateSavepoint() throws Exception {
		int parallelism = 128128;
		JobVertexID vertexId = new JobVertexID();

		TaskState state = mock(TaskState.class);
		when(state.getParallelism()).thenReturn(parallelism);
		when(state.getJobVertexID()).thenReturn(vertexId);

		Map<JobVertexID, TaskState> taskStates = new HashMap<>();
		taskStates.put(vertexId, state);

		CompletedCheckpoint stored = new CompletedCheckpoint(
				new JobID(),
				Integer.MAX_VALUE + 123123L,
				10200202,
				1020292988,
				taskStates,
				true);

		// Store savepoint
		SavepointV0 savepoint = new SavepointV0(stored.getCheckpointID(), taskStates.values());
		SavepointStore store = new HeapSavepointStore();
		String path = store.storeSavepoint(savepoint);

		JobID jobId = new JobID();

		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(parallelism);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(vertexId, vertex);

		// 1) Load and validate: everything correct
		CompletedCheckpoint loaded = SavepointLoader.loadAndValidateSavepoint(jobId, tasks, store, path);

		assertEquals(jobId, loaded.getJobId());
		assertEquals(stored.getCheckpointID(), loaded.getCheckpointID());

		// The loaded checkpoint should not discard state when its discarded
		loaded.discard(ClassLoader.getSystemClassLoader());
		verify(state, times(0)).discard(any(ClassLoader.class));

		// 2) Load and validate: parallelism mismatch
		when(vertex.getParallelism()).thenReturn(222);

		try {
			SavepointLoader.loadAndValidateSavepoint(jobId, tasks, store, path);
			fail("Did not throw expected Exception");
		} catch (IllegalStateException expected) {
			assertTrue(expected.getMessage().contains("Parallelism mismatch"));
		}

		// 3) Load and validate: missing vertex (this should be relaxed)
		assertNotNull(tasks.remove(vertexId));

		try {
			SavepointLoader.loadAndValidateSavepoint(jobId, tasks, store, path);
			fail("Did not throw expected Exception");
		} catch (IllegalStateException expected) {
			assertTrue(expected.getMessage().contains("Cannot map old state"));
		}
	}
}
