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
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A test that checks that checkpoint metadata loading works properly, including validation
 * of resumed state and dropped state.
 */
public class CheckpointMetadataLoadingTest {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	/**
	 * Tests loading and validation of savepoints with correct setup,
	 * parallelism mismatch, and a missing task.
	 */
	@Test
	public void testLoadAndValidateSavepoint() throws Exception {
		File tmp = tmpFolder.newFolder();

		int parallelism = 128128;
		long checkpointId = Integer.MAX_VALUE + 123123L;
		JobVertexID jobVertexID = new JobVertexID();
		OperatorID operatorID = OperatorID.fromJobVertexID(jobVertexID);

		OperatorSubtaskState subtaskState = new OperatorSubtaskState(
				new OperatorStreamStateHandle(
				Collections.emptyMap(),
				new ByteStreamStateHandle("testHandler", new byte[0])),
				null,
				null,
				null);

		OperatorState state = new OperatorState(operatorID, parallelism, parallelism);
		state.putState(0, subtaskState);

		Map<OperatorID, OperatorState> taskStates = new HashMap<>();
		taskStates.put(operatorID, state);

		JobID jobId = new JobID();

		// Store savepoint
		final SavepointV2 savepoint = new SavepointV2(checkpointId, taskStates.values(), Collections.emptyList());
		final StreamStateHandle serializedMetadata;

		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			Checkpoints.storeCheckpointMetadata(savepoint, os);
			serializedMetadata = new ByteStreamStateHandle("checkpoint", os.toByteArray());
		}

		final CompletedCheckpointStorageLocation storageLocation = new TestCompletedCheckpointStorageLocation(
				serializedMetadata, "dummy/pointer");

		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(parallelism);
		when(vertex.getOperatorIDs()).thenReturn(Collections.singletonList(operatorID));

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexID, vertex);

		ClassLoader ucl = Thread.currentThread().getContextClassLoader();

		// 1) Load and validate: everything correct
		CompletedCheckpoint loaded = Checkpoints.loadAndValidateCheckpoint(jobId, tasks, storageLocation, ucl, false);

		assertEquals(jobId, loaded.getJobId());
		assertEquals(checkpointId, loaded.getCheckpointID());

		// 2) Load and validate: max parallelism mismatch
		when(vertex.getMaxParallelism()).thenReturn(222);
		when(vertex.isMaxParallelismConfigured()).thenReturn(true);

		try {
			Checkpoints.loadAndValidateCheckpoint(jobId, tasks, storageLocation, ucl, false);
			fail("Did not throw expected Exception");
		} catch (IllegalStateException expected) {
			assertTrue(expected.getMessage().contains("Max parallelism mismatch"));
		}

		// 3) Load and validate: missing vertex
		assertNotNull(tasks.remove(jobVertexID));

		try {
			Checkpoints.loadAndValidateCheckpoint(jobId, tasks, storageLocation, ucl, false);
			fail("Did not throw expected Exception");
		} catch (IllegalStateException expected) {
			assertTrue(expected.getMessage().contains("allowNonRestoredState"));
		}

		// 4) Load and validate: ignore missing vertex
		Checkpoints.loadAndValidateCheckpoint(jobId, tasks, storageLocation, ucl, true);
	}
}
