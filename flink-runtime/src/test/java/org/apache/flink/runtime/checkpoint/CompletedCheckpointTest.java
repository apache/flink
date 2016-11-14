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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CompletedCheckpointTest {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	/**
	 * Tests that persistent checkpoints discard their header file.
	 */
	@Test
	public void testDiscard() throws Exception {
		File file = tmpFolder.newFile();
		assertEquals(true, file.exists());

		TaskState state = mock(TaskState.class);
		Map<JobVertexID, TaskState> taskStates = new HashMap<>();
		taskStates.put(new JobVertexID(), state);

		// Verify discard call is forwarded to state
		CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				new JobID(), 0, 0, 1, taskStates, CheckpointProperties.forStandardCheckpoint(), file.getAbsolutePath());

		checkpoint.discard(JobStatus.FAILED);

		assertEquals(false, file.exists());
	}

	/**
	 * Tests that the garbage collection properties are respected when subsuming checkpoints.
	 */
	@Test
	public void testCleanUpOnSubsume() throws Exception {
		TaskState state = mock(TaskState.class);
		Map<JobVertexID, TaskState> taskStates = new HashMap<>();
		taskStates.put(new JobVertexID(), state);

		boolean discardSubsumed = true;
		CheckpointProperties props = new CheckpointProperties(false, false, discardSubsumed, true, true, true, true);
		CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				new JobID(), 0, 0, 1, taskStates, props, null);

		// Subsume
		checkpoint.subsume();

		verify(state, times(1)).discardState();
	}

	/**
	 * Tests that the garbage collection properties are respected when shutting down.
	 */
	@Test
	public void testCleanUpOnShutdown() throws Exception {
		File file = tmpFolder.newFile();
		String externalPath = file.getAbsolutePath();

		JobStatus[] terminalStates = new JobStatus[] {
				JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.FAILED, JobStatus.SUSPENDED
		};

		TaskState state = mock(TaskState.class);
		Map<JobVertexID, TaskState> taskStates = new HashMap<>();
		taskStates.put(new JobVertexID(), state);

		for (JobStatus status : terminalStates) {
			Mockito.reset(state);

			// Keep
			CheckpointProperties props = new CheckpointProperties(false, true, false, false, false, false, false);
			CompletedCheckpoint checkpoint = new CompletedCheckpoint(
					new JobID(), 0, 0, 1, new HashMap<>(taskStates), props, externalPath);

			checkpoint.discard(status);
			verify(state, times(0)).discardState();
			assertEquals(true, file.exists());

			// Discard
			props = new CheckpointProperties(false, false, true, true, true, true, true);
			checkpoint = new CompletedCheckpoint(
					new JobID(), 0, 0, 1, new HashMap<>(taskStates), props, null);

			checkpoint.discard(status);
			verify(state, times(1)).discardState();
		}
	}
}
