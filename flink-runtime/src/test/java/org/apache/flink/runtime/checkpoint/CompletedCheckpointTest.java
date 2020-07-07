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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link CompletedCheckpoint}.
 */
public class CompletedCheckpointTest {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Test
	public void testCompareCheckpointsWithDifferentOrder() {

		CompletedCheckpoint checkpoint1 = new CompletedCheckpoint(
			new JobID(), 0, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		CompletedCheckpoint checkpoint2 = new CompletedCheckpoint(
			new JobID(), 1, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		List<CompletedCheckpoint> checkpoints1= new ArrayList<>();
		checkpoints1.add(checkpoint1);
		checkpoints1.add(checkpoint2);
		checkpoints1.add(checkpoint1);

		List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
		checkpoints2.add(checkpoint2);
		checkpoints2.add(checkpoint1);
		checkpoints2.add(checkpoint2);

		assertFalse(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2));
	}

	@Test
	public void testCompareCheckpointsWithSameOrder() {

		CompletedCheckpoint checkpoint1 = new CompletedCheckpoint(
			new JobID(), 0, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		CompletedCheckpoint checkpoint2 = new CompletedCheckpoint(
			new JobID(), 1, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		List<CompletedCheckpoint> checkpoints1= new ArrayList<>();
		checkpoints1.add(checkpoint1);
		checkpoints1.add(checkpoint2);
		checkpoints1.add(checkpoint1);

		List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
		checkpoints2.add(checkpoint1);
		checkpoints2.add(checkpoint2);
		checkpoints2.add(checkpoint1);

		assertTrue(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2));
	}

	/**
	 * Verify that both JobID and checkpoint id are taken into account when comparing.
	 */
	@Test
	public void testCompareCheckpointsWithSameJobID() {
		JobID jobID = new JobID();

		CompletedCheckpoint checkpoint1 = new CompletedCheckpoint(
			jobID, 0, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		CompletedCheckpoint checkpoint2 = new CompletedCheckpoint(
			jobID, 1, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		List<CompletedCheckpoint> checkpoints1= new ArrayList<>();
		checkpoints1.add(checkpoint1);

		List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
		checkpoints2.add(checkpoint2);

		assertFalse(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2));
	}

	/**
	 * Verify that both JobID and checkpoint id are taken into account when comparing.
	 */
	@Test
	public void testCompareCheckpointsWithSameCheckpointId() {
		JobID jobID1 = new JobID();
		JobID jobID2 = new JobID();

		CompletedCheckpoint checkpoint1 = new CompletedCheckpoint(
			jobID1, 0, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		CompletedCheckpoint checkpoint2 = new CompletedCheckpoint(
			jobID2, 0, 0, 1,
			new HashMap<>(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			new TestCompletedCheckpointStorageLocation());

		List<CompletedCheckpoint> checkpoints1= new ArrayList<>();
		checkpoints1.add(checkpoint1);

		List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
		checkpoints2.add(checkpoint2);

		assertFalse(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2));
	}

	@Test
	public void testRegisterStatesAtRegistry() {
		OperatorState state = mock(OperatorState.class);
		Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
		operatorStates.put(new OperatorID(), state);

		CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				new JobID(), 0, 0, 1,
				operatorStates,
				Collections.emptyList(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
				new TestCompletedCheckpointStorageLocation());

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		checkpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
		verify(state, times(1)).registerSharedStates(sharedStateRegistry);
	}

	/**
	 * Tests that the garbage collection properties are respected when subsuming checkpoints.
	 */
	@Test
	public void testCleanUpOnSubsume() throws Exception {
		OperatorState state = mock(OperatorState.class);
		Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
		operatorStates.put(new OperatorID(), state);

		EmptyStreamStateHandle metadata = new EmptyStreamStateHandle();
		TestCompletedCheckpointStorageLocation location =
				new TestCompletedCheckpointStorageLocation(metadata, "ptr");

		CheckpointProperties props = new CheckpointProperties(false, CheckpointType.CHECKPOINT, true, false, false, false, false);

		CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				new JobID(), 0, 0, 1,
				operatorStates,
				Collections.emptyList(),
				props,
				location);

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		checkpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
		verify(state, times(1)).registerSharedStates(sharedStateRegistry);

		// Subsume
		checkpoint.discardOnSubsume();

		verify(state, times(1)).discardState();
		assertTrue(location.isDisposed());
		assertTrue(metadata.isDisposed());
	}

	/**
	 * Tests that the garbage collection properties are respected when shutting down.
	 */
	@Test
	public void testCleanUpOnShutdown() throws Exception {
		JobStatus[] terminalStates = new JobStatus[] {
				JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.FAILED, JobStatus.SUSPENDED
		};

		for (JobStatus status : terminalStates) {

			OperatorState state = mock(OperatorState.class);
			Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
			operatorStates.put(new OperatorID(), state);

			EmptyStreamStateHandle retainedHandle = new EmptyStreamStateHandle();
			TestCompletedCheckpointStorageLocation retainedLocation =
					new TestCompletedCheckpointStorageLocation(retainedHandle, "ptr");

			// Keep
			CheckpointProperties retainProps = new CheckpointProperties(false, CheckpointType.CHECKPOINT, false, false, false, false, false);
			CompletedCheckpoint checkpoint = new CompletedCheckpoint(
					new JobID(), 0, 0, 1,
					new HashMap<>(operatorStates),
					Collections.emptyList(),
					retainProps,
					retainedLocation);

			checkpoint.discardOnShutdown(status);

			verify(state, times(0)).discardState();
			assertFalse(retainedLocation.isDisposed());
			assertFalse(retainedHandle.isDisposed());

			// Discard
			EmptyStreamStateHandle discardHandle = new EmptyStreamStateHandle();
			TestCompletedCheckpointStorageLocation discardLocation =
					new TestCompletedCheckpointStorageLocation(discardHandle, "ptr");

			// Keep
			CheckpointProperties discardProps = new CheckpointProperties(false, CheckpointType.CHECKPOINT, true, true, true, true, true);

			checkpoint = new CompletedCheckpoint(
					new JobID(), 0, 0, 1,
					new HashMap<>(operatorStates),
					Collections.emptyList(),
					discardProps,
					discardLocation);

			checkpoint.discardOnShutdown(status);

			verify(state, times(1)).discardState();
			assertTrue(discardLocation.isDisposed());
			assertTrue(discardHandle.isDisposed());
		}
	}

	/**
	 * Tests that the stats callbacks happen if the callback is registered.
	 */
	@Test
	public void testCompletedCheckpointStatsCallbacks() throws Exception {
		CompletedCheckpoint completed = new CompletedCheckpoint(
			new JobID(),
			0,
			0,
			1,
			Collections.emptyMap(),
			Collections.emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new TestCompletedCheckpointStorageLocation());

		CompletedCheckpointStats.DiscardCallback callback = mock(CompletedCheckpointStats.DiscardCallback.class);
		completed.setDiscardCallback(callback);

		completed.discardOnShutdown(JobStatus.FINISHED);
		verify(callback, times(1)).notifyDiscardedCheckpoint();
	}

	@Test
	public void testIsJavaSerializable() throws Exception {
		TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
		TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

		HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
		taskStats.put(task1.getJobVertexId(), task1);
		taskStats.put(task2.getJobVertexId(), task2);

		CompletedCheckpointStats completed = new CompletedCheckpointStats(
			123123123L,
			10123L,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			1337,
			taskStats,
			1337,
			123129837912L,
			new SubtaskStateStats(123, 213123, 123123, 0, 0, 0, 0),
			null);

		CompletedCheckpointStats copy = CommonTestUtils.createCopySerializable(completed);

		assertEquals(completed.getCheckpointId(), copy.getCheckpointId());
		assertEquals(completed.getTriggerTimestamp(), copy.getTriggerTimestamp());
		assertEquals(completed.getProperties(), copy.getProperties());
		assertEquals(completed.getNumberOfSubtasks(), copy.getNumberOfSubtasks());
		assertEquals(completed.getNumberOfAcknowledgedSubtasks(), copy.getNumberOfAcknowledgedSubtasks());
		assertEquals(completed.getEndToEndDuration(), copy.getEndToEndDuration());
		assertEquals(completed.getStateSize(), copy.getStateSize());
		assertEquals(completed.getLatestAcknowledgedSubtaskStats().getSubtaskIndex(), copy.getLatestAcknowledgedSubtaskStats().getSubtaskIndex());
		assertEquals(completed.getStatus(), copy.getStatus());
	}
}
