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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.junit.Ignore;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the savepoint coordinator.
 */
@Ignore
public class SavepointCoordinatorTest extends TestLogger {

	// ------------------------------------------------------------------------
	// Trigger and acknowledge
	// ------------------------------------------------------------------------

	/**
	 * Simple trigger-acknowledge test for a single savepoint.
	 */
	@Test
	public void testSimpleTriggerSavepoint() throws Exception {
		JobID jobId = new JobID();
		long checkpointTimeout = 60 * 1000;
		long timestamp = 1272635;
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();
		HeapSavepointStore savepointStore = new HeapSavepointStore();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				checkpointTimeout,
				vertices,
				vertices,
				vertices,
				checkpointIdCounter,
				savepointStore);

		// Trigger the savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(timestamp);
		assertFalse(savepointPathFuture.isCompleted());

		long checkpointId = checkpointIdCounter.getLastReturnedCount();
		assertEquals(0, checkpointId);

		// Verify send trigger messages
		for (ExecutionVertex vertex : vertices) {
			verifyTriggerCheckpoint(vertex, checkpointId, timestamp);
		}

		PendingCheckpoint pendingCheckpoint = coordinator.getPendingCheckpoints()
				.get(checkpointId);

		verifyPendingCheckpoint(pendingCheckpoint, jobId, checkpointId,
				timestamp, 0, 2, 0, false, false);

		// Acknowledge tasks
		for (ExecutionVertex vertex : vertices) {
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId,
				vertex.getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				createSerializedStateHandle(vertex),
				createSerializedStateHandleMap(vertex)));
		}

		// The pending checkpoint is completed
		assertTrue(pendingCheckpoint.isDiscarded());
		assertEquals(0, coordinator.getSuccessfulCheckpoints().size());

		// Verify send notify complete messages
		for (ExecutionVertex vertex : vertices) {
			verifyNotifyCheckpointComplete(vertex, checkpointId, timestamp);
		}

		// Verify that the future has been completed
		assertTrue(savepointPathFuture.isCompleted());
		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());

		// Verify the savepoint
		Savepoint savepoint = savepointStore.loadSavepoint(savepointPath);
		verifySavepoint(savepoint, checkpointId, vertices);

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	/**
	 * This test triggers a checkpoint and then sends a decline checkpoint message from
	 * one of the tasks. The expected behaviour is that said checkpoint is discarded and a new
	 * checkpoint is triggered.
	 */
	@Test
	public void testTriggerAndDeclineCheckpointSimple() throws Exception {
		JobID jobId = new JobID();
		long checkpointTimeout = 60 * 1000;
		long timestamp = 1272635;
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();
		SavepointStore savepointStore = new HeapSavepointStore();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				checkpointTimeout,
				vertices,
				vertices,
				vertices,
				checkpointIdCounter,
				savepointStore);

		// Trigger the savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(timestamp);
		assertFalse(savepointPathFuture.isCompleted());

		long checkpointId = checkpointIdCounter.getLastReturnedCount();
		assertEquals(0, checkpointId);

		// Verify send trigger messages
		for (ExecutionVertex vertex : vertices) {
			verifyTriggerCheckpoint(vertex, checkpointId, timestamp);
		}

		PendingCheckpoint pendingCheckpoint = coordinator.getPendingCheckpoints()
				.get(checkpointId);

		verifyPendingCheckpoint(pendingCheckpoint, jobId, checkpointId,
				timestamp, 0, 2, 0, false, false);

		// Acknowledge and decline tasks
		coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId, vertices[0].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId, createSerializedStateHandle(vertices[0]), createSerializedStateHandleMap(vertices[0])));

		coordinator.receiveDeclineMessage(new DeclineCheckpoint(
				jobId, vertices[1].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId, 0));


		// The pending checkpoint is completed
		assertTrue(pendingCheckpoint.isDiscarded());
		assertEquals(0, coordinator.getSuccessfulCheckpoints().size());

		// Verify that the future has been completed
		assertTrue(savepointPathFuture.isCompleted());

		try {
			Await.result(savepointPathFuture.failed(), FiniteDuration.Zero());
			fail("Did not throw expected exception");
		} catch (Throwable ignored) {}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	// ------------------------------------------------------------------------
	// Rollback
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("unchecked")
	public void testSimpleRollbackSavepoint() throws Exception {
		JobID jobId = new JobID();

		ExecutionJobVertex[] jobVertices = new ExecutionJobVertex[] {
				mockExecutionJobVertex(jobId, new JobVertexID(), 4),
				mockExecutionJobVertex(jobId, new JobVertexID(), 4) };

		ExecutionVertex[] triggerVertices = jobVertices[0].getTaskVertices();
		ExecutionVertex[] ackVertices = new ExecutionVertex[8];

		int i = 0;
		for (ExecutionJobVertex jobVertex : jobVertices) {
			for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
				ackVertices[i++] = vertex;
			}
		}

		MockCheckpointIdCounter idCounter = new MockCheckpointIdCounter();
		HeapSavepointStore savepointStore = new HeapSavepointStore();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				idCounter,
				savepointStore);

		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1231273123);

		// Acknowledge all tasks
		for (ExecutionVertex vertex : ackVertices) {
			ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId,
				attemptId,
				0,
				createSerializedStateHandle(vertex),
				createSerializedStateHandleMap(vertex)));
		}

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());
		assertNotNull(savepointPath);

		// Rollback
		coordinator.restoreSavepoint(createExecutionJobVertexMap(jobVertices), savepointPath);

		// Verify all executions have been reset
		for (ExecutionVertex vertex : ackVertices) {
			verify(vertex.getCurrentExecutionAttempt(), times(1)).setInitialState(
					any(ChainedStateHandle.class), any(List.class));
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		// Verify checkpoint ID counter started
		assertTrue(idCounter.isStarted());

		coordinator.shutdown();
	}

	@Test
	public void testRollbackParallelismMismatch() throws Exception {
		JobID jobId = new JobID();

		ExecutionJobVertex[] jobVertices = new ExecutionJobVertex[] {
				mockExecutionJobVertex(jobId, new JobVertexID(), 4),
				mockExecutionJobVertex(jobId, new JobVertexID(), 4) };

		ExecutionVertex[] triggerVertices = jobVertices[0].getTaskVertices();
		ExecutionVertex[] ackVertices = new ExecutionVertex[8];

		int index = 0;
		for (ExecutionJobVertex jobVertex : jobVertices) {
			for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
				ackVertices[index++] = vertex;
			}
		}

		HeapSavepointStore savepointStore = new HeapSavepointStore();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				savepointStore);

		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1231273123);

		// Acknowledge all tasks
		for (ExecutionVertex vertex : ackVertices) {
			ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId,
				attemptId,
				0,
				createSerializedStateHandle(vertex),
				createSerializedStateHandleMap(vertex)));
		}

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());
		assertNotNull(savepointPath);

		// Change parallelism lower than original (state without matching subtask). The
		// other way around (subtask without matching state) is OK.
		for (int i = 0; i < jobVertices.length; i++) {
			jobVertices[i] = mockExecutionJobVertex(jobId, jobVertices[i].getJobVertexId(), 2);
		}

		try {
			// Rollback
			coordinator.restoreSavepoint(
					createExecutionJobVertexMap(jobVertices),
					savepointPath);
			fail("Did not throw expected Exception after rollback with parallelism mismatch.");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testRollbackStateStoreFailure() throws Exception {
		JobID jobId = new JobID();
		ExecutionJobVertex jobVertex = mockExecutionJobVertex(jobId, new JobVertexID(), 4);
		HeapSavepointStore savepointStore = spy(new HeapSavepointStore());

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				jobVertex.getTaskVertices(),
				jobVertex.getTaskVertices(),
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				savepointStore);

		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1231273123);

		// Acknowledge all tasks
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId,
				attemptId,
				0,
				createSerializedStateHandle(vertex),
				createSerializedStateHandleMap(vertex)));
		}

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());
		assertNotNull(savepointPath);

		// Failure on get
		doThrow(new Exception("TestException")).when(savepointStore).loadSavepoint(anyString());

		try {
			// Rollback
			coordinator.restoreSavepoint(
					createExecutionJobVertexMap(jobVertex),
					savepointPath);

			fail("Did not throw expected Exception after rollback with savepoint store failure.");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testRollbackSetsCheckpointID() throws Exception {
		SavepointV1 savepoint = new SavepointV1(12312312L, Collections.<TaskState>emptyList());

		CheckpointIDCounter checkpointIdCounter = mock(CheckpointIDCounter.class);

		@SuppressWarnings("unchecked")
		SavepointStore savepointStore = mock(SavepointStore.class);
		when(savepointStore.loadSavepoint(anyString())).thenReturn(savepoint);

		SavepointCoordinator coordinator = createSavepointCoordinator(
				new JobID(),
				60 * 1000,
				new ExecutionVertex[] {},
				new ExecutionVertex[] {},
				new ExecutionVertex[] {},
				checkpointIdCounter,
				savepointStore);

		coordinator.restoreSavepoint(createExecutionJobVertexMap(), "any");

		verify(checkpointIdCounter).setCount(eq(12312312L + 1));

		coordinator.shutdown();
	}

	// ------------------------------------------------------------------------
	// Savepoint aborts and future notifications
	// ------------------------------------------------------------------------

	@Test
	public void testAbortSavepointIfTriggerTasksNotExecuted() throws Exception {
		JobID jobId = new JobID();
		ExecutionVertex[] triggerVertices = new ExecutionVertex[] {
				mock(ExecutionVertex.class),
				mock(ExecutionVertex.class) };
		ExecutionVertex[] ackVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				new HeapSavepointStore());

		// Trigger savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1238123);

		// Abort the savepoint, because the vertices are not running
		assertTrue(savepointPathFuture.isCompleted());

		try {
			Await.result(savepointPathFuture, FiniteDuration.Zero());
			fail("Did not throw expected Exception after shutdown");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testAbortSavepointIfTriggerTasksAreFinished() throws Exception {
		JobID jobId = new JobID();
		ExecutionVertex[] triggerVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId, ExecutionState.FINISHED) };
		ExecutionVertex[] ackVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				new HeapSavepointStore());

		// Trigger savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1238123);

		// Abort the savepoint, because the vertices are not running
		assertTrue(savepointPathFuture.isCompleted());

		try {
			Await.result(savepointPathFuture, FiniteDuration.Zero());
			fail("Did not throw expected Exception after shutdown");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testAbortSavepointIfAckTasksAreNotExecuted() throws Exception {
		JobID jobId = new JobID();
		ExecutionVertex[] triggerVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		ExecutionVertex[] ackVertices = new ExecutionVertex[] {
				mock(ExecutionVertex.class),
				mock(ExecutionVertex.class) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				new HeapSavepointStore());

		// Trigger savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1238123);

		// Abort the savepoint, because the vertices are not running
		assertTrue(savepointPathFuture.isCompleted());

		try {
			Await.result(savepointPathFuture, FiniteDuration.Zero());
			fail("Did not throw expected Exception after shutdown");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testAbortOnCheckpointTimeout() throws Exception {
		JobID jobId = new JobID();
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		ExecutionVertex commitVertex = mockExecutionVertex(jobId);
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		long checkpointTimeout = 1000;
		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				checkpointTimeout,
				vertices,
				vertices,
				new ExecutionVertex[] { commitVertex },
				checkpointIdCounter,
				new HeapSavepointStore());

		// Trigger the savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(12731273);
		assertFalse(savepointPathFuture.isCompleted());

		long checkpointId = checkpointIdCounter.getLastReturnedCount();
		PendingCheckpoint pendingCheckpoint = coordinator.getPendingCheckpoints()
				.get(checkpointId);

		assertNotNull("Checkpoint not pending (test race)", pendingCheckpoint);
		assertFalse("Checkpoint already discarded (test race)", pendingCheckpoint.isDiscarded());

		// Wait for savepoint to timeout
		Deadline deadline = FiniteDuration.apply(60, "s").fromNow();
		while (deadline.hasTimeLeft()
				&& !pendingCheckpoint.isDiscarded()
				&& coordinator.getNumberOfPendingCheckpoints() > 0) {

			Thread.sleep(250);
		}

		// Verify discarded
		assertTrue("Savepoint not discarded within timeout", pendingCheckpoint.isDiscarded());
		assertEquals(0, coordinator.getNumberOfPendingCheckpoints());
		assertEquals(0, coordinator.getNumberOfRetainedSuccessfulCheckpoints());

		// No commit for timeout
		verify(commitVertex, times(0)).sendMessageToCurrentExecution(
				any(NotifyCheckpointComplete.class), any(ExecutionAttemptID.class));

		assertTrue(savepointPathFuture.isCompleted());

		try {
			Await.result(savepointPathFuture, FiniteDuration.Zero());
			fail("Did not throw expected Exception after timeout");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testAbortSavepointsOnShutdown() throws Exception {
		JobID jobId = new JobID();
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				vertices,
				vertices,
				vertices,
				new MockCheckpointIdCounter(),
				new HeapSavepointStore());

		// Trigger savepoints
		List<Future<String>> savepointPathFutures = new ArrayList<>();
		savepointPathFutures.add(coordinator.triggerSavepoint(12731273));
		savepointPathFutures.add(coordinator.triggerSavepoint(12731273 + 123));

		for (Future<String> future : savepointPathFutures) {
			assertFalse(future.isCompleted());
		}

		coordinator.shutdown();

		// Verify futures failed
		for (Future<String> future : savepointPathFutures) {
			assertTrue(future.isCompleted());

			try {
				Await.result(future, FiniteDuration.Zero());
				fail("Did not throw expected Exception after shutdown");
			}
			catch (Exception ignored) {
			}
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());
	}

	@Test
	public void testAbortSavepointOnStateStoreFailure() throws Exception {
		JobID jobId = new JobID();
		ExecutionJobVertex jobVertex = mockExecutionJobVertex(jobId, new JobVertexID(), 4);
		HeapSavepointStore savepointStore = spy(new HeapSavepointStore());

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				60 * 1000,
				jobVertex.getTaskVertices(),
				jobVertex.getTaskVertices(),
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				savepointStore);

		// Failure on putState
		doThrow(new RuntimeException("TestException"))
				.when(savepointStore).storeSavepoint(any(Savepoint.class));

		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1231273123);

		// Acknowledge all tasks
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId,
				attemptId,
				0,
				createSerializedStateHandle(vertex),
				createSerializedStateHandleMap(vertex)));
		}

		try {
			Await.result(savepointPathFuture, FiniteDuration.Zero());
			fail("Did not throw expected Exception after rollback with savepoint store failure.");
		}
		catch (Exception ignored) {
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testAbortSavepointIfSubsumed() throws Exception {
		JobID jobId = new JobID();
		long checkpointTimeout = 60 * 1000;
		long[] timestamps = new long[] { 1272635, 1272635 + 10 };
		long[] checkpointIds = new long[2];
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();
		HeapSavepointStore savepointStore = new HeapSavepointStore();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				jobId,
				checkpointTimeout,
				vertices,
				vertices,
				vertices,
				checkpointIdCounter,
				savepointStore);

		// Trigger the savepoints
		List<Future<String>> savepointPathFutures = new ArrayList<>();

		savepointPathFutures.add(coordinator.triggerSavepoint(timestamps[0]));
		checkpointIds[0] = checkpointIdCounter.getLastReturnedCount();

		savepointPathFutures.add(coordinator.triggerSavepoint(timestamps[1]));
		checkpointIds[1] = checkpointIdCounter.getLastReturnedCount();

		for (Future<String> future : savepointPathFutures) {
			assertFalse(future.isCompleted());
		}

		// Verify send trigger messages
		for (ExecutionVertex vertex : vertices) {
			verifyTriggerCheckpoint(vertex, checkpointIds[0], timestamps[0]);
			verifyTriggerCheckpoint(vertex, checkpointIds[1], timestamps[1]);
		}

		PendingCheckpoint[] pendingCheckpoints = new PendingCheckpoint[] {
				coordinator.getPendingCheckpoints().get(checkpointIds[0]),
				coordinator.getPendingCheckpoints().get(checkpointIds[1]) };

		verifyPendingCheckpoint(pendingCheckpoints[0], jobId, checkpointIds[0],
				timestamps[0], 0, 2, 0, false, false);

		verifyPendingCheckpoint(pendingCheckpoints[1], jobId, checkpointIds[1],
				timestamps[1], 0, 2, 0, false, false);

		// Acknowledge second checkpoint...
		for (ExecutionVertex vertex : vertices) {
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId,
				vertex.getCurrentExecutionAttempt().getAttemptId(),
				checkpointIds[1],
				createSerializedStateHandle(vertex),
				createSerializedStateHandleMap(vertex)));
		}

		// ...and one task of first checkpoint
		coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
			jobId,
			vertices[0].getCurrentExecutionAttempt().getAttemptId(),
			checkpointIds[0],
			createSerializedStateHandle(vertices[0]),
			createSerializedStateHandleMap(vertices[0])));

		// The second pending checkpoint is completed and subsumes the first one
		assertTrue(pendingCheckpoints[0].isDiscarded());
		assertTrue(pendingCheckpoints[1].isDiscarded());
		assertEquals(0, coordinator.getSuccessfulCheckpoints().size());

		// Verify send notify complete messages for second checkpoint
		for (ExecutionVertex vertex : vertices) {
			verifyNotifyCheckpointComplete(vertex, checkpointIds[1], timestamps[1]);
		}

		Savepoint[] savepoints = new Savepoint[2];
		String[] savepointPaths = new String[2];

		// Verify that the futures have both been completed
		assertTrue(savepointPathFutures.get(0).isCompleted());

		try {
			savepointPaths[0] = Await.result(savepointPathFutures.get(0), FiniteDuration.Zero());
			fail("Did not throw expected exception");
		}
		catch (Exception ignored) {
		}

		// Verify the second savepoint
		assertTrue(savepointPathFutures.get(1).isCompleted());
		savepointPaths[1] = Await.result(savepointPathFutures.get(1), FiniteDuration.Zero());
		savepoints[1] = savepointStore.loadSavepoint(savepointPaths[1]);
		verifySavepoint(savepoints[1], checkpointIds[1], vertices);

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testShutdownDoesNotCleanUpCompletedCheckpointsWithFileSystemStore() throws Exception {
		JobID jobId = new JobID();
		long checkpointTimeout = 60 * 1000;
		long timestamp = 1272635;
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		// Temporary directory for file state backend
		final File tmpDir = CommonTestUtils.createTempDirectory();

		try {
			FsSavepointStore savepointStore = new FsSavepointStore(tmpDir.toURI().toString(), "sp-");

			SavepointCoordinator coordinator = createSavepointCoordinator(
					jobId,
					checkpointTimeout,
					vertices,
					vertices,
					vertices,
					checkpointIdCounter,
					savepointStore);

			// Trigger the savepoint
			Future<String> savepointPathFuture = coordinator.triggerSavepoint(timestamp);
			assertFalse(savepointPathFuture.isCompleted());

			long checkpointId = checkpointIdCounter.getLastReturnedCount();
			assertEquals(0, checkpointId);

			// Verify send trigger messages
			for (ExecutionVertex vertex : vertices) {
				verifyTriggerCheckpoint(vertex, checkpointId, timestamp);
			}

			PendingCheckpoint pendingCheckpoint = coordinator.getPendingCheckpoints()
					.get(checkpointId);

			verifyPendingCheckpoint(pendingCheckpoint, jobId, checkpointId,
					timestamp, 0, 2, 0, false, false);

			// Acknowledge tasks
			for (ExecutionVertex vertex : vertices) {
				coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
					jobId,
					vertex.getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					createSerializedStateHandle(vertex),
					createSerializedStateHandleMap(vertex)));
			}

			// The pending checkpoint is completed
			assertTrue(pendingCheckpoint.isDiscarded());
			assertEquals(0, coordinator.getSuccessfulCheckpoints().size());

			// Verify send notify complete messages
			for (ExecutionVertex vertex : vertices) {
				verifyNotifyCheckpointComplete(vertex, checkpointId, timestamp);
			}

			// Verify that the future has been completed
			assertTrue(savepointPathFuture.isCompleted());
			String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());

			// Verify all promises removed
			assertEquals(0, getSavepointPromises(coordinator).size());

			coordinator.shutdown();

			// Verify the savepoint is still available
			Savepoint savepoint = savepointStore.loadSavepoint(savepointPath);
			verifySavepoint(savepoint, checkpointId, vertices);
		}
		finally {
			FileUtils.deleteDirectory(tmpDir);
		}
	}

	/**
	 * Tests that the checkpointed partitioned and non-partitioned state is assigned properly to
	 * the {@link Execution} upon recovery.
	 *
	 * @throws Exception
	 */
	@Test
	public void testRestoreLatestCheckpointedState() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 2;
		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		final ExecutionJobVertex jobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		// set up the coordinator and validate the initial state
		SavepointCoordinator coord = createSavepointCoordinator(
			jid,
			600000,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			checkpointIdCounter,
			new HeapSavepointStore());

		// trigger the checkpoint
		Future<String> savepointPathFuture = coord.triggerSavepoint(timestamp);

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		final long checkpointId = checkpointIdCounter.getLastReturnedCount();

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID2, index);
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		// completed checkpoints are not stored in a CompletedCheckpointStore
		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();
		assertEquals(0, completedCheckpoints.size());

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		tasks.put(jobVertexID1, jobVertex1);
		tasks.put(jobVertexID2, jobVertex2);

		coord.restoreSavepoint(tasks, savepointPath);

		// verify the restored state
		CheckpointCoordinatorTest.verifiyStateRestore(jobVertexID1, jobVertex1, keyGroupPartitions1);
		CheckpointCoordinatorTest.verifiyStateRestore(jobVertexID2, jobVertex2, keyGroupPartitions2);
	}

	/**
	 * Tests that the checkpoint restoration fails if the max parallelism of the job vertices has
	 * changed.
	 *
	 * @throws Exception
	 */
	@Test(expected=IllegalStateException.class)
	public void testRestoreLatestCheckpointFailureWhenMaxParallelismChanges() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 2;
		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		final ExecutionJobVertex jobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		// set up the coordinator and validate the initial state
		SavepointCoordinator coord = createSavepointCoordinator(
			jid,
			600000,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			checkpointIdCounter,
			new HeapSavepointStore());

		// trigger the checkpoint
		Future<String> savepointPathFuture = coord.triggerSavepoint(timestamp);

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		final long checkpointId = checkpointIdCounter.getLastReturnedCount();

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID2, index);
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		// we don't store the completed checkpoints in the CompletedCheckpointStore
		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();
		assertEquals(0, completedCheckpoints.size());

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newMaxParallelism1 = 20;
		int newMaxParallelism2 = 42;

		final ExecutionJobVertex newJobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			newMaxParallelism1);

		final ExecutionJobVertex newJobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			newMaxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);

		coord.restoreSavepoint(tasks, savepointPath);

		fail("The restoration should have failed because the max parallelism changed.");
	}

	/**
	 * Tests that the checkpoint restoration fails if the parallelism of a job vertices with
	 * non-partitioned state has changed.
	 *
	 * @throws Exception
	 */
	@Test(expected=IllegalStateException.class)
	public void testRestoreLatestCheckpointFailureWhenParallelismChanges() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();


		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 2;
		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		final ExecutionJobVertex jobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		// set up the coordinator and validate the initial state
		SavepointCoordinator coord = createSavepointCoordinator(
			jid,
			600000,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			checkpointIdCounter,
			new HeapSavepointStore());

		// trigger the checkpoint
		Future<String> savepointPathFuture = coord.triggerSavepoint(timestamp);

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		final long checkpointId = checkpointIdCounter.getLastReturnedCount();

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle>  keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID2, index);
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		// we don't store completed checkpoints in the SavepointCoordinator
		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();
		assertEquals(0, completedCheckpoints.size());

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newParallelism1 = 4;
		int newParallelism2 = 3;

		final ExecutionJobVertex newJobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			newParallelism1,
			maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			newParallelism2,
			maxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);

		coord.restoreSavepoint(tasks, savepointPath);

		fail("The restoration should have failed because the parallelism of an vertex with " +
			"non-partitioned state changed.");
	}

	/**
	 * Tests the checkpoint restoration with changing parallelism of job vertex with partitioned
	 * state.
	 *
	 * @throws Exception
	 */
	@Test
	public void testRestoreLatestCheckpointedStateWithChangingParallelism() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 2;
		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		final ExecutionJobVertex jobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		// set up the coordinator and validate the initial state
		SavepointCoordinator coord = createSavepointCoordinator(
			jid,
			600000,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			checkpointIdCounter,
			new HeapSavepointStore());

		// trigger the checkpoint
		Future<String> savepointPathFuture = coord.triggerSavepoint(timestamp);

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		final long checkpointId = checkpointIdCounter.getLastReturnedCount();

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = CheckpointCoordinatorTest.generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			List<KeyGroupsStateHandle> keyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				null,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		// we don't store completed checkpoints in the SavepointCoordinator
		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();
		assertEquals(0, completedCheckpoints.size());

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newParallelism2 = 13;

		List<KeyGroupRange> newKeyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, newParallelism2);

		final ExecutionJobVertex newJobVertex1 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = CheckpointCoordinatorTest.mockExecutionJobVertex(
			jobVertexID2,
			newParallelism2,
			maxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);
		coord.restoreSavepoint(tasks, savepointPath);

		// verify the restored state
		// verify the restored state
		CheckpointCoordinatorTest.verifiyStateRestore(jobVertexID1, newJobVertex1, keyGroupPartitions1);

		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {
			List<KeyGroupsStateHandle> originalKeyGroupState = CheckpointCoordinatorTest.generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i));

			ChainedStateHandle<StreamStateHandle> operatorState = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getChainedStateHandle();
			List<KeyGroupsStateHandle> keyGroupState = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getKeyGroupsStateHandles();

			assertNull(operatorState);
			CheckpointCoordinatorTest.comparePartitionedState(originalKeyGroupState, keyGroupState);
		}
	}

	// ------------------------------------------------------------------------
	// Test helpers
	// ------------------------------------------------------------------------

	private static SavepointCoordinator createSavepointCoordinator(
			JobID jobId,
			long checkpointTimeout,
			ExecutionVertex[] triggerVertices,
			ExecutionVertex[] ackVertices,
			ExecutionVertex[] commitVertices,
			CheckpointIDCounter checkpointIdCounter,
			SavepointStore savepointStore) throws Exception {

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		return new SavepointCoordinator(
				jobId,
				checkpointTimeout,
				checkpointTimeout,
				triggerVertices,
				ackVertices,
				commitVertices,
				classLoader,
				checkpointIdCounter,
				savepointStore,
				new DisabledCheckpointStatsTracker());
	}

	private static Map<JobVertexID, ExecutionJobVertex> createExecutionJobVertexMap(
			ExecutionJobVertex... jobVertices) {

		Map<JobVertexID, ExecutionJobVertex> jobVertexMap = new HashMap<>();

		for (ExecutionJobVertex jobVertex : jobVertices) {
			jobVertexMap.put(jobVertex.getJobVertexId(), jobVertex);
		}

		return jobVertexMap;
	}

	static ChainedStateHandle<StreamStateHandle> createSerializedStateHandle(ExecutionVertex vertex) throws IOException {
		List<StreamStateHandle> listOfFileHandlesles = new ArrayList<>();
		ChainedStateHandle<StreamStateHandle> result = new ChainedStateHandle<>(listOfFileHandlesles);
		ExecutionAttemptID value = vertex.getCurrentExecutionAttempt().getAttemptId();
		StreamStateHandle fileStateHandle = new ByteStreamStateHandle(InstantiationUtil.serializeObject(value));
		listOfFileHandlesles.add(fileStateHandle);
		return result;
	}

	static List<KeyGroupsStateHandle> createSerializedStateHandleMap(
			ExecutionVertex vertex) throws IOException {
		List<byte[]> serializedGroupValues = new ArrayList<>();
		KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, new long[]{0L});
		// generate state for one keygroup
		ExecutionAttemptID value = vertex.getCurrentExecutionAttempt().getAttemptId();
		byte[] serializedValue = InstantiationUtil.serializeObject(value);
		serializedGroupValues.add(serializedValue);

		ByteStreamStateHandle allSerializedStatesHandle = new ByteStreamStateHandle(serializedValue);
		KeyGroupsStateHandle keyGroupsStateHandle = new KeyGroupsStateHandle(
				keyGroupRangeOffsets,
				allSerializedStatesHandle);
		List<KeyGroupsStateHandle> keyGroupsStateHandleList = new ArrayList<>();
		keyGroupsStateHandleList.add(keyGroupsStateHandle);
		return keyGroupsStateHandleList;
	}

	@SuppressWarnings("unchecked")
	private Map<Long, Promise<String>> getSavepointPromises(
			SavepointCoordinator coordinator)
			throws NoSuchFieldException, IllegalAccessException {

		Field field = SavepointCoordinator.class.getDeclaredField("savepointPromises");
		field.setAccessible(true);
		return (Map<Long, Promise<String>>) field.get(coordinator);
	}

	// ---- Verification ------------------------------------------------------

	private static void verifyTriggerCheckpoint(
			ExecutionVertex mockExecutionVertex,
			long expectedCheckpointId,
			long expectedTimestamp) {

		ExecutionAttemptID attemptId = mockExecutionVertex
				.getCurrentExecutionAttempt().getAttemptId();

		TriggerCheckpoint expectedMsg = new TriggerCheckpoint(
				mockExecutionVertex.getJobId(),
				attemptId,
				expectedCheckpointId,
				expectedTimestamp);

		verify(mockExecutionVertex).sendMessageToCurrentExecution(
				eq(expectedMsg), eq(attemptId));
	}

	private static void verifyNotifyCheckpointComplete(
			ExecutionVertex mockExecutionVertex,
			long expectedCheckpointId,
			long expectedTimestamp) {

		ExecutionAttemptID attemptId = mockExecutionVertex
				.getCurrentExecutionAttempt().getAttemptId();

		NotifyCheckpointComplete expectedMsg = new NotifyCheckpointComplete(
				mockExecutionVertex.getJobId(),
				attemptId,
				expectedCheckpointId,
				expectedTimestamp);

		verify(mockExecutionVertex).sendMessageToCurrentExecution(
				eq(expectedMsg), eq(attemptId));
	}

	private static void verifyPendingCheckpoint(
			PendingCheckpoint checkpoint,
			JobID expectedJobId,
			long expectedCheckpointId,
			long expectedTimestamp,
			int expectedNumberOfAcknowledgedTasks,
			int expectedNumberOfNonAcknowledgedTasks,
			int expectedNumberOfCollectedStates,
			boolean expectedIsDiscarded,
			boolean expectedIsFullyAcknowledged) {

		assertNotNull(checkpoint);
		assertEquals(expectedJobId, checkpoint.getJobId());
		assertEquals(expectedCheckpointId, checkpoint.getCheckpointId());
		assertEquals(expectedTimestamp, checkpoint.getCheckpointTimestamp());
		assertEquals(expectedNumberOfAcknowledgedTasks, checkpoint.getNumberOfAcknowledgedTasks());
		assertEquals(expectedNumberOfNonAcknowledgedTasks, checkpoint.getNumberOfNonAcknowledgedTasks());

		int actualNumberOfCollectedStates = 0;

		for (TaskState taskState : checkpoint.getTaskStates().values()) {
			actualNumberOfCollectedStates += taskState.getNumberCollectedStates();
		}

		assertEquals(expectedNumberOfCollectedStates, actualNumberOfCollectedStates);
		assertEquals(expectedIsDiscarded, checkpoint.isDiscarded());
		assertEquals(expectedIsFullyAcknowledged, checkpoint.isFullyAcknowledged());
	}

	private static void verifySavepoint(
			Savepoint savepoint,
			long expectedCheckpointId,
			ExecutionVertex[] expectedVertices) throws Exception {

		assertEquals(expectedCheckpointId, savepoint.getCheckpointId());

		for (TaskState taskState : savepoint.getTaskStates()) {
			JobVertexID jobVertexId = taskState.getJobVertexID();

			// Find matching execution vertex
			ExecutionVertex vertex = null;
			for (ExecutionVertex executionVertex : expectedVertices) {
				if (executionVertex.getJobvertexId().equals(jobVertexId)) {
					vertex = executionVertex;
					break;
				}
			}

			if (vertex == null) {
				fail("Did not find matching vertex");
			} else {
				SubtaskState subtaskState = taskState.getState(vertex.getParallelSubtaskIndex());
				ExecutionAttemptID vertexAttemptId = vertex.getCurrentExecutionAttempt().getAttemptId();

				ExecutionAttemptID stateAttemptId = (ExecutionAttemptID) InstantiationUtil.deserializeObject(
						subtaskState.getChainedStateHandle().get(0).openInputStream());

				assertEquals(vertexAttemptId, stateAttemptId);
			}
		}
	}

	// ---- Mocking -----------------------------------------------------------

	private static ExecutionJobVertex mockExecutionJobVertex(
			JobID jobId,
			JobVertexID jobVertexId,
			int parallelism) {

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobId()).thenReturn(jobId);
		when(jobVertex.getJobVertexId()).thenReturn(jobVertexId);
		when(jobVertex.getParallelism()).thenReturn(parallelism);
		when(jobVertex.getMaxParallelism()).thenReturn(parallelism);

		ExecutionVertex[] vertices = new ExecutionVertex[parallelism];

		for (int i = 0; i < vertices.length; i++) {
			vertices[i] = mockExecutionVertex(jobId, jobVertexId, i, parallelism, ExecutionState.RUNNING);
		}

		when(jobVertex.getTaskVertices()).thenReturn(vertices);

		return jobVertex;
	}

	private static ExecutionVertex mockExecutionVertex(JobID jobId) {
		return mockExecutionVertex(jobId, ExecutionState.RUNNING);
	}

	private static ExecutionVertex mockExecutionVertex(
			JobID jobId,
			ExecutionState state) {

		return mockExecutionVertex(jobId, new JobVertexID(), 0, 1, state);
	}

	private static ExecutionVertex mockExecutionVertex(
			JobID jobId,
			JobVertexID jobVertexId,
			int subtaskIndex,
			int parallelism,
			ExecutionState executionState) {

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(exec.getState()).thenReturn(executionState);

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobId()).thenReturn(jobId);
		when(vertex.getJobvertexId()).thenReturn(jobVertexId);
		when(vertex.getParallelSubtaskIndex()).thenReturn(subtaskIndex);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(parallelism);

		return vertex;
	}

	private static class MockCheckpointIdCounter implements CheckpointIDCounter {

		private boolean started;
		private long count;
		private long lastReturnedCount;

		@Override
		public void start() throws Exception {
			started = true;
		}

		@Override
		public void shutdown() throws Exception {
			started = false;
		}

		@Override
		public void suspend() throws Exception {
			started = false;
		}

		@Override
		public long getAndIncrement() throws Exception {
			lastReturnedCount = count;
			return count++;
		}

		@Override
		public void setCount(long newCount) {
			count = newCount;
		}

		long getLastReturnedCount() {
			return lastReturnedCount;
		}

		public boolean isStarted() {
			return started;
		}
	}
}
