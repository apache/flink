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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
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
public class SavepointCoordinatorTest {

	// ------------------------------------------------------------------------
	// Trigger and acknowledge
	// ------------------------------------------------------------------------

	/**
	 * Simple trigger-acknowledge test for a single savepoint.
	 */
	@Test
	public void testSimpleTriggerSavepoint() throws Exception {
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		long checkpointTimeout = 60 * 1000;
		long timestamp = 1272635;
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();
		HeapStateStore<Savepoint> savepointStore = new HeapStateStore<>();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
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
					jobId, vertex.getCurrentExecutionAttempt().getAttemptId(),
					checkpointId, createSerializedStateHandle(vertex), 0));
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
		Savepoint savepoint = savepointStore.getState(savepointPath);
		verifySavepoint(savepoint, appId, jobId, checkpointId, timestamp,
				vertices);

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
		ApplicationID appId = new ApplicationID();
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
		StateStore<Savepoint> savepointStore = new HeapStateStore<>();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
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
					jobId, attemptId, 0, createSerializedStateHandle(vertex), 0));
		}

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());
		assertNotNull(savepointPath);

		// Rollback
		assertEquals(appId, coordinator.restoreSavepoint(
				createExecutionJobVertexMap(jobVertices),
				savepointPath));

		// Verify all executions have been reset
		for (ExecutionVertex vertex : ackVertices) {
			verify(vertex.getCurrentExecutionAttempt(), times(1)).setInitialState(
					any(SerializedValue.class), anyLong());
		}

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		// Verify checkpoint ID counter started
		assertTrue(idCounter.isStarted());

		coordinator.shutdown();
	}

	@Test
	public void testRollbackParallelismMismatch() throws Exception {
		ApplicationID appId = new ApplicationID();
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

		StateStore<Savepoint> savepointStore = new HeapStateStore<>();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
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
					jobId, attemptId, 0, createSerializedStateHandle(vertex), 0));
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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionJobVertex jobVertex = mockExecutionJobVertex(jobId, new JobVertexID(), 4);
		HeapStateStore<Savepoint> savepointStore = spy(
				new HeapStateStore<Savepoint>());

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
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
					jobId, attemptId, 0, createSerializedStateHandle(vertex), 0));
		}

		String savepointPath = Await.result(savepointPathFuture, FiniteDuration.Zero());
		assertNotNull(savepointPath);

		// Failure on getState
		doThrow(new Exception("TestException")).when(savepointStore).getState(anyString());

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
	public void testRollbackUpdatesApplicationID() throws Exception {
		ApplicationID appId = new ApplicationID();

		CompletedCheckpoint checkpoint = mock(CompletedCheckpoint.class);
		when(checkpoint.getStates()).thenReturn(Collections.<StateForTask>emptyList());
		when(checkpoint.getCheckpointID()).thenReturn(12312312L);

		Savepoint savepoint = new Savepoint(appId, checkpoint);

		StateStore<Savepoint> savepointStore = mock(StateStore.class);
		when(savepointStore.getState(anyString())).thenReturn(savepoint);

		SavepointCoordinator coordinator = createSavepointCoordinator(
				new ApplicationID(),
				new JobID(),
				60 * 1000,
				new ExecutionVertex[] {},
				new ExecutionVertex[] {},
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				savepointStore);

		assertEquals(appId, coordinator.restoreSavepoint(createExecutionJobVertexMap(), "any"));

		coordinator.shutdown();
	}

	@Test
	public void testRollbackSetsCheckpointID() throws Exception {
		ApplicationID appId = new ApplicationID();

		CompletedCheckpoint checkpoint = mock(CompletedCheckpoint.class);
		when(checkpoint.getStates()).thenReturn(Collections.<StateForTask>emptyList());
		when(checkpoint.getCheckpointID()).thenReturn(12312312L);

		Savepoint savepoint = new Savepoint(appId, checkpoint);

		CheckpointIDCounter checkpointIdCounter = mock(CheckpointIDCounter.class);

		StateStore<Savepoint> savepointStore = mock(StateStore.class);
		when(savepointStore.getState(anyString())).thenReturn(savepoint);

		SavepointCoordinator coordinator = createSavepointCoordinator(
				new ApplicationID(),
				new JobID(),
				60 * 1000,
				new ExecutionVertex[] {},
				new ExecutionVertex[] {},
				new ExecutionVertex[] {},
				checkpointIdCounter,
				savepointStore);

		assertEquals(appId, coordinator.restoreSavepoint(createExecutionJobVertexMap(), "any"));

		verify(checkpointIdCounter).setCount(eq(12312312L + 1));

		coordinator.shutdown();
	}

	// ------------------------------------------------------------------------
	// Savepoint aborts and future notifications
	// ------------------------------------------------------------------------

	@Test
	public void testAbortSavepointIfTriggerTasksNotExecuted() throws Exception {
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionVertex[] triggerVertices = new ExecutionVertex[] {
				mock(ExecutionVertex.class),
				mock(ExecutionVertex.class) };
		ExecutionVertex[] ackVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				new HeapStateStore<Savepoint>());

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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionVertex[] triggerVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId, ExecutionState.FINISHED) };
		ExecutionVertex[] ackVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				new HeapStateStore<Savepoint>());

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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionVertex[] triggerVertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		ExecutionVertex[] ackVertices = new ExecutionVertex[] {
				mock(ExecutionVertex.class),
				mock(ExecutionVertex.class) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
				jobId,
				60 * 1000,
				triggerVertices,
				ackVertices,
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				new HeapStateStore<Savepoint>());

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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		ExecutionVertex commitVertex = mockExecutionVertex(jobId);
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
				jobId,
				20,
				vertices,
				vertices,
				new ExecutionVertex[] { commitVertex },
				checkpointIdCounter,
				new HeapStateStore<Savepoint>());

		// Trigger the savepoint
		Future<String> savepointPathFuture = coordinator.triggerSavepoint(12731273);
		assertFalse(savepointPathFuture.isCompleted());

		long checkpointId = checkpointIdCounter.getLastReturnedCount();

		// Acknowledge single task
		coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId, vertices[0].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId, createSerializedStateHandle(vertices[0]), 0));

		PendingCheckpoint pendingCheckpoint = coordinator.getPendingCheckpoints()
				.get(checkpointId);

		assertFalse(pendingCheckpoint.isDiscarded());

		// Wait for savepoint to timeout
		Deadline deadline = FiniteDuration.apply(5, "s").fromNow();
		while (deadline.hasTimeLeft()
				&& !pendingCheckpoint.isDiscarded()
				&& coordinator.getNumberOfPendingCheckpoints() > 0) {

			Thread.sleep(250);
		}

		// Verify discarded
		assertTrue(pendingCheckpoint.isDiscarded());
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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
				jobId,
				60 * 1000,
				vertices,
				vertices,
				vertices,
				new MockCheckpointIdCounter(),
				new HeapStateStore<Savepoint>());

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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		ExecutionJobVertex jobVertex = mockExecutionJobVertex(jobId, new JobVertexID(), 4);
		HeapStateStore<Savepoint> savepointStore = spy(
				new HeapStateStore<Savepoint>());

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
				jobId,
				60 * 1000,
				jobVertex.getTaskVertices(),
				jobVertex.getTaskVertices(),
				new ExecutionVertex[] {},
				new MockCheckpointIdCounter(),
				savepointStore);

		// Failure on putState
		doThrow(new Exception("TestException"))
				.when(savepointStore).putState(any(Savepoint.class));

		Future<String> savepointPathFuture = coordinator.triggerSavepoint(1231273123);

		// Acknowledge all tasks
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
			coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
					jobId, attemptId, 0, createSerializedStateHandle(vertex), 0));
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
		ApplicationID appId = new ApplicationID();
		JobID jobId = new JobID();
		long checkpointTimeout = 60 * 1000;
		long[] timestamps = new long[] { 1272635, 1272635 + 10 };
		long[] checkpointIds = new long[2];
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(jobId),
				mockExecutionVertex(jobId) };
		MockCheckpointIdCounter checkpointIdCounter = new MockCheckpointIdCounter();
		HeapStateStore<Savepoint> savepointStore = new HeapStateStore<>();

		SavepointCoordinator coordinator = createSavepointCoordinator(
				appId,
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
					jobId, vertex.getCurrentExecutionAttempt().getAttemptId(),
					checkpointIds[1], createSerializedStateHandle(vertex), 0));
		}

		// ...and one task of first checkpoint
		coordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(
				jobId, vertices[0].getCurrentExecutionAttempt().getAttemptId(),
				checkpointIds[0], createSerializedStateHandle(vertices[0]), 0));

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
		savepoints[1] = savepointStore.getState(savepointPaths[1]);
		verifySavepoint(savepoints[1], appId, jobId, checkpointIds[1], timestamps[1],
				vertices);

		// Verify all promises removed
		assertEquals(0, getSavepointPromises(coordinator).size());

		coordinator.shutdown();
	}

	@Test
	public void testShutdownDoesNotCleanUpCompletedCheckpointsWithFileSystemStore() throws Exception {
		ApplicationID appId = new ApplicationID();
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
			FileSystemStateStore<Savepoint> savepointStore = new FileSystemStateStore<>(
					tmpDir.toURI().toString(), "sp-");

			SavepointCoordinator coordinator = createSavepointCoordinator(
					appId,
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
						jobId, vertex.getCurrentExecutionAttempt().getAttemptId(),
						checkpointId, createSerializedStateHandle(vertex), 0));
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
			Savepoint savepoint = savepointStore.getState(savepointPath);
			verifySavepoint(savepoint, appId, jobId, checkpointId, timestamp,
					vertices);
		}
		finally {
			FileUtils.deleteDirectory(tmpDir);
		}
	}

	// ------------------------------------------------------------------------
	// Test helpers
	// ------------------------------------------------------------------------

	private static SavepointCoordinator createSavepointCoordinator(
			ApplicationID appId,
			JobID jobId,
			long checkpointTimeout,
			ExecutionVertex[] triggerVertices,
			ExecutionVertex[] ackVertices,
			ExecutionVertex[] commitVertices,
			CheckpointIDCounter checkpointIdCounter,
			StateStore<Savepoint> savepointStore) throws Exception {

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		return new SavepointCoordinator(
				appId,
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

	private static SerializedValue<StateHandle<?>> createSerializedStateHandle(
			ExecutionVertex vertex) throws IOException {

		return new SerializedValue<StateHandle<?>>(new LocalStateHandle<Serializable>(
				vertex.getCurrentExecutionAttempt().getAttemptId()));
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
		assertEquals(expectedNumberOfCollectedStates, checkpoint.getCollectedStates().size());
		assertEquals(expectedIsDiscarded, checkpoint.isDiscarded());
		assertEquals(expectedIsFullyAcknowledged, checkpoint.isFullyAcknowledged());
	}

	private static void verifySavepoint(
			Savepoint savepoint,
			ApplicationID expectedAppId,
			JobID expectedJobId,
			long expectedCheckpointId,
			long expectedTimestamp,
			ExecutionVertex[] expectedVertices) throws Exception {

		assertEquals(expectedAppId, savepoint.getApplicationId());

		verifyCompletedCheckpoint(
				savepoint.getCompletedCheckpoint(),
				expectedJobId,
				expectedCheckpointId,
				expectedTimestamp,
				expectedVertices
		);
	}

	private static void verifyCompletedCheckpoint(
			CompletedCheckpoint checkpoint,
			JobID expectedJobId,
			long expectedCheckpointId,
			long expectedTimestamp,
			ExecutionVertex[] expectedVertices) throws Exception {

		assertNotNull(checkpoint);
		assertEquals(expectedJobId, checkpoint.getJobId());
		assertEquals(expectedCheckpointId, checkpoint.getCheckpointID());
		assertEquals(expectedTimestamp, checkpoint.getTimestamp());

		List<StateForTask> states = checkpoint.getStates();
		assertEquals(expectedVertices.length, states.size());

		for (ExecutionVertex vertex : expectedVertices) {
			JobVertexID expectedOperatorId = vertex.getJobvertexId();

			boolean success = false;
			for (StateForTask state : states) {
				if (state.getOperatorId().equals(expectedOperatorId)) {
					ExecutionAttemptID vertexAttemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
					ExecutionAttemptID stateAttemptId = (ExecutionAttemptID) state.getState()
							.deserializeValue(Thread.currentThread().getContextClassLoader())
							.getState(Thread.currentThread().getContextClassLoader());

					assertEquals(vertexAttemptId, stateAttemptId);
					success = true;
					break;
				}
			}

			assertTrue(success);
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

		ExecutionVertex[] vertices = new ExecutionVertex[parallelism];

		for (int i = 0; i < vertices.length; i++) {
			vertices[i] = mockExecutionVertex(jobId, jobVertexId, i, ExecutionState.RUNNING);
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

		return mockExecutionVertex(jobId, new JobVertexID(), 0, state);
	}

	private static ExecutionVertex mockExecutionVertex(
			JobID jobId,
			JobVertexID jobVertexId,
			int subtaskIndex,
			ExecutionState executionState) {

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(exec.getState()).thenReturn(executionState);

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobId()).thenReturn(jobId);
		when(vertex.getJobvertexId()).thenReturn(jobVertexId);
		when(vertex.getParallelSubtaskIndex()).thenReturn(subtaskIndex);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

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
		public void stop() throws Exception {
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
