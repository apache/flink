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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.RecoverableCompletedCheckpointStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializableObject;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the checkpoint coordinator.
 */
public class CheckpointCoordinatorTest extends TestLogger {

	private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

	private CheckpointFailureManager failureManager;

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	@Before
	public void setUp() throws Exception {
		failureManager = new CheckpointFailureManager(
			0,
			NoOpFailJobCall.INSTANCE);
	}

	@Test
	public void testCheckpointAbortsIfTriggerTasksAreNotExecuted() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that receive the checkpoint trigger messages
			ExecutionVertex triggerVertex1 = mock(ExecutionVertex.class);
			ExecutionVertex triggerVertex2 = mock(ExecutionVertex.class);

			// create some mock Execution vertices that need to ack the checkpoint
			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] {},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			assertFalse(coord.triggerCheckpoint(timestamp, false));

			// still, nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCheckpointAbortsIfTriggerTasksAreFinished() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID triggerAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID triggerAttemptID2 = new ExecutionAttemptID();
			ExecutionVertex triggerVertex1 = mockExecutionVertex(triggerAttemptID1);
			JobVertexID jobVertexID2 = new JobVertexID();
			ExecutionVertex triggerVertex2 = mockExecutionVertex(
				triggerAttemptID2,
				jobVertexID2,
				Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID2)),
				1,
				1,
				ExecutionState.FINISHED);

			// create some mock Execution vertices that need to ack the checkpoint
			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] {},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			assertFalse(coord.triggerCheckpoint(timestamp, false));

			// still, nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCheckpointAbortsIfAckTasksAreNotExecuted() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that need to ack the checkpoint
			final ExecutionAttemptID triggerAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID triggerAttemptID2 = new ExecutionAttemptID();
			ExecutionVertex triggerVertex1 = mockExecutionVertex(triggerAttemptID1);
			ExecutionVertex triggerVertex2 = mockExecutionVertex(triggerAttemptID2);

			// create some mock Execution vertices that receive the checkpoint trigger messages
			ExecutionVertex ackVertex1 = mock(ExecutionVertex.class);
			ExecutionVertex ackVertex2 = mock(ExecutionVertex.class);

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] {},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			assertFalse(coord.triggerCheckpoint(timestamp, false));

			// still, nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTriggerAndDeclineCheckpointThenFailureManagerThrowsException() {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
		ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

		final String errorMsg = "Exceeded checkpoint failure tolerance number!";

		CheckpointFailureManager checkpointFailureManager = new CheckpointFailureManager(
			0,
			new CheckpointFailureManager.FailJobCallback() {
				@Override
				public void failJob(Throwable cause) {
					throw new RuntimeException(errorMsg);
				}

				@Override
				public void failJobDueToTaskFailure(Throwable cause, ExecutionAttemptID failingTask) {
					throw new RuntimeException(errorMsg);
				}
			});

		// set up the coordinator
		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2, checkpointFailureManager);

		try {
			// trigger the checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			// acknowledge from one of the tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId), TASK_MANAGER_LOCATION_INFO);
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// decline checkpoint from the other task
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);

			fail("Test failed.");
		}
		catch (Exception e) {
			//expected
			assertTrue(e instanceof RuntimeException);
			assertEquals(errorMsg, e.getMessage());
		} finally {
			try {
				coord.shutdown(JobStatus.FINISHED);
			} catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	/**
	 * This test triggers a checkpoint and then sends a decline checkpoint message from
	 * one of the tasks. The expected behaviour is that said checkpoint is discarded and a new
	 * checkpoint is triggered.
	 */
	@Test
	public void testTriggerAndDeclineCheckpointSimple() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
			ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2, failureManager);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			// validate that we have a pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// we have one task scheduled that will cancel after timeout
			assertEquals(1, coord.getNumScheduledTasks());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			assertNotNull(checkpoint);
			assertEquals(checkpointId, checkpoint.getCheckpointId());
			assertEquals(timestamp, checkpoint.getCheckpointTimestamp());
			assertEquals(jid, checkpoint.getJobId());
			assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint.getOperatorStates().size());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			verify(vertex1.getCurrentExecutionAttempt()).triggerCheckpoint(checkpointId, timestamp, CheckpointOptions.forCheckpointWithDefaultLocation());
			verify(vertex2.getCurrentExecutionAttempt()).triggerCheckpoint(checkpointId, timestamp, CheckpointOptions.forCheckpointWithDefaultLocation());

			// acknowledge from one of the tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId), "Unknown location");
			assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// acknowledge the same task again (should not matter)
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId), "Unknown location");
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());


			// decline checkpoint from the other task, this should cancel the checkpoint
			// and trigger a new one
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);
			assertTrue(checkpoint.isDiscarded());

			// the canceler is also removed
			assertEquals(0, coord.getNumScheduledTasks());

			// validate that we have no new pending checkpoint
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// decline again, nothing should happen
			// decline from the other task, nothing should happen
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID2, checkpointId), TASK_MANAGER_LOCATION_INFO);
			assertTrue(checkpoint.isDiscarded());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test triggers two checkpoints and then sends a decline message from one of the tasks
	 * for the first checkpoint. This should discard the first checkpoint while not triggering
	 * a new checkpoint because a later checkpoint is already in progress.
	 */
	@Test
	public void testTriggerAndDeclineCheckpointComplex() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
			ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2, failureManager);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumScheduledTasks());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			// trigger second checkpoint, should also succeed
			assertTrue(coord.triggerCheckpoint(timestamp + 2, false));

			// validate that we have a pending checkpoint
			assertEquals(2, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(2, coord.getNumScheduledTasks());

			Iterator<Map.Entry<Long, PendingCheckpoint>> it = coord.getPendingCheckpoints().entrySet().iterator();
			long checkpoint1Id = it.next().getKey();
			long checkpoint2Id = it.next().getKey();
			PendingCheckpoint checkpoint1 = coord.getPendingCheckpoints().get(checkpoint1Id);
			PendingCheckpoint checkpoint2 = coord.getPendingCheckpoints().get(checkpoint2Id);

			assertNotNull(checkpoint1);
			assertEquals(checkpoint1Id, checkpoint1.getCheckpointId());
			assertEquals(timestamp, checkpoint1.getCheckpointTimestamp());
			assertEquals(jid, checkpoint1.getJobId());
			assertEquals(2, checkpoint1.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint1.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint1.getOperatorStates().size());
			assertFalse(checkpoint1.isDiscarded());
			assertFalse(checkpoint1.isFullyAcknowledged());

			assertNotNull(checkpoint2);
			assertEquals(checkpoint2Id, checkpoint2.getCheckpointId());
			assertEquals(timestamp + 2, checkpoint2.getCheckpointTimestamp());
			assertEquals(jid, checkpoint2.getJobId());
			assertEquals(2, checkpoint2.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint2.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint2.getOperatorStates().size());
			assertFalse(checkpoint2.isDiscarded());
			assertFalse(checkpoint2.isFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint1Id), eq(timestamp), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint1Id), eq(timestamp), any(CheckpointOptions.class));
			}

			// check that the vertices received the trigger checkpoint message for the second checkpoint
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint2Id), eq(timestamp + 2), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint2Id), eq(timestamp + 2), any(CheckpointOptions.class));
			}

			// decline checkpoint from one of the tasks, this should cancel the checkpoint
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id), TASK_MANAGER_LOCATION_INFO);
			assertTrue(checkpoint1.isDiscarded());

			// validate that we have only one pending checkpoint left
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(1, coord.getNumScheduledTasks());

			// validate that it is the same second checkpoint from earlier
			long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpointNew = coord.getPendingCheckpoints().get(checkpointIdNew);
			assertEquals(checkpoint2Id, checkpointIdNew);

			assertNotNull(checkpointNew);
			assertEquals(checkpointIdNew, checkpointNew.getCheckpointId());
			assertEquals(jid, checkpointNew.getJobId());
			assertEquals(2, checkpointNew.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpointNew.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpointNew.getOperatorStates().size());
			assertFalse(checkpointNew.isDiscarded());
			assertFalse(checkpointNew.isFullyAcknowledged());
			assertNotEquals(checkpoint1.getCheckpointId(), checkpointNew.getCheckpointId());

			// decline again, nothing should happen
			// decline from the other task, nothing should happen
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id), TASK_MANAGER_LOCATION_INFO);
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID2, checkpoint1Id), TASK_MANAGER_LOCATION_INFO);
			assertTrue(checkpoint1.isDiscarded());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTriggerAndConfirmSimpleCheckpoint() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
			ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2, failureManager);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumScheduledTasks());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			// validate that we have a pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(1, coord.getNumScheduledTasks());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			assertNotNull(checkpoint);
			assertEquals(checkpointId, checkpoint.getCheckpointId());
			assertEquals(timestamp, checkpoint.getCheckpointTimestamp());
			assertEquals(jid, checkpoint.getJobId());
			assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint.getOperatorStates().size());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), eq(timestamp), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), eq(timestamp), any(CheckpointOptions.class));
			}

			OperatorID opID1 = OperatorID.fromJobVertexID(vertex1.getJobvertexId());
			OperatorID opID2 = OperatorID.fromJobVertexID(vertex2.getJobvertexId());
			TaskStateSnapshot taskOperatorSubtaskStates1 = mock(TaskStateSnapshot.class);
			TaskStateSnapshot taskOperatorSubtaskStates2 = mock(TaskStateSnapshot.class);
			OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState2 = mock(OperatorSubtaskState.class);
			when(taskOperatorSubtaskStates1.getSubtaskStateByOperatorID(opID1)).thenReturn(subtaskState1);
			when(taskOperatorSubtaskStates2.getSubtaskStateByOperatorID(opID2)).thenReturn(subtaskState2);

			// acknowledge from one of the tasks
			AcknowledgeCheckpoint acknowledgeCheckpoint1 = new AcknowledgeCheckpoint(jid, attemptID2, checkpointId, new CheckpointMetrics(), taskOperatorSubtaskStates2);
			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
			assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());
			verify(taskOperatorSubtaskStates2, never()).registerSharedStates(any(SharedStateRegistry.class));

			// acknowledge the same task again (should not matter)
			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());
			verify(subtaskState2, never()).registerSharedStates(any(SharedStateRegistry.class));

			// acknowledge the other task.
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId, new CheckpointMetrics(), taskOperatorSubtaskStates1), TASK_MANAGER_LOCATION_INFO);

			// the checkpoint is internally converted to a successful checkpoint and the
			// pending checkpoint object is disposed
			assertTrue(checkpoint.isDiscarded());

			// the now we should have a completed checkpoint
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// the canceler should be removed now
			assertEquals(0, coord.getNumScheduledTasks());

			// validate that the subtasks states have registered their shared states.
			{
				verify(subtaskState1, times(1)).registerSharedStates(any(SharedStateRegistry.class));
				verify(subtaskState2, times(1)).registerSharedStates(any(SharedStateRegistry.class));
			}

			// validate that the relevant tasks got a confirmation message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), eq(timestamp), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), eq(timestamp), any(CheckpointOptions.class));
			}

			CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, success.getJobId());
			assertEquals(timestamp, success.getTimestamp());
			assertEquals(checkpoint.getCheckpointId(), success.getCheckpointID());
			assertEquals(2, success.getOperatorStates().size());

			// ---------------
			// trigger another checkpoint and see that this one replaces the other checkpoint
			// ---------------
			final long timestampNew = timestamp + 7;
			coord.triggerCheckpoint(timestampNew, false);

			long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumScheduledTasks());

			CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, successNew.getJobId());
			assertEquals(timestampNew, successNew.getTimestamp());
			assertEquals(checkpointIdNew, successNew.getCheckpointID());
			assertTrue(successNew.getOperatorStates().isEmpty());

			// validate that the relevant tasks got a confirmation message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), eq(timestampNew), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), eq(timestampNew), any(CheckpointOptions.class));

				verify(vertex1.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), eq(timestampNew));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), eq(timestampNew));
			}

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultipleConcurrentCheckpoints() {
		try {
			final JobID jid = new JobID();
			final long timestamp1 = System.currentTimeMillis();
			final long timestamp2 = timestamp1 + 8617;

			// create some mock execution vertices

			final ExecutionAttemptID triggerAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID triggerAttemptID2 = new ExecutionAttemptID();

			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID3 = new ExecutionAttemptID();

			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex1 = mockExecutionVertex(triggerAttemptID1);
			ExecutionVertex triggerVertex2 = mockExecutionVertex(triggerAttemptID2);

			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);
			ExecutionVertex ackVertex3 = mockExecutionVertex(ackAttemptID3);

			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp1, false));

			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending1 = coord.getPendingCheckpoints().values().iterator().next();
			long checkpointId1 = pending1.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), eq(timestamp1), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), eq(timestamp1), any(CheckpointOptions.class));

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1), TASK_MANAGER_LOCATION_INFO);

			// start the second checkpoint
			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp2, false));

			assertEquals(2, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending2;
			{
				Iterator<PendingCheckpoint> all = coord.getPendingCheckpoints().values().iterator();
				PendingCheckpoint cc1 = all.next();
				PendingCheckpoint cc2 = all.next();
				pending2 = pending1 == cc1 ? cc2 : cc1;
			}
			long checkpointId2 = pending2.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), eq(timestamp2), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), eq(timestamp2), any(CheckpointOptions.class));

			// we acknowledge the remaining two tasks from the first
			// checkpoint and two tasks from the second checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId1), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId2), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId1), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId2), TASK_MANAGER_LOCATION_INFO);

			// now, the first checkpoint should be confirmed
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertTrue(pending1.isDiscarded());

			// the first confirm message should be out
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId1), eq(timestamp1));

			// send the last remaining ack for the second checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2), TASK_MANAGER_LOCATION_INFO);

			// now, the second checkpoint should be confirmed
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertTrue(pending2.isDiscarded());

			// the second commit message should be out
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId2), eq(timestamp2));

			// validate the committed checkpoints
			List<CompletedCheckpoint> scs = coord.getSuccessfulCheckpoints();

			CompletedCheckpoint sc1 = scs.get(0);
			assertEquals(checkpointId1, sc1.getCheckpointID());
			assertEquals(timestamp1, sc1.getTimestamp());
			assertEquals(jid, sc1.getJobId());
			assertTrue(sc1.getOperatorStates().isEmpty());

			CompletedCheckpoint sc2 = scs.get(1);
			assertEquals(checkpointId2, sc2.getCheckpointID());
			assertEquals(timestamp2, sc2.getTimestamp());
			assertEquals(jid, sc2.getJobId());
			assertTrue(sc2.getOperatorStates().isEmpty());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSuccessfulCheckpointSubsumesUnsuccessful() {
		try {
			final JobID jid = new JobID();
			final long timestamp1 = System.currentTimeMillis();
			final long timestamp2 = timestamp1 + 1552;

			// create some mock execution vertices
			final ExecutionAttemptID triggerAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID triggerAttemptID2 = new ExecutionAttemptID();

			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID3 = new ExecutionAttemptID();

			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex1 = mockExecutionVertex(triggerAttemptID1);
			ExecutionVertex triggerVertex2 = mockExecutionVertex(triggerAttemptID2);

			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);
			ExecutionVertex ackVertex3 = mockExecutionVertex(ackAttemptID3);

			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(10),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp1, false));

			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending1 = coord.getPendingCheckpoints().values().iterator().next();
			long checkpointId1 = pending1.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), eq(timestamp1), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), eq(timestamp1), any(CheckpointOptions.class));

			OperatorID opID1 = OperatorID.fromJobVertexID(ackVertex1.getJobvertexId());
			OperatorID opID2 = OperatorID.fromJobVertexID(ackVertex2.getJobvertexId());
			OperatorID opID3 = OperatorID.fromJobVertexID(ackVertex3.getJobvertexId());

			TaskStateSnapshot taskOperatorSubtaskStates1_1 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates1_2 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates1_3 = spy(new TaskStateSnapshot());

			OperatorSubtaskState subtaskState1_1 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState1_2 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState1_3 = mock(OperatorSubtaskState.class);
			taskOperatorSubtaskStates1_1.putSubtaskStateByOperatorID(opID1, subtaskState1_1);
			taskOperatorSubtaskStates1_2.putSubtaskStateByOperatorID(opID2, subtaskState1_2);
			taskOperatorSubtaskStates1_3.putSubtaskStateByOperatorID(opID3, subtaskState1_3);

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1, new CheckpointMetrics(), taskOperatorSubtaskStates1_2), TASK_MANAGER_LOCATION_INFO);

			// start the second checkpoint
			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp2, false));

			assertEquals(2, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending2;
			{
				Iterator<PendingCheckpoint> all = coord.getPendingCheckpoints().values().iterator();
				PendingCheckpoint cc1 = all.next();
				PendingCheckpoint cc2 = all.next();
				pending2 = pending1 == cc1 ? cc2 : cc1;
			}
			long checkpointId2 = pending2.getCheckpointId();

			TaskStateSnapshot taskOperatorSubtaskStates2_1 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates2_2 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates2_3 = spy(new TaskStateSnapshot());

			OperatorSubtaskState subtaskState2_1 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState2_2 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState2_3 = mock(OperatorSubtaskState.class);

			taskOperatorSubtaskStates2_1.putSubtaskStateByOperatorID(opID1, subtaskState2_1);
			taskOperatorSubtaskStates2_2.putSubtaskStateByOperatorID(opID2, subtaskState2_2);
			taskOperatorSubtaskStates2_3.putSubtaskStateByOperatorID(opID3, subtaskState2_3);

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), eq(timestamp2), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), eq(timestamp2), any(CheckpointOptions.class));

			// we acknowledge one more task from the first checkpoint and the second
			// checkpoint completely. The second checkpoint should then subsume the first checkpoint

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2, new CheckpointMetrics(), taskOperatorSubtaskStates2_3), TASK_MANAGER_LOCATION_INFO);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId2, new CheckpointMetrics(), taskOperatorSubtaskStates2_1), TASK_MANAGER_LOCATION_INFO);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId1, new CheckpointMetrics(), taskOperatorSubtaskStates1_1), TASK_MANAGER_LOCATION_INFO);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId2, new CheckpointMetrics(), taskOperatorSubtaskStates2_2), TASK_MANAGER_LOCATION_INFO);

			// now, the second checkpoint should be confirmed, and the first discarded
			// actually both pending checkpoints are discarded, and the second has been transformed
			// into a successful checkpoint
			assertTrue(pending1.isDiscarded());
			assertTrue(pending2.isDiscarded());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// validate that all received subtask states in the first checkpoint have been discarded
			verify(subtaskState1_1, times(1)).discardState();
			verify(subtaskState1_2, times(1)).discardState();

			// validate that all subtask states in the second checkpoint are not discarded
			verify(subtaskState2_1, never()).discardState();
			verify(subtaskState2_2, never()).discardState();
			verify(subtaskState2_3, never()).discardState();

			// validate the committed checkpoints
			List<CompletedCheckpoint> scs = coord.getSuccessfulCheckpoints();
			CompletedCheckpoint success = scs.get(0);
			assertEquals(checkpointId2, success.getCheckpointID());
			assertEquals(timestamp2, success.getTimestamp());
			assertEquals(jid, success.getJobId());
			assertEquals(3, success.getOperatorStates().size());

			// the first confirm message should be out
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId2), eq(timestamp2));

			// send the last remaining ack for the first checkpoint. This should not do anything
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId1, new CheckpointMetrics(), taskOperatorSubtaskStates1_3), TASK_MANAGER_LOCATION_INFO);
			verify(subtaskState1_3, times(1)).discardState();

			coord.shutdown(JobStatus.FINISHED);

			// validate that the states in the second checkpoint have been discarded
			verify(subtaskState2_1, times(1)).discardState();
			verify(subtaskState2_2, times(1)).discardState();
			verify(subtaskState2_3, times(1)).discardState();

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCheckpointTimeoutIsolated() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock execution vertices

			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();

			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();

			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);

			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);

			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			// set up the coordinator
			// the timeout for the checkpoint is a 200 milliseconds
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				200,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			// trigger a checkpoint, partially acknowledged
			assertTrue(coord.triggerCheckpoint(timestamp, false));
			assertEquals(1, coord.getNumberOfPendingCheckpoints());

			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().values().iterator().next();
			assertFalse(checkpoint.isDiscarded());

			OperatorID opID1 = OperatorID.fromJobVertexID(ackVertex1.getJobvertexId());

			TaskStateSnapshot taskOperatorSubtaskStates1 = spy(new TaskStateSnapshot());
			OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
			taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID1, subtaskState1);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpoint.getCheckpointId(), new CheckpointMetrics(), taskOperatorSubtaskStates1), TASK_MANAGER_LOCATION_INFO);

			// wait until the checkpoint must have expired.
			// we check every 250 msecs conservatively for 5 seconds
			// to give even slow build servers a very good chance of completing this
			long deadline = System.currentTimeMillis() + 5000;
			do {
				Thread.sleep(250);
			}
			while (!checkpoint.isDiscarded() &&
					coord.getNumberOfPendingCheckpoints() > 0 &&
					System.currentTimeMillis() < deadline);

			assertTrue("Checkpoint was not canceled by the timeout", checkpoint.isDiscarded());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// validate that the received states have been discarded
			verify(subtaskState1, times(1)).discardState();

			// no confirm message must have been sent
			verify(commitVertex.getCurrentExecutionAttempt(), times(0)).notifyCheckpointComplete(anyLong(), anyLong());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testHandleMessagesForNonExistingCheckpoints() {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock execution vertices and trigger some checkpoint

			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				200000,
				200000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			assertTrue(coord.triggerCheckpoint(timestamp, false));

			long checkpointId = coord.getPendingCheckpoints().keySet().iterator().next();

			// send some messages that do not belong to either the job or the any
			// of the vertices that need to be acknowledged.
			// non of the messages should throw an exception

			// wrong job id
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(new JobID(), ackAttemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);

			// unknown checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, 1L), TASK_MANAGER_LOCATION_INFO);

			// unknown ack vertex
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, new ExecutionAttemptID(), checkpointId), TASK_MANAGER_LOCATION_INFO);

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that late acknowledge checkpoint messages are properly cleaned up. Furthermore it tests
	 * that unknown checkpoint messages for the same job a are cleaned up as well. In contrast
	 * checkpointing messages from other jobs should not be touched. A late acknowledge
	 * message is an acknowledge message which arrives after the checkpoint has been declined.
	 *
	 * @throws Exception
	 */
	@Test
	public void testStateCleanupForLateOrUnknownMessages() throws Exception {
		final JobID jobId = new JobID();

		final ExecutionAttemptID triggerAttemptId = new ExecutionAttemptID();
		final ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptId);

		final ExecutionAttemptID ackAttemptId1 = new ExecutionAttemptID();
		final ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptId1);

		final ExecutionAttemptID ackAttemptId2 = new ExecutionAttemptID();
		final ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptId2);

		final long timestamp = 1L;

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			20000L,
			20000L,
			0L,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jobId,
			chkConfig,
			new ExecutionVertex[] { triggerVertex },
			new ExecutionVertex[] {triggerVertex, ackVertex1, ackVertex2},
			new ExecutionVertex[0],
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		assertTrue(coord.triggerCheckpoint(timestamp, false));

		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		PendingCheckpoint pendingCheckpoint = coord.getPendingCheckpoints().values().iterator().next();

		long checkpointId = pendingCheckpoint.getCheckpointId();

		OperatorID opIDtrigger = OperatorID.fromJobVertexID(triggerVertex.getJobvertexId());

		TaskStateSnapshot taskOperatorSubtaskStatesTrigger = spy(new TaskStateSnapshot());
		OperatorSubtaskState subtaskStateTrigger = mock(OperatorSubtaskState.class);
		taskOperatorSubtaskStatesTrigger.putSubtaskStateByOperatorID(opIDtrigger, subtaskStateTrigger);

		// acknowledge the first trigger vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, triggerAttemptId, checkpointId, new CheckpointMetrics(), taskOperatorSubtaskStatesTrigger), TASK_MANAGER_LOCATION_INFO);

		// verify that the subtask state has not been discarded
		verify(subtaskStateTrigger, never()).discardState();

		TaskStateSnapshot unknownSubtaskState = mock(TaskStateSnapshot.class);

		// receive an acknowledge message for an unknown vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), unknownSubtaskState), TASK_MANAGER_LOCATION_INFO);

		// we should discard acknowledge messages from an unknown vertex belonging to our job
		verify(unknownSubtaskState, times(1)).discardState();

		TaskStateSnapshot differentJobSubtaskState = mock(TaskStateSnapshot.class);

		// receive an acknowledge message from an unknown job
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(new JobID(), new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), differentJobSubtaskState), TASK_MANAGER_LOCATION_INFO);

		// we should not interfere with different jobs
		verify(differentJobSubtaskState, never()).discardState();

		// duplicate acknowledge message for the trigger vertex
		TaskStateSnapshot triggerSubtaskState = mock(TaskStateSnapshot.class);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, triggerAttemptId, checkpointId, new CheckpointMetrics(), triggerSubtaskState), TASK_MANAGER_LOCATION_INFO);

		// duplicate acknowledge messages for a known vertex should not trigger discarding the state
		verify(triggerSubtaskState, never()).discardState();

		// let the checkpoint fail at the first ack vertex
		reset(subtaskStateTrigger);
		coord.receiveDeclineMessage(new DeclineCheckpoint(jobId, ackAttemptId1, checkpointId), TASK_MANAGER_LOCATION_INFO);

		assertTrue(pendingCheckpoint.isDiscarded());

		// check that we've cleaned up the already acknowledged state
		verify(subtaskStateTrigger, times(1)).discardState();

		TaskStateSnapshot ackSubtaskState = mock(TaskStateSnapshot.class);

		// late acknowledge message from the second ack vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, ackAttemptId2, checkpointId, new CheckpointMetrics(), ackSubtaskState), TASK_MANAGER_LOCATION_INFO);

		// check that we also cleaned up this state
		verify(ackSubtaskState, times(1)).discardState();

		// receive an acknowledge message from an unknown job
		reset(differentJobSubtaskState);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(new JobID(), new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), differentJobSubtaskState), TASK_MANAGER_LOCATION_INFO);

		// we should not interfere with different jobs
		verify(differentJobSubtaskState, never()).discardState();

		TaskStateSnapshot unknownSubtaskState2 = mock(TaskStateSnapshot.class);

		// receive an acknowledge message for an unknown vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), unknownSubtaskState2), TASK_MANAGER_LOCATION_INFO);

		// we should discard acknowledge messages from an unknown vertex belonging to our job
		verify(unknownSubtaskState2, times(1)).discardState();
	}

	@Test
	public void testPeriodicTriggering() {
		try {
			final JobID jid = new JobID();
			final long start = System.currentTimeMillis();

			// create some mock execution vertices and trigger some checkpoint

			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex = mockExecutionVertex(ackAttemptID);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			final AtomicInteger numCalls = new AtomicInteger();

			final Execution execution = triggerVertex.getCurrentExecutionAttempt();

			doAnswer(new Answer<Void>() {

				private long lastId = -1;
				private long lastTs = -1;

				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					long id = (Long) invocation.getArguments()[0];
					long ts = (Long) invocation.getArguments()[1];

					assertTrue(id > lastId);
					assertTrue(ts >= lastTs);
					assertTrue(ts >= start);

					lastId = id;
					lastTs = ts;
					numCalls.incrementAndGet();
					return null;
				}
			}).when(execution).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			coord.startCheckpointScheduler();

			long timeout = System.currentTimeMillis() + 60000;
			do {
				Thread.sleep(20);
			}
			while (timeout > System.currentTimeMillis() && numCalls.get() < 5);
			assertTrue(numCalls.get() >= 5);

			coord.stopCheckpointScheduler();


			// for 400 ms, no further calls may come.
			// there may be the case that one trigger was fired and about to
			// acquire the lock, such that after cancelling it will still do
			// the remainder of its work
			int numCallsSoFar = numCalls.get();
			Thread.sleep(400);
			assertTrue(numCallsSoFar == numCalls.get() ||
					numCallsSoFar+1 == numCalls.get());

			// start another sequence of periodic scheduling
			numCalls.set(0);
			coord.startCheckpointScheduler();

			timeout = System.currentTimeMillis() + 60000;
			do {
				Thread.sleep(20);
			}
			while (timeout > System.currentTimeMillis() && numCalls.get() < 5);
			assertTrue(numCalls.get() >= 5);

			coord.stopCheckpointScheduler();

			// for 400 ms, no further calls may come
			// there may be the case that one trigger was fired and about to
			// acquire the lock, such that after cancelling it will still do
			// the remainder of its work
			numCallsSoFar = numCalls.get();
			Thread.sleep(400);
			assertTrue(numCallsSoFar == numCalls.get() ||
					numCallsSoFar + 1 == numCalls.get());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test verified that after a completed checkpoint a certain time has passed before
	 * another is triggered.
	 */
	@Test
	public void testMinTimeBetweenCheckpointsInterval() throws Exception {
		final JobID jid = new JobID();

		// create some mock execution vertices and trigger some checkpoint
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		final ExecutionVertex vertex = mockExecutionVertex(attemptID);
		final Execution executionAttempt = vertex.getCurrentExecutionAttempt();

		final BlockingQueue<Long> triggerCalls = new LinkedBlockingQueue<>();

		doAnswer(invocation -> {
			triggerCalls.add((Long) invocation.getArguments()[0]);
			return null;
		}).when(executionAttempt).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

		final long delay = 50;

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			12,           // periodic interval is 12 ms
			200_000,     // timeout is very long (200 s)
			delay,       // 50 ms delay between checkpoints
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		final CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { vertex },
				new ExecutionVertex[] { vertex },
				new ExecutionVertex[] { vertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

		try {
			coord.startCheckpointScheduler();

			// wait until the first checkpoint was triggered
			Long firstCallId = triggerCalls.take();
			assertEquals(1L, firstCallId.longValue());

			AcknowledgeCheckpoint ackMsg = new AcknowledgeCheckpoint(jid, attemptID, 1L);

			// tell the coordinator that the checkpoint is done
			final long ackTime = System.nanoTime();
			coord.receiveAcknowledgeMessage(ackMsg, TASK_MANAGER_LOCATION_INFO);

			// wait until the next checkpoint is triggered
			Long nextCallId = triggerCalls.take();
			final long nextCheckpointTime = System.nanoTime();
			assertEquals(2L, nextCallId.longValue());

			final long delayMillis = (nextCheckpointTime - ackTime) / 1_000_000;

			// we need to add one ms here to account for rounding errors
			if (delayMillis + 1 < delay) {
				fail("checkpoint came too early: delay was " + delayMillis + " but should have been at least " + delay);
			}
		}
		finally {
			coord.stopCheckpointScheduler();
			coord.shutdown(JobStatus.FINISHED);
		}
	}

	@Test
	public void testMaxConcurrentAttempts1() {
		testMaxConcurrentAttempts(1);
	}

	@Test
	public void testMaxConcurrentAttempts2() {
		testMaxConcurrentAttempts(2);
	}

	@Test
	public void testMaxConcurrentAttempts5() {
		testMaxConcurrentAttempts(5);
	}

	@Test
	public void testTriggerAndConfirmSimpleSavepoint() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
		ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2, failureManager);

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

		// trigger the first checkpoint. this should succeed
		String savepointDir = tmpFolder.newFolder().getAbsolutePath();
		CompletableFuture<CompletedCheckpoint> savepointFuture = coord.triggerSavepoint(timestamp, savepointDir);
		assertFalse(savepointFuture.isDone());

		// validate that we have a pending savepoint
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
		PendingCheckpoint pending = coord.getPendingCheckpoints().get(checkpointId);

		assertNotNull(pending);
		assertEquals(checkpointId, pending.getCheckpointId());
		assertEquals(timestamp, pending.getCheckpointTimestamp());
		assertEquals(jid, pending.getJobId());
		assertEquals(2, pending.getNumberOfNonAcknowledgedTasks());
		assertEquals(0, pending.getNumberOfAcknowledgedTasks());
		assertEquals(0, pending.getOperatorStates().size());
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(pending.canBeSubsumed());

		OperatorID opID1 = OperatorID.fromJobVertexID(vertex1.getJobvertexId());
		OperatorID opID2 = OperatorID.fromJobVertexID(vertex2.getJobvertexId());
		TaskStateSnapshot taskOperatorSubtaskStates1 = mock(TaskStateSnapshot.class);
		TaskStateSnapshot taskOperatorSubtaskStates2 = mock(TaskStateSnapshot.class);
		OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
		OperatorSubtaskState subtaskState2 = mock(OperatorSubtaskState.class);
		when(taskOperatorSubtaskStates1.getSubtaskStateByOperatorID(opID1)).thenReturn(subtaskState1);
		when(taskOperatorSubtaskStates2.getSubtaskStateByOperatorID(opID2)).thenReturn(subtaskState2);

		// acknowledge from one of the tasks
		AcknowledgeCheckpoint acknowledgeCheckpoint2 = new AcknowledgeCheckpoint(jid, attemptID2, checkpointId, new CheckpointMetrics(), taskOperatorSubtaskStates2);
		coord.receiveAcknowledgeMessage(acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
		assertEquals(1, pending.getNumberOfAcknowledgedTasks());
		assertEquals(1, pending.getNumberOfNonAcknowledgedTasks());
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(savepointFuture.isDone());

		// acknowledge the same task again (should not matter)
		coord.receiveAcknowledgeMessage(acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(savepointFuture.isDone());

		// acknowledge the other task.
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId, new CheckpointMetrics(), taskOperatorSubtaskStates1), TASK_MANAGER_LOCATION_INFO);

		// the checkpoint is internally converted to a successful checkpoint and the
		// pending checkpoint object is disposed
		assertTrue(pending.isDiscarded());
		assertTrue(savepointFuture.isDone());

		// the now we should have a completed checkpoint
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertEquals(0, coord.getNumberOfPendingCheckpoints());

		// validate that the relevant tasks got a confirmation message
		{
			verify(vertex1.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId), eq(timestamp));
			verify(vertex2.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId), eq(timestamp));
		}

		// validate that the shared states are registered
		{
			verify(subtaskState1, times(1)).registerSharedStates(any(SharedStateRegistry.class));
			verify(subtaskState2, times(1)).registerSharedStates(any(SharedStateRegistry.class));
		}

		CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
		assertEquals(jid, success.getJobId());
		assertEquals(timestamp, success.getTimestamp());
		assertEquals(pending.getCheckpointId(), success.getCheckpointID());
		assertEquals(2, success.getOperatorStates().size());

		// ---------------
		// trigger another checkpoint and see that this one replaces the other checkpoint
		// ---------------
		final long timestampNew = timestamp + 7;
		savepointFuture = coord.triggerSavepoint(timestampNew, savepointDir);
		assertFalse(savepointFuture.isDone());

		long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
		assertEquals(jid, successNew.getJobId());
		assertEquals(timestampNew, successNew.getTimestamp());
		assertEquals(checkpointIdNew, successNew.getCheckpointID());
		assertTrue(successNew.getOperatorStates().isEmpty());
		assertTrue(savepointFuture.isDone());

		// validate that the first savepoint does not discard its private states.
		verify(subtaskState1, never()).discardState();
		verify(subtaskState2, never()).discardState();

		// validate that the relevant tasks got a confirmation message
		{
			verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), eq(timestampNew), any(CheckpointOptions.class));
			verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), eq(timestampNew), any(CheckpointOptions.class));

			verify(vertex1.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), eq(timestampNew));
			verify(vertex2.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), eq(timestampNew));
		}

		coord.shutdown(JobStatus.FINISHED);
	}

	/**
	 * Triggers a savepoint and two checkpoints. The second checkpoint completes
	 * and subsumes the first checkpoint, but not the first savepoint. Then we
	 * trigger another checkpoint and savepoint. The 2nd savepoint completes and
	 * subsumes the last checkpoint, but not the first savepoint.
	 */
	@Test
	public void testSavepointsAreNotSubsumed() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
		ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

		StandaloneCheckpointIDCounter counter = new StandaloneCheckpointIDCounter();

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			chkConfig,
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			counter,
			new StandaloneCompletedCheckpointStore(10),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		// Trigger savepoint and checkpoint
		CompletableFuture<CompletedCheckpoint> savepointFuture1 = coord.triggerSavepoint(timestamp, savepointDir);
		long savepointId1 = counter.getLast();
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		assertTrue(coord.triggerCheckpoint(timestamp + 1, false));
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		assertTrue(coord.triggerCheckpoint(timestamp + 2, false));
		long checkpointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd checkpoint should subsume the 1st checkpoint, but not the savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId2), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId2), TASK_MANAGER_LOCATION_INFO);

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());
		assertFalse(savepointFuture1.isDone());

		assertTrue(coord.triggerCheckpoint(timestamp + 3, false));
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		CompletableFuture<CompletedCheckpoint> savepointFuture2 = coord.triggerSavepoint(timestamp + 4, savepointDir);
		long savepointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd savepoint should subsume the last checkpoint, but not the 1st savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId2), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId2), TASK_MANAGER_LOCATION_INFO);

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());

		assertFalse(savepointFuture1.isDone());
		assertTrue(savepointFuture2.isDone());

		// Ack first savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId1), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId1), TASK_MANAGER_LOCATION_INFO);

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(3, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertTrue(savepointFuture1.isDone());
	}

	private void testMaxConcurrentAttempts(int maxConcurrentAttempts) {
		try {
			final JobID jid = new JobID();

			// create some mock execution vertices and trigger some checkpoint
			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex = mockExecutionVertex(ackAttemptID);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			final AtomicInteger numCalls = new AtomicInteger();

			final Execution execution = triggerVertex.getCurrentExecutionAttempt();

			doAnswer(invocation -> {
				numCalls.incrementAndGet();
				return null;
			}).when(execution).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

			doAnswer(invocation -> {
				numCalls.incrementAndGet();
				return null;
			}).when(execution).notifyCheckpointComplete(anyLong(), anyLong());

			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0L,        // no extra delay
				maxConcurrentAttempts,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			coord.startCheckpointScheduler();

			// after a while, there should be exactly as many checkpoints
			// as concurrently permitted
			long now = System.currentTimeMillis();
			long timeout = now + 60000;
			long minDuration = now + 100;
			do {
				Thread.sleep(20);
			}
			while ((now = System.currentTimeMillis()) < minDuration ||
					(numCalls.get() < maxConcurrentAttempts && now < timeout));

			assertEquals(maxConcurrentAttempts, numCalls.get());

			verify(triggerVertex.getCurrentExecutionAttempt(), times(maxConcurrentAttempts))
					.triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

			// now, once we acknowledge one checkpoint, it should trigger the next one
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID, 1L), TASK_MANAGER_LOCATION_INFO);

			// this should have immediately triggered a new checkpoint
			now = System.currentTimeMillis();
			timeout = now + 60000;
			do {
				Thread.sleep(20);
			}
			while (numCalls.get() < maxConcurrentAttempts + 1 && now < timeout);

			assertEquals(maxConcurrentAttempts + 1, numCalls.get());

			// no further checkpoints should happen
			Thread.sleep(200);
			assertEquals(maxConcurrentAttempts + 1, numCalls.get());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMaxConcurrentAttempsWithSubsumption() {
		try {
			final int maxConcurrentAttempts = 2;
			final JobID jid = new JobID();

			// create some mock execution vertices and trigger some checkpoint
			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex = mockExecutionVertex(ackAttemptID);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0L,        // no extra delay
				maxConcurrentAttempts, // max two concurrent checkpoints
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			coord.startCheckpointScheduler();

			// after a while, there should be exactly as many checkpoints
			// as concurrently permitted
			long now = System.currentTimeMillis();
			long timeout = now + 60000;
			long minDuration = now + 100;
			do {
				Thread.sleep(20);
			}
			while ((now = System.currentTimeMillis()) < minDuration ||
					(coord.getNumberOfPendingCheckpoints() < maxConcurrentAttempts && now < timeout));

			// validate that the pending checkpoints are there
			assertEquals(maxConcurrentAttempts, coord.getNumberOfPendingCheckpoints());
			assertNotNull(coord.getPendingCheckpoints().get(1L));
			assertNotNull(coord.getPendingCheckpoints().get(2L));

			// now we acknowledge the second checkpoint, which should subsume the first checkpoint
			// and allow two more checkpoints to be triggered
			// now, once we acknowledge one checkpoint, it should trigger the next one
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID, 2L), TASK_MANAGER_LOCATION_INFO);

			// after a while, there should be the new checkpoints
			final long newTimeout = System.currentTimeMillis() + 60000;
			do {
				Thread.sleep(20);
			}
			while (coord.getPendingCheckpoints().get(4L) == null &&
					System.currentTimeMillis() < newTimeout);

			// do the final check
			assertEquals(maxConcurrentAttempts, coord.getNumberOfPendingCheckpoints());
			assertNotNull(coord.getPendingCheckpoints().get(3L));
			assertNotNull(coord.getPendingCheckpoints().get(4L));

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPeriodicSchedulingWithInactiveTasks() {
		try {
			final JobID jid = new JobID();

			// create some mock execution vertices and trigger some checkpoint
			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex = mockExecutionVertex(ackAttemptID);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			final AtomicReference<ExecutionState> currentState = new AtomicReference<>(ExecutionState.CREATED);
			when(triggerVertex.getCurrentExecutionAttempt().getState()).thenAnswer(invocation -> currentState.get());

			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0L,        // no extra delay
				2, // max two concurrent checkpoints
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			coord.startCheckpointScheduler();

			// no checkpoint should have started so far
			Thread.sleep(200);
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// now move the state to RUNNING
			currentState.set(ExecutionState.RUNNING);

			// the coordinator should start checkpointing now
			final long timeout = System.currentTimeMillis() + 10000;
			do {
				Thread.sleep(20);
			}
			while (System.currentTimeMillis() < timeout &&
					coord.getNumberOfPendingCheckpoints() == 0);

			assertTrue(coord.getNumberOfPendingCheckpoints() > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that the savepoints can be triggered concurrently.
	 */
	@Test
	public void testConcurrentSavepoints() throws Exception {
		JobID jobId = new JobID();

		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);

		StandaloneCheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			100000,
			200000,
			0L,
			1, // max one checkpoint at a time => should not affect savepoints
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jobId,
			chkConfig,
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			checkpointIDCounter,
			new StandaloneCompletedCheckpointStore(2),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		List<CompletableFuture<CompletedCheckpoint>> savepointFutures = new ArrayList<>();

		int numSavepoints = 5;

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		// Trigger savepoints
		for (int i = 0; i < numSavepoints; i++) {
			savepointFutures.add(coord.triggerSavepoint(i, savepointDir));
		}

		// After triggering multiple savepoints, all should in progress
		for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
			assertFalse(savepointFuture.isDone());
		}

		// ACK all savepoints
		long checkpointId = checkpointIDCounter.getLast();
		for (int i = 0; i < numSavepoints; i++, checkpointId--) {
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, attemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);
		}

		// After ACKs, all should be completed
		for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
			assertTrue(savepointFuture.isDone());
		}
	}

	/**
	 * Tests that no minimum delay between savepoints is enforced.
	 */
	@Test
	public void testMinDelayBetweenSavepoints() throws Exception {
		JobID jobId = new JobID();

		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			100000,
			200000,
			100000000L, // very long min delay => should not affect savepoints
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jobId,
			chkConfig,
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(2),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		CompletableFuture<CompletedCheckpoint> savepoint0 = coord.triggerSavepoint(0, savepointDir);
		assertFalse("Did not trigger savepoint", savepoint0.isDone());

		CompletableFuture<CompletedCheckpoint> savepoint1 = coord.triggerSavepoint(1, savepointDir);
		assertFalse("Did not trigger savepoint", savepoint1.isDone());
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

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices =
				allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		CompletedCheckpointStore store = new RecoverableCompletedCheckpointStore();

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			chkConfig,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			store,
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			TaskStateSnapshot subtaskState = mockSubtaskState(jobVertexID1, index, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					subtaskState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}

		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			TaskStateSnapshot subtaskState = mockSubtaskState(jobVertexID2, index, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					subtaskState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		// shutdown the store
		store.shutdown(JobStatus.SUSPENDED);

		// restore the store
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		tasks.put(jobVertexID1, jobVertex1);
		tasks.put(jobVertexID2, jobVertex2);

		coord.restoreLatestCheckpointedState(tasks, true, false);

		// validate that all shared states are registered again after the recovery.
		for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
			for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
				for (OperatorSubtaskState subtaskState : taskState.getStates()) {
					verify(subtaskState, times(2)).registerSharedStates(any(SharedStateRegistry.class));
				}
			}
		}

		// verify the restored state
		verifyStateRestore(jobVertexID1, jobVertex1, keyGroupPartitions1);
		verifyStateRestore(jobVertexID2, jobVertex2, keyGroupPartitions2);
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

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			chkConfig,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(null, null, keyGroupState, null);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
				taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(null, null, keyGroupState, null);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID2), operatorSubtaskState);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newMaxParallelism1 = 20;
		int newMaxParallelism2 = 42;

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			newMaxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			newMaxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);

		coord.restoreLatestCheckpointedState(tasks, true, false);

		fail("The restoration should have failed because the max parallelism changed.");
	}

	@Test
	public void testRestoreLatestCheckpointedStateScaleIn() throws Exception {
		testRestoreLatestCheckpointedStateWithChangingParallelism(false);
	}

	@Test
	public void testRestoreLatestCheckpointedStateScaleOut() throws Exception {
		testRestoreLatestCheckpointedStateWithChangingParallelism(true);
	}

	@Test
	public void testRestoreLatestCheckpointWhenPreferCheckpoint() throws Exception {
		testRestoreLatestCheckpointIsPreferSavepoint(true);
	}

	@Test
	public void testRestoreLatestCheckpointWhenPreferSavepoint() throws Exception {
		testRestoreLatestCheckpointIsPreferSavepoint(false);
	}

	private void testRestoreLatestCheckpointIsPreferSavepoint(boolean isPreferCheckpoint) {
		try {
			final JobID jid = new JobID();
			long timestamp = System.currentTimeMillis();
			StandaloneCheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();

			final JobVertexID statefulId = new JobVertexID();
			final JobVertexID statelessId = new JobVertexID();

			Execution statefulExec1 = mockExecution();
			Execution statelessExec1 = mockExecution();

			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0, 1);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0, 1);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
				new ExecutionVertex[] { stateful1 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
				new ExecutionVertex[] { stateless1 });

			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);

			CompletedCheckpointStore store = new RecoverableCompletedCheckpointStore(2);

			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				isPreferCheckpoint,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { stateful1, stateless1 },
				new ExecutionVertex[] { stateful1, stateless1 },
				new ExecutionVertex[] { stateful1, stateless1 },
				checkpointIDCounter,
				store,
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			//trigger a checkpoint and wait to become a completed checkpoint
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			long checkpointId = checkpointIDCounter.getLast();

			KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
			List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
			KeyedStateHandle serializedKeyGroupStates = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, testStates);

			TaskStateSnapshot subtaskStatesForCheckpoint = new TaskStateSnapshot();

			subtaskStatesForCheckpoint.putSubtaskStateByOperatorID(
				OperatorID.fromJobVertexID(statefulId),
				new OperatorSubtaskState(
					StateObjectCollection.empty(),
					StateObjectCollection.empty(),
					StateObjectCollection.singleton(serializedKeyGroupStates),
					StateObjectCollection.empty()));

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStatesForCheckpoint), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId), TASK_MANAGER_LOCATION_INFO);

			CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, success.getJobId());

			// trigger a savepoint and wait it to be finished
			String savepointDir = tmpFolder.newFolder().getAbsolutePath();
			timestamp = System.currentTimeMillis();
			CompletableFuture<CompletedCheckpoint> savepointFuture = coord.triggerSavepoint(timestamp, savepointDir);


			KeyGroupRange keyGroupRangeForSavepoint = KeyGroupRange.of(1, 1);
			List<SerializableObject> testStatesForSavepoint = Collections.singletonList(new SerializableObject());
			KeyedStateHandle serializedKeyGroupStatesForSavepoint = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRangeForSavepoint, testStatesForSavepoint);

			TaskStateSnapshot subtaskStatesForSavepoint = new TaskStateSnapshot();

			subtaskStatesForSavepoint.putSubtaskStateByOperatorID(
				OperatorID.fromJobVertexID(statefulId),
				new OperatorSubtaskState(
					StateObjectCollection.empty(),
					StateObjectCollection.empty(),
					StateObjectCollection.singleton(serializedKeyGroupStatesForSavepoint),
					StateObjectCollection.empty()));

			checkpointId = checkpointIDCounter.getLast();
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStatesForSavepoint), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId), TASK_MANAGER_LOCATION_INFO);

			assertTrue(savepointFuture.isDone());

			//restore and jump the latest savepoint
			coord.restoreLatestCheckpointedState(map, true, false);

			//compare and see if it used the checkpoint's subtaskStates
			BaseMatcher<JobManagerTaskRestore> matcher = new BaseMatcher<JobManagerTaskRestore>() {
				@Override
				public boolean matches(Object o) {
					if (o instanceof JobManagerTaskRestore) {
						JobManagerTaskRestore taskRestore = (JobManagerTaskRestore) o;
						if (isPreferCheckpoint) {
							return Objects.equals(taskRestore.getTaskStateSnapshot(), subtaskStatesForCheckpoint);
						} else {
							return Objects.equals(taskRestore.getTaskStateSnapshot(), subtaskStatesForSavepoint);
						}
					}
					return false;
				}

				@Override
				public void describeTo(Description description) {
					if (isPreferCheckpoint) {
						description.appendValue(subtaskStatesForCheckpoint);
					} else {
						description.appendValue(subtaskStatesForSavepoint);
					}
				}
			};

			verify(statefulExec1, times(1)).setInitialState(MockitoHamcrest.argThat(matcher));
			verify(statelessExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());

			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateRecoveryWhenTopologyChangeOut() throws Exception {
		testStateRecoveryWithTopologyChange(0);
	}

	@Test
	public void testStateRecoveryWhenTopologyChangeIn() throws Exception {
		testStateRecoveryWithTopologyChange(1);
	}

	@Test
	public void testStateRecoveryWhenTopologyChange() throws Exception {
		testStateRecoveryWithTopologyChange(2);
	}


	/**
	 * Tests the checkpoint restoration with changing parallelism of job vertex with partitioned
	 * state.
	 *
	 * @throws Exception
	 */
	private void testRestoreLatestCheckpointedStateWithChangingParallelism(boolean scaleOut) throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = scaleOut ? 2 : 13;

		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		int newParallelism2 = scaleOut ? 13 : 2;

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
				jobVertexID1,
				parallelism1,
				maxParallelism1);
		final ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(
				jobVertexID2,
				parallelism2,
				maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices =
				allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			chkConfig,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		//vertex 1
		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			OperatorStateHandle opStateBackend = generatePartitionableStateHandle(jobVertexID1, index, 2, 8, false);
			KeyGroupsStateHandle keyedStateBackend = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
			KeyGroupsStateHandle keyedStateRaw = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), true);
			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(opStateBackend, null, keyedStateBackend, keyedStateRaw);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}

		//vertex 2
		final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesBackend = new ArrayList<>(jobVertex2.getParallelism());
		final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesRaw = new ArrayList<>(jobVertex2.getParallelism());
		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			KeyGroupsStateHandle keyedStateBackend = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
			KeyGroupsStateHandle keyedStateRaw = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), true);
			OperatorStateHandle opStateBackend = generatePartitionableStateHandle(jobVertexID2, index, 2, 8, false);
			OperatorStateHandle opStateRaw = generatePartitionableStateHandle(jobVertexID2, index, 2, 8, true);
			expectedOpStatesBackend.add(new ChainedStateHandle<>(Collections.singletonList(opStateBackend)));
			expectedOpStatesRaw.add(new ChainedStateHandle<>(Collections.singletonList(opStateRaw)));

			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(opStateBackend, opStateRaw, keyedStateBackend, keyedStateRaw);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID2), operatorSubtaskState);

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		List<KeyGroupRange> newKeyGroupPartitions2 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
				jobVertexID1,
				parallelism1,
				maxParallelism1);

		// rescale vertex 2
		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
				jobVertexID2,
				newParallelism2,
				maxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);
		coord.restoreLatestCheckpointedState(tasks, true, false);

		// verify the restored state
		verifyStateRestore(jobVertexID1, newJobVertex1, keyGroupPartitions1);
		List<List<Collection<OperatorStateHandle>>> actualOpStatesBackend = new ArrayList<>(newJobVertex2.getParallelism());
		List<List<Collection<OperatorStateHandle>>> actualOpStatesRaw = new ArrayList<>(newJobVertex2.getParallelism());
		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {

			List<OperatorID> operatorIDs = newJobVertex2.getOperatorIDs();

			KeyGroupsStateHandle originalKeyedStateBackend = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), false);
			KeyGroupsStateHandle originalKeyedStateRaw = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), true);

			JobManagerTaskRestore taskRestore = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
			Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot taskStateHandles = taskRestore.getTaskStateSnapshot();

			final int headOpIndex = operatorIDs.size() - 1;
			List<Collection<OperatorStateHandle>> allParallelManagedOpStates = new ArrayList<>(operatorIDs.size());
			List<Collection<OperatorStateHandle>> allParallelRawOpStates = new ArrayList<>(operatorIDs.size());

			for (int idx = 0; idx < operatorIDs.size(); ++idx) {
				OperatorID operatorID = operatorIDs.get(idx);
				OperatorSubtaskState opState = taskStateHandles.getSubtaskStateByOperatorID(operatorID);
				Collection<OperatorStateHandle> opStateBackend = opState.getManagedOperatorState();
				Collection<OperatorStateHandle> opStateRaw = opState.getRawOperatorState();
				allParallelManagedOpStates.add(opStateBackend);
				allParallelRawOpStates.add(opStateRaw);
				if (idx == headOpIndex) {
					Collection<KeyedStateHandle> keyedStateBackend = opState.getManagedKeyedState();
					Collection<KeyedStateHandle> keyGroupStateRaw = opState.getRawKeyedState();
					compareKeyedState(Collections.singletonList(originalKeyedStateBackend), keyedStateBackend);
					compareKeyedState(Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
				}
			}
			actualOpStatesBackend.add(allParallelManagedOpStates);
			actualOpStatesRaw.add(allParallelRawOpStates);
		}

		comparePartitionableState(expectedOpStatesBackend, actualOpStatesBackend);
		comparePartitionableState(expectedOpStatesRaw, actualOpStatesRaw);
	}

	private static Tuple2<JobVertexID, OperatorID> generateIDPair() {
		JobVertexID jobVertexID = new JobVertexID();
		OperatorID operatorID = OperatorID.fromJobVertexID(jobVertexID);
		return new Tuple2<>(jobVertexID, operatorID);
	}
	
	/**
	 * old topology
	 * [operator1,operator2] * parallelism1 -> [operator3,operator4] * parallelism2
	 *
	 *
	 * new topology
	 *
	 * [operator5,operator1,operator3] * newParallelism1 -> [operator3, operator6] * newParallelism2
	 *
	 * scaleType:
	 * 0  increase parallelism
	 * 1  decrease parallelism
	 * 2  same parallelism
	 */
	public void testStateRecoveryWithTopologyChange(int scaleType) throws Exception {

		/*
		 * Old topology
		 * CHAIN(op1 -> op2) * parallelism1 -> CHAIN(op3 -> op4) * parallelism2
		 */
		Tuple2<JobVertexID, OperatorID> id1 = generateIDPair();
		Tuple2<JobVertexID, OperatorID> id2 = generateIDPair();
		int parallelism1 = 10;
		int maxParallelism1 = 64;

		Tuple2<JobVertexID, OperatorID> id3 = generateIDPair();
		Tuple2<JobVertexID, OperatorID> id4 = generateIDPair();
		int parallelism2 = 10;
		int maxParallelism2 = 64;

		List<KeyGroupRange> keyGroupPartitions2 =
			StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		Map<OperatorID, OperatorState> operatorStates = new HashMap<>();

		//prepare vertex1 state
		for (Tuple2<JobVertexID, OperatorID> id : Arrays.asList(id1, id2)) {
			OperatorState taskState = new OperatorState(id.f1, parallelism1, maxParallelism1);
			operatorStates.put(id.f1, taskState);
			for (int index = 0; index < taskState.getParallelism(); index++) {
				OperatorStateHandle subManagedOperatorState =
					generatePartitionableStateHandle(id.f0, index, 2, 8, false);
				OperatorStateHandle subRawOperatorState =
					generatePartitionableStateHandle(id.f0, index, 2, 8, true);
				OperatorSubtaskState subtaskState = new OperatorSubtaskState(
					subManagedOperatorState,
					subRawOperatorState,
					null,
					null);
				taskState.putState(index, subtaskState);
			}
		}

		List<List<ChainedStateHandle<OperatorStateHandle>>> expectedManagedOperatorStates = new ArrayList<>();
		List<List<ChainedStateHandle<OperatorStateHandle>>> expectedRawOperatorStates = new ArrayList<>();
		//prepare vertex2 state
		for (Tuple2<JobVertexID, OperatorID> id : Arrays.asList(id3, id4)) {
			OperatorState operatorState = new OperatorState(id.f1, parallelism2, maxParallelism2);
			operatorStates.put(id.f1, operatorState);
			List<ChainedStateHandle<OperatorStateHandle>> expectedManagedOperatorState = new ArrayList<>();
			List<ChainedStateHandle<OperatorStateHandle>> expectedRawOperatorState = new ArrayList<>();
			expectedManagedOperatorStates.add(expectedManagedOperatorState);
			expectedRawOperatorStates.add(expectedRawOperatorState);

			for (int index = 0; index < operatorState.getParallelism(); index++) {
				OperatorStateHandle subManagedOperatorState =
					generateChainedPartitionableStateHandle(id.f0, index, 2, 8, false)
						.get(0);
				OperatorStateHandle subRawOperatorState =
					generateChainedPartitionableStateHandle(id.f0, index, 2, 8, true)
						.get(0);
				KeyGroupsStateHandle subManagedKeyedState = id.f0.equals(id3.f0)
					? generateKeyGroupState(id.f0, keyGroupPartitions2.get(index), false)
					: null;
				KeyGroupsStateHandle subRawKeyedState = id.f0.equals(id3.f0)
					? generateKeyGroupState(id.f0, keyGroupPartitions2.get(index), true)
					: null;

				expectedManagedOperatorState.add(ChainedStateHandle.wrapSingleHandle(subManagedOperatorState));
				expectedRawOperatorState.add(ChainedStateHandle.wrapSingleHandle(subRawOperatorState));

				OperatorSubtaskState subtaskState = new OperatorSubtaskState(
					subManagedOperatorState,
					subRawOperatorState,
					subManagedKeyedState,
					subRawKeyedState);
				operatorState.putState(index, subtaskState);
			}
		}

		/*
		 * New topology
		 * CHAIN(op5 -> op1 -> op2) * newParallelism1 -> CHAIN(op3 -> op6) * newParallelism2
		 */
		Tuple2<JobVertexID, OperatorID> id5 = generateIDPair();
		int newParallelism1 = 10;

		Tuple2<JobVertexID, OperatorID> id6 = generateIDPair();
		int newParallelism2 = parallelism2;

		if (scaleType == 0) {
			newParallelism2 = 20;
		} else if (scaleType == 1) {
			newParallelism2 = 8;
		}

		List<KeyGroupRange> newKeyGroupPartitions2 =
			StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
			id5.f0,
			Arrays.asList(id2.f1, id1.f1, id5.f1),
			newParallelism1,
			maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
			id3.f0,
			Arrays.asList(id6.f1, id3.f1),
			newParallelism2,
			maxParallelism2);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		tasks.put(id5.f0, newJobVertex1);
		tasks.put(id3.f0, newJobVertex2);

		JobID jobID = new JobID();
		StandaloneCompletedCheckpointStore standaloneCompletedCheckpointStore =
			spy(new StandaloneCompletedCheckpointStore(1));

		CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(
				jobID,
				2,
				System.currentTimeMillis(),
				System.currentTimeMillis() + 3000,
				operatorStates,
				Collections.<MasterState>emptyList(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				new TestCompletedCheckpointStorageLocation());

		when(standaloneCompletedCheckpointStore.getLatestCheckpoint(false)).thenReturn(completedCheckpoint);

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			chkConfig,
			newJobVertex1.getTaskVertices(),
			newJobVertex1.getTaskVertices(),
			newJobVertex1.getTaskVertices(),
			new StandaloneCheckpointIDCounter(),
			standaloneCompletedCheckpointStore,
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		coord.restoreLatestCheckpointedState(tasks, false, true);

		for (int i = 0; i < newJobVertex1.getParallelism(); i++) {

			final List<OperatorID> operatorIds = newJobVertex1.getOperatorIDs();

			JobManagerTaskRestore taskRestore = newJobVertex1.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
			Assert.assertEquals(2L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			OperatorSubtaskState headOpState = stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIds.size() - 1));
			assertTrue(headOpState.getManagedKeyedState().isEmpty());
			assertTrue(headOpState.getRawKeyedState().isEmpty());

			// operator5
			{
				int operatorIndexInChain = 2;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				assertTrue(opState.getManagedOperatorState().isEmpty());
				assertTrue(opState.getRawOperatorState().isEmpty());
			}
			// operator1
			{
				int operatorIndexInChain = 1;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				OperatorStateHandle expectedManagedOpState = generatePartitionableStateHandle(
					id1.f0, i, 2, 8, false);
				OperatorStateHandle expectedRawOpState = generatePartitionableStateHandle(
					id1.f0, i, 2, 8, true);

				Collection<OperatorStateHandle> managedOperatorState = opState.getManagedOperatorState();
				assertEquals(1, managedOperatorState.size());
				assertTrue(CommonTestUtils.isStreamContentEqual(expectedManagedOpState.openInputStream(),
					managedOperatorState.iterator().next().openInputStream()));

				Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
				assertEquals(1, rawOperatorState.size());
				assertTrue(CommonTestUtils.isStreamContentEqual(expectedRawOpState.openInputStream(),
					rawOperatorState.iterator().next().openInputStream()));
			}
			// operator2
			{
				int operatorIndexInChain = 0;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				OperatorStateHandle expectedManagedOpState = generatePartitionableStateHandle(
					id2.f0, i, 2, 8, false);
				OperatorStateHandle expectedRawOpState = generatePartitionableStateHandle(
					id2.f0, i, 2, 8, true);

				Collection<OperatorStateHandle> managedOperatorState = opState.getManagedOperatorState();
				assertEquals(1, managedOperatorState.size());
				assertTrue(CommonTestUtils.isStreamContentEqual(expectedManagedOpState.openInputStream(),
					managedOperatorState.iterator().next().openInputStream()));

				Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
				assertEquals(1, rawOperatorState.size());
				assertTrue(CommonTestUtils.isStreamContentEqual(expectedRawOpState.openInputStream(),
					rawOperatorState.iterator().next().openInputStream()));
			}
		}

		List<List<Collection<OperatorStateHandle>>> actualManagedOperatorStates = new ArrayList<>(newJobVertex2.getParallelism());
		List<List<Collection<OperatorStateHandle>>> actualRawOperatorStates = new ArrayList<>(newJobVertex2.getParallelism());

		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {

			final List<OperatorID> operatorIds = newJobVertex2.getOperatorIDs();

			JobManagerTaskRestore taskRestore = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
			Assert.assertEquals(2L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			// operator 3
			{
				int operatorIndexInChain = 1;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				List<Collection<OperatorStateHandle>> actualSubManagedOperatorState = new ArrayList<>(1);
				actualSubManagedOperatorState.add(opState.getManagedOperatorState());

				List<Collection<OperatorStateHandle>> actualSubRawOperatorState = new ArrayList<>(1);
				actualSubRawOperatorState.add(opState.getRawOperatorState());

				actualManagedOperatorStates.add(actualSubManagedOperatorState);
				actualRawOperatorStates.add(actualSubRawOperatorState);
			}

			// operator 6
			{
				int operatorIndexInChain = 0;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));
				assertTrue(opState.getManagedOperatorState().isEmpty());
				assertTrue(opState.getRawOperatorState().isEmpty());

			}

			KeyGroupsStateHandle originalKeyedStateBackend = generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), false);
			KeyGroupsStateHandle originalKeyedStateRaw = generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), true);

			OperatorSubtaskState headOpState =
				stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIds.size() - 1));

			Collection<KeyedStateHandle> keyedStateBackend = headOpState.getManagedKeyedState();
			Collection<KeyedStateHandle> keyGroupStateRaw = headOpState.getRawKeyedState();


			compareKeyedState(Collections.singletonList(originalKeyedStateBackend), keyedStateBackend);
			compareKeyedState(Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
		}

		comparePartitionableState(expectedManagedOperatorStates.get(0), actualManagedOperatorStates);
		comparePartitionableState(expectedRawOperatorStates.get(0), actualRawOperatorStates);
	}

	/**
	 * Tests that the externalized checkpoint configuration is respected.
	 */
	@Test
	public void testExternalizedCheckpoints() throws Exception {
		try {
			final JobID jid = new JobID();
			final long timestamp = System.currentTimeMillis();

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.RETAIN_ON_FAILURE,
				true,
				false,
				0);
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);

			assertTrue(coord.triggerCheckpoint(timestamp, false));

			for (PendingCheckpoint checkpoint : coord.getPendingCheckpoints().values()) {
				CheckpointProperties props = checkpoint.getProps();
				CheckpointProperties expected = CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE);

				assertEquals(expected, props);
			}

			// the now we should have a completed checkpoint
			coord.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public static KeyGroupsStateHandle generateKeyGroupState(
			JobVertexID jobVertexID,
			KeyGroupRange keyGroupPartition, boolean rawState) throws IOException {

		List<Integer> testStatesLists = new ArrayList<>(keyGroupPartition.getNumberOfKeyGroups());

		// generate state for one keygroup
		for (int keyGroupIndex : keyGroupPartition) {
			int vertexHash = jobVertexID.hashCode();
			int seed = rawState ? (vertexHash * (31 + keyGroupIndex)) : (vertexHash + keyGroupIndex);
			Random random = new Random(seed);
			int simulatedStateValue = random.nextInt();
			testStatesLists.add(simulatedStateValue);
		}

		return generateKeyGroupState(keyGroupPartition, testStatesLists);
	}

	public static KeyGroupsStateHandle generateKeyGroupState(
			KeyGroupRange keyGroupRange,
			List<? extends Serializable> states) throws IOException {

		Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() == states.size());

		Tuple2<byte[], List<long[]>> serializedDataWithOffsets =
				serializeTogetherAndTrackOffsets(Collections.<List<? extends Serializable>>singletonList(states));

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, serializedDataWithOffsets.f1.get(0));

		ByteStreamStateHandle allSerializedStatesHandle = new ByteStreamStateHandle(
				String.valueOf(UUID.randomUUID()),
				serializedDataWithOffsets.f0);

		return new KeyGroupsStateHandle(keyGroupRangeOffsets, allSerializedStatesHandle);
	}

	public static Tuple2<byte[], List<long[]>> serializeTogetherAndTrackOffsets(
			List<List<? extends Serializable>> serializables) throws IOException {

		List<long[]> offsets = new ArrayList<>(serializables.size());
		List<byte[]> serializedGroupValues = new ArrayList<>();

		int runningGroupsOffset = 0;
		for(List<? extends Serializable> list : serializables) {

			long[] currentOffsets = new long[list.size()];
			offsets.add(currentOffsets);

			for (int i = 0; i < list.size(); ++i) {
				currentOffsets[i] = runningGroupsOffset;
				byte[] serializedValue = InstantiationUtil.serializeObject(list.get(i));
				serializedGroupValues.add(serializedValue);
				runningGroupsOffset += serializedValue.length;
			}
		}

		//write all generated values in a single byte array, which is index by groupOffsetsInFinalByteArray
		byte[] allSerializedValuesConcatenated = new byte[runningGroupsOffset];
		runningGroupsOffset = 0;
		for (byte[] serializedGroupValue : serializedGroupValues) {
			System.arraycopy(
					serializedGroupValue,
					0,
					allSerializedValuesConcatenated,
					runningGroupsOffset,
					serializedGroupValue.length);
			runningGroupsOffset += serializedGroupValue.length;
		}
		return new Tuple2<>(allSerializedValuesConcatenated, offsets);
	}

	public static OperatorStateHandle generatePartitionableStateHandle(
		JobVertexID jobVertexID,
		int index,
		int namedStates,
		int partitionsPerState,
		boolean rawState) throws IOException {

		Map<String, List<? extends Serializable>> statesListsMap = new HashMap<>(namedStates);

		for (int i = 0; i < namedStates; ++i) {
			List<Integer> testStatesLists = new ArrayList<>(partitionsPerState);
			// generate state
			int seed = jobVertexID.hashCode() * index + i * namedStates;
			if (rawState) {
				seed = (seed + 1) * 31;
			}
			Random random = new Random(seed);
			for (int j = 0; j < partitionsPerState; ++j) {
				int simulatedStateValue = random.nextInt();
				testStatesLists.add(simulatedStateValue);
			}
			statesListsMap.put("state-" + i, testStatesLists);
		}

		return generatePartitionableStateHandle(statesListsMap);
	}

	public static ChainedStateHandle<OperatorStateHandle> generateChainedPartitionableStateHandle(
			JobVertexID jobVertexID,
			int index,
			int namedStates,
			int partitionsPerState,
			boolean rawState) throws IOException {

		Map<String, List<? extends Serializable>> statesListsMap = new HashMap<>(namedStates);

		for (int i = 0; i < namedStates; ++i) {
			List<Integer> testStatesLists = new ArrayList<>(partitionsPerState);
			// generate state
			int seed = jobVertexID.hashCode() * index + i * namedStates;
			if (rawState) {
				seed = (seed + 1) * 31;
			}
			Random random = new Random(seed);
			for (int j = 0; j < partitionsPerState; ++j) {
				int simulatedStateValue = random.nextInt();
				testStatesLists.add(simulatedStateValue);
			}
			statesListsMap.put("state-" + i, testStatesLists);
		}

		return ChainedStateHandle.wrapSingleHandle(generatePartitionableStateHandle(statesListsMap));
	}

	private static OperatorStateHandle generatePartitionableStateHandle(
		Map<String, List<? extends Serializable>> states) throws IOException {

		List<List<? extends Serializable>> namedStateSerializables = new ArrayList<>(states.size());

		for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
			namedStateSerializables.add(entry.getValue());
		}

		Tuple2<byte[], List<long[]>> serializationWithOffsets = serializeTogetherAndTrackOffsets(namedStateSerializables);

		Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>(states.size());

		int idx = 0;
		for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
			offsetsMap.put(
				entry.getKey(),
				new OperatorStateHandle.StateMetaInfo(
					serializationWithOffsets.f1.get(idx),
					OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
			++idx;
		}

		ByteStreamStateHandle streamStateHandle = new ByteStreamStateHandle(
			String.valueOf(UUID.randomUUID()),
			serializationWithOffsets.f0);

		return new OperatorStreamStateHandle(offsetsMap, streamStateHandle);
	}

	static ExecutionJobVertex mockExecutionJobVertex(
			JobVertexID jobVertexID,
			int parallelism,
			int maxParallelism) {

		return mockExecutionJobVertex(
			jobVertexID,
			Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID)),
			parallelism,
			maxParallelism
		);
	}

	static ExecutionJobVertex mockExecutionJobVertex(
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		int parallelism,
		int maxParallelism) {
		final ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);

		ExecutionVertex[] executionVertices = new ExecutionVertex[parallelism];

		for (int i = 0; i < parallelism; i++) {
			executionVertices[i] = mockExecutionVertex(
				new ExecutionAttemptID(),
				jobVertexID,
				jobVertexIDs,
				parallelism,
				maxParallelism,
				ExecutionState.RUNNING);

			when(executionVertices[i].getParallelSubtaskIndex()).thenReturn(i);
		}

		when(executionJobVertex.getJobVertexId()).thenReturn(jobVertexID);
		when(executionJobVertex.getTaskVertices()).thenReturn(executionVertices);
		when(executionJobVertex.getParallelism()).thenReturn(parallelism);
		when(executionJobVertex.getMaxParallelism()).thenReturn(maxParallelism);
		when(executionJobVertex.isMaxParallelismConfigured()).thenReturn(true);
		when(executionJobVertex.getOperatorIDs()).thenReturn(jobVertexIDs);
		when(executionJobVertex.getUserDefinedOperatorIDs()).thenReturn(Arrays.asList(new OperatorID[jobVertexIDs.size()]));

		return executionJobVertex;
	}

	static ExecutionVertex mockExecutionVertex(ExecutionAttemptID attemptID) {
		JobVertexID jobVertexID = new JobVertexID();
		return mockExecutionVertex(
			attemptID,
			jobVertexID,
			Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID)),
			1,
			1,
			ExecutionState.RUNNING);
	}

	private static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		int parallelism,
		int maxParallelism,
		ExecutionState state,
		ExecutionState ... successiveStates) {

		ExecutionVertex vertex = mock(ExecutionVertex.class);

		final Execution exec = spy(new Execution(
			mock(Executor.class),
			vertex,
			1,
			1L,
			1L,
			Time.milliseconds(500L)
		));
		when(exec.getAttemptId()).thenReturn(attemptID);
		when(exec.getState()).thenReturn(state, successiveStates);

		when(vertex.getJobvertexId()).thenReturn(jobVertexID);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(maxParallelism);

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getOperatorIDs()).thenReturn(jobVertexIDs);
		
		when(vertex.getJobVertex()).thenReturn(jobVertex);

		return vertex;
	}

	static TaskStateSnapshot mockSubtaskState(
		JobVertexID jobVertexID,
		int index,
		KeyGroupRange keyGroupRange) throws IOException {

		OperatorStateHandle partitionableState = generatePartitionableStateHandle(jobVertexID, index, 2, 8, false);
		KeyGroupsStateHandle partitionedKeyGroupState = generateKeyGroupState(jobVertexID, keyGroupRange, false);

		TaskStateSnapshot subtaskStates = spy(new TaskStateSnapshot());
		OperatorSubtaskState subtaskState = spy(new OperatorSubtaskState(
			partitionableState, null, partitionedKeyGroupState, null)
		);

		subtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID), subtaskState);

		return subtaskStates;
	}

	public static void verifyStateRestore(
			JobVertexID jobVertexID, ExecutionJobVertex executionJobVertex,
			List<KeyGroupRange> keyGroupPartitions) throws Exception {

		for (int i = 0; i < executionJobVertex.getParallelism(); i++) {

			JobManagerTaskRestore taskRestore = executionJobVertex.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
			Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			OperatorSubtaskState operatorState = stateSnapshot.getSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID));

			ChainedStateHandle<OperatorStateHandle> expectedOpStateBackend =
					generateChainedPartitionableStateHandle(jobVertexID, i, 2, 8, false);

			assertTrue(CommonTestUtils.isStreamContentEqual(
					expectedOpStateBackend.get(0).openInputStream(),
					operatorState.getManagedOperatorState().iterator().next().openInputStream()));

			KeyGroupsStateHandle expectPartitionedKeyGroupState = generateKeyGroupState(
					jobVertexID, keyGroupPartitions.get(i), false);
			compareKeyedState(Collections.singletonList(expectPartitionedKeyGroupState), operatorState.getManagedKeyedState());
		}
	}

	public static void compareKeyedState(
			Collection<KeyGroupsStateHandle> expectPartitionedKeyGroupState,
			Collection<? extends KeyedStateHandle> actualPartitionedKeyGroupState) throws Exception {

		KeyGroupsStateHandle expectedHeadOpKeyGroupStateHandle = expectPartitionedKeyGroupState.iterator().next();
		int expectedTotalKeyGroups = expectedHeadOpKeyGroupStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
		int actualTotalKeyGroups = 0;
		for(KeyedStateHandle keyedStateHandle: actualPartitionedKeyGroupState) {
			assertTrue(keyedStateHandle instanceof KeyGroupsStateHandle);

			actualTotalKeyGroups += keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
		}

		assertEquals(expectedTotalKeyGroups, actualTotalKeyGroups);

		try (FSDataInputStream inputStream = expectedHeadOpKeyGroupStateHandle.openInputStream()) {
			for (int groupId : expectedHeadOpKeyGroupStateHandle.getKeyGroupRange()) {
				long offset = expectedHeadOpKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
				inputStream.seek(offset);
				int expectedKeyGroupState =
						InstantiationUtil.deserializeObject(inputStream, Thread.currentThread().getContextClassLoader());
				for (KeyedStateHandle oneActualKeyedStateHandle : actualPartitionedKeyGroupState) {

					assertTrue(oneActualKeyedStateHandle instanceof KeyGroupsStateHandle);

					KeyGroupsStateHandle oneActualKeyGroupStateHandle = (KeyGroupsStateHandle) oneActualKeyedStateHandle;
					if (oneActualKeyGroupStateHandle.getKeyGroupRange().contains(groupId)) {
						long actualOffset = oneActualKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
						try (FSDataInputStream actualInputStream = oneActualKeyGroupStateHandle.openInputStream()) {
							actualInputStream.seek(actualOffset);
							int actualGroupState = InstantiationUtil.
									deserializeObject(actualInputStream, Thread.currentThread().getContextClassLoader());
							assertEquals(expectedKeyGroupState, actualGroupState);
						}
					}
				}
			}
		}
	}

	public static void comparePartitionableState(
			List<ChainedStateHandle<OperatorStateHandle>> expected,
			List<List<Collection<OperatorStateHandle>>> actual) throws Exception {

		List<String> expectedResult = new ArrayList<>();
		for (ChainedStateHandle<OperatorStateHandle> chainedStateHandle : expected) {
			for (int i = 0; i < chainedStateHandle.getLength(); ++i) {
				OperatorStateHandle operatorStateHandle = chainedStateHandle.get(i);
				collectResult(i, operatorStateHandle, expectedResult);
			}
		}
		Collections.sort(expectedResult);

		List<String> actualResult = new ArrayList<>();
		for (List<Collection<OperatorStateHandle>> collectionList : actual) {
			if (collectionList != null) {
				for (int i = 0; i < collectionList.size(); ++i) {
					Collection<OperatorStateHandle> stateHandles = collectionList.get(i);
					Assert.assertNotNull(stateHandles);
					for (OperatorStateHandle operatorStateHandle : stateHandles) {
						collectResult(i, operatorStateHandle, actualResult);
					}
				}
			}
		}

		Collections.sort(actualResult);
		Assert.assertEquals(expectedResult, actualResult);
	}

	private static void collectResult(int opIdx, OperatorStateHandle operatorStateHandle, List<String> resultCollector) throws Exception {
		try (FSDataInputStream in = operatorStateHandle.openInputStream()) {
			for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry : operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {
				for (long offset : entry.getValue().getOffsets()) {
					in.seek(offset);
					Integer state = InstantiationUtil.
							deserializeObject(in, Thread.currentThread().getContextClassLoader());
					resultCollector.add(opIdx + " : " + entry.getKey() + " : " + state);
				}
			}
		}
	}


	@Test
	public void testCreateKeyGroupPartitions() {
		testCreateKeyGroupPartitions(1, 1);
		testCreateKeyGroupPartitions(13, 1);
		testCreateKeyGroupPartitions(13, 2);
		testCreateKeyGroupPartitions(Short.MAX_VALUE, 1);
		testCreateKeyGroupPartitions(Short.MAX_VALUE, 13);
		testCreateKeyGroupPartitions(Short.MAX_VALUE, Short.MAX_VALUE);

		Random r = new Random(1234);
		for (int k = 0; k < 1000; ++k) {
			int maxParallelism = 1 + r.nextInt(Short.MAX_VALUE - 1);
			int parallelism = 1 + r.nextInt(maxParallelism);
			testCreateKeyGroupPartitions(maxParallelism, parallelism);
		}
	}

	@Test
	public void testStopPeriodicScheduler() throws Exception {
		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			chkConfig,
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		// Periodic
		try {
			coord.triggerCheckpoint(
					System.currentTimeMillis(),
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
					null,
					true,
					false);
			fail("The triggerCheckpoint call expected an exception");
		} catch (CheckpointException e) {
			assertEquals(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN, e.getCheckpointFailureReason());
		}

		// Not periodic
		try {
			coord.triggerCheckpoint(
					System.currentTimeMillis(),
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
					null,
					false,
					false);
		} catch (CheckpointException e) {
			fail("Unexpected exception : " + e.getCheckpointFailureReason().message());
		}
	}

	private void testCreateKeyGroupPartitions(int maxParallelism, int parallelism) {
		List<KeyGroupRange> ranges = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism, parallelism);
		for (int i = 0; i < maxParallelism; ++i) {
			KeyGroupRange range = ranges.get(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, i));
			if (!range.contains(i)) {
				Assert.fail("Could not find expected key-group " + i + " in range " + range);
			}
		}
	}

	@Test
	public void testPartitionableStateRepartitioning() {
		Random r = new Random(42);

		for (int run = 0; run < 10000; ++run) {
			int oldParallelism = 1 + r.nextInt(9);
			int newParallelism = 1 + r.nextInt(9);

			int numNamedStates = 1 + r.nextInt(9);
			int maxPartitionsPerState = 1 + r.nextInt(9);

			doTestPartitionableStateRepartitioning(
					r, oldParallelism, newParallelism, numNamedStates, maxPartitionsPerState);
		}
	}

	private void doTestPartitionableStateRepartitioning(
			Random r, int oldParallelism, int newParallelism, int numNamedStates, int maxPartitionsPerState) {

		List<List<OperatorStateHandle>> previousParallelOpInstanceStates = new ArrayList<>(oldParallelism);

		for (int i = 0; i < oldParallelism; ++i) {
			Path fakePath = new Path("/fake-" + i);
			Map<String, OperatorStateHandle.StateMetaInfo> namedStatesToOffsets = new HashMap<>();
			int off = 0;
			for (int s = 0; s < numNamedStates - 1; ++s) {
				long[] offs = new long[1 + r.nextInt(maxPartitionsPerState)];

				for (int o = 0; o < offs.length; ++o) {
					offs[o] = off;
					++off;
				}

				OperatorStateHandle.Mode mode = r.nextInt(10) == 0 ?
					OperatorStateHandle.Mode.UNION : OperatorStateHandle.Mode.SPLIT_DISTRIBUTE;
				namedStatesToOffsets.put(
						"State-" + s,
						new OperatorStateHandle.StateMetaInfo(offs, mode));

			}

			if (numNamedStates % 2 == 0) {
				// finally add a broadcast state
				long[] offs = {off + 1, off + 2, off + 3, off + 4};

				namedStatesToOffsets.put(
						"State-" + (numNamedStates - 1),
						new OperatorStateHandle.StateMetaInfo(offs, OperatorStateHandle.Mode.BROADCAST));
			}

			previousParallelOpInstanceStates.add(
					Collections.singletonList(new OperatorStreamStateHandle(namedStatesToOffsets, new FileStateHandle(fakePath, -1))));
		}

		Map<StreamStateHandle, Map<String, List<Long>>> expected = new HashMap<>();

		int taskIndex = 0;
		int expectedTotalPartitions = 0;
		for (List<OperatorStateHandle> previousParallelOpInstanceState : previousParallelOpInstanceStates) {
			Assert.assertEquals(1, previousParallelOpInstanceState.size());

			for (OperatorStateHandle psh : previousParallelOpInstanceState) {
				Map<String, OperatorStateHandle.StateMetaInfo> offsMap = psh.getStateNameToPartitionOffsets();
				Map<String, List<Long>> offsMapWithList = new HashMap<>(offsMap.size());
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> e : offsMap.entrySet()) {

					long[] offs = e.getValue().getOffsets();
					int replication;
					switch (e.getValue().getDistributionMode()) {
						case UNION:
							replication = newParallelism;
							break;
						case BROADCAST:
							int extra = taskIndex < (newParallelism % oldParallelism) ? 1 : 0;
							replication = newParallelism / oldParallelism + extra;
							break;
						case SPLIT_DISTRIBUTE:
							replication = 1;
							break;
						default:
							throw new RuntimeException("Unknown distribution mode " + e.getValue().getDistributionMode());
					}

					if (replication > 0) {
						expectedTotalPartitions += replication * offs.length;
						List<Long> offsList = new ArrayList<>(offs.length);

						for (long off : offs) {
							for (int p = 0; p < replication; ++p) {
								offsList.add(off);
							}
						}
						offsMapWithList.put(e.getKey(), offsList);
					}
				}

				if (!offsMapWithList.isEmpty()) {
					expected.put(psh.getDelegateStateHandle(), offsMapWithList);
				}
				taskIndex++;
			}
		}

		OperatorStateRepartitioner repartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		List<List<OperatorStateHandle>> pshs =
				repartitioner.repartitionState(previousParallelOpInstanceStates, oldParallelism, newParallelism);

		Map<StreamStateHandle, Map<String, List<Long>>> actual = new HashMap<>();

		int minCount = Integer.MAX_VALUE;
		int maxCount = 0;
		int actualTotalPartitions = 0;
		for (int p = 0; p < newParallelism; ++p) {
			int partitionCount = 0;

			Collection<OperatorStateHandle> pshc = pshs.get(p);
			for (OperatorStateHandle sh : pshc) {
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> namedState : sh.getStateNameToPartitionOffsets().entrySet()) {

					Map<String, List<Long>> stateToOffsets = actual.get(sh.getDelegateStateHandle());
					if (stateToOffsets == null) {
						stateToOffsets = new HashMap<>();
						actual.put(sh.getDelegateStateHandle(), stateToOffsets);
					}

					List<Long> actualOffs = stateToOffsets.get(namedState.getKey());
					if (actualOffs == null) {
						actualOffs = new ArrayList<>();
						stateToOffsets.put(namedState.getKey(), actualOffs);
					}
					long[] add = namedState.getValue().getOffsets();
					for (long l : add) {
						actualOffs.add(l);
					}

					partitionCount += namedState.getValue().getOffsets().length;
				}
			}

			minCount = Math.min(minCount, partitionCount);
			maxCount = Math.max(maxCount, partitionCount);
			actualTotalPartitions += partitionCount;
		}

		for (Map<String, List<Long>> v : actual.values()) {
			for (List<Long> l : v.values()) {
				Collections.sort(l);
			}
		}

		// if newParallelism equals to oldParallelism, we would only redistribute UNION state if possible.
		if (oldParallelism != newParallelism) {
			int maxLoadDiff = maxCount - minCount;
			Assert.assertTrue("Difference in partition load is > 1 : " + maxLoadDiff, maxLoadDiff <= 1);
		}
		Assert.assertEquals(expectedTotalPartitions, actualTotalPartitions);
		Assert.assertEquals(expected, actual);
	}

	/**
	 * Tests that the pending checkpoint stats callbacks are created.
	 */
	@Test
	public void testCheckpointStatsTrackerPendingCheckpointCallback() {
		final long timestamp = System.currentTimeMillis();
		ExecutionVertex vertex1 = mockExecutionVertex(new ExecutionAttemptID());

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			chkConfig,
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		coord.setCheckpointStatsTracker(tracker);

		when(tracker.reportPendingCheckpoint(anyLong(), anyLong(), any(CheckpointProperties.class)))
			.thenReturn(mock(PendingCheckpointStats.class));

		// Trigger a checkpoint and verify callback
		assertTrue(coord.triggerCheckpoint(timestamp, false));

		verify(tracker, times(1))
			.reportPendingCheckpoint(eq(1L), eq(timestamp), eq(CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)));
	}

	/**
	 * Tests that the restore callbacks are called if registered.
	 */
	@Test
	public void testCheckpointStatsTrackerRestoreCallback() throws Exception {
		ExecutionVertex vertex1 = mockExecutionVertex(new ExecutionAttemptID());

		StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(1);

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			chkConfig,
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new StandaloneCheckpointIDCounter(),
			store,
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		store.addCheckpoint(new CompletedCheckpoint(
			new JobID(),
			0,
			0,
			0,
			Collections.<OperatorID, OperatorState>emptyMap(),
			Collections.<MasterState>emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new TestCompletedCheckpointStorageLocation()));

		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		coord.setCheckpointStatsTracker(tracker);

		assertTrue(coord.restoreLatestCheckpointedState(Collections.<JobVertexID, ExecutionJobVertex>emptyMap(), false, true));

		verify(tracker, times(1))
			.reportRestoredCheckpoint(any(RestoredCheckpointStats.class));
	}

	@Test
	public void testSharedStateRegistrationOnRestore() throws Exception {

		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();

		int parallelism1 = 2;
		int maxParallelism1 = 4;

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices =
			allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		RecoverableCompletedCheckpointStore store = new RecoverableCompletedCheckpointStore(10);

		final List<SharedStateRegistry> createdSharedStateRegistries = new ArrayList<>(2);

		// set up the coordinator and validate the initial state
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			chkConfig,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			store,
			new MemoryStateBackend(),
			Executors.directExecutor(),
				deleteExecutor -> {
					SharedStateRegistry instance = new SharedStateRegistry(deleteExecutor);
					createdSharedStateRegistries.add(instance);
					return instance;
				},
			failureManager);

		final int numCheckpoints = 3;

		List<KeyGroupRange> keyGroupPartitions1 =
			StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);

		for (int i = 0; i < numCheckpoints; ++i) {
			performIncrementalCheckpoint(jid, coord, jobVertex1, keyGroupPartitions1, timestamp + i, i);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();
		assertEquals(numCheckpoints, completedCheckpoints.size());

		int sharedHandleCount = 0;

		List<Map<StateHandleID, StreamStateHandle>> sharedHandlesByCheckpoint = new ArrayList<>(numCheckpoints);

		for (int i = 0; i < numCheckpoints; ++i) {
			sharedHandlesByCheckpoint.add(new HashMap<>(2));
		}

		int cp = 0;
		for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
			for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
				for (OperatorSubtaskState subtaskState : taskState.getStates()) {
					for (KeyedStateHandle keyedStateHandle : subtaskState.getManagedKeyedState()) {
						// test we are once registered with the current registry
						verify(keyedStateHandle, times(1)).registerSharedStates(createdSharedStateRegistries.get(0));
						IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle = (IncrementalRemoteKeyedStateHandle) keyedStateHandle;

						sharedHandlesByCheckpoint.get(cp).putAll(incrementalKeyedStateHandle.getSharedState());

						for (StreamStateHandle streamStateHandle : incrementalKeyedStateHandle.getSharedState().values()) {
							assertTrue(!(streamStateHandle instanceof PlaceholderStreamStateHandle));
							verify(streamStateHandle, never()).discardState();
							++sharedHandleCount;
						}

						for (StreamStateHandle streamStateHandle : incrementalKeyedStateHandle.getPrivateState().values()) {
							verify(streamStateHandle, never()).discardState();
						}

						verify(incrementalKeyedStateHandle.getMetaStateHandle(), never()).discardState();
					}

					verify(subtaskState, never()).discardState();
				}
			}
			++cp;
		}

		// 2 (parallelism) x (1 (CP0) + 2 (CP1) + 2 (CP2)) = 10
		assertEquals(10, sharedHandleCount);

		// discard CP0
		store.removeOldestCheckpoint();

		// we expect no shared state was discarded because the state of CP0 is still referenced by CP1
		for (Map<StateHandleID, StreamStateHandle> cpList : sharedHandlesByCheckpoint) {
			for (StreamStateHandle streamStateHandle : cpList.values()) {
				verify(streamStateHandle, never()).discardState();
			}
		}

		// shutdown the store
		store.shutdown(JobStatus.SUSPENDED);

		// restore the store
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexID1, jobVertex1);
		coord.restoreLatestCheckpointedState(tasks, true, false);

		// validate that all shared states are registered again after the recovery.
		cp = 0;
		for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
			for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
				for (OperatorSubtaskState subtaskState : taskState.getStates()) {
					for (KeyedStateHandle keyedStateHandle : subtaskState.getManagedKeyedState()) {
						VerificationMode verificationMode;
						// test we are once registered with the new registry
						if (cp > 0) {
							verificationMode = times(1);
						} else {
							verificationMode = never();
						}

						//check that all are registered with the new registry
						verify(keyedStateHandle, verificationMode).registerSharedStates(createdSharedStateRegistries.get(1));
					}
				}
			}
			++cp;
		}

		// discard CP1
		store.removeOldestCheckpoint();

		// we expect that all shared state from CP0 is no longer referenced and discarded. CP2 is still live and also
		// references the state from CP1, so we expect they are not discarded.
		for (Map<StateHandleID, StreamStateHandle> cpList : sharedHandlesByCheckpoint) {
			for (Map.Entry<StateHandleID, StreamStateHandle> entry : cpList.entrySet()) {
				String key = entry.getKey().getKeyString();
				int belongToCP = Integer.parseInt(String.valueOf(key.charAt(key.length() - 1)));
				if (belongToCP == 0) {
					verify(entry.getValue(), times(1)).discardState();
				} else {
					verify(entry.getValue(), never()).discardState();
				}
			}
		}

		// discard CP2
		store.removeOldestCheckpoint();

		// we expect all shared state was discarded now, because all CPs are
		for (Map<StateHandleID, StreamStateHandle> cpList : sharedHandlesByCheckpoint) {
			for (StreamStateHandle streamStateHandle : cpList.values()) {
				verify(streamStateHandle, times(1)).discardState();
			}
		}
	}

	@Test
	public void jobFailsIfInFlightSynchronousSavepointIsDiscarded() throws Exception {
		final Tuple2<Integer, Throwable> invocationCounterAndException = Tuple2.of(0, null);
		final Throwable expectedRootCause = new IOException("Custom-Exception");

		final JobID jobId = new JobID();

		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		final ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
		final ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

		// set up the coordinator and validate the initial state
		final CheckpointCoordinator coordinator = getCheckpointCoordinator(jobId, vertex1, vertex2,
				new CheckpointFailureManager(
					0,
					new CheckpointFailureManager.FailJobCallback() {
						@Override
						public void failJob(Throwable cause) {
							invocationCounterAndException.f0 += 1;
							invocationCounterAndException.f1 = cause;
						}

						@Override
						public void failJobDueToTaskFailure(Throwable cause, ExecutionAttemptID failingTask) {
							throw new AssertionError("This method should not be called for the test.");
						}
					}));

		final CompletableFuture<CompletedCheckpoint> savepointFuture = coordinator
				.triggerSynchronousSavepoint(10L, false, "test-dir");

		final PendingCheckpoint syncSavepoint = declineSynchronousSavepoint(jobId, coordinator, attemptID1, expectedRootCause);

		assertTrue(syncSavepoint.isDiscarded());

		try {
			savepointFuture.get();
			fail("Expected Exception not found.");
		} catch (ExecutionException e) {
			final Throwable cause = ExceptionUtils.stripExecutionException(e);
			assertTrue(cause instanceof CheckpointException);
			assertEquals(expectedRootCause.getMessage(), cause.getCause().getMessage());
		}

		assertEquals(1L, invocationCounterAndException.f0.intValue());
		assertTrue(
				invocationCounterAndException.f1 instanceof CheckpointException &&
				invocationCounterAndException.f1.getCause().getMessage().equals(expectedRootCause.getMessage()));

		coordinator.shutdown(JobStatus.FAILING);
	}

	/**
	 * Tests that do not trigger checkpoint when stop the coordinator after the eager pre-check.
	 */
	@Test
	public void testTriggerCheckpointAfterCancel() throws Exception {
		ExecutionVertex vertex1 = mockExecutionVertex(new ExecutionAttemptID());

		// set up the coordinator
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		TestingCheckpointIDCounter idCounter = new TestingCheckpointIDCounter();
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			chkConfig,
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			idCounter,
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);
		idCounter.setOwner(coord);

		try {
			// start the coordinator
			coord.startCheckpointScheduler();
			try {
				coord.triggerCheckpoint(System.currentTimeMillis(), CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION), null, true, false);
				fail("should not trigger periodic checkpoint after stop the coordinator.");
			} catch (CheckpointException e) {
				assertEquals(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN, e.getCheckpointFailureReason());
			}
		} finally {
			coord.shutdown(JobStatus.FINISHED);
		}
	}

	private CheckpointCoordinator getCheckpointCoordinator(
			final JobID jobId,
			final ExecutionVertex vertex1,
			final ExecutionVertex vertex2,
			final CheckpointFailureManager failureManager) {

		final CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true,
				false,
				0);

		return new CheckpointCoordinator(
				jobId,
				chkConfig,
				new ExecutionVertex[]{vertex1, vertex2},
				new ExecutionVertex[]{vertex1, vertex2},
				new ExecutionVertex[]{vertex1, vertex2},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY,
				failureManager);
	}

	private PendingCheckpoint declineSynchronousSavepoint(
			final JobID jobId,
			final CheckpointCoordinator coordinator,
			final ExecutionAttemptID attemptID,
			final Throwable reason) {

		final long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
		final PendingCheckpoint checkpoint = coordinator.getPendingCheckpoints().get(checkpointId);
		coordinator.receiveDeclineMessage(new DeclineCheckpoint(jobId, attemptID, checkpointId, reason), TASK_MANAGER_LOCATION_INFO);
		return checkpoint;
	}

	private void performIncrementalCheckpoint(
		JobID jid,
		CheckpointCoordinator coord,
		ExecutionJobVertex jobVertex1,
		List<KeyGroupRange> keyGroupPartitions1,
		long timestamp,
		int cpSequenceNumber) throws Exception {

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {

			KeyGroupRange keyGroupRange = keyGroupPartitions1.get(index);

			Map<StateHandleID, StreamStateHandle> privateState = new HashMap<>();
			privateState.put(
				new StateHandleID("private-1"),
				spy(new ByteStreamStateHandle("private-1", new byte[]{'p'})));

			Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();

			// let all but the first CP overlap by one shared state.
			if (cpSequenceNumber > 0) {
				sharedState.put(
					new StateHandleID("shared-" + (cpSequenceNumber - 1)),
					spy(new PlaceholderStreamStateHandle()));
			}

			sharedState.put(
				new StateHandleID("shared-" + cpSequenceNumber),
				spy(new ByteStreamStateHandle("shared-" + cpSequenceNumber + "-" + keyGroupRange, new byte[]{'s'})));

			IncrementalRemoteKeyedStateHandle managedState =
				spy(new IncrementalRemoteKeyedStateHandle(
					new UUID(42L, 42L),
					keyGroupRange,
					checkpointId,
					sharedState,
					privateState,
					spy(new ByteStreamStateHandle("meta", new byte[]{'m'}))));

			OperatorSubtaskState operatorSubtaskState =
				spy(new OperatorSubtaskState(
					StateObjectCollection.empty(),
					StateObjectCollection.empty(),
					StateObjectCollection.singleton(managedState),
					StateObjectCollection.empty()));

			Map<OperatorID, OperatorSubtaskState> opStates = new HashMap<>();

			opStates.put(jobVertex1.getOperatorIDs().get(0), operatorSubtaskState);

			TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot(opStates);

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				new CheckpointMetrics(),
				taskStateSnapshot);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
		}
	}

	private Execution mockExecution() {
		Execution mock = mock(Execution.class);
		when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(mock.getState()).thenReturn(ExecutionState.RUNNING);
		return mock;
	}

	private ExecutionVertex mockExecutionVertex(Execution execution, JobVertexID vertexId, int subtask, int parallelism) {
		ExecutionVertex mock = mock(ExecutionVertex.class);
		when(mock.getJobvertexId()).thenReturn(vertexId);
		when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
		when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
		when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(mock.getMaxParallelism()).thenReturn(parallelism);
		return mock;
	}

	private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(vertices.length);
		when(vertex.getMaxParallelism()).thenReturn(vertices.length);
		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getTaskVertices()).thenReturn(vertices);
		when(vertex.getOperatorIDs()).thenReturn(Collections.singletonList(OperatorID.fromJobVertexID(id)));
		when(vertex.getUserDefinedOperatorIDs()).thenReturn(Collections.<OperatorID>singletonList(null));

		for (ExecutionVertex v : vertices) {
			when(v.getJobVertex()).thenReturn(vertex);
		}
		return vertex;
	}

	private static class TestingCheckpointIDCounter extends StandaloneCheckpointIDCounter {
		private CheckpointCoordinator owner;

		@Override
		public long getAndIncrement() throws Exception {
			checkNotNull(owner);
			owner.stopCheckpointScheduler();
			return super.getAndIncrement();
		}

		void setOwner(CheckpointCoordinator coordinator) {
			this.owner = checkNotNull(coordinator);
		}
	}
}
