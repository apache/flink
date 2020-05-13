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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testutils.RecoverableCompletedCheckpointStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionJobVertex;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
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

	private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	@Before
	public void setUp() throws Exception {
		manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
	}

	@Test
	public void testCheckpointAbortsIfTriggerTasksAreNotExecuted() {
		try {

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator();

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertTrue(checkpointFuture.isCompletedExceptionally());

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
			CheckpointCoordinator coord = getCheckpointCoordinator();

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertTrue(checkpointFuture.isCompletedExceptionally());

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
			CheckpointCoordinator coord = getCheckpointCoordinator();

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertTrue(checkpointFuture.isCompletedExceptionally());

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
	public void testTriggerAndDeclineCheckpointThenFailureManagerThrowsException() throws Exception {
		final JobID jid = new JobID();

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
			final CompletableFuture<CompletedCheckpoint> checkPointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkPointFuture.isCompletedExceptionally());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			// acknowledge from one of the tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId), TASK_MANAGER_LOCATION_INFO);
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.areTasksFullyAcknowledged());

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

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
			ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture.isCompletedExceptionally());

			// validate that we have a pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// we have one task scheduled that will cancel after timeout
			assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			assertNotNull(checkpoint);
			assertEquals(checkpointId, checkpoint.getCheckpointId());
			assertEquals(jid, checkpoint.getJobId());
			assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint.getOperatorStates().size());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.areTasksFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			verify(vertex1.getCurrentExecutionAttempt()).triggerCheckpoint(checkpointId, checkpoint.getCheckpointTimestamp(), CheckpointOptions.forCheckpointWithDefaultLocation());
			verify(vertex2.getCurrentExecutionAttempt()).triggerCheckpoint(checkpointId, checkpoint.getCheckpointTimestamp(), CheckpointOptions.forCheckpointWithDefaultLocation());

			// acknowledge from one of the tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId), "Unknown location");
			assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.areTasksFullyAcknowledged());

			// acknowledge the same task again (should not matter)
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId), "Unknown location");
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.areTasksFullyAcknowledged());

			// decline checkpoint from the other task, this should cancel the checkpoint
			// and trigger a new one
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);
			assertTrue(checkpoint.isDiscarded());

			// the canceler is also removed
			assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

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

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
			ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);
			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture1 = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture1.isCompletedExceptionally());

			// trigger second checkpoint, should also succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture2 = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture2.isCompletedExceptionally());

			// validate that we have a pending checkpoint
			assertEquals(2, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(2, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			Iterator<Map.Entry<Long, PendingCheckpoint>> it = coord.getPendingCheckpoints().entrySet().iterator();
			long checkpoint1Id = it.next().getKey();
			long checkpoint2Id = it.next().getKey();
			PendingCheckpoint checkpoint1 = coord.getPendingCheckpoints().get(checkpoint1Id);
			PendingCheckpoint checkpoint2 = coord.getPendingCheckpoints().get(checkpoint2Id);

			assertNotNull(checkpoint1);
			assertEquals(checkpoint1Id, checkpoint1.getCheckpointId());
			assertEquals(jid, checkpoint1.getJobId());
			assertEquals(2, checkpoint1.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint1.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint1.getOperatorStates().size());
			assertFalse(checkpoint1.isDiscarded());
			assertFalse(checkpoint1.areTasksFullyAcknowledged());

			assertNotNull(checkpoint2);
			assertEquals(checkpoint2Id, checkpoint2.getCheckpointId());
			assertEquals(jid, checkpoint2.getJobId());
			assertEquals(2, checkpoint2.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint2.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint2.getOperatorStates().size());
			assertFalse(checkpoint2.isDiscarded());
			assertFalse(checkpoint2.areTasksFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint1Id), any(Long.class), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint1Id), any(Long.class), any(CheckpointOptions.class));
			}

			// check that the vertices received the trigger checkpoint message for the second checkpoint
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint2Id), any(Long.class), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpoint2Id), any(Long.class), any(CheckpointOptions.class));
			}

			// decline checkpoint from one of the tasks, this should cancel the checkpoint
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id), TASK_MANAGER_LOCATION_INFO);
			assertTrue(checkpoint1.isDiscarded());

			// validate that we have only one pending checkpoint left
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

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
			assertFalse(checkpointNew.areTasksFullyAcknowledged());
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

			// create some mock Execution vertices that receive the checkpoint trigger messages
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
			ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture.isCompletedExceptionally());

			// validate that we have a pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			assertNotNull(checkpoint);
			assertEquals(checkpointId, checkpoint.getCheckpointId());
			assertEquals(jid, checkpoint.getJobId());
			assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint.getOperatorStates().size());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.areTasksFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), any(Long.class), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), any(Long.class), any(CheckpointOptions.class));
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
			assertFalse(checkpoint.areTasksFullyAcknowledged());
			verify(taskOperatorSubtaskStates2, never()).registerSharedStates(any(SharedStateRegistry.class));

			// acknowledge the same task again (should not matter)
			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.areTasksFullyAcknowledged());
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
			assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			// validate that the subtasks states have registered their shared states.
			{
				verify(subtaskState1, times(1)).registerSharedStates(any(SharedStateRegistry.class));
				verify(subtaskState2, times(1)).registerSharedStates(any(SharedStateRegistry.class));
			}

			// validate that the relevant tasks got a confirmation message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), any(Long.class), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), any(Long.class), any(CheckpointOptions.class));
			}

			CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, success.getJobId());
			assertEquals(checkpoint.getCheckpointId(), success.getCheckpointID());
			assertEquals(2, success.getOperatorStates().size());

			// ---------------
			// trigger another checkpoint and see that this one replaces the other checkpoint
			// ---------------
			coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();

			long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

			CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, successNew.getJobId());
			assertEquals(checkpointIdNew, successNew.getCheckpointID());
			assertTrue(successNew.getOperatorStates().isEmpty());

			// validate that the relevant tasks got a confirmation message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), any(Long.class), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), any(Long.class), any(CheckpointOptions.class));

				verify(vertex1.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), any(Long.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), any(Long.class));
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
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setCheckpointCoordinatorConfiguration(CheckpointCoordinatorConfiguration.builder().setMaxConcurrentCheckpoints(Integer.MAX_VALUE).build())
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex1, triggerVertex2 })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture1 = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture1.isCompletedExceptionally());

			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending1 = coord.getPendingCheckpoints().values().iterator().next();
			long checkpointId1 = pending1.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), any(Long.class), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), any(Long.class), any(CheckpointOptions.class));

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1), TASK_MANAGER_LOCATION_INFO);

			// start the second checkpoint
			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture2 = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture2.isCompletedExceptionally());

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
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), any(Long.class), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), any(Long.class), any(CheckpointOptions.class));

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
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId1), any(Long.class));

			// send the last remaining ack for the second checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2), TASK_MANAGER_LOCATION_INFO);

			// now, the second checkpoint should be confirmed
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertTrue(pending2.isDiscarded());

			// the second commit message should be out
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId2), any(Long.class));

			// validate the committed checkpoints
			List<CompletedCheckpoint> scs = coord.getSuccessfulCheckpoints();

			CompletedCheckpoint sc1 = scs.get(0);
			assertEquals(checkpointId1, sc1.getCheckpointID());
			assertEquals(jid, sc1.getJobId());
			assertTrue(sc1.getOperatorStates().isEmpty());

			CompletedCheckpoint sc2 = scs.get(1);
			assertEquals(checkpointId2, sc2.getCheckpointID());
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
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setCheckpointCoordinatorConfiguration(CheckpointCoordinatorConfiguration.builder().setMaxConcurrentCheckpoints(Integer.MAX_VALUE).build())
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex1, triggerVertex2 })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(10))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture1 = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture1.isCompletedExceptionally());

			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending1 = coord.getPendingCheckpoints().values().iterator().next();
			long checkpointId1 = pending1.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), any(Long.class), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId1), any(Long.class), any(CheckpointOptions.class));

			OperatorID opID1 = OperatorID.fromJobVertexID(ackVertex1.getJobvertexId());
			OperatorID opID2 = OperatorID.fromJobVertexID(ackVertex2.getJobvertexId());
			OperatorID opID3 = OperatorID.fromJobVertexID(ackVertex3.getJobvertexId());

			TaskStateSnapshot taskOperatorSubtaskStates11 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates12 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates13 = spy(new TaskStateSnapshot());

			OperatorSubtaskState subtaskState11 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState12 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState13 = mock(OperatorSubtaskState.class);
			taskOperatorSubtaskStates11.putSubtaskStateByOperatorID(opID1, subtaskState11);
			taskOperatorSubtaskStates12.putSubtaskStateByOperatorID(opID2, subtaskState12);
			taskOperatorSubtaskStates13.putSubtaskStateByOperatorID(opID3, subtaskState13);

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1, new CheckpointMetrics(), taskOperatorSubtaskStates12), TASK_MANAGER_LOCATION_INFO);

			// start the second checkpoint
			// trigger the first checkpoint. this should succeed
			final CompletableFuture<CompletedCheckpoint> checkpointFuture2 =
				coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture2.isCompletedExceptionally());

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

			TaskStateSnapshot taskOperatorSubtaskStates21 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates22 = spy(new TaskStateSnapshot());
			TaskStateSnapshot taskOperatorSubtaskStates23 = spy(new TaskStateSnapshot());

			OperatorSubtaskState subtaskState21 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState22 = mock(OperatorSubtaskState.class);
			OperatorSubtaskState subtaskState23 = mock(OperatorSubtaskState.class);

			taskOperatorSubtaskStates21.putSubtaskStateByOperatorID(opID1, subtaskState21);
			taskOperatorSubtaskStates22.putSubtaskStateByOperatorID(opID2, subtaskState22);
			taskOperatorSubtaskStates23.putSubtaskStateByOperatorID(opID3, subtaskState23);

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), any(Long.class), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), any(Long.class), any(CheckpointOptions.class));

			// we acknowledge one more task from the first checkpoint and the second
			// checkpoint completely. The second checkpoint should then subsume the first checkpoint

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2, new CheckpointMetrics(), taskOperatorSubtaskStates23), TASK_MANAGER_LOCATION_INFO);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId2, new CheckpointMetrics(), taskOperatorSubtaskStates21), TASK_MANAGER_LOCATION_INFO);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId1, new CheckpointMetrics(), taskOperatorSubtaskStates11), TASK_MANAGER_LOCATION_INFO);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId2, new CheckpointMetrics(), taskOperatorSubtaskStates22), TASK_MANAGER_LOCATION_INFO);

			// now, the second checkpoint should be confirmed, and the first discarded
			// actually both pending checkpoints are discarded, and the second has been transformed
			// into a successful checkpoint
			assertTrue(pending1.isDiscarded());
			assertTrue(pending2.isDiscarded());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// validate that all received subtask states in the first checkpoint have been discarded
			verify(subtaskState11, times(1)).discardState();
			verify(subtaskState12, times(1)).discardState();

			// validate that all subtask states in the second checkpoint are not discarded
			verify(subtaskState21, never()).discardState();
			verify(subtaskState22, never()).discardState();
			verify(subtaskState23, never()).discardState();

			// validate the committed checkpoints
			List<CompletedCheckpoint> scs = coord.getSuccessfulCheckpoints();
			CompletedCheckpoint success = scs.get(0);
			assertEquals(checkpointId2, success.getCheckpointID());
			assertEquals(jid, success.getJobId());
			assertEquals(3, success.getOperatorStates().size());

			// the first confirm message should be out
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId2), any(Long.class));

			// send the last remaining ack for the first checkpoint. This should not do anything
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId1, new CheckpointMetrics(), taskOperatorSubtaskStates13), TASK_MANAGER_LOCATION_INFO);
			verify(subtaskState13, times(1)).discardState();

			coord.shutdown(JobStatus.FINISHED);

			// validate that the states in the second checkpoint have been discarded
			verify(subtaskState21, times(1)).discardState();
			verify(subtaskState22, times(1)).discardState();
			verify(subtaskState23, times(1)).discardState();

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
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex1, ackVertex2 })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			// trigger a checkpoint, partially acknowledged
			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture.isCompletedExceptionally());
			assertEquals(1, coord.getNumberOfPendingCheckpoints());

			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().values().iterator().next();
			assertFalse(checkpoint.isDiscarded());

			OperatorID opID1 = OperatorID.fromJobVertexID(ackVertex1.getJobvertexId());

			TaskStateSnapshot taskOperatorSubtaskStates1 = spy(new TaskStateSnapshot());
			OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
			taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID1, subtaskState1);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpoint.getCheckpointId(), new CheckpointMetrics(), taskOperatorSubtaskStates1), TASK_MANAGER_LOCATION_INFO);

			// triggers cancelling
			manuallyTriggeredScheduledExecutor.triggerScheduledTasks();
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

			// create some mock execution vertices and trigger some checkpoint

			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex1, ackVertex2 })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture.isCompletedExceptionally());

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

		CheckpointCoordinatorConfiguration chkConfig =
			new CheckpointCoordinatorConfigurationBuilder()
				.setMaxConcurrentCheckpoints(1)
				.build();
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setJobId(jobId)
				.setCheckpointCoordinatorConfiguration(chkConfig)
				.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
				.setTasksToWaitFor(new ExecutionVertex[] {triggerVertex, ackVertex1, ackVertex2})
				.setTasksToCommitTo(new ExecutionVertex[0])
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		final CompletableFuture<CompletedCheckpoint> checkpointFuture = coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture.isCompletedExceptionally());

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

		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
		ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex1, vertex2);

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

		// trigger the first checkpoint. this should succeed
		String savepointDir = tmpFolder.newFolder().getAbsolutePath();
		CompletableFuture<CompletedCheckpoint> savepointFuture = coord.triggerSavepoint(savepointDir);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(savepointFuture.isDone());

		// validate that we have a pending savepoint
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
		PendingCheckpoint pending = coord.getPendingCheckpoints().get(checkpointId);

		assertNotNull(pending);
		assertEquals(checkpointId, pending.getCheckpointId());
		assertEquals(jid, pending.getJobId());
		assertEquals(2, pending.getNumberOfNonAcknowledgedTasks());
		assertEquals(0, pending.getNumberOfAcknowledgedTasks());
		assertEquals(0, pending.getOperatorStates().size());
		assertFalse(pending.isDiscarded());
		assertFalse(pending.areTasksFullyAcknowledged());
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
		assertFalse(pending.areTasksFullyAcknowledged());
		assertFalse(savepointFuture.isDone());

		// acknowledge the same task again (should not matter)
		coord.receiveAcknowledgeMessage(acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
		assertFalse(pending.isDiscarded());
		assertFalse(pending.areTasksFullyAcknowledged());
		assertFalse(savepointFuture.isDone());

		// acknowledge the other task.
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId, new CheckpointMetrics(), taskOperatorSubtaskStates1), TASK_MANAGER_LOCATION_INFO);

		// the checkpoint is internally converted to a successful checkpoint and the
		// pending checkpoint object is disposed
		assertTrue(pending.isDiscarded());
		assertNotNull(savepointFuture.get());

		// the now we should have a completed checkpoint
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertEquals(0, coord.getNumberOfPendingCheckpoints());

		// validate that the relevant tasks got a confirmation message
		{
			verify(vertex1.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId), any(Long.class));
			verify(vertex2.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId), any(Long.class));
		}

		// validate that the shared states are registered
		{
			verify(subtaskState1, times(1)).registerSharedStates(any(SharedStateRegistry.class));
			verify(subtaskState2, times(1)).registerSharedStates(any(SharedStateRegistry.class));
		}

		CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
		assertEquals(jid, success.getJobId());
		assertEquals(pending.getCheckpointId(), success.getCheckpointID());
		assertEquals(2, success.getOperatorStates().size());

		// ---------------
		// trigger another checkpoint and see that this one replaces the other checkpoint
		// ---------------
		savepointFuture = coord.triggerSavepoint(savepointDir);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(savepointFuture.isDone());

		long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew), TASK_MANAGER_LOCATION_INFO);

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
		assertEquals(jid, successNew.getJobId());
		assertEquals(checkpointIdNew, successNew.getCheckpointID());
		assertTrue(successNew.getOperatorStates().isEmpty());
		assertNotNull(savepointFuture.get());

		// validate that the first savepoint does not discard its private states.
		verify(subtaskState1, never()).discardState();
		verify(subtaskState2, never()).discardState();

		// validate that the relevant tasks got a confirmation message
		{
			verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), any(Long.class), any(CheckpointOptions.class));
			verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointIdNew), any(Long.class), any(CheckpointOptions.class));

			verify(vertex1.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), any(Long.class));
			verify(vertex2.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointIdNew), any(Long.class));
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

		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);
		ExecutionVertex vertex2 = mockExecutionVertex(attemptID2);

		StandaloneCheckpointIDCounter counter = new StandaloneCheckpointIDCounter();

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setJobId(jid)
				.setCheckpointCoordinatorConfiguration(CheckpointCoordinatorConfiguration.builder().setMaxConcurrentCheckpoints(Integer.MAX_VALUE).build())
				.setTasks(new ExecutionVertex[]{ vertex1, vertex2 })
				.setCheckpointIDCounter(counter)
				.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(10))
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		// Trigger savepoint and checkpoint
		CompletableFuture<CompletedCheckpoint> savepointFuture1 = coord.triggerSavepoint(savepointDir);

		manuallyTriggeredScheduledExecutor.triggerAll();
		long savepointId1 = counter.getLast();
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		CompletableFuture<CompletedCheckpoint> checkpointFuture1 = coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertEquals(2, coord.getNumberOfPendingCheckpoints());
		assertFalse(checkpointFuture1.isCompletedExceptionally());

		CompletableFuture<CompletedCheckpoint> checkpointFuture2 = coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture2.isCompletedExceptionally());
		long checkpointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd checkpoint should subsume the 1st checkpoint, but not the savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId2), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId2), TASK_MANAGER_LOCATION_INFO);

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());
		assertFalse(savepointFuture1.isDone());

		CompletableFuture<CompletedCheckpoint> checkpointFuture3 = coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture3.isCompletedExceptionally());
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		CompletableFuture<CompletedCheckpoint> savepointFuture2 = coord.triggerSavepoint(savepointDir);
		manuallyTriggeredScheduledExecutor.triggerAll();
		long savepointId2 = counter.getLast();
		assertFalse(savepointFuture2.isCompletedExceptionally());
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd savepoint should subsume the last checkpoint, but not the 1st savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId2), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId2), TASK_MANAGER_LOCATION_INFO);

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());

		assertFalse(savepointFuture1.isDone());
		assertNotNull(savepointFuture2.get());

		// Ack first savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId1), TASK_MANAGER_LOCATION_INFO);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId1), TASK_MANAGER_LOCATION_INFO);

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(3, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertNotNull(savepointFuture1.get());
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

			CheckpointCoordinatorConfiguration chkConfig =
				new CheckpointCoordinatorConfigurationBuilder()
					.setCheckpointInterval(10) // periodic interval is 10 ms
					.setCheckpointTimeout(200000) // timeout is very long (200 s)
					.setMinPauseBetweenCheckpoints(0L) // no extra delay
					.setMaxConcurrentCheckpoints(maxConcurrentAttempts)
					.build();
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setCheckpointCoordinatorConfiguration(chkConfig)
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			coord.startCheckpointScheduler();

			for (int i = 0; i < maxConcurrentAttempts; i++) {
				manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
				manuallyTriggeredScheduledExecutor.triggerAll();
			}

			assertEquals(maxConcurrentAttempts, numCalls.get());

			verify(triggerVertex.getCurrentExecutionAttempt(), times(maxConcurrentAttempts))
					.triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

			// now, once we acknowledge one checkpoint, it should trigger the next one
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID, 1L), TASK_MANAGER_LOCATION_INFO);

			final Collection<ScheduledFuture<?>> periodicScheduledTasks =
				manuallyTriggeredScheduledExecutor.getPeriodicScheduledTask();
			assertEquals(1, periodicScheduledTasks.size());
			final ScheduledFuture scheduledFuture = periodicScheduledTasks.iterator().next();

			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			manuallyTriggeredScheduledExecutor.triggerAll();

			assertEquals(maxConcurrentAttempts + 1, numCalls.get());

			// no further checkpoints should happen
			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			manuallyTriggeredScheduledExecutor.triggerAll();
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

			CheckpointCoordinatorConfiguration chkConfig =
				new CheckpointCoordinatorConfigurationBuilder()
					.setCheckpointInterval(10) // periodic interval is 10 ms
					.setCheckpointTimeout(200000) // timeout is very long (200 s)
					.setMinPauseBetweenCheckpoints(0L) // no extra delay
					.setMaxConcurrentCheckpoints(maxConcurrentAttempts)
					.build();
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setCheckpointCoordinatorConfiguration(chkConfig)
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			coord.startCheckpointScheduler();

			do {
				manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
				manuallyTriggeredScheduledExecutor.triggerAll();
			}
			while (coord.getNumberOfPendingCheckpoints() < maxConcurrentAttempts);

			// validate that the pending checkpoints are there
			assertEquals(maxConcurrentAttempts, coord.getNumberOfPendingCheckpoints());
			assertNotNull(coord.getPendingCheckpoints().get(1L));
			assertNotNull(coord.getPendingCheckpoints().get(2L));

			// now we acknowledge the second checkpoint, which should subsume the first checkpoint
			// and allow two more checkpoints to be triggered
			// now, once we acknowledge one checkpoint, it should trigger the next one
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID, 2L), TASK_MANAGER_LOCATION_INFO);

			// after a while, there should be the new checkpoints
			do {
				manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
				manuallyTriggeredScheduledExecutor.triggerAll();
			}
			while (coord.getNumberOfPendingCheckpoints() < maxConcurrentAttempts);

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

			CheckpointCoordinatorConfiguration chkConfig =
				new CheckpointCoordinatorConfigurationBuilder()
					.setCheckpointInterval(10) // periodic interval is 10 ms
					.setCheckpointTimeout(200000) // timeout is very long (200 s)
					.setMinPauseBetweenCheckpoints(0) // no extra delay
					.setMaxConcurrentCheckpoints(2) // max two concurrent checkpoints
					.build();
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setCheckpointCoordinatorConfiguration(chkConfig)
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			coord.startCheckpointScheduler();

			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			manuallyTriggeredScheduledExecutor.triggerAll();
			// no checkpoint should have started so far
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// now move the state to RUNNING
			currentState.set(ExecutionState.RUNNING);

			// the coordinator should start checkpointing now
			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			manuallyTriggeredScheduledExecutor.triggerAll();

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
		int numSavepoints = 5;

		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);

		StandaloneCheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();

		CheckpointCoordinatorConfiguration chkConfig =
			new CheckpointCoordinatorConfigurationBuilder()
				.setMaxConcurrentCheckpoints(1) // max one checkpoint at a time => should not affect savepoints
				.build();
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setJobId(jobId)
				.setCheckpointCoordinatorConfiguration(chkConfig)
				.setTasks(new ExecutionVertex[] { vertex1 })
				.setCheckpointIDCounter(checkpointIDCounter)
				.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		List<CompletableFuture<CompletedCheckpoint>> savepointFutures = new ArrayList<>();

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		// Trigger savepoints
		for (int i = 0; i < numSavepoints; i++) {
			savepointFutures.add(coord.triggerSavepoint(savepointDir));
		}

		// After triggering multiple savepoints, all should in progress
		for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
			assertFalse(savepointFuture.isDone());
		}

		manuallyTriggeredScheduledExecutor.triggerAll();

		// ACK all savepoints
		long checkpointId = checkpointIDCounter.getLast();
		for (int i = 0; i < numSavepoints; i++, checkpointId--) {
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, attemptID1, checkpointId), TASK_MANAGER_LOCATION_INFO);
		}

		// After ACKs, all should be completed
		for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
			assertNotNull(savepointFuture.get());
		}
	}

	/**
	 * Tests that no minimum delay between savepoints is enforced.
	 */
	@Test
	public void testMinDelayBetweenSavepoints() throws Exception {
		CheckpointCoordinatorConfiguration chkConfig =
			new CheckpointCoordinatorConfigurationBuilder()
				.setMinPauseBetweenCheckpoints(100000000L) // very long min delay => should not affect savepoints
				.setMaxConcurrentCheckpoints(1)
				.build();
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setCheckpointCoordinatorConfiguration(chkConfig)
				.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		CompletableFuture<CompletedCheckpoint> savepoint0 = coord.triggerSavepoint(savepointDir);
		assertFalse("Did not trigger savepoint", savepoint0.isDone());

		CompletableFuture<CompletedCheckpoint> savepoint1 = coord.triggerSavepoint(savepointDir);
		assertFalse("Did not trigger savepoint", savepoint1.isDone());
	}

	/**
	 * Tests that the externalized checkpoint configuration is respected.
	 */
	@Test
	public void testExternalizedCheckpoints() throws Exception {
		try {

			// set up the coordinator and validate the initial state
			CheckpointCoordinatorConfiguration chkConfig =
				new CheckpointCoordinatorConfigurationBuilder()
					.setCheckpointRetentionPolicy(CheckpointRetentionPolicy.RETAIN_ON_FAILURE)
					.build();
			CheckpointCoordinator coord =
				new CheckpointCoordinatorBuilder()
					.setCheckpointCoordinatorConfiguration(chkConfig)
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			CompletableFuture<CompletedCheckpoint> checkpointFuture =
				coord.triggerCheckpoint(false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertFalse(checkpointFuture.isCompletedExceptionally());

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
	public void testCheckpointStatsTrackerPendingCheckpointCallback() throws Exception {

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		coord.setCheckpointStatsTracker(tracker);

		when(tracker.reportPendingCheckpoint(anyLong(), anyLong(), any(CheckpointProperties.class)))
			.thenReturn(mock(PendingCheckpointStats.class));

		// Trigger a checkpoint and verify callback
		CompletableFuture<CompletedCheckpoint> checkpointFuture =
			coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture.isCompletedExceptionally());

		verify(tracker, times(1))
			.reportPendingCheckpoint(eq(1L), any(Long.class), eq(CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)));
	}

	/**
	 * Tests that the restore callbacks are called if registered.
	 */
	@Test
	public void testCheckpointStatsTrackerRestoreCallback() throws Exception {
		StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(1);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setCompletedCheckpointStore(store)
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

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

		assertTrue(coord.restoreLatestCheckpointedStateToAll(Collections.emptySet(), true));

		verify(tracker, times(1))
			.reportRestoredCheckpoint(any(RestoredCheckpointStats.class));
	}

	@Test
	public void testSharedStateRegistrationOnRestore() throws Exception {

		final JobID jid = new JobID();

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
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setJobId(jid)
				.setTasks(arrayExecutionVertices)
				.setCompletedCheckpointStore(store)
				.setTimer(manuallyTriggeredScheduledExecutor)
				.setSharedStateRegistryFactory(
					deleteExecutor -> {
						SharedStateRegistry instance = new SharedStateRegistry(deleteExecutor);
						createdSharedStateRegistries.add(instance);
						return instance;
					})
				.build();

		final int numCheckpoints = 3;

		List<KeyGroupRange> keyGroupPartitions1 =
			StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);

		for (int i = 0; i < numCheckpoints; ++i) {
			performIncrementalCheckpoint(jid, coord, jobVertex1, keyGroupPartitions1, i);
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
		Set<ExecutionJobVertex> tasks = new HashSet<>();
		tasks.add(jobVertex1);
		assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

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
				.triggerSynchronousSavepoint(false, "test-dir");

		manuallyTriggeredScheduledExecutor.triggerAll();
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
		// set up the coordinator
		TestingCheckpointIDCounter idCounter = new TestingCheckpointIDCounter();
		CheckpointCoordinator coord =
			new CheckpointCoordinatorBuilder()
				.setCheckpointIDCounter(idCounter)
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();
		idCounter.setOwner(coord);

		try {
			// start the coordinator
			coord.startCheckpointScheduler();
			final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
				coord.triggerCheckpoint(
					CheckpointProperties
						.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
					null,
					true,
					false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			try {
				onCompletionPromise.get();
				fail("should not trigger periodic checkpoint after stop the coordinator.");
			} catch (ExecutionException e) {
				final Optional<CheckpointException> checkpointExceptionOptional =
					ExceptionUtils.findThrowable(e, CheckpointException.class);
				assertTrue(checkpointExceptionOptional.isPresent());
				assertEquals(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN,
					checkpointExceptionOptional.get().getCheckpointFailureReason());
			}
		} finally {
			coord.shutdown(JobStatus.FINISHED);
		}
	}

	@Test
	public void testSavepointScheduledInUnalignedMode() throws Exception {
		int maxConcurrentCheckpoints = 1;
		int checkpointRequestsToSend = 10;
		int activeRequests = 0;
		JobID jobId = new JobID();
		CheckpointCoordinator coordinator = new CheckpointCoordinatorBuilder()
			.setCheckpointCoordinatorConfiguration(CheckpointCoordinatorConfiguration
				.builder()
				.setUnalignedCheckpointsEnabled(true)
				.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints)
				.build())
			.setJobId(jobId)
			.setTimer(manuallyTriggeredScheduledExecutor)
			.build();
		try {
			List<Future<?>> checkpointFutures = new ArrayList<>(checkpointRequestsToSend);
			coordinator.startCheckpointScheduler();
			while (activeRequests < checkpointRequestsToSend) {
				checkpointFutures.add(coordinator.triggerCheckpoint(true));
				activeRequests++;
			}
			assertEquals(activeRequests - maxConcurrentCheckpoints, coordinator.getNumQueuedRequests());

			Future<?> savepointFuture = coordinator.triggerSavepoint("/tmp");
			manuallyTriggeredScheduledExecutor.triggerAll();
			assertEquals(++activeRequests - maxConcurrentCheckpoints, coordinator.getNumQueuedRequests());

			coordinator.receiveDeclineMessage(new DeclineCheckpoint(jobId, new ExecutionAttemptID(), 1L), "none");
			manuallyTriggeredScheduledExecutor.triggerAll();

			activeRequests--; // savepoint triggered
			assertEquals(activeRequests - maxConcurrentCheckpoints , coordinator.getNumQueuedRequests());
			assertEquals(1, checkpointFutures.stream().filter(Future::isDone).count());

			assertFalse(savepointFuture.isDone());
			assertEquals(maxConcurrentCheckpoints, coordinator.getNumberOfPendingCheckpoints());
			CheckpointProperties props = coordinator.getPendingCheckpoints().values().iterator().next().getProps();
			assertTrue(props.isSavepoint());
			assertFalse(props.forceCheckpoint());
		} finally {
			coordinator.shutdown(JobStatus.FINISHED);
		}
	}

	private CheckpointCoordinator getCheckpointCoordinator(
		JobID jobId,
		ExecutionVertex vertex1,
		ExecutionVertex vertex2) {

		return new CheckpointCoordinatorBuilder()
			.setJobId(jobId)
			.setTasks(new ExecutionVertex[]{ vertex1, vertex2 })
			.setCheckpointCoordinatorConfiguration(CheckpointCoordinatorConfiguration.builder().setMaxConcurrentCheckpoints(Integer.MAX_VALUE).build())
			.setTimer(manuallyTriggeredScheduledExecutor)
			.build();
	}

	private CheckpointCoordinator getCheckpointCoordinator(
		JobID jobId,
		ExecutionVertex vertex1,
		ExecutionVertex vertex2,
		CheckpointFailureManager failureManager) {

		return new CheckpointCoordinatorBuilder()
			.setJobId(jobId)
			.setTasks(new ExecutionVertex[]{ vertex1, vertex2 })
			.setTimer(manuallyTriggeredScheduledExecutor)
			.setFailureManager(failureManager)
			.build();
	}

	private CheckpointCoordinator getCheckpointCoordinator() {
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
		return new CheckpointCoordinatorBuilder()
			.setTasksToTrigger(new ExecutionVertex[] { triggerVertex1, triggerVertex2 })
			.setTasksToWaitFor(new ExecutionVertex[] { ackVertex1, ackVertex2 })
			.setTasksToCommitTo(new ExecutionVertex[] {})
			.setTimer(manuallyTriggeredScheduledExecutor)
			.build();
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
		int cpSequenceNumber) throws Exception {

		// trigger the checkpoint
		coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();

		assertEquals(1, coord.getPendingCheckpoints().size());
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

			opStates.put(jobVertex1.getOperatorIDs().get(0).getGeneratedOperatorID(), operatorSubtaskState);

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
