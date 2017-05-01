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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.RecoverableCompletedCheckpointStore;
import org.apache.flink.runtime.util.TestByteStreamStateHandleDeepCompare;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for the checkpoint coordinator.
 */
public class CheckpointCoordinatorTest {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] {},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				null,
				Executors.directExecutor());

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
				Lists.newArrayList(OperatorID.fromJobVertexID(jobVertexID2)),
				1,
				1,
				ExecutionState.FINISHED);

			// create some mock Execution vertices that need to ack the checkpoint
			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] {},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				null,
				Executors.directExecutor());

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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] {},
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				null,
				Executors.directExecutor());

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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				null,
				Executors.directExecutor());

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
			verify(vertex1.getCurrentExecutionAttempt()).triggerCheckpoint(checkpointId, timestamp, CheckpointOptions.forFullCheckpoint());
			verify(vertex2.getCurrentExecutionAttempt()).triggerCheckpoint(checkpointId, timestamp, CheckpointOptions.forFullCheckpoint());

			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 0L);

			// acknowledge from one of the tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId));
			assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// acknowledge the same task again (should not matter)
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId));
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());


			// decline checkpoint from the other task, this should cancel the checkpoint
			// and trigger a new one
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId));
			assertTrue(checkpoint.isDiscarded());

			// the canceler is also removed
			assertEquals(0, coord.getNumScheduledTasks());

			// validate that we have no new pending checkpoint
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// decline again, nothing should happen
			// decline from the other task, nothing should happen
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId));
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID2, checkpointId));
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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				null,
				Executors.directExecutor());

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
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id));
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
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id));
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID2, checkpoint1Id));
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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				null,
				Executors.directExecutor());

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

			OperatorID opID1 = OperatorID.fromJobVertexID(vertex1.getJobvertexId());
			OperatorID opID2 = OperatorID.fromJobVertexID(vertex2.getJobvertexId());

			Map<OperatorID, OperatorState> operatorStates = checkpoint.getOperatorStates();

			operatorStates.put(opID1, new SpyInjectingOperatorState(
				opID1, vertex1.getTotalNumberOfParallelSubtasks(), vertex1.getMaxParallelism()));
			operatorStates.put(opID2, new SpyInjectingOperatorState(
				opID2, vertex2.getTotalNumberOfParallelSubtasks(), vertex2.getMaxParallelism()));

			// check that the vertices received the trigger checkpoint message
			{
				verify(vertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), eq(timestamp), any(CheckpointOptions.class));
				verify(vertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId), eq(timestamp), any(CheckpointOptions.class));
			}

			// acknowledge from one of the tasks
			AcknowledgeCheckpoint acknowledgeCheckpoint1 = new AcknowledgeCheckpoint(jid, attemptID2, checkpointId, new CheckpointMetrics(), mock(SubtaskState.class));
			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint1);
			OperatorSubtaskState subtaskState2 = operatorStates.get(opID2).getState(vertex2.getParallelSubtaskIndex());
			assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());
			verify(subtaskState2, never()).registerSharedStates(any(SharedStateRegistry.class));

			// acknowledge the same task again (should not matter)
			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint1);
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());
			verify(subtaskState2, never()).registerSharedStates(any(SharedStateRegistry.class));

			// acknowledge the other task.
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId, new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState1 = operatorStates.get(opID1).getState(vertex1.getParallelSubtaskIndex());

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
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew));
			subtaskState1 = operatorStates.get(opID1).getState(vertex1.getParallelSubtaskIndex());
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew));
			subtaskState2 = operatorStates.get(opID2).getState(vertex2.getParallelSubtaskIndex());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumScheduledTasks());

			CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, successNew.getJobId());
			assertEquals(timestampNew, successNew.getTimestamp());
			assertEquals(checkpointIdNew, successNew.getCheckpointID());
			assertTrue(successNew.getOperatorStates().isEmpty());

			// validate that the subtask states in old savepoint have unregister their shared states
			{
				verify(subtaskState1, times(1)).unregisterSharedStates(any(SharedStateRegistry.class));
				verify(subtaskState2, times(1)).unregisterSharedStates(any(SharedStateRegistry.class));
			}

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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());

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
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1));

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
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId1));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId2));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId1));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId2));

			// now, the first checkpoint should be confirmed
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertTrue(pending1.isDiscarded());

			// the first confirm message should be out
			verify(commitVertex.getCurrentExecutionAttempt(), times(1)).notifyCheckpointComplete(eq(checkpointId1), eq(timestamp1));

			// send the last remaining ack for the second checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2));

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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
				new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(10),
				null,
				Executors.directExecutor());

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

			Map<OperatorID, OperatorState> operatorStates1 = pending1.getOperatorStates();

			operatorStates1.put(opID1, new SpyInjectingOperatorState(
				opID1, ackVertex1.getTotalNumberOfParallelSubtasks(), ackVertex1.getMaxParallelism()));
			operatorStates1.put(opID2, new SpyInjectingOperatorState(
				opID2, ackVertex2.getTotalNumberOfParallelSubtasks(), ackVertex2.getMaxParallelism()));
			operatorStates1.put(opID3, new SpyInjectingOperatorState(
				opID3, ackVertex3.getTotalNumberOfParallelSubtasks(), ackVertex3.getMaxParallelism()));

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1, new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState1_2 = operatorStates1.get(opID2).getState(ackVertex2.getParallelSubtaskIndex());
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

			Map<OperatorID, OperatorState> operatorStates2 = pending2.getOperatorStates();

			operatorStates2.put(opID1, new SpyInjectingOperatorState(
				opID1, ackVertex1.getTotalNumberOfParallelSubtasks(), ackVertex1.getMaxParallelism()));
			operatorStates2.put(opID2, new SpyInjectingOperatorState(
				opID2, ackVertex2.getTotalNumberOfParallelSubtasks(), ackVertex2.getMaxParallelism()));
			operatorStates2.put(opID3, new SpyInjectingOperatorState(
				opID3, ackVertex3.getTotalNumberOfParallelSubtasks(), ackVertex3.getMaxParallelism()));

			// trigger messages should have been sent
			verify(triggerVertex1.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), eq(timestamp2), any(CheckpointOptions.class));
			verify(triggerVertex2.getCurrentExecutionAttempt(), times(1)).triggerCheckpoint(eq(checkpointId2), eq(timestamp2), any(CheckpointOptions.class));

			// we acknowledge one more task from the first checkpoint and the second
			// checkpoint completely. The second checkpoint should then subsume the first checkpoint

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2, new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState2_3 = operatorStates2.get(opID3).getState(ackVertex3.getParallelSubtaskIndex());

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId2, new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState2_1 = operatorStates2.get(opID1).getState(ackVertex1.getParallelSubtaskIndex());

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId1, new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState1_1 = operatorStates1.get(opID1).getState(ackVertex1.getParallelSubtaskIndex());

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId2, new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState2_2 = operatorStates2.get(opID2).getState(ackVertex2.getParallelSubtaskIndex());

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
			verify(subtaskState2_1, never()).unregisterSharedStates(any(SharedStateRegistry.class));
			verify(subtaskState2_2, never()).unregisterSharedStates(any(SharedStateRegistry.class));
			verify(subtaskState2_3, never()).unregisterSharedStates(any(SharedStateRegistry.class));
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
			SubtaskState subtaskState1_3 = mock(SubtaskState.class);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId1, new CheckpointMetrics(), subtaskState1_3));
			verify(subtaskState1_3, times(1)).discardState();

			coord.shutdown(JobStatus.FINISHED);

			// validate that the states in the second checkpoint have been discarded
			verify(subtaskState2_1, times(1)).unregisterSharedStates(any(SharedStateRegistry.class));
			verify(subtaskState2_2, times(1)).unregisterSharedStates(any(SharedStateRegistry.class));
			verify(subtaskState2_3, times(1)).unregisterSharedStates(any(SharedStateRegistry.class));
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

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				200,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());

			// trigger a checkpoint, partially acknowledged
			assertTrue(coord.triggerCheckpoint(timestamp, false));
			assertEquals(1, coord.getNumberOfPendingCheckpoints());

			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().values().iterator().next();
			assertFalse(checkpoint.isDiscarded());

			OperatorID opID1 = OperatorID.fromJobVertexID(ackVertex1.getJobvertexId());

			Map<OperatorID, OperatorState> operatorStates = checkpoint.getOperatorStates();

			operatorStates.put(opID1, new SpyInjectingOperatorState(
				opID1, ackVertex1.getTotalNumberOfParallelSubtasks(), ackVertex1.getMaxParallelism()));

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpoint.getCheckpointId(), new CheckpointMetrics(), mock(SubtaskState.class)));
			OperatorSubtaskState subtaskState = operatorStates.get(opID1).getState(ackVertex1.getParallelSubtaskIndex());

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
			verify(subtaskState, times(1)).discardState();

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

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				200000,
				200000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex1, ackVertex2 },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());

			assertTrue(coord.triggerCheckpoint(timestamp, false));

			long checkpointId = coord.getPendingCheckpoints().keySet().iterator().next();

			// send some messages that do not belong to either the job or the any
			// of the vertices that need to be acknowledged.
			// non of the messages should throw an exception

			// wrong job id
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(new JobID(), ackAttemptID1, checkpointId));

			// unknown checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, 1L));

			// unknown ack vertex
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, new ExecutionAttemptID(), checkpointId));

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

		CheckpointCoordinator coord = new CheckpointCoordinator(
			jobId,
			20000L,
			20000L,
			0L,
			1,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[] { triggerVertex },
			new ExecutionVertex[] {triggerVertex, ackVertex1, ackVertex2},
			new ExecutionVertex[0],
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		assertTrue(coord.triggerCheckpoint(timestamp, false));

		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		PendingCheckpoint pendingCheckpoint = coord.getPendingCheckpoints().values().iterator().next();

		long checkpointId = pendingCheckpoint.getCheckpointId();

		OperatorID opIDtrigger = OperatorID.fromJobVertexID(triggerVertex.getJobvertexId());
		OperatorID opID1 = OperatorID.fromJobVertexID(ackVertex1.getJobvertexId());
		OperatorID opID2 = OperatorID.fromJobVertexID(ackVertex2.getJobvertexId());

		Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();

		operatorStates.put(opIDtrigger, new SpyInjectingOperatorState(
			opIDtrigger, triggerVertex.getTotalNumberOfParallelSubtasks(), triggerVertex.getMaxParallelism()));
		operatorStates.put(opID1, new SpyInjectingOperatorState(
			opID1, ackVertex1.getTotalNumberOfParallelSubtasks(), ackVertex1.getMaxParallelism()));
		operatorStates.put(opID2, new SpyInjectingOperatorState(
			opID2, ackVertex2.getTotalNumberOfParallelSubtasks(), ackVertex2.getMaxParallelism()));

		// acknowledge the first trigger vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, triggerAttemptId, checkpointId, new CheckpointMetrics(), mock(SubtaskState.class)));
		OperatorSubtaskState storedTriggerSubtaskState = operatorStates.get(opIDtrigger).getState(triggerVertex.getParallelSubtaskIndex());

		// verify that the subtask state has not been discarded
		verify(storedTriggerSubtaskState, never()).discardState();

		SubtaskState unknownSubtaskState = mock(SubtaskState.class);

		// receive an acknowledge message for an unknown vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), unknownSubtaskState));

		// we should discard acknowledge messages from an unknown vertex belonging to our job
		verify(unknownSubtaskState, times(1)).discardState();

		SubtaskState differentJobSubtaskState = mock(SubtaskState.class);

		// receive an acknowledge message from an unknown job
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(new JobID(), new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), differentJobSubtaskState));

		// we should not interfere with different jobs
		verify(differentJobSubtaskState, never()).discardState();

		// duplicate acknowledge message for the trigger vertex
		SubtaskState triggerSubtaskState = mock(SubtaskState.class);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, triggerAttemptId, checkpointId, new CheckpointMetrics(), triggerSubtaskState));

		// duplicate acknowledge messages for a known vertex should not trigger discarding the state
		verify(triggerSubtaskState, never()).discardState();

		// let the checkpoint fail at the first ack vertex
		reset(storedTriggerSubtaskState);
		coord.receiveDeclineMessage(new DeclineCheckpoint(jobId, ackAttemptId1, checkpointId));

		assertTrue(pendingCheckpoint.isDiscarded());

		// check that we've cleaned up the already acknowledged state
		verify(storedTriggerSubtaskState, times(1)).discardState();

		SubtaskState ackSubtaskState = mock(SubtaskState.class);

		// late acknowledge message from the second ack vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, ackAttemptId2, checkpointId, new CheckpointMetrics(), ackSubtaskState));

		// check that we also cleaned up this state
		verify(ackSubtaskState, times(1)).discardState();

		// receive an acknowledge message from an unknown job
		reset(differentJobSubtaskState);
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(new JobID(), new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), differentJobSubtaskState));

		// we should not interfere with different jobs
		verify(differentJobSubtaskState, never()).discardState();

		SubtaskState unknownSubtaskState2 = mock(SubtaskState.class);

		// receive an acknowledge message for an unknown vertex
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, new ExecutionAttemptID(), checkpointId, new CheckpointMetrics(), unknownSubtaskState2));

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

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());


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

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				triggerCalls.add((Long) invocation.getArguments()[0]);
				return null;
			}
		}).when(executionAttempt).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

		final long delay = 50;

		final CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				2,           // periodic interval is 2 ms
				200_000,     // timeout is very long (200 s)
				delay,       // 50 ms delay between checkpoints
				1,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { vertex },
				new ExecutionVertex[] { vertex },
				new ExecutionVertex[] { vertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				"dummy-path",
				Executors.directExecutor());

		try {
			coord.startCheckpointScheduler();

			// wait until the first checkpoint was triggered
			Long firstCallId = triggerCalls.take();
			assertEquals(1L, firstCallId.longValue());

			AcknowledgeCheckpoint ackMsg = new AcknowledgeCheckpoint(jid, attemptID, 1L);

			// tell the coordinator that the checkpoint is done
			final long ackTime = System.nanoTime();
			coord.receiveAcknowledgeMessage(ackMsg);

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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

		// trigger the first checkpoint. this should succeed
		String savepointDir = tmpFolder.newFolder().getAbsolutePath();
		Future<CompletedCheckpoint> savepointFuture = coord.triggerSavepoint(timestamp, savepointDir);
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

		Map<OperatorID, OperatorState> operatorStates = pending.getOperatorStates();

		operatorStates.put(opID1, new SpyInjectingOperatorState(
			opID1, vertex1.getTotalNumberOfParallelSubtasks(), vertex1.getMaxParallelism()));
		operatorStates.put(opID2, new SpyInjectingOperatorState(
			opID2, vertex2.getTotalNumberOfParallelSubtasks(), vertex1.getMaxParallelism()));

		// acknowledge from one of the tasks
		AcknowledgeCheckpoint acknowledgeCheckpoint2 = new AcknowledgeCheckpoint(jid, attemptID2, checkpointId, new CheckpointMetrics(), mock(SubtaskState.class));
		coord.receiveAcknowledgeMessage(acknowledgeCheckpoint2);
		OperatorSubtaskState subtaskState2 = operatorStates.get(opID2).getState(vertex2.getParallelSubtaskIndex());
		assertEquals(1, pending.getNumberOfAcknowledgedTasks());
		assertEquals(1, pending.getNumberOfNonAcknowledgedTasks());
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(savepointFuture.isDone());

		// acknowledge the same task again (should not matter)
		coord.receiveAcknowledgeMessage(acknowledgeCheckpoint2);
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(savepointFuture.isDone());

		// acknowledge the other task.
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId, new CheckpointMetrics(), mock(SubtaskState.class)));
		OperatorSubtaskState subtaskState1 = operatorStates.get(opID1).getState(vertex1.getParallelSubtaskIndex());

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
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew));

		subtaskState1 = operatorStates.get(opID1).getState(vertex1.getParallelSubtaskIndex());
		subtaskState2 = operatorStates.get(opID2).getState(vertex2.getParallelSubtaskIndex());

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

		// Savepoints are not supposed to have any shared state.
		verify(subtaskState1, never()).unregisterSharedStates(any(SharedStateRegistry.class));
		verify(subtaskState2, never()).unregisterSharedStates(any(SharedStateRegistry.class));

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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			counter,
			new StandaloneCompletedCheckpointStore(10),
			null,
			Executors.directExecutor());

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		// Trigger savepoint and checkpoint
		Future<CompletedCheckpoint> savepointFuture1 = coord.triggerSavepoint(timestamp, savepointDir);
		long savepointId1 = counter.getLast();
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		assertTrue(coord.triggerCheckpoint(timestamp + 1, false));
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		assertTrue(coord.triggerCheckpoint(timestamp + 2, false));
		long checkpointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd checkpoint should subsume the 1st checkpoint, but not the savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId2));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId2));

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());
		assertFalse(savepointFuture1.isDone());

		assertTrue(coord.triggerCheckpoint(timestamp + 3, false));
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		Future<CompletedCheckpoint> savepointFuture2 = coord.triggerSavepoint(timestamp + 4, savepointDir);
		long savepointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd savepoint should subsume the last checkpoint, but not the 1st savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId2));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId2));

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());

		assertFalse(savepointFuture1.isDone());
		assertTrue(savepointFuture2.isDone());

		// Ack first savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId1));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId1));

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

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					numCalls.incrementAndGet();
					return null;
				}
			}).when(execution).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					numCalls.incrementAndGet();
					return null;
				}
			}).when(execution).notifyCheckpointComplete(anyLong(), anyLong());

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0L,        // no extra delay
				maxConcurrentAttempts,
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());

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
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID, 1L));

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

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0L,        // no extra delay
				maxConcurrentAttempts, // max two concurrent checkpoints
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());

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
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID, 2L));

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
			when(triggerVertex.getCurrentExecutionAttempt().getState()).thenAnswer(
					new Answer<ExecutionState>() {
						@Override
						public ExecutionState answer(InvocationOnMock invocation){
							return currentState.get();
						}
					});

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				10,        // periodic interval is 10 ms
				200000,    // timeout is very long (200 s)
				0L,        // no extra delay
				2, // max two concurrent checkpoints
				ExternalizedCheckpointSettings.none(),
				new ExecutionVertex[] { triggerVertex },
				new ExecutionVertex[] { ackVertex },
				new ExecutionVertex[] { commitVertex },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2),
				null,
				Executors.directExecutor());

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

		CheckpointCoordinator coord = new CheckpointCoordinator(
			jobId,
			100000,
			200000,
			0L,
			1, // max one checkpoint at a time => should not affect savepoints
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			checkpointIDCounter,
			new StandaloneCompletedCheckpointStore(2),
			null,
			Executors.directExecutor());

		List<Future<CompletedCheckpoint>> savepointFutures = new ArrayList<>();

		int numSavepoints = 5;

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		// Trigger savepoints
		for (int i = 0; i < numSavepoints; i++) {
			savepointFutures.add(coord.triggerSavepoint(i, savepointDir));
		}

		// After triggering multiple savepoints, all should in progress
		for (Future<CompletedCheckpoint> savepointFuture : savepointFutures) {
			assertFalse(savepointFuture.isDone());
		}

		// ACK all savepoints
		long checkpointId = checkpointIDCounter.getLast();
		for (int i = 0; i < numSavepoints; i++, checkpointId--) {
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, attemptID1, checkpointId));
		}

		// After ACKs, all should be completed
		for (Future<CompletedCheckpoint> savepointFuture : savepointFutures) {
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

		CheckpointCoordinator coord = new CheckpointCoordinator(
			jobId,
			100000,
			200000,
			100000000L, // very long min delay => should not affect savepoints
			1,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(2),
			null,
			Executors.directExecutor());

		String savepointDir = tmpFolder.newFolder().getAbsolutePath();

		Future<CompletedCheckpoint> savepoint0 = coord.triggerSavepoint(0, savepointDir);
		assertFalse("Did not trigger savepoint", savepoint0.isDone());

		Future<CompletedCheckpoint> savepoint1 = coord.triggerSavepoint(1, savepointDir);
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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			store,
			null,
			Executors.directExecutor());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		PendingCheckpoint pending = coord.getPendingCheckpoints().get(checkpointId);

		OperatorID opID1 = OperatorID.fromJobVertexID(jobVertexID1);
		OperatorID opID2 = OperatorID.fromJobVertexID(jobVertexID2);

		Map<OperatorID, OperatorState> operatorStates = pending.getOperatorStates();

		operatorStates.put(opID1, new SpyInjectingOperatorState(
			opID1, jobVertex1.getParallelism(), jobVertex1.getMaxParallelism()));
		operatorStates.put(opID2, new SpyInjectingOperatorState(
			opID2, jobVertex2.getParallelism(), jobVertex2.getMaxParallelism()));

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			SubtaskState subtaskState = mockSubtaskState(jobVertexID1, index, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					subtaskState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			SubtaskState subtaskState = mockSubtaskState(jobVertexID2, index, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					subtaskState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		// shutdown the store
		store.shutdown(JobStatus.SUSPENDED);

		// All shared states should be unregistered once the store is shut down
		for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
			for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
				for (OperatorSubtaskState subtaskState : taskState.getStates()) {
					verify(subtaskState, times(1)).unregisterSharedStates(any(SharedStateRegistry.class));
				}
			}
		}

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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 0L);

		List<KeyGroupRange> keyGroupPartitions1 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID1, index);
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
			SubtaskState checkpointStateHandles = new SubtaskState(valueSizeTuple, null, null, keyGroupState, null);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					checkpointStateHandles);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID2, index);
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
			SubtaskState checkpointStateHandles = new SubtaskState(valueSizeTuple, null, null, keyGroupState, null);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					checkpointStateHandles);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 0L);

		List<KeyGroupRange> keyGroupPartitions1 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID1, index);
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(
					jobVertexID1, keyGroupPartitions1.get(index), false);

			SubtaskState checkpointStateHandles = new SubtaskState(valueSizeTuple, null, null, keyGroupState, null);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					checkpointStateHandles);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {

			ChainedStateHandle<StreamStateHandle> state = generateStateForVertex(jobVertexID2, index);
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(
					jobVertexID2, keyGroupPartitions2.get(index), false);

			SubtaskState checkpointStateHandles = new SubtaskState(state, null, null, keyGroupState, null);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					checkpointStateHandles);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newParallelism1 = 4;
		int newParallelism2 = 3;

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			newParallelism1,
			maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			newParallelism2,
			maxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);

		coord.restoreLatestCheckpointedState(tasks, true, false);

		fail("The restoration should have failed because the parallelism of an vertex with " +
			"non-partitioned state changed.");
	}

	@Test
	public void testRestoreLatestCheckpointedStateScaleIn() throws Exception {
		testRestoreLatestCheckpointedStateWithChangingParallelism(false);
	}

	@Test
	public void testRestoreLatestCheckpointedStateScaleOut() throws Exception {
		testRestoreLatestCheckpointedStateWithChangingParallelism(false);
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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 0L);

		List<KeyGroupRange> keyGroupPartitions1 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		//vertex 1
		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID1, index);
			ChainedStateHandle<OperatorStateHandle> opStateBackend = generateChainedPartitionableStateHandle(jobVertexID1, index, 2, 8, false);
			KeyGroupsStateHandle keyedStateBackend = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
			KeyGroupsStateHandle keyedStateRaw = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), true);

			SubtaskState checkpointStateHandles = new SubtaskState(valueSizeTuple, opStateBackend, null, keyedStateBackend, keyedStateRaw);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					checkpointStateHandles);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		//vertex 2
		final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesBackend = new ArrayList<>(jobVertex2.getParallelism());
		final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesRaw = new ArrayList<>(jobVertex2.getParallelism());
		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			KeyGroupsStateHandle keyedStateBackend = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
			KeyGroupsStateHandle keyedStateRaw = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), true);
			ChainedStateHandle<OperatorStateHandle> opStateBackend = generateChainedPartitionableStateHandle(jobVertexID2, index, 2, 8, false);
			ChainedStateHandle<OperatorStateHandle> opStateRaw = generateChainedPartitionableStateHandle(jobVertexID2, index, 2, 8, true);
			expectedOpStatesBackend.add(opStateBackend);
			expectedOpStatesRaw.add(opStateRaw);
			SubtaskState checkpointStateHandles =
					new SubtaskState(new ChainedStateHandle<>(
							Collections.<StreamStateHandle>singletonList(null)), opStateBackend, opStateRaw, keyedStateBackend, keyedStateRaw);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					checkpointStateHandles);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
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
			KeyGroupsStateHandle originalKeyedStateBackend = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), false);
			KeyGroupsStateHandle originalKeyedStateRaw = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), true);

			TaskStateHandles taskStateHandles = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskStateHandles();

			ChainedStateHandle<StreamStateHandle> operatorState = taskStateHandles.getLegacyOperatorState();
			List<Collection<OperatorStateHandle>> opStateBackend = taskStateHandles.getManagedOperatorState();
			List<Collection<OperatorStateHandle>> opStateRaw = taskStateHandles.getRawOperatorState();
			Collection<KeyedStateHandle> keyedStateBackend = taskStateHandles.getManagedKeyedState();
			Collection<KeyedStateHandle> keyGroupStateRaw = taskStateHandles.getRawKeyedState();

			actualOpStatesBackend.add(opStateBackend);
			actualOpStatesRaw.add(opStateRaw);
			// the 'non partition state' is not null because it is recombined.
			assertNotNull(operatorState);
			for (int index = 0; index < operatorState.getLength(); index++) {
				assertNull(operatorState.get(index));
			}
			compareKeyedState(Collections.singletonList(originalKeyedStateBackend), keyedStateBackend);
			compareKeyedState(Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
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

		/**
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
		for (Tuple2<JobVertexID, OperatorID> id : Lists.newArrayList(id1, id2)) {
			OperatorState taskState = new OperatorState(id.f1, parallelism1, maxParallelism1);
			operatorStates.put(id.f1, taskState);
			for (int index = 0; index < taskState.getParallelism(); index++) {
				StreamStateHandle subNonPartitionedState = 
					generateStateForVertex(id.f0, index)
						.get(0);
				OperatorStateHandle subManagedOperatorState =
					generateChainedPartitionableStateHandle(id.f0, index, 2, 8, false)
						.get(0);
				OperatorStateHandle subRawOperatorState =
					generateChainedPartitionableStateHandle(id.f0, index, 2, 8, true)
						.get(0);

				OperatorSubtaskState subtaskState = new OperatorSubtaskState(subNonPartitionedState,
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
		for (Tuple2<JobVertexID, OperatorID> id : Lists.newArrayList(id3, id4)) {
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
					null,
					subManagedOperatorState,
					subRawOperatorState,
					subManagedKeyedState,
					subRawKeyedState);
				operatorState.putState(index, subtaskState);
			}
		}

		/**
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
			Lists.newArrayList(id2.f1, id1.f1, id5.f1),
			newParallelism1,
			maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
			id3.f0,
			Lists.newArrayList(id6.f1, id3.f1),
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
				CheckpointProperties.forStandardCheckpoint(),
				null,
				null);

		when(standaloneCompletedCheckpointStore.getLatestCheckpoint()).thenReturn(completedCheckpoint);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			newJobVertex1.getTaskVertices(),
			newJobVertex1.getTaskVertices(),
			newJobVertex1.getTaskVertices(),
			new StandaloneCheckpointIDCounter(),
			standaloneCompletedCheckpointStore,
			null,
			Executors.directExecutor());

		coord.restoreLatestCheckpointedState(tasks, false, true);

		for (int i = 0; i < newJobVertex1.getParallelism(); i++) {

			TaskStateHandles taskStateHandles = newJobVertex1.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskStateHandles();
			ChainedStateHandle<StreamStateHandle> actualSubNonPartitionedState = taskStateHandles.getLegacyOperatorState();
			List<Collection<OperatorStateHandle>> actualSubManagedOperatorState = taskStateHandles.getManagedOperatorState();
			List<Collection<OperatorStateHandle>> actualSubRawOperatorState = taskStateHandles.getRawOperatorState();

			assertNull(taskStateHandles.getManagedKeyedState());
			assertNull(taskStateHandles.getRawKeyedState());

			// operator5
			{
				int operatorIndexInChain = 2;
				assertNull(actualSubNonPartitionedState.get(operatorIndexInChain));
				assertNull(actualSubManagedOperatorState.get(operatorIndexInChain));
				assertNull(actualSubRawOperatorState.get(operatorIndexInChain));
			}
			// operator1
			{
				int operatorIndexInChain = 1;
				ChainedStateHandle<StreamStateHandle> expectSubNonPartitionedState = generateStateForVertex(id1.f0, i);
				ChainedStateHandle<OperatorStateHandle> expectedManagedOpState = generateChainedPartitionableStateHandle(
					id1.f0, i, 2, 8, false);
				ChainedStateHandle<OperatorStateHandle> expectedRawOpState = generateChainedPartitionableStateHandle(
					id1.f0, i, 2, 8, true);

				assertTrue(CommonTestUtils.isSteamContentEqual(
					expectSubNonPartitionedState.get(0).openInputStream(),
					actualSubNonPartitionedState.get(operatorIndexInChain).openInputStream()));

				assertTrue(CommonTestUtils.isSteamContentEqual(expectedManagedOpState.get(0).openInputStream(),
					actualSubManagedOperatorState.get(operatorIndexInChain).iterator().next().openInputStream()));

				assertTrue(CommonTestUtils.isSteamContentEqual(expectedRawOpState.get(0).openInputStream(),
					actualSubRawOperatorState.get(operatorIndexInChain).iterator().next().openInputStream()));
			}
			// operator2
			{
				int operatorIndexInChain = 0;
				ChainedStateHandle<StreamStateHandle> expectSubNonPartitionedState = generateStateForVertex(id2.f0, i);
				ChainedStateHandle<OperatorStateHandle> expectedManagedOpState = generateChainedPartitionableStateHandle(
					id2.f0, i, 2, 8, false);
				ChainedStateHandle<OperatorStateHandle> expectedRawOpState = generateChainedPartitionableStateHandle(
					id2.f0, i, 2, 8, true);

				assertTrue(CommonTestUtils.isSteamContentEqual(expectSubNonPartitionedState.get(0).openInputStream(),
					actualSubNonPartitionedState.get(operatorIndexInChain).openInputStream()));

				assertTrue(CommonTestUtils.isSteamContentEqual(expectedManagedOpState.get(0).openInputStream(),
					actualSubManagedOperatorState.get(operatorIndexInChain).iterator().next().openInputStream()));

				assertTrue(CommonTestUtils.isSteamContentEqual(expectedRawOpState.get(0).openInputStream(),
					actualSubRawOperatorState.get(operatorIndexInChain).iterator().next().openInputStream()));
			}
		}

		List<List<Collection<OperatorStateHandle>>> actualManagedOperatorStates = new ArrayList<>(newJobVertex2.getParallelism());
		List<List<Collection<OperatorStateHandle>>> actualRawOperatorStates = new ArrayList<>(newJobVertex2.getParallelism());

		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {
			TaskStateHandles taskStateHandles = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskStateHandles();

			// operator 3
			{
				int operatorIndexInChain = 1;
				List<Collection<OperatorStateHandle>> actualSubManagedOperatorState = new ArrayList<>(1);
				actualSubManagedOperatorState.add(taskStateHandles.getManagedOperatorState().get(operatorIndexInChain));

				List<Collection<OperatorStateHandle>> actualSubRawOperatorState = new ArrayList<>(1);
				actualSubRawOperatorState.add(taskStateHandles.getRawOperatorState().get(operatorIndexInChain));

				actualManagedOperatorStates.add(actualSubManagedOperatorState);
				actualRawOperatorStates.add(actualSubRawOperatorState);

				assertNull(taskStateHandles.getLegacyOperatorState().get(operatorIndexInChain));
			}

			// operator 6
			{
				int operatorIndexInChain = 0;
				assertNull(taskStateHandles.getManagedOperatorState().get(operatorIndexInChain));
				assertNull(taskStateHandles.getRawOperatorState().get(operatorIndexInChain));
				assertNull(taskStateHandles.getLegacyOperatorState().get(operatorIndexInChain));

			}

			KeyGroupsStateHandle originalKeyedStateBackend = generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), false);
			KeyGroupsStateHandle originalKeyedStateRaw = generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), true);


			Collection<KeyedStateHandle> keyedStateBackend = taskStateHandles.getManagedKeyedState();
			Collection<KeyedStateHandle> keyGroupStateRaw = taskStateHandles.getRawKeyedState();


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
			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				ExternalizedCheckpointSettings.externalizeCheckpoints(true),
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				"fake-directory",
				Executors.directExecutor());

			assertTrue(coord.triggerCheckpoint(timestamp, false));

			for (PendingCheckpoint checkpoint : coord.getPendingCheckpoints().values()) {
				CheckpointProperties props = checkpoint.getProps();
				CheckpointProperties expected = CheckpointProperties.forExternalizedCheckpoint(true);

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
	public void testReplicateModeStateHandle() {
		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap = new HashMap<>(1);
		metaInfoMap.put("t-1", new OperatorStateHandle.StateMetaInfo(new long[]{0, 23}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap.put("t-2", new OperatorStateHandle.StateMetaInfo(new long[]{42, 64}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap.put("t-3", new OperatorStateHandle.StateMetaInfo(new long[]{72, 83}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		OperatorStateHandle osh = new OperatorStateHandle(metaInfoMap, new ByteStreamStateHandle("test", new byte[100]));

		OperatorStateRepartitioner repartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;
		List<Collection<OperatorStateHandle>> repartitionedStates =
				repartitioner.repartitionState(Collections.singletonList(osh), 3);

		Map<String, Integer> checkCounts = new HashMap<>(3);

		for (Collection<OperatorStateHandle> operatorStateHandles : repartitionedStates) {
			for (OperatorStateHandle operatorStateHandle : operatorStateHandles) {
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> stateNameToMetaInfo :
						operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {

					String stateName = stateNameToMetaInfo.getKey();
					Integer count = checkCounts.get(stateName);
					if (null == count) {
						checkCounts.put(stateName, 1);
					} else {
						checkCounts.put(stateName, 1 + count);
					}

					OperatorStateHandle.StateMetaInfo stateMetaInfo = stateNameToMetaInfo.getValue();
					if (OperatorStateHandle.Mode.SPLIT_DISTRIBUTE.equals(stateMetaInfo.getDistributionMode())) {
						Assert.assertEquals(1, stateNameToMetaInfo.getValue().getOffsets().length);
					} else {
						Assert.assertEquals(2, stateNameToMetaInfo.getValue().getOffsets().length);
					}
				}
			}
		}

		Assert.assertEquals(3, checkCounts.size());
		Assert.assertEquals(3, checkCounts.get("t-1").intValue());
		Assert.assertEquals(3, checkCounts.get("t-2").intValue());
		Assert.assertEquals(2, checkCounts.get("t-3").intValue());
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

		ByteStreamStateHandle allSerializedStatesHandle = new TestByteStreamStateHandleDeepCompare(
				String.valueOf(UUID.randomUUID()),
				serializedDataWithOffsets.f0);
		KeyGroupsStateHandle keyGroupsStateHandle = new KeyGroupsStateHandle(
				keyGroupRangeOffsets,
				allSerializedStatesHandle);
		return keyGroupsStateHandle;
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

	public static ChainedStateHandle<StreamStateHandle> generateStateForVertex(
			JobVertexID jobVertexID,
			int index) throws IOException {

		Random random = new Random(jobVertexID.hashCode() + index);
		int value = random.nextInt();
		return generateChainedStateHandle(value);
	}

	public static ChainedStateHandle<StreamStateHandle> generateChainedStateHandle(
			Serializable value) throws IOException {
		return ChainedStateHandle.wrapSingleHandle(
				TestByteStreamStateHandleDeepCompare.fromSerializable(String.valueOf(UUID.randomUUID()), value));
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

		return generateChainedPartitionableStateHandle(statesListsMap);
	}

	private static ChainedStateHandle<OperatorStateHandle> generateChainedPartitionableStateHandle(
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

		ByteStreamStateHandle streamStateHandle = new TestByteStreamStateHandleDeepCompare(
				String.valueOf(UUID.randomUUID()),
				serializationWithOffsets.f0);

		OperatorStateHandle operatorStateHandle =
				new OperatorStateHandle(offsetsMap, streamStateHandle);
		return ChainedStateHandle.wrapSingleHandle(operatorStateHandle);
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
			Arrays.asList(OperatorID.fromJobVertexID(jobVertexID)),
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

	static SubtaskState mockSubtaskState(
		JobVertexID jobVertexID,
		int index,
		KeyGroupRange keyGroupRange) throws IOException {

		ChainedStateHandle<StreamStateHandle> nonPartitionedState = generateStateForVertex(jobVertexID, index);
		ChainedStateHandle<OperatorStateHandle> partitionableState = generateChainedPartitionableStateHandle(jobVertexID, index, 2, 8, false);
		KeyGroupsStateHandle partitionedKeyGroupState = generateKeyGroupState(jobVertexID, keyGroupRange, false);

		SubtaskState subtaskState = mock(SubtaskState.class, withSettings().serializable());

		doReturn(nonPartitionedState).when(subtaskState).getLegacyOperatorState();
		doReturn(partitionableState).when(subtaskState).getManagedOperatorState();
		doReturn(null).when(subtaskState).getRawOperatorState();
		doReturn(partitionedKeyGroupState).when(subtaskState).getManagedKeyedState();
		doReturn(null).when(subtaskState).getRawKeyedState();

		return subtaskState;
	}

	public static void verifyStateRestore(
			JobVertexID jobVertexID, ExecutionJobVertex executionJobVertex,
			List<KeyGroupRange> keyGroupPartitions) throws Exception {

		for (int i = 0; i < executionJobVertex.getParallelism(); i++) {

			TaskStateHandles taskStateHandles = executionJobVertex.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskStateHandles();

			ChainedStateHandle<StreamStateHandle> expectNonPartitionedState = generateStateForVertex(jobVertexID, i);
			ChainedStateHandle<StreamStateHandle> actualNonPartitionedState = taskStateHandles.getLegacyOperatorState();
			assertTrue(CommonTestUtils.isSteamContentEqual(
					expectNonPartitionedState.get(0).openInputStream(),
					actualNonPartitionedState.get(0).openInputStream()));

			ChainedStateHandle<OperatorStateHandle> expectedOpStateBackend =
					generateChainedPartitionableStateHandle(jobVertexID, i, 2, 8, false);

			List<Collection<OperatorStateHandle>> actualPartitionableState = taskStateHandles.getManagedOperatorState();

			assertTrue(CommonTestUtils.isSteamContentEqual(
					expectedOpStateBackend.get(0).openInputStream(),
					actualPartitionableState.get(0).iterator().next().openInputStream()));

			KeyGroupsStateHandle expectPartitionedKeyGroupState = generateKeyGroupState(
					jobVertexID, keyGroupPartitions.get(i), false);
			Collection<KeyedStateHandle> actualPartitionedKeyGroupState = taskStateHandles.getManagedKeyedState();
			compareKeyedState(Collections.singletonList(expectPartitionedKeyGroupState), actualPartitionedKeyGroupState);
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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new ExecutionVertex[] { vertex1 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		// Periodic
		CheckpointTriggerResult triggerResult = coord.triggerCheckpoint(
				System.currentTimeMillis(),
				CheckpointProperties.forStandardCheckpoint(),
				null,
				true);

		assertTrue(triggerResult.isFailure());
		assertEquals(CheckpointDeclineReason.PERIODIC_SCHEDULER_SHUTDOWN, triggerResult.getFailureReason());

		// Not periodic
		triggerResult = coord.triggerCheckpoint(
				System.currentTimeMillis(),
				CheckpointProperties.forStandardCheckpoint(),
				null,
				false);

		assertFalse(triggerResult.isFailure());
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

		List<OperatorStateHandle> previousParallelOpInstanceStates = new ArrayList<>(oldParallelism);

		for (int i = 0; i < oldParallelism; ++i) {
			Path fakePath = new Path("/fake-" + i);
			Map<String, OperatorStateHandle.StateMetaInfo> namedStatesToOffsets = new HashMap<>();
			int off = 0;
			for (int s = 0; s < numNamedStates; ++s) {
				long[] offs = new long[1 + r.nextInt(maxPartitionsPerState)];

				for (int o = 0; o < offs.length; ++o) {
					offs[o] = off;
					++off;
				}

				OperatorStateHandle.Mode mode = r.nextInt(10) == 0 ?
						OperatorStateHandle.Mode.BROADCAST : OperatorStateHandle.Mode.SPLIT_DISTRIBUTE;
				namedStatesToOffsets.put(
						"State-" + s,
						new OperatorStateHandle.StateMetaInfo(offs, mode));

			}

			previousParallelOpInstanceStates.add(
					new OperatorStateHandle(namedStatesToOffsets, new FileStateHandle(fakePath, -1)));
		}

		Map<StreamStateHandle, Map<String, List<Long>>> expected = new HashMap<>();

		int expectedTotalPartitions = 0;
		for (OperatorStateHandle psh : previousParallelOpInstanceStates) {
			Map<String, OperatorStateHandle.StateMetaInfo> offsMap = psh.getStateNameToPartitionOffsets();
			Map<String, List<Long>> offsMapWithList = new HashMap<>(offsMap.size());
			for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> e : offsMap.entrySet()) {

				long[] offs = e.getValue().getOffsets();
				int replication = e.getValue().getDistributionMode().equals(OperatorStateHandle.Mode.BROADCAST) ?
						newParallelism : 1;

				expectedTotalPartitions += replication * offs.length;
				List<Long> offsList = new ArrayList<>(offs.length);

				for (int i = 0; i < offs.length; ++i) {
					for(int p = 0; p < replication; ++p) {
						offsList.add(offs[i]);
					}
				}
				offsMapWithList.put(e.getKey(), offsList);
			}
			expected.put(psh.getDelegateStateHandle(), offsMapWithList);
		}

		OperatorStateRepartitioner repartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		List<Collection<OperatorStateHandle>> pshs =
				repartitioner.repartitionState(previousParallelOpInstanceStates, newParallelism);

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
					for (int i = 0; i < add.length; ++i) {
						actualOffs.add(add[i]);
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

		int maxLoadDiff = maxCount - minCount;
		Assert.assertTrue("Difference in partition load is > 1 : " + maxLoadDiff, maxLoadDiff <= 1);
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
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			null,
			Executors.directExecutor());

		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		coord.setCheckpointStatsTracker(tracker);

		when(tracker.reportPendingCheckpoint(anyLong(), anyLong(), any(CheckpointProperties.class)))
			.thenReturn(mock(PendingCheckpointStats.class));

		// Trigger a checkpoint and verify callback
		assertTrue(coord.triggerCheckpoint(timestamp, false));

		verify(tracker, times(1))
			.reportPendingCheckpoint(eq(1L), eq(timestamp), eq(CheckpointProperties.forStandardCheckpoint()));
	}

	/**
	 * Tests that the restore callbacks are called if registered.
	 */
	@Test
	public void testCheckpointStatsTrackerRestoreCallback() throws Exception {
		ExecutionVertex vertex1 = mockExecutionVertex(new ExecutionAttemptID());

		StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(1);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new ExecutionVertex[]{vertex1},
			new StandaloneCheckpointIDCounter(),
			store,
			null,
			Executors.directExecutor());

		store.addCheckpoint(new CompletedCheckpoint(
			new JobID(),
			0,
			0,
			0,
			Collections.<OperatorID, OperatorState>emptyMap(),
			Collections.<MasterState>emptyList(),
			CheckpointProperties.forStandardCheckpoint(),
			null,
			null));

		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		coord.setCheckpointStatsTracker(tracker);

		assertTrue(coord.restoreLatestCheckpointedState(Collections.<JobVertexID, ExecutionJobVertex>emptyMap(), false, true));

		verify(tracker, times(1))
			.reportRestoredCheckpoint(any(RestoredCheckpointStats.class));
	}

	private static final class SpyInjectingOperatorState extends OperatorState {

		private static final long serialVersionUID = -4004437428483663815L;

		public SpyInjectingOperatorState(OperatorID taskID, int parallelism, int maxParallelism) {
			super(taskID, parallelism, maxParallelism);
		}

		public void putState(int subtaskIndex, OperatorSubtaskState subtaskState) {
			super.putState(subtaskIndex, spy(subtaskState));
		}
	}
}
