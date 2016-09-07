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
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.savepoint.HeapSavepointStore;
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
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the checkpoint coordinator.
 */
public class CheckpointCoordinatorTest {

	private static final ClassLoader cl = Thread.currentThread().getContextClassLoader();

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
					0, Integer.MAX_VALUE,
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] {},
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			assertFalse(coord.triggerCheckpoint(timestamp));

			// still, nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			coord.shutdown();
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
			ExecutionVertex triggerVertex2 = mockExecutionVertex(triggerAttemptID2, new JobVertexID(), 1, 1, ExecutionState.FINISHED);

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
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] {},
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			assertFalse(coord.triggerCheckpoint(timestamp));

			// still, nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			coord.shutdown();
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
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] {},
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			// nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should not succeed
			assertFalse(coord.triggerCheckpoint(timestamp));

			// still, nothing should be happening
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			coord.shutdown();
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
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp));

			// validate that we have a pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			assertNotNull(checkpoint);
			assertEquals(checkpointId, checkpoint.getCheckpointId());
			assertEquals(timestamp, checkpoint.getCheckpointTimestamp());
			assertEquals(jid, checkpoint.getJobId());
			assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint.getTaskStates().size());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpointId, timestamp);
				TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpointId, timestamp);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));
			}

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
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId, checkpoint.getCheckpointTimestamp()));
			assertTrue(checkpoint.isDiscarded());

			// validate that we have a new pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpointNew = coord.getPendingCheckpoints().get(checkpointIdNew);

			assertNotNull(checkpointNew);
			assertEquals(checkpointIdNew, checkpointNew.getCheckpointId());
			assertEquals(jid, checkpointNew.getJobId());
			assertEquals(2, checkpointNew.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpointNew.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpointNew.getTaskStates().size());
			assertFalse(checkpointNew.isDiscarded());
			assertFalse(checkpointNew.isFullyAcknowledged());
			assertNotEquals(checkpoint.getCheckpointId(), checkpointNew.getCheckpointId());

			// check that the vertices received the new trigger checkpoint message
			{
				TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpointIdNew, checkpointNew.getCheckpointTimestamp());
				TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpointIdNew, checkpointNew.getCheckpointTimestamp());
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));
			}

			// decline again, nothing should happen
			// decline from the other task, nothing should happen
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpointId, checkpoint.getCheckpointTimestamp()));
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID2, checkpointId, checkpoint.getCheckpointTimestamp()));
			assertTrue(checkpoint.isDiscarded());

			// should still have the same second checkpoint pending
			long checkpointIdNew2 = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			assertEquals(checkpointIdNew2, checkpointIdNew);

			coord.shutdown();
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
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp));

			// trigger second checkpoint, should also succeed
			assertTrue(coord.triggerCheckpoint(timestamp + 2));

			// validate that we have a pending checkpoint
			assertEquals(2, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

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
			assertEquals(0, checkpoint1.getTaskStates().size());
			assertFalse(checkpoint1.isDiscarded());
			assertFalse(checkpoint1.isFullyAcknowledged());

			assertNotNull(checkpoint2);
			assertEquals(checkpoint2Id, checkpoint2.getCheckpointId());
			assertEquals(timestamp + 2, checkpoint2.getCheckpointTimestamp());
			assertEquals(jid, checkpoint2.getJobId());
			assertEquals(2, checkpoint2.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint2.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint2.getTaskStates().size());
			assertFalse(checkpoint2.isDiscarded());
			assertFalse(checkpoint2.isFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpoint1Id, timestamp);
				TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpoint1Id, timestamp);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));
			}

			// check that the vertices received the trigger checkpoint message for the second checkpoint
			{
				TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpoint2Id, timestamp + 2);
				TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpoint2Id, timestamp + 2);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));
			}

			// decline checkpoint from one of the tasks, this should cancel the checkpoint
			// and trigger a new one
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id, checkpoint1.getCheckpointTimestamp()));
			assertTrue(checkpoint1.isDiscarded());

			// validate that we have only one pending checkpoint left
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// validate that it is the same second checkpoint from earlier
			long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpointNew = coord.getPendingCheckpoints().get(checkpointIdNew);
			assertEquals(checkpoint2Id, checkpointIdNew);

			assertNotNull(checkpointNew);
			assertEquals(checkpointIdNew, checkpointNew.getCheckpointId());
			assertEquals(jid, checkpointNew.getJobId());
			assertEquals(2, checkpointNew.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpointNew.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpointNew.getTaskStates().size());
			assertFalse(checkpointNew.isDiscarded());
			assertFalse(checkpointNew.isFullyAcknowledged());
			assertNotEquals(checkpoint1.getCheckpointId(), checkpointNew.getCheckpointId());

			// decline again, nothing should happen
			// decline from the other task, nothing should happen
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID1, checkpoint1Id, checkpoint1.getCheckpointTimestamp()));
			coord.receiveDeclineMessage(new DeclineCheckpoint(jid, attemptID2, checkpoint1Id, checkpoint1.getCheckpointTimestamp()));
			assertTrue(checkpoint1.isDiscarded());

			coord.shutdown();
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
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp));

			// validate that we have a pending checkpoint
			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			long checkpointId = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().get(checkpointId);

			assertNotNull(checkpoint);
			assertEquals(checkpointId, checkpoint.getCheckpointId());
			assertEquals(timestamp, checkpoint.getCheckpointTimestamp());
			assertEquals(jid, checkpoint.getJobId());
			assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
			assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
			assertEquals(0, checkpoint.getTaskStates().size());
			assertFalse(checkpoint.isDiscarded());
			assertFalse(checkpoint.isFullyAcknowledged());

			// check that the vertices received the trigger checkpoint message
			{
				TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpointId, timestamp);
				TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpointId, timestamp);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));
			}

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

			// acknowledge the other task.
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId));

			// the checkpoint is internally converted to a successful checkpoint and the
			// pending checkpoint object is disposed
			assertTrue(checkpoint.isDiscarded());

			// the now we should have a completed checkpoint
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// validate that the relevant tasks got a confirmation message
			{
				NotifyCheckpointComplete confirmMessage1 = new NotifyCheckpointComplete(jid, attemptID1, checkpointId, timestamp);
				NotifyCheckpointComplete confirmMessage2 = new NotifyCheckpointComplete(jid, attemptID2, checkpointId, timestamp);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(confirmMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(confirmMessage2), eq(attemptID2));
			}

			CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, success.getJobId());
			assertEquals(timestamp, success.getTimestamp());
			assertEquals(checkpoint.getCheckpointId(), success.getCheckpointID());
			assertTrue(success.getTaskStates().isEmpty());

			// ---------------
			// trigger another checkpoint and see that this one replaces the other checkpoint
			// ---------------
			final long timestampNew = timestamp + 7;
			coord.triggerCheckpoint(timestampNew);

			long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew));

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

			CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
			assertEquals(jid, successNew.getJobId());
			assertEquals(timestampNew, successNew.getTimestamp());
			assertEquals(checkpointIdNew, successNew.getCheckpointID());
			assertTrue(successNew.getTaskStates().isEmpty());

			// validate that the relevant tasks got a confirmation message
			{
				TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpointIdNew, timestampNew);
				TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpointIdNew, timestampNew);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));

				NotifyCheckpointComplete confirmMessage1 = new NotifyCheckpointComplete(jid, attemptID1, checkpointIdNew, timestampNew);
				NotifyCheckpointComplete confirmMessage2 = new NotifyCheckpointComplete(jid, attemptID2, checkpointIdNew, timestampNew);
				verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(confirmMessage1), eq(attemptID1));
				verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(confirmMessage2), eq(attemptID2));
			}

			coord.shutdown();
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
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
					new ExecutionVertex[] { commitVertex },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp1));

			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending1 = coord.getPendingCheckpoints().values().iterator().next();
			long checkpointId1 = pending1.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID1, checkpointId1, timestamp1), triggerAttemptID1);
			verify(triggerVertex2, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID2, checkpointId1, timestamp1), triggerAttemptID2);

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1));

			// start the second checkpoint
			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp2));

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
			verify(triggerVertex1, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID1, checkpointId2, timestamp2), triggerAttemptID1);
			verify(triggerVertex2, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID2, checkpointId2, timestamp2), triggerAttemptID2);

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
			verify(commitVertex, times(1)).sendMessageToCurrentExecution(
					new NotifyCheckpointComplete(jid, commitAttemptID, checkpointId1, timestamp1), commitAttemptID);

			// send the last remaining ack for the second checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2));

			// now, the second checkpoint should be confirmed
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertTrue(pending2.isDiscarded());

			// the second commit message should be out
			verify(commitVertex, times(1)).sendMessageToCurrentExecution(
					new NotifyCheckpointComplete(jid, commitAttemptID, checkpointId2, timestamp2), commitAttemptID);

			// validate the committed checkpoints
			List<CompletedCheckpoint> scs = coord.getSuccessfulCheckpoints();

			CompletedCheckpoint sc1 = scs.get(0);
			assertEquals(checkpointId1, sc1.getCheckpointID());
			assertEquals(timestamp1, sc1.getTimestamp());
			assertEquals(jid, sc1.getJobId());
			assertTrue(sc1.getTaskStates().isEmpty());

			CompletedCheckpoint sc2 = scs.get(1);
			assertEquals(checkpointId2, sc2.getCheckpointID());
			assertEquals(timestamp2, sc2.getTimestamp());
			assertEquals(jid, sc2.getJobId());
			assertTrue(sc2.getTaskStates().isEmpty());

			coord.shutdown();
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
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
					new ExecutionVertex[] { commitVertex },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(10, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp1));

			assertEquals(1, coord.getNumberOfPendingCheckpoints());
			assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

			PendingCheckpoint pending1 = coord.getPendingCheckpoints().values().iterator().next();
			long checkpointId1 = pending1.getCheckpointId();

			// trigger messages should have been sent
			verify(triggerVertex1, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID1, checkpointId1, timestamp1), triggerAttemptID1);
			verify(triggerVertex2, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID2, checkpointId1, timestamp1), triggerAttemptID2);

			// acknowledge one of the three tasks
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId1));

			// start the second checkpoint
			// trigger the first checkpoint. this should succeed
			assertTrue(coord.triggerCheckpoint(timestamp2));

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
			verify(triggerVertex1, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID1, checkpointId2, timestamp2), triggerAttemptID1);
			verify(triggerVertex2, times(1)).sendMessageToCurrentExecution(
					new TriggerCheckpoint(jid, triggerAttemptID2, checkpointId2, timestamp2), triggerAttemptID2);

			// we acknowledge one more task from the first checkpoint and the second
			// checkpoint completely. The second checkpoint should then subsume the first checkpoint
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId2));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId2));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpointId1));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID2, checkpointId2));

			// now, the second checkpoint should be confirmed, and the first discarded
			// actually both pending checkpoints are discarded, and the second has been transformed
			// into a successful checkpoint
			assertTrue(pending1.isDiscarded());
			assertTrue(pending2.isDiscarded());

			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

			// validate the committed checkpoints
			List<CompletedCheckpoint> scs = coord.getSuccessfulCheckpoints();
			CompletedCheckpoint success = scs.get(0);
			assertEquals(checkpointId2, success.getCheckpointID());
			assertEquals(timestamp2, success.getTimestamp());
			assertEquals(jid, success.getJobId());
			assertTrue(success.getTaskStates().isEmpty());

			// the first confirm message should be out
			verify(commitVertex, times(1)).sendMessageToCurrentExecution(
					new NotifyCheckpointComplete(jid, commitAttemptID, checkpointId2, timestamp2), commitAttemptID);

			// send the last remaining ack for the first checkpoint. This should not do anything
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID3, checkpointId1));

			coord.shutdown();
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
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] { commitVertex },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			// trigger a checkpoint, partially acknowledged
			assertTrue(coord.triggerCheckpoint(timestamp));
			assertEquals(1, coord.getNumberOfPendingCheckpoints());

			PendingCheckpoint checkpoint = coord.getPendingCheckpoints().values().iterator().next();
			assertFalse(checkpoint.isDiscarded());

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, ackAttemptID1, checkpoint.getCheckpointId()));

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

			// no confirm message must have been sent
			verify(commitVertex, times(0))
					.sendMessageToCurrentExecution(any(NotifyCheckpointComplete.class), any(ExecutionAttemptID.class));

			coord.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void handleMessagesForNonExistingCheckpoints() {
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
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] { commitVertex },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			assertTrue(coord.triggerCheckpoint(timestamp));

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

			coord.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
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
			
			doAnswer(new Answer<Void>() {
				
				private long lastId = -1;
				private long lastTs = -1;
				
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					TriggerCheckpoint message = (TriggerCheckpoint) invocation.getArguments()[0];
					long id = message.getCheckpointId();
					long ts = message.getTimestamp();
					
					assertTrue(id > lastId);
					assertTrue(ts >= lastTs);
					assertTrue(ts >= start);
					
					lastId = id;
					lastTs = ts;
					numCalls.incrementAndGet();
					return null;
				}
			}).when(triggerVertex).sendMessageToCurrentExecution(any(Serializable.class), any(ExecutionAttemptID.class));
			
			CheckpointCoordinator coord = new CheckpointCoordinator(
					jid,
					10,        // periodic interval is 10 ms
					200000,    // timeout is very long (200 s)
					0,
					Integer.MAX_VALUE,
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			
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

			coord.shutdown();
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
	public void testMinInterval() {
		try {
			final JobID jid = new JobID();

			// create some mock execution vertices and trigger some checkpoint
			final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
			ExecutionVertex vertex1 = mockExecutionVertex(attemptID1);

			final AtomicInteger numCalls = new AtomicInteger();

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					if (invocation.getArguments()[0] instanceof TriggerCheckpoint) {
						numCalls.incrementAndGet();
					}
					return null;
				}
			}).when(vertex1).sendMessageToCurrentExecution(any(Serializable.class), any(ExecutionAttemptID.class));

			CheckpointCoordinator coord = new CheckpointCoordinator(
					jid,
					10,        // periodic interval is 10 ms
					200000,    // timeout is very long (200 s)
					500,    // 500ms delay between checkpoints
					10,
					new ExecutionVertex[] { vertex1 },
					new ExecutionVertex[] { vertex1 },
					new ExecutionVertex[] { vertex1 },
					cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

			coord.startCheckpointScheduler();

			//wait until the first checkpoint was triggered
			for (int x=0; x<20; x++) {
				Thread.sleep(100);
				if (numCalls.get() > 0) {
					break;
				}
			}

			if (numCalls.get() == 0) {
				fail("No checkpoint was triggered within the first 2000 ms.");
			}
			
			long start = System.currentTimeMillis();

			for (int x = 0; x < 20; x++) {
				Thread.sleep(100);
				int triggeredCheckpoints = numCalls.get();
				long curT = System.currentTimeMillis();

				/**
				 * Within a given time-frame T only T/500 checkpoints may be triggered due to the configured minimum
				 * interval between checkpoints. This value however does not not take the first triggered checkpoint
				 * into account (=> +1). Furthermore we have to account for the mis-alignment between checkpoints
				 * being triggered and our time measurement (=> +1); for T=1200 a total of 3-4 checkpoints may have been
				 * triggered depending on whether the end of the minimum interval for the first checkpoints ends before
				 * or after T=200.
				 */
				long maxAllowedCheckpoints = (curT - start) / 500 + 2;
				assertTrue(maxAllowedCheckpoints >= triggeredCheckpoints);
			}

			coord.stopCheckpointScheduler();

			coord.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
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
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				cl,
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1, cl),
				new HeapSavepointStore(),
				new DisabledCheckpointStatsTracker());

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

		// trigger the first checkpoint. this should succeed
		Future<String> savepointFuture = coord.triggerSavepoint(timestamp);
		assertFalse(savepointFuture.isCompleted());

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
		assertEquals(0, pending.getTaskStates().size());
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(pending.canBeSubsumed());
		assertTrue(pending instanceof PendingSavepoint);


		// acknowledge from one of the tasks
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId));
		assertEquals(1, pending.getNumberOfAcknowledgedTasks());
		assertEquals(1, pending.getNumberOfNonAcknowledgedTasks());
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(savepointFuture.isCompleted());

		// acknowledge the same task again (should not matter)
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId));
		assertFalse(pending.isDiscarded());
		assertFalse(pending.isFullyAcknowledged());
		assertFalse(savepointFuture.isCompleted());

		// acknowledge the other task.
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId));

		// the checkpoint is internally converted to a successful checkpoint and the
		// pending checkpoint object is disposed
		assertTrue(pending.isDiscarded());
		assertTrue(savepointFuture.isCompleted());

		// the now we should have a completed checkpoint
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertEquals(0, coord.getNumberOfPendingCheckpoints());

		// validate that the relevant tasks got a confirmation message
		{
			NotifyCheckpointComplete confirmMessage1 = new NotifyCheckpointComplete(jid, attemptID1, checkpointId, timestamp);
			NotifyCheckpointComplete confirmMessage2 = new NotifyCheckpointComplete(jid, attemptID2, checkpointId, timestamp);
			verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(confirmMessage1), eq(attemptID1));
			verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(confirmMessage2), eq(attemptID2));
		}

		CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
		assertEquals(jid, success.getJobId());
		assertEquals(timestamp, success.getTimestamp());
		assertEquals(pending.getCheckpointId(), success.getCheckpointID());
		assertTrue(success.getTaskStates().isEmpty());

		// ---------------
		// trigger another checkpoint and see that this one replaces the other checkpoint
		// ---------------
		final long timestampNew = timestamp + 7;
		savepointFuture = coord.triggerSavepoint(timestampNew);
		assertFalse(savepointFuture.isCompleted());

		long checkpointIdNew = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointIdNew));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointIdNew));

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		CompletedCheckpoint successNew = coord.getSuccessfulCheckpoints().get(0);
		assertEquals(jid, successNew.getJobId());
		assertEquals(timestampNew, successNew.getTimestamp());
		assertEquals(checkpointIdNew, successNew.getCheckpointID());
		assertTrue(successNew.getTaskStates().isEmpty());
		assertTrue(savepointFuture.isCompleted());

		// validate that the relevant tasks got a confirmation message
		{
			TriggerCheckpoint expectedMessage1 = new TriggerCheckpoint(jid, attemptID1, checkpointIdNew, timestampNew);
			TriggerCheckpoint expectedMessage2 = new TriggerCheckpoint(jid, attemptID2, checkpointIdNew, timestampNew);
			verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(expectedMessage1), eq(attemptID1));
			verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(expectedMessage2), eq(attemptID2));

			NotifyCheckpointComplete confirmMessage1 = new NotifyCheckpointComplete(jid, attemptID1, checkpointIdNew, timestampNew);
			NotifyCheckpointComplete confirmMessage2 = new NotifyCheckpointComplete(jid, attemptID2, checkpointIdNew, timestampNew);
			verify(vertex1, times(1)).sendMessageToCurrentExecution(eq(confirmMessage1), eq(attemptID1));
			verify(vertex2, times(1)).sendMessageToCurrentExecution(eq(confirmMessage2), eq(attemptID2));
		}

		coord.shutdown();
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
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				new ExecutionVertex[] { vertex1, vertex2 },
				cl,
				counter,
				new StandaloneCompletedCheckpointStore(10, cl),
				new HeapSavepointStore(),
				new DisabledCheckpointStatsTracker());

		// Trigger savepoint and checkpoint
		Future<String> savepointFuture1 = coord.triggerSavepoint(timestamp);
		long savepointId1 = counter.getLast();
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		assertTrue(coord.triggerCheckpoint(timestamp + 1));
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		assertTrue(coord.triggerCheckpoint(timestamp + 2));
		long checkpointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd checkpoint should subsume the 1st checkpoint, but not the savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId2));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId2));

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());

		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());
		assertFalse(savepointFuture1.isCompleted());

		assertTrue(coord.triggerCheckpoint(timestamp + 3));
		assertEquals(2, coord.getNumberOfPendingCheckpoints());

		Future<String> savepointFuture2 = coord.triggerSavepoint(timestamp + 4);
		long savepointId2 = counter.getLast();
		assertEquals(3, coord.getNumberOfPendingCheckpoints());

		// 2nd savepoint should subsume the last checkpoint, but not the 1st savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId2));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId2));

		assertEquals(1, coord.getNumberOfPendingCheckpoints());
		assertEquals(2, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertFalse(coord.getPendingCheckpoints().get(savepointId1).isDiscarded());

		assertFalse(savepointFuture1.isCompleted());
		assertTrue(savepointFuture2.isCompleted());

		// Ack first savepoint
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, savepointId1));
		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, savepointId1));

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(3, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertTrue(savepointFuture1.isCompleted());
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

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					numCalls.incrementAndGet();
					return null;
				}
			}).when(triggerVertex).sendMessageToCurrentExecution(any(Serializable.class), any(ExecutionAttemptID.class));

			CheckpointCoordinator coord = new CheckpointCoordinator(
					jid,
					10,        // periodic interval is 10 ms
					200000,    // timeout is very long (200 s)
					0L,        // no extra delay
					maxConcurrentAttempts,
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter
					(), new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

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
			
			verify(triggerVertex, times(maxConcurrentAttempts))
					.sendMessageToCurrentExecution(any(TriggerCheckpoint.class), eq(triggerAttemptID));
			
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
			
			coord.shutdown();
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
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter
					(), new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());

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
			
			coord.shutdown();
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
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl),
					new HeapSavepointStore(),
					new DisabledCheckpointStatsTracker());
			
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
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				cl,
				checkpointIDCounter,
				new StandaloneCompletedCheckpointStore(2, cl),
				new HeapSavepointStore(),
				new DisabledCheckpointStatsTracker());

		List<Future<String>> savepointFutures = new ArrayList<>();

		int numSavepoints = 5;

		// Trigger savepoints
		for (int i = 0; i < numSavepoints; i++) {
			savepointFutures.add(coord.triggerSavepoint(i));
		}

		// After triggering multiple savepoints, all should in progress
		for (Future<String> savepointFuture : savepointFutures) {
			assertFalse(savepointFuture.isCompleted());
		}

		// ACK all savepoints
		long checkpointId = checkpointIDCounter.getLast();
		for (int i = 0; i < numSavepoints; i++, checkpointId--) {
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobId, attemptID1, checkpointId));
		}

		// After ACKs, all should be completed
		for (Future<String> savepointFuture : savepointFutures) {
			assertTrue(savepointFuture.isCompleted());
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
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				new ExecutionVertex[] { vertex1 },
				cl,
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(2, cl),
				new HeapSavepointStore(),
				new DisabledCheckpointStatsTracker());

		Future<String> savepoint0 = coord.triggerSavepoint(0);
		assertFalse("Did not trigger savepoint", savepoint0.isCompleted());

		Future<String> savepoint1 = coord.triggerSavepoint(1);
		assertFalse("Did not trigger savepoint", savepoint1.isCompleted());
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

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

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
				0,
				Integer.MAX_VALUE,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			cl,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1, cl),
			new HeapSavepointStore(),
			new DisabledCheckpointStatsTracker());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> nonPartitionedState = generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> partitionedKeyGroupState = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				nonPartitionedState,
				partitionedKeyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> nonPartitionedState = generateStateForVertex(jobVertexID2, index);
			List<KeyGroupsStateHandle> partitionedKeyGroupState = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				nonPartitionedState,
				partitionedKeyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		tasks.put(jobVertexID1, jobVertex1);
		tasks.put(jobVertexID2, jobVertex2);

		coord.restoreLatestCheckpointedState(tasks, true, true);

		// verify the restored state
		verifiyStateRestore(jobVertexID1, jobVertex1, keyGroupPartitions1);
		verifiyStateRestore(jobVertexID2, jobVertex2, keyGroupPartitions2);
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

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			cl,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1, cl),
			new HeapSavepointStore(),
			new DisabledCheckpointStatsTracker());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID2, index);
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

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

		coord.restoreLatestCheckpointedState(tasks, true, true);

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

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			cl,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1, cl),
			new HeapSavepointStore(),
			new DisabledCheckpointStatsTracker());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(
					jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				jid,
				jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
				checkpointId,
				valueSizeTuple,
				keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {

			ChainedStateHandle<StreamStateHandle> state = generateStateForVertex(jobVertexID2, index);
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(
					jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					state,
					keyGroupState);

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

		coord.restoreLatestCheckpointedState(tasks, true, true);

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

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[0]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000,
				600000,
				0,
				Integer.MAX_VALUE,
				arrayExecutionVertices,
				arrayExecutionVertices,
				arrayExecutionVertices,
				cl,
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1, cl),
				new HeapSavepointStore(),
				new DisabledCheckpointStatsTracker());

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = coord.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> valueSizeTuple = generateStateForVertex(jobVertexID1, index);
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					valueSizeTuple,
					keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					null,
					keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newParallelism2 = 13;

		List<KeyGroupRange> newKeyGroupPartitions2 = coord.createKeyGroupPartitions(maxParallelism2, newParallelism2);

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
				jobVertexID1,
				parallelism1,
				maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
				jobVertexID2,
				newParallelism2,
				maxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);
		coord.restoreLatestCheckpointedState(tasks, true, true);

		// verify the restored state
		verifiyStateRestore(jobVertexID1, newJobVertex1, keyGroupPartitions1);

		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {
			List<KeyGroupsStateHandle> originalKeyGroupState = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i));

			ChainedStateHandle<StreamStateHandle> operatorState = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getChainedStateHandle();
			List<KeyGroupsStateHandle> keyGroupState = newJobVertex2.getTaskVertices()[i].getCurrentExecutionAttempt().getKeyGroupsStateHandles();

			assertNull(operatorState);
			comparePartitionedState(originalKeyGroupState, keyGroupState);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static void sendAckMessageToCoordinator(
			CheckpointCoordinator coord,
			long checkpointId, JobID jid,
			ExecutionJobVertex jobVertex,
			JobVertexID jobVertexID,
			List<KeyGroupRange> keyGroupPartitions) throws Exception {

		for (int index = 0; index < jobVertex.getParallelism(); index++) {
			ChainedStateHandle<StreamStateHandle> state = generateStateForVertex(jobVertexID, index);
			List<KeyGroupsStateHandle> keyGroupState = generateKeyGroupState(
					jobVertexID,
					keyGroupPartitions.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					state,
					keyGroupState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}
	}

	public static List<KeyGroupsStateHandle> generateKeyGroupState(
			JobVertexID jobVertexID,
			KeyGroupRange keyGroupPartition) throws IOException {

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupPartition);
		List<Integer> testStatesLists = new ArrayList<>(keyGroupPartition.getNumberOfKeyGroups());
		int runningGroupsOffset = 0;
		// generate state for one keygroup
		for (int keyGroupIndex : keyGroupPartition) {
			Random random = new Random(jobVertexID.hashCode() + keyGroupIndex);
			int simulatedStateValue = random.nextInt();
			testStatesLists.add(simulatedStateValue);
		}

		return generateKeyGroupState(keyGroupPartition, testStatesLists);
	}

	public static List<KeyGroupsStateHandle> generateKeyGroupState(KeyGroupRange keyGroupRange, List< ? extends Serializable> states) throws IOException {
		Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() == states.size());

		long[] offsets = new long[keyGroupRange.getNumberOfKeyGroups()];
		List<byte[]> serializedGroupValues = new ArrayList<>(offsets.length);

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, offsets);

		int runningGroupsOffset = 0;
		// generate test state for all keygroups
		int idx = 0;
		for (int keyGroup : keyGroupRange) {
			keyGroupRangeOffsets.setKeyGroupOffset(keyGroup,runningGroupsOffset);
			byte[] serializedValue = InstantiationUtil.serializeObject(states.get(idx));
			runningGroupsOffset += serializedValue.length;
			serializedGroupValues.add(serializedValue);
			++idx;
		}

		//write all generated values in a single byte array, which is index by groupOffsetsInFinalByteArray
		byte[] allSerializedValuesConcatenated = new byte[runningGroupsOffset];
		runningGroupsOffset = 0;
		byte[] old = null;
		for(byte[] serializedGroupValue : serializedGroupValues) {
			System.arraycopy(
					serializedGroupValue,
					0,
					allSerializedValuesConcatenated,
					runningGroupsOffset,
					serializedGroupValue.length);
			runningGroupsOffset += serializedGroupValue.length;
			old = serializedGroupValue;
		}

		ByteStreamStateHandle allSerializedStatesHandle = new ByteStreamStateHandle(
				allSerializedValuesConcatenated);
		KeyGroupsStateHandle keyGroupsStateHandle = new KeyGroupsStateHandle(
				keyGroupRangeOffsets,
				allSerializedStatesHandle);
		List<KeyGroupsStateHandle> keyGroupsStateHandleList = new ArrayList<>();
		keyGroupsStateHandleList.add(keyGroupsStateHandle);
		return keyGroupsStateHandleList;
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
		return ChainedStateHandle.wrapSingleHandle(ByteStreamStateHandle.fromSerializable(value));
	}

	public static ExecutionJobVertex mockExecutionJobVertex(
		JobVertexID jobVertexID,
		int parallelism,
		int maxParallelism) {
		final ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);

		ExecutionVertex[] executionVertices = new ExecutionVertex[parallelism];

		for (int i = 0; i < parallelism; i++) {
			executionVertices[i] = mockExecutionVertex(
				new ExecutionAttemptID(),
				jobVertexID,
				parallelism,
				maxParallelism,
				ExecutionState.RUNNING);

			when(executionVertices[i].getParallelSubtaskIndex()).thenReturn(i);
		}

		when(executionJobVertex.getJobVertexId()).thenReturn(jobVertexID);
		when(executionJobVertex.getTaskVertices()).thenReturn(executionVertices);
		when(executionJobVertex.getParallelism()).thenReturn(parallelism);
		when(executionJobVertex.getMaxParallelism()).thenReturn(maxParallelism);

		return executionJobVertex;
	}

	private static ExecutionVertex mockExecutionVertex(ExecutionAttemptID attemptID) {
		return mockExecutionVertex(
			attemptID,
			new JobVertexID(),
			1,
			1,
			ExecutionState.RUNNING);
	}

	private static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		JobVertexID jobVertexID,
		int parallelism,
		int maxParallelism,
		ExecutionState state,
		ExecutionState ... successiveStates) {

		ExecutionVertex vertex = mock(ExecutionVertex.class);

		final Execution exec = spy(new Execution(
			mock(ExecutionContext.class),
			vertex,
			1,
			1L,
			null
		));
		when(exec.getAttemptId()).thenReturn(attemptID);
		when(exec.getState()).thenReturn(state, successiveStates);

		when(vertex.getJobvertexId()).thenReturn(jobVertexID);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(maxParallelism);

		return vertex;
	}

	public static void verifiyStateRestore(
			JobVertexID jobVertexID, ExecutionJobVertex executionJobVertex,
			List<KeyGroupRange> keyGroupPartitions) throws Exception {

		for (int i = 0; i < executionJobVertex.getParallelism(); i++) {

			ChainedStateHandle<StreamStateHandle> expectNonPartitionedState = generateStateForVertex(jobVertexID, i);
			ChainedStateHandle<StreamStateHandle> actualNonPartitionedState = executionJobVertex.
					getTaskVertices()[i].getCurrentExecutionAttempt().getChainedStateHandle();
			assertEquals(expectNonPartitionedState.get(0), actualNonPartitionedState.get(0));

			List<KeyGroupsStateHandle> expectPartitionedKeyGroupState = generateKeyGroupState(
					jobVertexID,
					keyGroupPartitions.get(i));
			List<KeyGroupsStateHandle> actualPartitionedKeyGroupState = executionJobVertex.
					getTaskVertices()[i].getCurrentExecutionAttempt().getKeyGroupsStateHandles();
			comparePartitionedState(expectPartitionedKeyGroupState, actualPartitionedKeyGroupState);
		}
	}

	public static void comparePartitionedState(
			List<KeyGroupsStateHandle> expectPartitionedKeyGroupState,
			List<KeyGroupsStateHandle> actualPartitionedKeyGroupState) throws Exception {

		KeyGroupsStateHandle expectedHeadOpKeyGroupStateHandle = expectPartitionedKeyGroupState.get(0);
		int expectedTotalKeyGroups = expectedHeadOpKeyGroupStateHandle.getNumberOfKeyGroups();
		int actualTotalKeyGroups = 0;
		for(KeyGroupsStateHandle keyGroupsStateHandle: actualPartitionedKeyGroupState) {
			actualTotalKeyGroups += keyGroupsStateHandle.getNumberOfKeyGroups();
		}

		assertEquals(expectedTotalKeyGroups, actualTotalKeyGroups);

		FSDataInputStream inputStream = expectedHeadOpKeyGroupStateHandle.getStateHandle().openInputStream();
		for(int groupId : expectedHeadOpKeyGroupStateHandle.keyGroups()) {
			long offset = expectedHeadOpKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
			inputStream.seek(offset);
			int expectedKeyGroupState = InstantiationUtil.deserializeObject(inputStream);
			for(KeyGroupsStateHandle oneActualKeyGroupStateHandle : actualPartitionedKeyGroupState) {
				if (oneActualKeyGroupStateHandle.containsKeyGroup(groupId)) {
					long actualOffset = oneActualKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
					FSDataInputStream actualInputStream = oneActualKeyGroupStateHandle.getStateHandle().openInputStream();
					actualInputStream.seek(actualOffset);
					int actualGroupState = InstantiationUtil.deserializeObject(actualInputStream);

					assertEquals(expectedKeyGroupState, actualGroupState);
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

	private void testCreateKeyGroupPartitions(int maxParallelism, int parallelism) {
		List<KeyGroupRange> ranges = CheckpointCoordinator.createKeyGroupPartitions(maxParallelism, parallelism);
		for (int i = 0; i < maxParallelism; ++i) {
			KeyGroupRange range = ranges.get(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, i));
			if (!range.contains(i)) {
				Assert.fail("Could not find expected key-group " + i + " in range " + range);
			}
		}
	}

}
