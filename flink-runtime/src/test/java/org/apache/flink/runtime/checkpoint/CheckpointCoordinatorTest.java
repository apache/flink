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
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;

import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
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
					jid, 600000, 600000,
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] {}, cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl), RecoveryMode.STANDALONE);

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
			ExecutionVertex triggerVertex2 = mockExecutionVertex(triggerAttemptID2, ExecutionState.FINISHED);

			// create some mock Execution vertices that need to ack the checkpoint
			final ExecutionAttemptID ackAttemptID1 = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID2 = new ExecutionAttemptID();
			ExecutionVertex ackVertex1 = mockExecutionVertex(ackAttemptID1);
			ExecutionVertex ackVertex2 = mockExecutionVertex(ackAttemptID2);

			// set up the coordinator and validate the initial state
			CheckpointCoordinator coord = new CheckpointCoordinator(
					jid, 600000, 600000,
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] {}, cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl), RecoveryMode.STANDALONE);

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
					jid, 600000, 600000,
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] {}, cl, new StandaloneCheckpointIDCounter(), new
					StandaloneCompletedCheckpointStore(1, cl), RecoveryMode.STANDALONE);

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
					jid, 600000, 600000,
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 },
					new ExecutionVertex[] { vertex1, vertex2 }, cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1, cl), RecoveryMode.STANDALONE);

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
			assertEquals(0, checkpoint.getCollectedStates().size());
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
			assertTrue(success.getStates().isEmpty());

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
			assertTrue(successNew.getStates().isEmpty());

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
					jid, 600000, 600000,
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
					new ExecutionVertex[] { commitVertex }, cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE);

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
			assertTrue(sc1.getStates().isEmpty());

			CompletedCheckpoint sc2 = scs.get(1);
			assertEquals(checkpointId2, sc2.getCheckpointID());
			assertEquals(timestamp2, sc2.getTimestamp());
			assertEquals(jid, sc2.getJobId());
			assertTrue(sc2.getStates().isEmpty());

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
					jid, 600000, 600000,
					new ExecutionVertex[] { triggerVertex1, triggerVertex2 },
					new ExecutionVertex[] { ackVertex1, ackVertex2, ackVertex3 },
					new ExecutionVertex[] { commitVertex }, cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(10, cl), RecoveryMode.STANDALONE);

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
			assertTrue(success.getStates().isEmpty());

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
					jid, 600000, 200,
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] { commitVertex }, cl,
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE);

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
					jid, 200000, 200000,
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex1, ackVertex2 },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter
					(), new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE);

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
					10,		// periodic interval is 10 ms
					200000,	// timeout is very long (200 s)
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter
					(), new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE);

			
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

	@Test
	public void testMaxConcurrentAttempts1() {
		testMaxConcurrentAttemps(1);
	}

	@Test
	public void testMaxConcurrentAttempts2() {
		testMaxConcurrentAttemps(2);
	}

	@Test
	public void testMaxConcurrentAttempts5() {
		testMaxConcurrentAttemps(5);
	}
	
	private void testMaxConcurrentAttemps(int maxConcurrentAttempts) {
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
					10,		// periodic interval is 10 ms
					200000,	// timeout is very long (200 s)
					0L,		// no extra delay
					maxConcurrentAttempts,
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter
					(), new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE,
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
					10,		// periodic interval is 10 ms
					200000,	// timeout is very long (200 s)
					0L,		// no extra delay
					maxConcurrentAttempts, // max two concurrent checkpoints
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter
					(), new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE,
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
					10,		// periodic interval is 10 ms
					200000,	// timeout is very long (200 s)
					0L,		// no extra delay
					2, // max two concurrent checkpoints
					new ExecutionVertex[] { triggerVertex },
					new ExecutionVertex[] { ackVertex },
					new ExecutionVertex[] { commitVertex }, cl, new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(2, cl), RecoveryMode.STANDALONE,
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
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static ExecutionVertex mockExecutionVertex(ExecutionAttemptID attemptID) {
		return mockExecutionVertex(attemptID, ExecutionState.RUNNING);
	}

	private static ExecutionVertex mockExecutionVertex(ExecutionAttemptID attemptID, 
														ExecutionState state, ExecutionState ... successiveStates) {
		final Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(attemptID);
		when(exec.getState()).thenReturn(state, successiveStates);

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobvertexId()).thenReturn(new JobVertexID());
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

		return vertex;
	}
}
