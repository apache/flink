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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(PendingCheckpoint.class)
public class CheckpointCoordinatorFailureTest extends TestLogger {

	/**
	 * Tests that a failure while storing a completed checkpoint in the completed checkpoint store
	 * will properly fail the originating pending checkpoint and clean upt the completed checkpoint.
	 */
	@Test
	public void testFailingCompletedCheckpointStoreAdd() throws Exception {
		JobID jid = new JobID();

		final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
		final ExecutionVertex vertex = CheckpointCoordinatorTest.mockExecutionVertex(executionAttemptId);

		final long triggerTimestamp = 1L;

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			ExternalizedCheckpointSettings.none(),
			new ExecutionVertex[]{vertex},
			new ExecutionVertex[]{vertex},
			new ExecutionVertex[]{vertex},
			new StandaloneCheckpointIDCounter(),
			new FailingCompletedCheckpointStore(),
			null,
			Executors.directExecutor());

		coord.triggerCheckpoint(triggerTimestamp, false);

		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		PendingCheckpoint pendingCheckpoint = coord.getPendingCheckpoints().values().iterator().next();

		assertFalse(pendingCheckpoint.isDiscarded());

		final long checkpointId = coord.getPendingCheckpoints().keySet().iterator().next();
		
		SubtaskState subtaskState = mock(SubtaskState.class);
		PowerMockito.when(subtaskState.getLegacyOperatorState()).thenReturn(null);
		PowerMockito.when(subtaskState.getManagedOperatorState()).thenReturn(null);
		
		AcknowledgeCheckpoint acknowledgeMessage = new AcknowledgeCheckpoint(jid, executionAttemptId, checkpointId, new CheckpointMetrics(), subtaskState);
		
		try {
			coord.receiveAcknowledgeMessage(acknowledgeMessage);
			fail("Expected a checkpoint exception because the completed checkpoint store could not " +
				"store the completed checkpoint.");
		} catch (CheckpointException e) {
			// ignore because we expected this exception
		}

		// make sure that the pending checkpoint has been discarded after we could not complete it
		assertTrue(pendingCheckpoint.isDiscarded());

		// make sure that the subtask state has been discarded after we could not complete it.
		verify(subtaskState, times(1)).discardSharedStatesOnFail();
		verify(subtaskState).discardState();
	}

	private static final class FailingCompletedCheckpointStore implements CompletedCheckpointStore {

		@Override
		public void recover(SharedStateRegistry sharedStateRegistry) throws Exception {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public void addCheckpoint(CompletedCheckpoint checkpoint, SharedStateRegistry sharedStateRegistry) throws Exception {
			throw new Exception("The failing completed checkpoint store failed again... :-(");
		}

		@Override
		public CompletedCheckpoint getLatestCheckpoint() throws Exception {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public void shutdown(JobStatus jobStatus, SharedStateRegistry sharedStateRegistry) throws Exception {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public int getNumberOfRetainedCheckpoints() {
			return -1;
		}

		@Override
		public int getMaxNumberOfRetainedCheckpoints() {
			return 1;
		}

		@Override
		public boolean requiresExternalizedCheckpoints() {
			return false;
		}
	}
}
