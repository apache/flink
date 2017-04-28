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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointLoader;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * CheckpointCoordinator tests for externalized checkpoints.
 *
 * <p>This is separate from {@link CheckpointCoordinatorTest}, because that
 * test is already huge and covers many different configurations.
 */
public class CheckpointCoordinatorExternalizedCheckpointsTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	/**
	 * Triggers multiple externalized checkpoints and verifies that the metadata
	 * files have been created.
	 */
	@Test
	public void testTriggerAndConfirmSimpleExternalizedCheckpoint()
		throws Exception {
		final JobID jid = new JobID();

		final ExternalizedCheckpointSettings externalizedCheckpointSettings =
			ExternalizedCheckpointSettings.externalizeCheckpoints(false);

		final File checkpointDir = tmp.newFolder();

		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		final ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
		ExecutionVertex vertex1 = CheckpointCoordinatorTest.mockExecutionVertex(attemptID1);
		ExecutionVertex vertex2 = CheckpointCoordinatorTest.mockExecutionVertex(attemptID2);

		Map<JobVertexID, ExecutionJobVertex> jobVertices = new HashMap<>();
		jobVertices.put(vertex1.getJobvertexId(), vertex1.getJobVertex());
		jobVertices.put(vertex2.getJobvertexId(), vertex2.getJobVertex());

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			externalizedCheckpointSettings,
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			new ExecutionVertex[] { vertex1, vertex2 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			checkpointDir.getAbsolutePath(),
			Executors.directExecutor());

		assertEquals(0, coord.getNumberOfPendingCheckpoints());
		assertEquals(0, coord.getNumberOfRetainedSuccessfulCheckpoints());

		// ---------------
		// trigger checkpoint 1
		// ---------------

		{
			final long timestamp1 = System.currentTimeMillis();

			coord.triggerCheckpoint(timestamp1, false);

			long checkpointId1 = coord.getPendingCheckpoints().entrySet().iterator().next()
				.getKey();

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId1));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId1));

			CompletedCheckpoint latest = coord.getCheckpointStore().getLatestCheckpoint();

			verifyExternalizedCheckpoint(latest, jid, checkpointId1, timestamp1);
			verifyExternalizedCheckpointRestore(latest, jobVertices, vertex1, vertex2);
		}

		// ---------------
		// trigger checkpoint 2
		// ---------------

		{
			final long timestamp2 = System.currentTimeMillis() + 7;
			coord.triggerCheckpoint(timestamp2, false);

			long checkpointId2 = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId2));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId2));

			CompletedCheckpoint latest = coord.getCheckpointStore().getLatestCheckpoint();
			verifyExternalizedCheckpoint(latest, jid, checkpointId2, timestamp2);
			verifyExternalizedCheckpointRestore(latest, jobVertices, vertex1, vertex2);
		}

		// ---------------
		// trigger checkpoint 3
		// ---------------

		{
			final long timestamp3 = System.currentTimeMillis() + 146;
			coord.triggerCheckpoint(timestamp3, false);

			long checkpointId3 = coord.getPendingCheckpoints().entrySet().iterator().next().getKey();

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID1, checkpointId3));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptID2, checkpointId3));

			CompletedCheckpoint latest = coord.getCheckpointStore().getLatestCheckpoint();
			verifyExternalizedCheckpoint(latest, jid, checkpointId3, timestamp3);
			verifyExternalizedCheckpointRestore(latest, jobVertices, vertex1, vertex2);
		}

		coord.shutdown(JobStatus.FINISHED);
	}

	/**
	 * Verifies an externalized completed checkpoint instance.
	 *
	 * <p>The provided JobID, checkpoint ID, timestamp need to match. Also, the
	 * external pointer and external metadata need to be notNull and exist (currently
	 * assuming that they are file system based).
	 *
	 * @param checkpoint Completed checkpoint to check.
	 * @param jid JobID of the job the checkpoint belongs to.
	 * @param checkpointId Checkpoint ID of the checkpoint to check.
	 * @param timestamp Timestamp of the checkpoint to check.
	 */
	private static void verifyExternalizedCheckpoint(CompletedCheckpoint checkpoint, JobID jid, long checkpointId, long timestamp) {
		assertEquals(jid, checkpoint.getJobId());
		assertEquals(checkpointId, checkpoint.getCheckpointID());
		assertEquals(timestamp, checkpoint.getTimestamp());
		assertNotNull(checkpoint.getExternalPointer());
		assertNotNull(checkpoint.getExternalizedMetadata());
		FileStateHandle fsHandle = (FileStateHandle) checkpoint.getExternalizedMetadata();
		assertTrue(new File(fsHandle.getFilePath().getPath()).exists());
	}

	private static void verifyExternalizedCheckpointRestore(
			CompletedCheckpoint checkpoint,
			Map<JobVertexID, ExecutionJobVertex> jobVertices,
			ExecutionVertex... vertices) throws IOException {

		CompletedCheckpoint loaded = SavepointLoader.loadAndValidateSavepoint(
				checkpoint.getJobId(),
				jobVertices,
				checkpoint.getExternalPointer(),
				Thread.currentThread().getContextClassLoader(),
				false);

		for (ExecutionVertex vertex : vertices) {
			for (OperatorID operatorID : vertex.getJobVertex().getOperatorIDs()) {
				assertEquals(checkpoint.getOperatorStates().get(operatorID), loaded.getOperatorStates().get(operatorID));
			}
		}
	}

}
