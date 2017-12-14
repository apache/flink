/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;

public class TaskStateManagerImplTest {

	/**
	 * TODO complete this test to also include the local state part!!
	 */
	@Test
	public void testStateReportingAndRetrieving() {

		JobID jobID = new JobID(42L, 43L);
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID(23L, 24L);
		TestCheckpointResponder checkpointResponderMock = new TestCheckpointResponder();

		TaskStateManager taskStateManager = taskStateManager(
			jobID,
			executionAttemptID,
			checkpointResponderMock,
			null);

		//---------------------------------------- test reporting -----------------------------------------

		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(74L, 11L);
		CheckpointMetrics checkpointMetrics = new CheckpointMetrics();
		TaskStateSnapshot jmTaskStateSnapshot = new TaskStateSnapshot();

		OperatorID operatorID_1 = new OperatorID(1L, 1L);
		OperatorID operatorID_2 = new OperatorID(2L, 2L);
		OperatorID operatorID_3 = new OperatorID(3L, 3L);

		Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_1).isRestored());
		Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_2).isRestored());
		Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_3).isRestored());

		OperatorSubtaskState jmOperatorSubtaskState_1 = new OperatorSubtaskState();
		OperatorSubtaskState jmOperatorSubtaskState_2 = new OperatorSubtaskState();

		jmTaskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, jmOperatorSubtaskState_1);
		jmTaskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, jmOperatorSubtaskState_2);

		//---

		TaskStateSnapshot tmTaskStateSnapshot = new TaskStateSnapshot();

		//TODO orthogonal for tmTaskStateSnapshot!

		taskStateManager.reportTaskStateSnapshots(
			checkpointMetaData,
			checkpointMetrics,
			jmTaskStateSnapshot,
			tmTaskStateSnapshot);

		TestCheckpointResponder.AcknowledgeReport acknowledgeReport =
			checkpointResponderMock.getAcknowledgeReports().get(0);

		Assert.assertEquals(checkpointMetaData.getCheckpointId(), acknowledgeReport.getCheckpointId());
		Assert.assertEquals(checkpointMetrics, acknowledgeReport.getCheckpointMetrics());
		Assert.assertEquals(executionAttemptID, acknowledgeReport.getExecutionAttemptID());
		Assert.assertEquals(jobID, acknowledgeReport.getJobID());
		Assert.assertEquals(jmTaskStateSnapshot, acknowledgeReport.getSubtaskState());

		//---------------------------------------- test retrieving -----------------------------------------

		JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(
			0L,
			acknowledgeReport.getSubtaskState());

		taskStateManager = taskStateManager(
			jobID,
			executionAttemptID,
			checkpointResponderMock,
			taskRestore);
//TODO
//		Assert.assertTrue(jmOperatorSubtaskState_1 == taskStateManager.prioritizedOperatorState(operatorID_1));
//		Assert.assertTrue(jmOperatorSubtaskState_2 == taskStateManager.prioritizedOperatorState(operatorID_2));
		Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_3).isRestored());
	}

	/**
	 * This tests if the {@link TaskStateManager} properly returns the the subtask local state dir from the
	 * corresponding {@link TaskLocalStateStore}.
	 */
	@Test
	public void testForwardingSubtaskLocalStateBaseDirFromLocalStateStore() throws IOException {
		JobID jobID = new JobID(42L, 43L);
		AllocationID allocationID = new AllocationID(4711L, 23L);
		JobVertexID jobVertexID = new JobVertexID(12L, 34L);
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID(23L, 24L);
		TestCheckpointResponder checkpointResponderMock = new TestCheckpointResponder();

		Executor directExecutor = Executors.directExecutor();

		TemporaryFolder tmpFolder = new TemporaryFolder();

		try {
			tmpFolder.create();

			File[] rootDirs = new File[]{tmpFolder.newFolder(), tmpFolder.newFolder(), tmpFolder.newFolder()};

			TaskLocalStateStore taskLocalStateStore =
				new TaskLocalStateStore(jobID, allocationID, jobVertexID, 13, rootDirs, directExecutor);

			TaskStateManager taskStateManager = taskStateManager(
				jobID,
				executionAttemptID,
				checkpointResponderMock,
				null,
				taskLocalStateStore);

			LocalRecoveryDirectoryProvider directoryProviderFromTaskLocalStateStore =
				taskLocalStateStore.getLocalRecoveryRootDirectoryProvider();

			LocalRecoveryDirectoryProvider directoryProviderFromTaskStateManager =
				taskStateManager.createLocalRecoveryRootDirectoryProvider();


			for (int i = 0; i < 10; ++i) {
				Assert.assertEquals(rootDirs[i % rootDirs.length],
					directoryProviderFromTaskLocalStateStore.rootDirectory(i));
				Assert.assertEquals(rootDirs[i % rootDirs.length],
					directoryProviderFromTaskStateManager.rootDirectory(i));
			}

//			Assert.assertEquals(
//				directoryProviderFromTaskLocalStateStore.getSubtaskSpecificPath(),
//				directoryProviderFromTaskStateManager.getSubtaskSpecificPath());

		} finally {
			tmpFolder.delete();
		}
	}

	public static TaskStateManager taskStateManager(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponderMock,
		JobManagerTaskRestore jobManagerTaskRestore) {

		// for now just a mock because this is not yet implemented
		TaskLocalStateStore taskLocalStateStore = mock(TaskLocalStateStore.class);

		return taskStateManager(
			jobID,
			executionAttemptID,
			checkpointResponderMock,
			jobManagerTaskRestore,
			taskLocalStateStore);
	}

	public static TaskStateManager taskStateManager(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponderMock,
		JobManagerTaskRestore jobManagerTaskRestore,
		TaskLocalStateStore localStateStore) {

		return new TaskStateManagerImpl(
			jobID,
			executionAttemptID,
			localStateStore,
			jobManagerTaskRestore,
			checkpointResponderMock);
	}
}
