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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TaskStateManagerImplTest {

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
		TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();

		OperatorID operatorID_1 = new OperatorID(1L, 1L);
		OperatorID operatorID_2 = new OperatorID(2L, 2L);
		OperatorID operatorID_3 = new OperatorID(3L, 3L);

		Assert.assertNull(taskStateManager.operatorStates(operatorID_1));
		Assert.assertNull(taskStateManager.operatorStates(operatorID_2));
		Assert.assertNull(taskStateManager.operatorStates(operatorID_3));

		OperatorSubtaskState operatorSubtaskState_1 = new OperatorSubtaskState();
		OperatorSubtaskState operatorSubtaskState_2 = new OperatorSubtaskState();

		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1);
		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, operatorSubtaskState_2);

		taskStateManager.reportStateHandles(checkpointMetaData, checkpointMetrics, taskStateSnapshot);

		TestCheckpointResponder.AcknowledgeReport acknowledgeReport =
			checkpointResponderMock.getAcknowledgeReports().get(0);

		Assert.assertEquals(checkpointMetaData.getCheckpointId(), acknowledgeReport.getCheckpointId());
		Assert.assertEquals(checkpointMetrics, acknowledgeReport.getCheckpointMetrics());
		Assert.assertEquals(executionAttemptID, acknowledgeReport.getExecutionAttemptID());
		Assert.assertEquals(jobID, acknowledgeReport.getJobID());
		Assert.assertEquals(taskStateSnapshot, acknowledgeReport.getSubtaskState());

		//---------------------------------------- test retrieving -----------------------------------------

		JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(
			0L,
			acknowledgeReport.getSubtaskState());

		taskStateManager = taskStateManager(
			jobID,
			executionAttemptID,
			checkpointResponderMock,
			taskRestore);

		Assert.assertEquals(operatorSubtaskState_1, taskStateManager.operatorStates(operatorID_1));
		Assert.assertEquals(operatorSubtaskState_2, taskStateManager.operatorStates(operatorID_2));
		Assert.assertNull(taskStateManager.operatorStates(operatorID_3));
	}

	public static TaskStateManager taskStateManager(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponderMock,
		JobManagerTaskRestore jobManagerTaskRestore) {

		// for now just a mock because this is not yet implemented
		TaskLocalStateStore taskLocalStateStore = mock(TaskLocalStateStore.class);

		return new TaskStateManagerImpl(
			jobID,
			executionAttemptID,
			taskLocalStateStore,
			jobManagerTaskRestore,
			checkpointResponderMock);
	}
}
