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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Validates the results of execution graph recovery based on various task states reports
 */
public class ExecutionGraphRecoveryTest {

	private JobVertexID v1ID = new JobVertexID();
	private JobVertexID v2ID = new JobVertexID();
	private JobVertexID v3ID = new JobVertexID();

	@Test
	public void testRecoverAllRunningExecutions() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());

		recoverVertex(eg, v1ID, ExecutionState.RUNNING, v2ID);
		recoverVertex(eg, v2ID, ExecutionState.RUNNING, v3ID);
		recoverVertex(eg, v3ID, ExecutionState.RUNNING, null);

		eg.tryRunOrFail();

		assertEquals(JobStatus.RUNNING, eg.getState());
	}

	@Test
	public void testRecoverAllFinishedExecutions() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());

		recoverVertex(eg, v1ID, ExecutionState.FINISHED, v2ID);
		recoverVertex(eg, v2ID, ExecutionState.FINISHED, v3ID);
		recoverVertex(eg, v3ID, ExecutionState.FINISHED, null);

		eg.tryRunOrFail();

		assertEquals(JobStatus.FINISHED, eg.getState());
	}

	@Test
	public void testRecoverPartialFinishedExecutions() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());

		recoverVertex(eg, v1ID, ExecutionState.RUNNING, v2ID);
		recoverVertex(eg, v2ID, ExecutionState.RUNNING, v3ID);
		recoverVertex(eg, v3ID, ExecutionState.FINISHED, null);

		eg.tryRunOrFail();

		assertEquals(JobStatus.RUNNING, eg.getState());
	}

	@Test
	public void testRecoverPartialExecutions() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());

		recoverVertex(eg, v1ID, ExecutionState.RUNNING, v2ID);
		recoverVertex(eg, v2ID, ExecutionState.RUNNING, v3ID);

		eg.tryRunOrFail();

		assertEquals(JobStatus.FAILING, eg.getState());
	}

	@Test
	public void testRecoverFailedExecution() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());

		recoverVertex(eg, v1ID, ExecutionState.RUNNING, v2ID);
		recoverVertex(eg, v2ID, ExecutionState.RUNNING, v3ID);
		recoverVertex(eg, v3ID, ExecutionState.FAILED, null);

		eg.tryRunOrFail();

		assertEquals(JobStatus.FAILING, eg.getState());
	}

	@Test
	public void testRecoverCanceledExecution() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());

		recoverVertex(eg, v1ID, ExecutionState.RUNNING, v2ID);
		recoverVertex(eg, v2ID, ExecutionState.RUNNING, v3ID);
		recoverVertex(eg, v3ID, ExecutionState.CANCELED, null);

		eg.tryRunOrFail();

		assertEquals(JobStatus.FAILING, eg.getState());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private void recoverVertex(
			ExecutionGraph eg,
			JobVertexID recoverVertexID,
			ExecutionState recoverState,
			JobVertexID downstreamID) throws Exception {

		final int subtask = 0;
		final int attemptNumber = 1;
		final long startTimestamp = System.currentTimeMillis();

		final ExecutionAttemptID attemptID = new ExecutionAttemptID();

		final ResultPartitionID[] partitionIDs;
		if (downstreamID != null) {
			partitionIDs = new ResultPartitionID[1];
			partitionIDs[0] = new ResultPartitionID(new IntermediateResultPartitionID(), attemptID);
		} else {
			partitionIDs = new ResultPartitionID[0];
		}

		final SimpleSlot slot = new SimpleSlot(mock(AllocatedSlot.class), mock(SlotOwner.class), 0);
		when(slot.getTaskManagerGateway()).thenReturn(spy(new SimpleAckingTaskManagerGateway()));

		final ExecutionVertex ev = eg.getJobVertex(recoverVertexID).getTaskVertices()[subtask];

		eg.recoverForExecution(
			recoverVertexID,
			subtask,
			attemptID,
			attemptNumber,
			recoverState,
			startTimestamp,
			partitionIDs,
			slot);

		final Execution curExecution = ev.getCurrentExecutionAttempt();

		// verify the execution basic information
		assertEquals(curExecution.getAttemptId(), attemptID);
		assertEquals(curExecution.getAttemptNumber(), attemptNumber);
		assertEquals(curExecution.getStateTimestamp(ExecutionState.CREATED), startTimestamp);
		assertEquals(curExecution.getState(), recoverState);
		assertEquals(curExecution.getAssignedResource(), slot);

		// verify the result partition and the input edge of downstream vertex for non-sink vertex
		if (downstreamID != null ) {
			assertTrue(ev.getProducedPartitions().containsKey(partitionIDs[0].getPartitionId()));
			assertNotNull(ev.getJobVertex().getProducedDataSets()[0].getPartitionById(partitionIDs[0].getPartitionId()));

			final ExecutionVertex downstreamVertex = eg.getJobVertex(downstreamID).getTaskVertices()[0];
			assertEquals(downstreamVertex.getInputEdges(0)[0].getSource().getPartitionId(), partitionIDs[0].getPartitionId());
			assertNotNull(downstreamVertex.getJobVertex().getInputs().get(0).getPartitionById(partitionIDs[0].getPartitionId()));
		}
	}

	private ExecutionGraph createExecutionGraph() throws Exception {
		final JobVertex v1 = new JobVertex("v1", v1ID);
		final JobVertex v2 = new JobVertex("v2", v2ID);
		final JobVertex v3 = new JobVertex("v3", v3ID);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);

		v1.setParallelism(1);
		v2.setParallelism(1);
		v3.setParallelism(1);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		List<JobVertex> vertices = new ArrayList<>(Arrays.asList(v1, v2, v3));

		final ExecutionGraph eg =  new ExecutionGraph(
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new JobID(),
			"test job",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new Scheduler(TestingUtils.defaultExecutionContext()));

		eg.attachJobGraph(vertices);

		return eg;
	}

}
