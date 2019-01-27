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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class ExecutionGraphVariousFailuesTest extends TestLogger {

	/**
	 * Test that failing in state restarting will retrigger the restarting logic. This means that
	 * it only goes into the state FAILED after the restart strategy says the job is no longer
	 * restartable.
	 */
	@Test
	public void testFailureWhileRestarting() throws Exception {
		final TaskManagerGateway taskManagerGateway = spy(new SimpleAckingTaskManagerGateway());
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(
				new JobID(), taskManagerGateway, new InfiniteDelayRestartStrategy(2), createNoOpVertex(10));
		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, eg.getState());

		verify(taskManagerGateway, Mockito.timeout(2000L).times(eg.getTotalNumberOfVertices()))
				.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("Test 1"));
		assertEquals(JobStatus.FAILING, eg.getState());
		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);

		// we should restart since we have two restart attempts left
		assertEquals(JobStatus.RESTARTING, eg.getState());

		eg.failGlobal(new Exception("Test 2"));

		// we should restart since we have one restart attempts left
		assertEquals(JobStatus.RESTARTING, eg.getState());

		eg.failGlobal(new Exception("Test 3"));

		// after depleting all our restart attempts we should go into Failed
		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that a {@link SuppressRestartsException} in state RESTARTING stops the restarting
	 * immediately and sets the execution graph's state to FAILED.
	 */
	@Test
	public void testSuppressRestartFailureWhileRestarting() throws Exception {
		final TaskManagerGateway taskManagerGateway = spy(new SimpleAckingTaskManagerGateway());
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(
				new JobID(), taskManagerGateway, new InfiniteDelayRestartStrategy(10), createNoOpVertex(10));
		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, eg.getState());
		verify(taskManagerGateway, Mockito.timeout(2000L).times(eg.getTotalNumberOfVertices()))
				.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("test"));
		assertEquals(JobStatus.FAILING, eg.getState());

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.RESTARTING, eg.getState());

		// suppress a possible restart
		eg.failGlobal(new SuppressRestartsException(new Exception("Test")));

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that a failing scheduleOrUpdateConsumers call with a non-existing execution attempt
	 * id, will not fail the execution graph.
	 */
	@Test
	public void testFailingScheduleOrUpdateConsumers() throws Exception {
		final TaskManagerGateway taskManagerGateway = spy(new SimpleAckingTaskManagerGateway());
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(
				new JobID(), taskManagerGateway, new InfiniteDelayRestartStrategy(10), createNoOpVertex(10));
		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, eg.getState());
		verify(taskManagerGateway, Mockito.timeout(2000L).times(eg.getTotalNumberOfVertices()))
				.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		IntermediateResultPartitionID intermediateResultPartitionId = new IntermediateResultPartitionID();
		ExecutionAttemptID producerId = new ExecutionAttemptID();
		ResultPartitionID resultPartitionId = new ResultPartitionID(intermediateResultPartitionId, producerId);

		// The execution attempt id does not exist and thus the scheduleOrUpdateConsumers call
		// should fail

		try {
			eg.scheduleOrUpdateConsumers(resultPartitionId);
			fail("Expected ExecutionGraphException.");
		} catch (ExecutionGraphException e) {
			// we've expected this exception to occur
		}

		assertEquals(JobStatus.RUNNING, eg.getState());
	}
}
