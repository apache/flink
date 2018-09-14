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
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Validates that stop() calls are handled correctly.
 */
public class ExecutionGraphStopTest extends TestLogger {

	/**
	 * Tests that STOP is only supported if all sources are stoppable
	 */
	@Test
	public void testStopIfSourcesNotStoppable() throws Exception {
		final ExecutionGraph graph = ExecutionGraphTestUtils.createSimpleTestGraph();

		try {
			graph.stop();
			fail("exception expected");
		}
		catch (StoppingException e) {
			// expected
		}
	}

	/**
	 * Validates that stop is only sent to the sources
	 * 
	 * This test build a simple job with two sources and two non-source vertices.
	 */
	@Test
	public void testStop() throws Exception {
		final int sourcePar1 = 11;
		final int sourcePar2 = 7;

		final JobVertex source1 = new JobVertex("source 1");
		source1.setInvokableClass(StoppableInvokable.class);
		source1.setParallelism(sourcePar1);

		final JobVertex source2 = new JobVertex("source 2");
		source2.setInvokableClass(StoppableInvokable.class);
		source2.setParallelism(sourcePar2);

		final JobVertex nonSource1 = new JobVertex("non-source-1");
		nonSource1.setInvokableClass(NoOpInvokable.class);
		nonSource1.setParallelism(10);

		final JobVertex nonSource2 = new JobVertex("non-source-2");
		nonSource2.setInvokableClass(NoOpInvokable.class);
		nonSource2.setParallelism(10);

		nonSource1.connectNewDataSetAsInput(source1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		nonSource1.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		nonSource2.connectNewDataSetAsInput(nonSource1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobID jid = new JobID();
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(
				jid, source1, source2, nonSource1, nonSource2);

		// we use different gateways for sources and non-sources to make sure the right ones
		// get the RPC calls
		final TaskManagerGateway sourceGateway = spy(new SimpleAckingTaskManagerGateway());
		final TaskManagerGateway nonSourceGateway = spy(new SimpleAckingTaskManagerGateway());

		// deploy source 1
		for (ExecutionVertex ev : eg.getJobVertex(source1.getID()).getTaskVertices()) {
			SimpleSlot slot = ExecutionGraphTestUtils.createMockSimpleSlot(sourceGateway);
			ev.getCurrentExecutionAttempt().tryAssignResource(slot);
			ev.getCurrentExecutionAttempt().deploy();
		}

		// deploy source 2
		for (ExecutionVertex ev : eg.getJobVertex(source2.getID()).getTaskVertices()) {
			SimpleSlot slot = ExecutionGraphTestUtils.createMockSimpleSlot(sourceGateway);
			ev.getCurrentExecutionAttempt().tryAssignResource(slot);
			ev.getCurrentExecutionAttempt().deploy();
		}

		// deploy non-source 1
		for (ExecutionVertex ev : eg.getJobVertex(nonSource1.getID()).getTaskVertices()) {
			SimpleSlot slot = ExecutionGraphTestUtils.createMockSimpleSlot(nonSourceGateway);
			ev.getCurrentExecutionAttempt().tryAssignResource(slot);
			ev.getCurrentExecutionAttempt().deploy();
		}

		// deploy non-source 2
		for (ExecutionVertex ev : eg.getJobVertex(nonSource2.getID()).getTaskVertices()) {
			SimpleSlot slot = ExecutionGraphTestUtils.createMockSimpleSlot(nonSourceGateway);
			ev.getCurrentExecutionAttempt().tryAssignResource(slot);
			ev.getCurrentExecutionAttempt().deploy();
		}

		eg.stop();

		verify(sourceGateway, timeout(1000).times(sourcePar1 + sourcePar2)).stopTask(any(ExecutionAttemptID.class), any(Time.class));
		verify(nonSourceGateway, times(0)).stopTask(any(ExecutionAttemptID.class), any(Time.class));

		ExecutionGraphTestUtils.finishAllVertices(eg);
	}

	/**
	 * Tests that the stopping RPC call is sent upon stopping requests.
	 */
	@Test
	public void testStopRpc() throws Exception {
		final JobID jid = new JobID();
		final JobVertex vertex = new JobVertex("vertex");
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(5);

		final ExecutionGraph graph = ExecutionGraphTestUtils.createSimpleTestGraph(jid, vertex);
		final Execution exec = graph.getJobVertex(vertex.getID()).getTaskVertices()[0].getCurrentExecutionAttempt();

		final TaskManagerGateway gateway = mock(TaskManagerGateway.class);
		when(gateway.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(gateway.stopTask(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final SimpleSlot slot = ExecutionGraphTestUtils.createMockSimpleSlot(gateway);

		exec.tryAssignResource(slot);
		exec.deploy();
		exec.switchToRunning();
		assertEquals(ExecutionState.RUNNING, exec.getState());

		exec.stop();
		assertEquals(ExecutionState.RUNNING, exec.getState());

		verify(gateway, times(1)).stopTask(any(ExecutionAttemptID.class), any(Time.class));

		exec.markFinished();
		assertEquals(ExecutionState.FINISHED, exec.getState());
	}
}
