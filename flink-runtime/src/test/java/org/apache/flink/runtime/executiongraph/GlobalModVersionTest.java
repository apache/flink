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
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GlobalModVersionTest extends TestLogger {

	/**
	 * Tests that failures during a global cancellation are not handed to the local
	 * failover strategy.
	 */
	@Test
	public void testNoLocalFailoverWhileCancelling() throws Exception {
		final FailoverStrategy mockStrategy = mock(FailoverStrategy.class);

		final TaskManagerGateway taskManagerGateway = spy(new SimpleAckingTaskManagerGateway());
		final ExecutionGraph graph = createSampleGraph(mockStrategy, taskManagerGateway);

		final ExecutionVertex testVertex = getRandomVertex(graph);

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(1L, graph.getGlobalModVersion());

		// wait until everything is running
		verify(taskManagerGateway, Mockito.timeout(2000L).times(graph.getTotalNumberOfVertices()))
				.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		ExecutionGraphTestUtils.switchAllVerticesToRunning(graph);

		// now cancel the job
		graph.cancel();
		assertEquals(2L, graph.getGlobalModVersion());

		// everything should be cancelling
		for (ExecutionVertex v : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			final Execution exec = v.getCurrentExecutionAttempt();
			assertEquals(ExecutionState.CANCELING, exec.getState());
		}

		// let a vertex fail
		testVertex.getCurrentExecutionAttempt().fail(new Exception("test exception"));

		// all cancellations are done now
		for (ExecutionVertex v : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			final Execution exec = v.getCurrentExecutionAttempt();
			exec.cancelingComplete();
		}

		assertEquals(JobStatus.CANCELED, graph.getTerminationFuture().get());

		// no failure notification at all
		verify(mockStrategy, times(0)).onTaskFailure(any(Execution.class), any(Throwable.class));
	}

	/**
	 * Tests that failures during a global failover are not handed to the local
	 * failover strategy.
	 */
	@Test
	public void testNoLocalFailoverWhileFailing() throws Exception {
		final FailoverStrategy mockStrategy = mock(FailoverStrategy.class);

		final TaskManagerGateway taskManagerGateway = spy(new SimpleAckingTaskManagerGateway());
		final ExecutionGraph graph = createSampleGraph(mockStrategy, taskManagerGateway);

		final ExecutionVertex testVertex = getRandomVertex(graph);

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());

		// wait until everything is running
		verify(taskManagerGateway, Mockito.timeout(2000L).times(graph.getTotalNumberOfVertices()))
				.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		ExecutionGraphTestUtils.switchAllVerticesToRunning(graph);

		// now send the job into global failover
		graph.failGlobal(new Exception("global failover"));
		assertEquals(JobStatus.FAILING, graph.getState());
		assertEquals(2L, graph.getGlobalModVersion());

		// another attempt to fail global should not do anything
		graph.failGlobal(new Exception("should be ignored"));
		assertEquals(JobStatus.FAILING, graph.getState());
		assertEquals(2L, graph.getGlobalModVersion());

		// everything should be cancelling
		for (ExecutionVertex v : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			final Execution exec = v.getCurrentExecutionAttempt();
			assertEquals(ExecutionState.CANCELING, exec.getState());
		}

		// let a vertex fail
		testVertex.getCurrentExecutionAttempt().fail(new Exception("test exception"));

		// all cancellations are done now
		for (ExecutionVertex v : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			final Execution exec = v.getCurrentExecutionAttempt();
			exec.cancelingComplete();
		}

		assertEquals(JobStatus.RESTARTING, graph.getState());

		// no failure notification at all
		verify(mockStrategy, times(0)).onTaskFailure(any(Execution.class), any(Throwable.class));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createSampleGraph(FailoverStrategy failoverStrategy, TaskManagerGateway taskManagerGateway) throws Exception {

		final JobID jid = new JobID();
		final int parallelism = new Random().nextInt(10) + 1;

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism, taskManagerGateway);

		JobVertex jv = new JobVertex("test vertex");
		jv.setInvokableClass(NoOpInvokable.class);
		jv.setParallelism(parallelism);

		// build a simple execution graph with on job vertex, parallelism 2
		final ExecutionGraph graph = ExecutionGraphTestUtils.createExecutionGraphDirectly(
			new DummyJobInformation(
				jid,
				"test job"),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			Time.seconds(10),
			new InfiniteDelayRestartStrategy(),
			new CustomStrategy(failoverStrategy),
			slotProvider,
			VoidBlobWriter.getInstance(),
			Collections.singletonList(jv));

		return graph;
	}

	private static ExecutionVertex getRandomVertex(ExecutionGraph eg) {
		final ExecutionVertex[] vertices = eg.getVerticesTopologically().iterator().next().getTaskVertices();
		return vertices[new Random().nextInt(vertices.length)];
	}

	// ------------------------------------------------------------------------

	private static class CustomStrategy implements Factory {

		private final FailoverStrategy failoverStrategy;

		CustomStrategy(FailoverStrategy failoverStrategy) {
			this.failoverStrategy = failoverStrategy;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return failoverStrategy;
		}
	}
}
