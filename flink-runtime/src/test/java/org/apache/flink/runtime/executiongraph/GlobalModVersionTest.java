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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Random;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
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

		final ExecutionGraph graph = createSampleGraph(mockStrategy);

		final ExecutionVertex testVertex = getRandomVertex(graph);

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(1L, graph.getGlobalModVersion());

		// wait until everything is running
		for (ExecutionVertex v : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			final Execution exec = v.getCurrentExecutionAttempt();
			waitUntilExecutionState(exec, ExecutionState.DEPLOYING, 1000);
			exec.switchToRunning();
			assertEquals(ExecutionState.RUNNING, exec.getState());
		}

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
			exec.completeCancelling();
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

		final ExecutionGraph graph = createSampleGraph(mockStrategy);

		final ExecutionVertex testVertex = getRandomVertex(graph);

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());

		// wait until everything is running
		for (ExecutionVertex v : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			final Execution exec = v.getCurrentExecutionAttempt();
			waitUntilExecutionState(exec, ExecutionState.DEPLOYING, 1000);
			exec.switchToRunning();
			assertEquals(ExecutionState.RUNNING, exec.getState());
		}

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
			exec.completeCancelling();
		}

		assertEquals(JobStatus.RESTARTING, graph.getState());

		// no failure notification at all
		verify(mockStrategy, times(0)).onTaskFailure(any(Execution.class), any(Throwable.class));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createSampleGraph(FailoverStrategy failoverStrategy) throws Exception {
		final JobID jid = new JobID();
		final int parallelism = new Random().nextInt(10) + 1;

		JobVertex jv = new JobVertex("test vertex");
		jv.setInvokableClass(NoOpInvokable.class);
		jv.setParallelism(parallelism);

		JobGraph jg = new JobGraph(jid, "testjob", jv);

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(parallelism);

		// build a simple execution graph with on job vertex, parallelism 2
		final ExecutionGraph graph = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(jg)
			.setRestartStrategy(new InfiniteDelayRestartStrategy())
			.setFailoverStrategyFactory(new CustomStrategy(failoverStrategy))
			.setSlotProvider(slotProvider)
			.build();

		graph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

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
