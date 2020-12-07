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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecutionGraphMetricsTest extends TestLogger {

	private final ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

	/**
	 * This test tests that the restarting time metric correctly displays restarting times.
	 */
	@Test
	public void testExecutionGraphRestartTimeMetric() throws Exception {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			// setup execution graph with mocked scheduling logic
			int parallelism = 1;

			JobVertex jobVertex = new JobVertex("TestVertex");
			jobVertex.setParallelism(parallelism);
			jobVertex.setInvokableClass(NoOpInvokable.class);
			JobGraph jobGraph = new JobGraph("Test Job", jobVertex);

			CompletableFuture<LogicalSlot> slotFuture1 = CompletableFuture.completedFuture(new TestingLogicalSlotBuilder().createTestingLogicalSlot());
			CompletableFuture<LogicalSlot> slotFuture2 = CompletableFuture.completedFuture(new TestingLogicalSlotBuilder().createTestingLogicalSlot());
			ArrayDeque<CompletableFuture<LogicalSlot>> slotFutures = new ArrayDeque<>();
			slotFutures.addLast(slotFuture1);
			slotFutures.addLast(slotFuture2);

			TestRestartStrategy testingRestartStrategy = TestRestartStrategy.manuallyTriggered();

			ExecutionGraph executionGraph = TestingExecutionGraphBuilder
				.newBuilder()
				.setJobGraph(jobGraph)
				.setFutureExecutor(executor)
				.setIoExecutor(executor)
				.setRestartStrategy(testingRestartStrategy)
				.setSlotProvider(new TestingSlotProvider(ignore -> slotFutures.removeFirst()))
				.build();

			executionGraph.start(mainThreadExecutor);

			RestartTimeGauge restartingTime = new RestartTimeGauge(executionGraph);

			// check that the restarting time is 0 since it's the initial start
			assertEquals(0L, restartingTime.getValue().longValue());

			// start execution
			executionGraph.scheduleForExecution();
			assertEquals(0L, restartingTime.getValue().longValue());

			List<ExecutionAttemptID> executionIDs = new ArrayList<>();

			for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
				executionIDs.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
			}

			// tell execution graph that the tasks are in state running --> job status switches to state running
			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.RUNNING));
			}

			assertEquals(JobStatus.RUNNING, executionGraph.getState());
			assertEquals(0L, restartingTime.getValue().longValue());

			// add some pause such that RUNNING and RESTARTING timestamps are not the same
			Thread.sleep(1L);

			// fail the job so that it goes into state restarting
			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.FAILED, new Exception()));
			}

			assertEquals(JobStatus.RESTARTING, executionGraph.getState());

			long firstRestartingTimestamp = executionGraph.getStatusTimestamp(JobStatus.RESTARTING);

			long previousRestartingTime = restartingTime.getValue();

			// check that the restarting time is monotonically increasing
			for (int i = 0; i < 2; i++) {
				// add some pause to let the currentRestartingTime increase
				Thread.sleep(1L);

				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime >= previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}

			// check that we have measured some restarting time
			assertTrue(previousRestartingTime > 0);

			// restart job
			testingRestartStrategy.triggerAll().join();

			executionIDs.clear();

			for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
				executionIDs.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
			}

			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.RUNNING));
			}

			assertEquals(JobStatus.RUNNING, executionGraph.getState());

			assertTrue(firstRestartingTimestamp != 0);

			previousRestartingTime = restartingTime.getValue();

			// check that the restarting time does not increase after we've reached the running state
			for (int i = 0; i < 2; i++) {
				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime == previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}

			// add some pause such that the RUNNING and RESTARTING timestamps are not the same
			Thread.sleep(1L);

			// fail job again
			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.FAILED, new Exception()));
			}

			assertEquals(JobStatus.RESTARTING, executionGraph.getState());

			long secondRestartingTimestamp = executionGraph.getStatusTimestamp(JobStatus.RESTARTING);

			assertTrue(firstRestartingTimestamp != secondRestartingTimestamp);

			previousRestartingTime = restartingTime.getValue();

			// check that the restarting time is increasing again
			for (int i = 0; i < 2; i++) {
				// add some pause to the let currentRestartingTime increase
				Thread.sleep(1L);
				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime >= previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}

			assertTrue(previousRestartingTime > 0);

			// now lets fail the job while it is in restarting and see whether the restarting time then stops to increase
			// for this to work, we have to use a SuppressRestartException
			executionGraph.failGlobal(new SuppressRestartsException(new Exception()));

			assertEquals(JobStatus.FAILED, executionGraph.getState());

			previousRestartingTime = restartingTime.getValue();

			for (int i = 0; i < 10; i++) {
				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime == previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}
		} finally {
			executor.shutdownNow();
		}
	}
}
