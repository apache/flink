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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionGraphMetricsTest extends TestLogger {

	/**
	 * This test tests that the restarting time metric correctly displays restarting times.
	 */
	@Test
	public void testExecutionGraphRestartTimeMetric() throws JobException, IOException, InterruptedException {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			// setup execution graph with mocked scheduling logic
			int parallelism = 1;

			JobVertex jobVertex = new JobVertex("TestVertex");
			jobVertex.setParallelism(parallelism);
			jobVertex.setInvokableClass(NoOpInvokable.class);
			JobGraph jobGraph = new JobGraph("Test Job", jobVertex);

			Configuration jobConfig = new Configuration();
			Time timeout = Time.seconds(10L);
			Scheduler scheduler = mock(Scheduler.class);

			ResourceID taskManagerId = ResourceID.generate();

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(taskManagerId);
			when(taskManagerLocation.getHostname()).thenReturn("localhost");

			TaskManagerGateway taskManagerGateway = mock(TaskManagerGateway.class);

			Instance instance = mock(Instance.class);
			when(instance.getTaskManagerLocation()).thenReturn(taskManagerLocation);
			when(instance.getTaskManagerID()).thenReturn(taskManagerId);
			when(instance.getTaskManagerGateway()).thenReturn(taskManagerGateway);

			Slot rootSlot = mock(Slot.class);

			AllocatedSlot mockAllocatedSlot = mock(AllocatedSlot.class);
			when(mockAllocatedSlot.getSlotAllocationId()).thenReturn(new AllocationID());

			SimpleSlot simpleSlot = mock(SimpleSlot.class);
			when(simpleSlot.isAlive()).thenReturn(true);
			when(simpleSlot.getTaskManagerLocation()).thenReturn(taskManagerLocation);
			when(simpleSlot.getTaskManagerID()).thenReturn(taskManagerId);
			when(simpleSlot.getTaskManagerGateway()).thenReturn(taskManagerGateway);
			when(simpleSlot.setExecutedVertex(Matchers.any(Execution.class))).thenReturn(true);
			when(simpleSlot.getRoot()).thenReturn(rootSlot);
			when(simpleSlot.getAllocatedSlot()).thenReturn(mockAllocatedSlot);

			FlinkCompletableFuture<SimpleSlot> future = new FlinkCompletableFuture<>();
			future.complete(simpleSlot);
			when(scheduler.allocateSlot(any(ScheduledUnit.class), anyBoolean())).thenReturn(future);

			when(rootSlot.getSlotNumber()).thenReturn(0);

			when(taskManagerGateway.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class))).thenReturn(FlinkCompletableFuture.completed(Acknowledge.get()));

			TestingRestartStrategy testingRestartStrategy = new TestingRestartStrategy();

			ExecutionGraph executionGraph = new ExecutionGraph(
				executor,
				executor,
				jobGraph.getJobID(),
				jobGraph.getName(),
				jobConfig,
				new SerializedValue<ExecutionConfig>(null),
				timeout,
				testingRestartStrategy,
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList(),
				scheduler,
				getClass().getClassLoader());

			RestartTimeGauge restartingTime = new RestartTimeGauge(executionGraph);

			// check that the restarting time is 0 since it's the initial start
			assertEquals(0L, restartingTime.getValue().longValue());

			executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

			// start execution
			executionGraph.scheduleForExecution();
			assertEquals(0L, restartingTime.getValue().longValue());

			List<ExecutionAttemptID> executionIDs = new ArrayList<>();

			for (ExecutionVertex executionVertex: executionGraph.getAllExecutionVertices()) {
				executionIDs.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
			}

			// tell execution graph that the tasks are in state running --> job status switches to state running
			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.RUNNING));
			}

			assertEquals(JobStatus.RUNNING, executionGraph.getState());
			assertEquals(0L, restartingTime.getValue().longValue());

			// fail the job so that it goes into state restarting
			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.FAILED, new Exception()));
			}

			assertEquals(JobStatus.RESTARTING, executionGraph.getState());

			long firstRestartingTimestamp = executionGraph.getStatusTimestamp(JobStatus.RESTARTING);

			// wait some time so that the restarting time gauge shows a value different from 0
			Thread.sleep(50);

			long previousRestartingTime = restartingTime.getValue();

			// check that the restarting time is monotonically increasing
			for (int i = 0; i < 10; i++) {
				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime >= previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}

			// check that we have measured some restarting time
			assertTrue(previousRestartingTime > 0);

			// restart job
			testingRestartStrategy.restartExecutionGraph();

			executionIDs.clear();

			for (ExecutionVertex executionVertex: executionGraph.getAllExecutionVertices()) {
				executionIDs.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
			}

			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.RUNNING));
			}

			assertEquals(JobStatus.RUNNING, executionGraph.getState());

			assertTrue(firstRestartingTimestamp != 0);

			previousRestartingTime = restartingTime.getValue();

			// check that the restarting time does not increase after we've reached the running state
			for (int i = 0; i < 10; i++) {
				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime == previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}

			// fail job again
			for (ExecutionAttemptID executionID : executionIDs) {
				executionGraph.updateState(new TaskExecutionState(jobGraph.getJobID(), executionID, ExecutionState.FAILED, new Exception()));
			}

			assertEquals(JobStatus.RESTARTING, executionGraph.getState());

			long secondRestartingTimestamp = executionGraph.getStatusTimestamp(JobStatus.RESTARTING);

			assertTrue(firstRestartingTimestamp != secondRestartingTimestamp);

			Thread.sleep(50);

			previousRestartingTime = restartingTime.getValue();

			// check that the restarting time is increasing again
			for (int i = 0; i < 10; i++) {
				long currentRestartingTime = restartingTime.getValue();

				assertTrue(currentRestartingTime >= previousRestartingTime);
				previousRestartingTime = currentRestartingTime;
			}

			assertTrue(previousRestartingTime > 0);

			// now lets fail the job while it is in restarting and see whether the restarting time then stops to increase
			// for this to work, we have to use a SuppressRestartException
			executionGraph.fail(new SuppressRestartsException(new Exception()));

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

	static class TestingRestartStrategy implements RestartStrategy {

		private boolean restartable = true;
		private ExecutionGraph executionGraph = null;

		@Override
		public boolean canRestart() {
			return restartable;
		}

		@Override
		public void restart(ExecutionGraph executionGraph) {
			this.executionGraph = executionGraph;
		}

		public void setRestartable(boolean restartable) {
			this.restartable = restartable;
		}

		public void restartExecutionGraph() {
			executionGraph.restart();
		}
	}

}
