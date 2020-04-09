/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link AdaptedRestartPipelinedRegionStrategyNG}.
 */
public class AdaptedRestartPipelinedRegionStrategyNGAbortPendingCheckpointsTest extends TestLogger {

	private ManuallyTriggeredScheduledExecutor manualMainThreadExecutor;

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	@Before
	public void setUp() {
		manualMainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
		componentMainThreadExecutor = new ComponentMainThreadExecutorServiceAdapter(manualMainThreadExecutor, Thread.currentThread());
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
	}

	@Test
	public void abortPendingCheckpointsWhenRestartingTasks() throws Exception {
		final JobGraph jobGraph = createStreamingJobGraph();
		final CountDownLatch checkpointTriggeredLatch = new CountDownLatch(1);
		taskManagerGateway.setCheckpointConsumer(
			(executionAttemptID, jobId, checkpointId, timestamp, checkpointOptions, advanceToEndOfEventTime) -> {
				checkpointTriggeredLatch.countDown();
			});
		final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);

		final Iterator<ExecutionVertex> vertexIterator = executionGraph.getAllExecutionVertices().iterator();
		final ExecutionVertex firstExecutionVertex = vertexIterator.next();

		setTasksRunning(executionGraph, firstExecutionVertex, vertexIterator.next());

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		checkState(checkpointCoordinator != null);

		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(), false);
		// there is an async call in IO thread
		// here we need to retry to make sure the callback of the async call could be executed
		manualMainThreadExecutor.triggerAll();
		while (!checkpointTriggeredLatch.await(10, TimeUnit.MILLISECONDS)) {
			manualMainThreadExecutor.triggerAll();
		}
		assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
		long checkpointId = checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();

		AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
			jobGraph.getJobID(),
			firstExecutionVertex.getCurrentExecutionAttempt().getAttemptId(),
			checkpointId);

		// let the first vertex acknowledge the checkpoint, and fail it afterwards
		// the failover strategy should then cancel all pending checkpoints on restart
		checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "Unknown location");
		assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

		failVertex(firstExecutionVertex);
		assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
		manualMainThreadExecutor.triggerScheduledTasks();

		assertNoPendingCheckpoints(checkpointCoordinator);
	}

	private void setTasksRunning(final ExecutionGraph executionGraph, final ExecutionVertex... executionVertices) {
		for (ExecutionVertex executionVertex : executionVertices) {
			executionGraph.updateState(
				new TaskExecutionState(executionGraph.getJobID(),
					executionVertex.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.RUNNING));
		}
	}

	private void failVertex(final ExecutionVertex onlyExecutionVertex) {
		onlyExecutionVertex.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		manualMainThreadExecutor.triggerAll();
	}

	private static JobGraph createStreamingJobGraph() {
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		final JobGraph jobGraph = new JobGraph(v1, v2);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		return jobGraph;
	}

	private ExecutionGraph createExecutionGraph(final JobGraph jobGraph) throws Exception {
		final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(jobGraph)
			.setRestartStrategy(new FixedDelayRestartStrategy(10, 0))
			.setFailoverStrategyFactory(AdaptedRestartPipelinedRegionStrategyNG::new)
			.setSlotProvider(new SimpleSlotProvider(2, taskManagerGateway))
			.build();

		enableCheckpointing(executionGraph);
		executionGraph.start(componentMainThreadExecutor);
		executionGraph.scheduleForExecution();
		manualMainThreadExecutor.triggerAll();
		return executionGraph;
	}

	private static void enableCheckpointing(final ExecutionGraph executionGraph) {
		final List<ExecutionJobVertex> jobVertices = new ArrayList<>(executionGraph.getAllVertices().values());
		final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			Long.MAX_VALUE,
			Long.MAX_VALUE,
			0,
			1,
			CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION,
			true,
			false,
			0);

		executionGraph.enableCheckpointing(
			checkpointCoordinatorConfiguration,
			jobVertices,
			jobVertices,
			jobVertices,
			Collections.emptyList(),
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			new CheckpointStatsTracker(
				0,
				jobVertices,
				checkpointCoordinatorConfiguration,
				new UnregisteredMetricsGroup()));
	}

	private static void assertNoPendingCheckpoints(final CheckpointCoordinator checkpointCoordinator) {
		assertThat(checkpointCoordinator.getPendingCheckpoints().entrySet(), is(empty()));
	}
}
