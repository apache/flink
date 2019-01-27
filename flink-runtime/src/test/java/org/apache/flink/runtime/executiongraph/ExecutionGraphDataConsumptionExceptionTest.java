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

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.io.network.partition.DataConsumptionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Test that the upstrem task is restarted on receiving DataConsumptionException.
 */
public class ExecutionGraphDataConsumptionExceptionTest {
	private final static int NUM_TASKS = 31;

	private final static int STATE_TRANSFER_TIMEOUT = 10000;

	/**
	 * Check that for a three level job, 1->2->3, the task 3[0] throws DataConsumptionException when
	 * reading data from 2[0] via a blocking edge. The task 2[0] and 3[0] should be restarted and other
	 * tasks should not.
	 *
	 * @throws Exception
	 */
	@Test
	public void testProcessingDataConsumptionExceptionInMultiRegion() throws Exception {
		final int[] parallelism = new int[]{2, 2, 2};
		final int[][] expectedLatestExecutionAttemptNumber = new int[][]{
			{0, 0},
			{1, 0},
			{1, 0}
		};

		final ExecutionState[][] expectedFinalState = new ExecutionState[][]{
			// The first level of tasks should be not affected by the failover.
			{ExecutionState.FINISHED, ExecutionState.FINISHED},
			// The first of the second level tasks should be deploying because of restarting.
			{ExecutionState.DEPLOYING, ExecutionState.FINISHED},
			// The first of the third level tasks should be created because of its producer after failover
			// has not been finished yet.
			{ExecutionState.CREATED, ExecutionState.DEPLOYING}
		};

		JobGraph jobGraph = createBatchJobGraph(parallelism);
		List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		RestartStrategy restartStrategy = new FixedDelayRestartStrategy(100, 0);
		ExecutionGraphTestUtils.SimpleActorGateway tmGateway =
			spy(new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.directExecutionContext()));
		ExecutionGraph executionGraph = createExecutionGraph(tmGateway, jobGraph, restartStrategy, false);

		executionGraph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		List<List<ExecutionVertex>> executionVertices = new ArrayList<>();
		for (int i = 0; i < jobVertices.size(); ++i) {
			executionVertices.add(new ArrayList<>());
		}

		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			for (int i = 0; i < jobVertices.size(); ++i) {
				if (executionVertex.getJobvertexId().equals(jobVertices.get(i).getID())) {
					executionVertices.get(i).add(executionVertex);
				}
			}
		}

		// Finish the first two level tasks
		for (int i = 0; i < parallelism.length - 1; ++i) {
			// Verify the task has been deploying based on whether TM has received the submitTask message.
			// This is because the deploying has not finished after its state get to
			verify(tmGateway, Mockito.timeout(STATE_TRANSFER_TIMEOUT).times(executionVertices.get(i).size()))
				.handleMessage(any(TaskMessages.SubmitTask.class));
			reset(tmGateway);

			for (ExecutionVertex executionVertex : executionVertices.get(i)) {
				assertEquals(ExecutionState.DEPLOYING, executionVertex.getCurrentExecutionAttempt().getState());

				executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(),
					executionVertex.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.FINISHED));

				ExecutionGraphTestUtils.waitUntilExecutionState(executionVertex.getCurrentExecutionAttempt(),
					ExecutionState.FINISHED,
					STATE_TRANSFER_TIMEOUT);
			}
		}

		// for the three level 1->2->3, mock that 2[0]->3[0] throws DataConsumptionException
		ExecutionVertex failConsumerVertex = executionVertices.get(parallelism.length - 1).get(0);
		ExecutionVertex failProducerVertex = executionVertices.get(parallelism.length - 2).get(0);

		IntermediateResultPartitionID failInterPartitionId = failConsumerVertex.getInputEdges(0)[0].getSource().getPartitionId();
		ExecutionAttemptID failProducerExecutionId = failProducerVertex.getCurrentExecutionAttempt().getAttemptId();

		verify(tmGateway, Mockito.timeout(STATE_TRANSFER_TIMEOUT).times(executionVertices.get(parallelism.length - 1).size()))
				.handleMessage(any(TaskMessages.SubmitTask.class));
		executionGraph.updateState(new TaskExecutionState(
			executionGraph.getJobID(),
			failConsumerVertex.getCurrentExecutionAttempt().getAttemptId(),
			ExecutionState.FAILED,
			new RuntimeException(new DataConsumptionException(new ResultPartitionID(failInterPartitionId, failProducerExecutionId),
				new RuntimeException()))));

		// Still use waitUntilExecutionVertexState since we have no more actions after waiting
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(failProducerVertex,
			ExecutionState.DEPLOYING,
			STATE_TRANSFER_TIMEOUT);

		ExecutionGraphTestUtils.waitUntilExecutionVertexState(failConsumerVertex,
			ExecutionState.CREATED,
			STATE_TRANSFER_TIMEOUT);

		for (int i = 0; i < executionVertices.size(); ++i) {
			for (int j = 0; j < executionVertices.get(i).size(); ++j) {
				Execution execution = executionVertices.get(i).get(j).getCurrentExecutionAttempt();

				assertEquals(expectedLatestExecutionAttemptNumber[i][j], execution.getAttemptNumber());
				assertEquals(expectedFinalState[i][j], execution.getState());
			}
		}
	}

	/**
	 * Check that for a three level job, 1->2->3, the task 3[0] throws DataConsumptionException when
	 * reading data from 2[0] via a blocking edge. The task 2[0] and 3[0] should be restarted and other
	 * tasks should not.
	 *
	 * @throws Exception
	 */
	@Test
	public void testProcessingDataConsumptionExceptionInSingleRegion() throws Exception {
		final int[] parallelism = new int[]{2, 2};
		final int[][] expectedLatestExecutionAttemptNumber = new int[][]{
			{1, 1},
			{1, 1}
		};

		final ExecutionState[][] expectedFinalState = new ExecutionState[][]{
			{ExecutionState.DEPLOYING, ExecutionState.DEPLOYING},
			{ExecutionState.DEPLOYING, ExecutionState.DEPLOYING}
		};

		JobGraph jobGraph = createStreamJobGraph(parallelism);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		RestartStrategy restartStrategy = new FixedDelayRestartStrategy(100, 0);
		ExecutionGraphTestUtils.SimpleActorGateway tmGateway =
			spy(new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.directExecutionContext()));
		ExecutionGraph executionGraph = createExecutionGraph(tmGateway, jobGraph, restartStrategy, false);

		executionGraph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		List<List<ExecutionVertex>> executionVertices = new ArrayList<>();
		for (int i = 0; i < jobVertices.size(); ++i) {
			executionVertices.add(new ArrayList<>());
		}

		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			for (int i = 0; i < jobVertices.size(); ++i) {
				if (executionVertex.getJobvertexId().equals(jobVertices.get(i).getID())) {
					executionVertices.get(i).add(executionVertex);
				}
			}
		}

		verify(tmGateway, Mockito.timeout(STATE_TRANSFER_TIMEOUT).times(4))
			.handleSubmittedTask(any(TaskMessages.SubmitTask.class));

		// Finish the first two level tasks
		for (int i = 0; i < parallelism.length; ++i) {
			for (ExecutionVertex executionVertex : executionVertices.get(i)) {
				assertEquals(ExecutionState.DEPLOYING, executionVertex.getCurrentExecutionAttempt().getState());

				executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(),
					executionVertex.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.RUNNING));

				ExecutionGraphTestUtils.waitUntilExecutionState(executionVertex.getCurrentExecutionAttempt(),
					ExecutionState.RUNNING,
					STATE_TRANSFER_TIMEOUT);
			}
		}

		// for the three level 1->2->3, mock that 2[0]->3[0] throws DataConsumptionException
		ExecutionVertex failConsumerVertex = executionVertices.get(parallelism.length - 1).get(0);
		ExecutionVertex failProducerVertex = executionVertices.get(parallelism.length - 2).get(0);

		IntermediateResultPartitionID failInterPartitionId = failConsumerVertex.getInputEdges(0)[0].getSource().getPartitionId();
		ExecutionAttemptID failProducerExecutionId = failProducerVertex.getCurrentExecutionAttempt().getAttemptId();

		executionGraph.updateState(new TaskExecutionState(
			executionGraph.getJobID(),
			failConsumerVertex.getCurrentExecutionAttempt().getAttemptId(),
			ExecutionState.FAILED,
			new RuntimeException(new DataConsumptionException(new ResultPartitionID(failInterPartitionId, failProducerExecutionId),
				new RuntimeException()))));

		for (int i = 0; i < executionVertices.size(); ++i) {
			for (int j = 0; j < executionVertices.get(i).size(); ++j) {
				Execution execution = executionVertices.get(i).get(j).getCurrentExecutionAttempt();
				execution.cancelingComplete();
			}
		}

		// Still use waitUntilExecutionVertexState since we have no more actions after waiting
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(failProducerVertex,
			ExecutionState.DEPLOYING,
			STATE_TRANSFER_TIMEOUT);

		ExecutionGraphTestUtils.waitUntilExecutionVertexState(failConsumerVertex,
			ExecutionState.DEPLOYING,
			STATE_TRANSFER_TIMEOUT);

		verify(tmGateway, Mockito.timeout(STATE_TRANSFER_TIMEOUT).times(3))
			.handleCanceledTask(any(TaskMessages.CancelTask.class));

		verify(tmGateway, Mockito.timeout(STATE_TRANSFER_TIMEOUT).times(8))
			.handleSubmittedTask(any(TaskMessages.SubmitTask.class));

		for (int i = 0; i < executionVertices.size(); ++i) {
			for (int j = 0; j < executionVertices.get(i).size(); ++j) {
				Execution execution = executionVertices.get(i).get(j).getCurrentExecutionAttempt();

				assertEquals(expectedLatestExecutionAttemptNumber[i][j], execution.getAttemptNumber());
				assertEquals(expectedFinalState[i][j], execution.getState());
			}
		}
	}

	private ExecutionGraph createExecutionGraph(ExecutionGraphTestUtils.SimpleActorGateway tmGateway,
												JobGraph jobGraph,
												RestartStrategy restartStrategy,
												boolean isSpy) throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
			new ActorTaskManagerGateway(
				tmGateway),
			NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraphDirectly(
			jobGraph,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			restartStrategy,
			new RestartPipelinedRegionStrategy.Factory(),
			scheduler,
			VoidBlobWriter.getInstance());

		if (isSpy) {
			eg = spy(eg);
		}

		assertEquals(JobStatus.CREATED, eg.getState());

		return eg;
	}

	private JobGraph createBatchJobGraph(int[] parallelisms) throws IOException {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED);

		JobGraph jobGraph = new JobGraph(ExecutionGraphDataConsumptionExceptionTest.class.getName());
		SlotSharingGroup sharingGroup = new SlotSharingGroup();

		jobGraph.setExecutionConfig(env.getConfig());
		jobGraph.setAllowQueuedScheduling(true);

		JobVertex lastJobVertex = null;
		for (int i = 0; i < parallelisms.length; ++i) {
			JobVertex jobVertex = new JobVertex("Level " + i);
			jobGraph.addVertex(jobVertex);
			jobVertex.setSlotSharingGroup(sharingGroup);

			jobVertex.setInvokableClass(TestInvokable.class);
			jobVertex.setParallelism(parallelisms[i]);

			if (lastJobVertex != null) {
				jobVertex.connectNewDataSetAsInput(lastJobVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
			}

			lastJobVertex = jobVertex;
		}

		return jobGraph;
	}

	private JobGraph createStreamJobGraph(int[] parallelisms) throws IOException {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		JobGraph jobGraph = new JobGraph(ExecutionGraphDataConsumptionExceptionTest.class.getName());
		SlotSharingGroup sharingGroup = new SlotSharingGroup();

		jobGraph.setExecutionConfig(env.getConfig());
		jobGraph.setAllowQueuedScheduling(true);

		JobVertex lastJobVertex = null;
		for (int i = 0; i < parallelisms.length; ++i) {
			JobVertex jobVertex = new JobVertex("Level " + i);
			jobGraph.addVertex(jobVertex);
			jobVertex.setSlotSharingGroup(sharingGroup);

			jobVertex.setInvokableClass(TestInvokable.class);
			jobVertex.setParallelism(parallelisms[i]);

			if (lastJobVertex != null) {
				jobVertex.connectNewDataSetAsInput(lastJobVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
			}

			lastJobVertex = jobVertex;
		}

		return jobGraph;
	}

	public static class TestInvokable extends AbstractInvokable {
		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {

		}
	}
}
