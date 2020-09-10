/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests for the interactions of the {@link ExecutionGraph} and {@link PartitionReleaseStrategy}.
 */
public class ExecutionGraphPartitionReleaseTest extends TestLogger {

	private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
	private static final TestingComponentMainThreadExecutor mainThreadExecutor =
		new TestingComponentMainThreadExecutor(
			ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(scheduledExecutorService));

	@Test
	public void testStrategyNotifiedOfFinishedVerticesAndResultsRespected() throws Exception {
		// setup a simple pipeline of 3 operators with blocking partitions
		final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex operatorVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex sinkVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

		operatorVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
		sinkVertex.connectNewDataSetAsInput(operatorVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		// setup partition tracker to intercept partition release calls
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		final Queue<ResultPartitionID> releasedPartitions = new ArrayDeque<>();
		partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
			partitionIds -> releasedPartitions.add(partitionIds.iterator().next()));

		final ExecutionGraph executionGraph = createExecutionGraph(partitionTracker, sourceVertex, operatorVertex, sinkVertex);

		// finish vertices one after another, and verify that the appropriate partitions are released
		mainThreadExecutor.execute(() -> {
			final Execution sourceExecution = getCurrentExecution(sourceVertex, executionGraph);
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), sourceExecution.getAttemptId(), ExecutionState.FINISHED));
			assertThat(releasedPartitions, empty());
		});

		mainThreadExecutor.execute(() -> {
			final Execution sourceExecution = getCurrentExecution(sourceVertex, executionGraph);
			final Execution operatorExecution = getCurrentExecution(operatorVertex, executionGraph);
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), operatorExecution.getAttemptId(), ExecutionState.FINISHED));
			assertThat(releasedPartitions, hasSize(1));
			assertThat(releasedPartitions.remove(), equalTo(new ResultPartitionID(
				sourceExecution.getVertex().getProducedPartitions().keySet().iterator().next(),
				sourceExecution.getAttemptId())));
		});

		mainThreadExecutor.execute(() -> {
			final Execution operatorExecution = getCurrentExecution(operatorVertex, executionGraph);
			final Execution sinkExecution = getCurrentExecution(sinkVertex, executionGraph);
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), sinkExecution.getAttemptId(), ExecutionState.FINISHED));

			assertThat(releasedPartitions, hasSize(1));
			assertThat(releasedPartitions.remove(), equalTo(new ResultPartitionID(
				operatorExecution.getVertex().getProducedPartitions().keySet().iterator().next(),
				operatorExecution.getAttemptId())));
		});
	}

	@Test
	public void testStrategyNotifiedOfUnFinishedVertices() throws Exception {
		// setup a pipeline of 2 failover regions (f1 -> f2), where
		// f1 is just a source
		// f2 consists of 3 operators (o1,o2,o3), where o1 consumes f1, and o2/o3 consume o1
		final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex("source", 1);
		final JobVertex operator1Vertex = ExecutionGraphTestUtils.createNoOpVertex("operator1", 1);
		final JobVertex operator2Vertex = ExecutionGraphTestUtils.createNoOpVertex("operator2", 1);
		final JobVertex operator3Vertex = ExecutionGraphTestUtils.createNoOpVertex("operator3", 1);

		operator1Vertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
		operator2Vertex.connectNewDataSetAsInput(operator1Vertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		operator3Vertex.connectNewDataSetAsInput(operator1Vertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		// setup partition tracker to intercept partition release calls
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		final Queue<ResultPartitionID> releasedPartitions = new ArrayDeque<>();
		partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
			partitionIds -> releasedPartitions.add(partitionIds.iterator().next()));

		final ExecutionGraph executionGraph = createExecutionGraph(
			partitionTracker, sourceVertex, operator1Vertex, operator2Vertex, operator3Vertex);

		mainThreadExecutor.execute(() -> {
			final Execution sourceExecution = getCurrentExecution(sourceVertex, executionGraph);
			// finish the source; this should not result in any release calls since the consumer o1 was not finished
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), sourceExecution.getAttemptId(), ExecutionState.FINISHED));
			assertThat(releasedPartitions, empty());
		});

		mainThreadExecutor.execute(() -> {
			final Execution operator1Execution = getCurrentExecution(operator1Vertex, executionGraph);
			// finish o1 and schedule the consumers (o2,o3); this should not result in any release calls since not all operators of the pipelined region are finished
			for (final IntermediateResultPartitionID partitionId : operator1Execution.getVertex().getProducedPartitions().keySet()) {
				executionGraph.scheduleOrUpdateConsumers(new ResultPartitionID(partitionId, operator1Execution.getAttemptId()));
			}
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), operator1Execution.getAttemptId(), ExecutionState.FINISHED));
			assertThat(releasedPartitions, empty());
		});

		mainThreadExecutor.execute(() -> {
			final Execution operator2Execution = getCurrentExecution(operator2Vertex, executionGraph);
			// finish o2; this should not result in any release calls since o3 was not finished
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), operator2Execution.getAttemptId(), ExecutionState.FINISHED));
			assertThat(releasedPartitions, empty());
		});

		mainThreadExecutor.execute(() -> {
			final Execution operator2Execution = getCurrentExecution(operator2Vertex, executionGraph);
			// reset o2
			operator2Execution.getVertex().resetForNewExecution(0L, 1L);
			assertThat(releasedPartitions, empty());
		});

		mainThreadExecutor.execute(() -> {
			final Execution operator3Execution = getCurrentExecution(operator3Vertex, executionGraph);
			// finish o3; this should not result in any release calls since o2 was reset
			executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(), operator3Execution.getAttemptId(), ExecutionState.FINISHED));
			assertThat(releasedPartitions, empty());
		});
	}

	private static Execution getCurrentExecution(final JobVertex jobVertex, final ExecutionGraph executionGraph) {
		return executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[0].getCurrentExecutionAttempt();
	}

	private ExecutionGraph createExecutionGraph(final JobMasterPartitionTracker partitionTracker, final JobVertex... vertices) throws Exception {
		final ExecutionGraph executionGraph = ExecutionGraphBuilder.buildGraph(
			null,
			new JobGraph(new JobID(), "test job", vertices),
			new Configuration(),
			scheduledExecutorService,
			mainThreadExecutor.getMainThreadExecutor(),
			new TestingSlotProvider(ignored -> CompletableFuture.completedFuture(new TestingLogicalSlotBuilder().createTestingLogicalSlot())),
			ExecutionGraphPartitionReleaseTest.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			log,
			NettyShuffleMaster.INSTANCE,
			partitionTracker,
			System.currentTimeMillis());

		executionGraph.start(mainThreadExecutor.getMainThreadExecutor());
		mainThreadExecutor.execute(executionGraph::scheduleForExecution);

		return executionGraph;
	}
}
