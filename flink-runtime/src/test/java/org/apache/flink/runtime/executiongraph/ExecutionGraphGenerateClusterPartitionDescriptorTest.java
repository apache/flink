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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PersistedIntermediateResultDescriptor;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExecutionGraphGenerateClusterPartitionDescriptorTest extends TestLogger {

	private static final ScheduledExecutorService scheduledExecutorService =
		Executors.newSingleThreadScheduledExecutor();
	private static final TestingComponentMainThreadExecutor mainThreadExecutor =
		new TestingComponentMainThreadExecutor(
			ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(scheduledExecutorService));

	@Test
	public void testGenerateClusterPartitionDescriptor() throws Exception {
		final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex operatorVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex sinkVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

		operatorVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE,
			ResultPartitionType.BLOCKING);
		sinkVertex.connectNewDataSetAsInput(operatorVertex, DistributionPattern.POINTWISE,
			ResultPartitionType.BLOCKING_PERSISTENT);

		final ExecutionGraph executionGraph =
			createExecutionGraph(sourceVertex, operatorVertex, sinkVertex);

		mainThreadExecutor.execute(() -> {
			executionGraph.getAllExecutionVertices().forEach(executionVertex -> {
				executionGraph.updateState(new TaskExecutionState(executionGraph.getJobID(),
					executionVertex.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.FINISHED));
			});
		});

		final Map<IntermediateDataSetID, PersistedIntermediateResultDescriptor> persistedIntermediateResult =
			executionGraph.getPersistedIntermediateResult();
		assertEquals(1, persistedIntermediateResult.size());
	}

	@Test
	public void testGenerateClusterPartitionDescriptorBeforeFinishReturnNull() throws Exception {
		final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex operatorVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
		final JobVertex sinkVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

		operatorVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE,
			ResultPartitionType.BLOCKING_PERSISTENT);
		sinkVertex.connectNewDataSetAsInput(operatorVertex, DistributionPattern.POINTWISE,
			ResultPartitionType.BLOCKING_PERSISTENT);

		final ExecutionGraph executionGraph =
			createExecutionGraph(sourceVertex, operatorVertex, sinkVertex);

		assertNull(executionGraph.getPersistedIntermediateResult());
	}

	private ExecutionGraph createExecutionGraph(final JobVertex... vertices) throws Exception {
		final ExecutionGraph executionGraph = ExecutionGraphBuilder.buildGraph(
			null,
			new JobGraph(new JobID(), "test job", vertices),
			new Configuration(),
			scheduledExecutorService,
			mainThreadExecutor.getMainThreadExecutor(),
			new TestingSlotProvider(ignored -> CompletableFuture.completedFuture(
				new TestingLogicalSlotBuilder().createTestingLogicalSlot())),
			ExecutionGraphPartitionReleaseTest.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			log,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			System.currentTimeMillis());

		executionGraph.start(mainThreadExecutor.getMainThreadExecutor());
		mainThreadExecutor.execute(executionGraph::scheduleForExecution);

		return executionGraph;
	}
}
