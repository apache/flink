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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * This class contains tests that verify when rescaling a {@link JobGraph},
 * constructed {@link ExecutionGraph}s are correct.
 */
public class ExecutionGraphRescalingTest extends TestLogger {

	private static final Logger TEST_LOGGER = LoggerFactory.getLogger(ExecutionGraphRescalingTest.class);

	@Test
	public void testExecutionGraphArbitraryDopConstructionTest() throws Exception {

		final Configuration config = new Configuration();

		final int initialParallelism = 5;
		final int maxParallelism = 10;
		final JobVertex[] jobVertices = createVerticesForSimpleBipartiteJobGraph(initialParallelism, maxParallelism);
		final JobGraph jobGraph = new JobGraph(jobVertices);

		ExecutionGraph eg = ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			config,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new TestingSlotProvider(ignore -> new CompletableFuture<>()),
			Thread.currentThread().getContextClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			TEST_LOGGER,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			System.currentTimeMillis());

		for (JobVertex jv : jobVertices) {
			assertThat(jv.getParallelism(), is(initialParallelism));
		}
		verifyGeneratedExecutionGraphOfSimpleBitartiteJobGraph(eg, jobVertices);

		// --- verify scaling down works correctly ---

		final int scaleDownParallelism = 1;

		for (JobVertex jv : jobVertices) {
			jv.setParallelism(scaleDownParallelism);
		}

		eg = ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			config,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new TestingSlotProvider(ignore -> new CompletableFuture<>()),
			Thread.currentThread().getContextClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			TEST_LOGGER,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			System.currentTimeMillis());

		for (JobVertex jv : jobVertices) {
			assertThat(jv.getParallelism(), is(1));
		}
		verifyGeneratedExecutionGraphOfSimpleBitartiteJobGraph(eg, jobVertices);

		// --- verify scaling up works correctly ---

		final int scaleUpParallelism = 10;

		for (JobVertex jv : jobVertices) {
			jv.setParallelism(scaleUpParallelism);
		}

		eg = ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			config,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new TestingSlotProvider(ignore -> new CompletableFuture<>()),
			Thread.currentThread().getContextClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			TEST_LOGGER,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			System.currentTimeMillis());

		for (JobVertex jv : jobVertices) {
			assertThat(jv.getParallelism(), is(scaleUpParallelism));
		}
		verifyGeneratedExecutionGraphOfSimpleBitartiteJobGraph(eg, jobVertices);
	}

	/**
	 * Verifies that building an {@link ExecutionGraph} from a {@link JobGraph} with
	 * parallelism higher than the maximum parallelism fails.
	 */
	@Test
	public void testExecutionGraphConstructionFailsRescaleDopExceedMaxParallelism() throws Exception {

		final Configuration config = new Configuration();

		final int initialParallelism = 1;
		final int maxParallelism = 10;
		final JobVertex[] jobVertices = createVerticesForSimpleBipartiteJobGraph(initialParallelism,  maxParallelism);
		final JobGraph jobGraph = new JobGraph(jobVertices);

		for (JobVertex jv : jobVertices) {
			jv.setParallelism(maxParallelism + 1);
		}

		try {
			// this should fail since we set the parallelism to maxParallelism + 1
			ExecutionGraphBuilder.buildGraph(
				null,
				jobGraph,
				config,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				new TestingSlotProvider(ignore -> new CompletableFuture<>()),
				Thread.currentThread().getContextClassLoader(),
				new StandaloneCheckpointRecoveryFactory(),
				AkkaUtils.getDefaultTimeout(),
				new NoRestartStrategy(),
				new UnregisteredMetricsGroup(),
				VoidBlobWriter.getInstance(),
				AkkaUtils.getDefaultTimeout(),
				TEST_LOGGER,
				NettyShuffleMaster.INSTANCE,
				NoOpJobMasterPartitionTracker.INSTANCE,
				System.currentTimeMillis());

			fail("Building the ExecutionGraph with a parallelism higher than the max parallelism should fail.");
		} catch (JobException e) {
			// expected, ignore
		}
	}

	private static JobVertex[] createVerticesForSimpleBipartiteJobGraph(int parallelism, int maxParallelism) {
		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");

		JobVertex[] jobVertices = new JobVertex[]{v1, v2, v3, v4, v5};

		for (JobVertex jobVertex : jobVertices) {
			jobVertex.setInvokableClass(AbstractInvokable.class);
			jobVertex.setParallelism(parallelism);
			jobVertex.setMaxParallelism(maxParallelism);
		}

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		return jobVertices;
	}

	private static void verifyGeneratedExecutionGraphOfSimpleBitartiteJobGraph(
			ExecutionGraph generatedExecutionGraph, JobVertex[] jobVertices) {

		ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
				generatedExecutionGraph, jobVertices[0],
				null, Collections.singletonList(jobVertices[1]));

		ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
				generatedExecutionGraph, jobVertices[1],
				Collections.singletonList(jobVertices[0]), Collections.singletonList(jobVertices[3]));

		ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
				generatedExecutionGraph, jobVertices[2],
				null, Arrays.asList(jobVertices[3], jobVertices[4]));

		ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
				generatedExecutionGraph, jobVertices[3],
				Arrays.asList(jobVertices[1], jobVertices[2]), Collections.singletonList(jobVertices[4]));

		ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
				generatedExecutionGraph, jobVertices[4],
				Arrays.asList(jobVertices[3], jobVertices[2]), null);
	}
}
