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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.PipelinedFailoverRegionBuildingTest;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategyBuildingTest;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link AdaptedRestartPipelinedRegionStrategyNG}.
 */
public class AdaptedRestartPipelinedRegionStrategyNGTest extends TestLogger {

	/**
	 * Test that {@link AdaptedRestartPipelinedRegionStrategyNG} is loaded
	 * as failover strategy if {@link JobManagerOptions#EXECUTION_FAILOVER_STRATEGY}
	 * value is "region".
	 */
	@Test
	public void testAdaptedRestartPipelinedRegionStrategyNGLoading() throws Exception {
		final ExecutionGraph eg = createExecutionGraph(new JobGraph());

		assertTrue(eg.getFailoverStrategy() instanceof AdaptedRestartPipelinedRegionStrategyNG);
	}

	/**
	 * Test whether region building works. This helps to make sure that the
	 * overall region building process is not breaking.
	 * More detailed region building verification is covered
	 * in {@link RestartPipelinedRegionStrategyBuildingTest}
	 * <pre>
	 *     (v11) --> (v21) -+-> (v31) --> (v41)
	 *                      x
	 *     (v12) --> (v22) -+-> (v32) --> (v42)
	 *
	 *                      ^
	 *                      |
	 *                  (blocking)
	 * </pre>
	 * 4 regions. Each region has 2 pipelined connected vertices.
	 */
	@Test
	public void testRegionBuilding() throws Exception {

		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");

		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(v1, v2, v3, v4);

		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();
		final ExecutionVertex ev31 = vertexIterator.next();
		final ExecutionVertex ev32 = vertexIterator.next();
		final ExecutionVertex ev41 = vertexIterator.next();
		final ExecutionVertex ev42 = vertexIterator.next();

		AdaptedRestartPipelinedRegionStrategyNG strategy = (AdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();

		RestartPipelinedRegionStrategyBuildingTest.assertSameRegion(
			strategy.getFailoverRegion(ev11),
			strategy.getFailoverRegion(ev21));
		RestartPipelinedRegionStrategyBuildingTest.assertSameRegion(
			strategy.getFailoverRegion(ev12),
			strategy.getFailoverRegion(ev22));
		RestartPipelinedRegionStrategyBuildingTest.assertSameRegion(
			strategy.getFailoverRegion(ev31),
			strategy.getFailoverRegion(ev41));
		RestartPipelinedRegionStrategyBuildingTest.assertSameRegion(
			strategy.getFailoverRegion(ev32),
			strategy.getFailoverRegion(ev42));

		RestartPipelinedRegionStrategyBuildingTest.assertDistinctRegions(
			strategy.getFailoverRegion(ev11),
			strategy.getFailoverRegion(ev12),
			strategy.getFailoverRegion(ev31),
			strategy.getFailoverRegion(ev32));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph) throws JobException, JobExecutionException {
		// configure the pipelined failover strategy
		final Configuration jobManagerConfig = new Configuration();
		jobManagerConfig.setString(
			JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
			FailoverStrategyLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);

		final Time timeout = Time.seconds(10L);
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobManagerConfig,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			mock(SlotProvider.class),
			PipelinedFailoverRegionBuildingTest.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			timeout,
			log);
	}
}
