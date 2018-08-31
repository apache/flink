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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests that make sure that the building of pipelined connected failover regions works
 * correctly.
 */
public class PipelinedFailoverRegionBuildingTest extends TestLogger {

	/**
	 * Tests that validates that a graph with single unconnected vertices works correctly.
	 * 
	 * <pre>
	 *     (v1)
	 *     
	 *     (v2)
	 *     
	 *     (v3)
	 *     
	 *     ...
	 * </pre>
	 */
	@Test
	public void testIndividualVertices() throws Exception {
		final JobVertex source1 = new JobVertex("source1");
		source1.setInvokableClass(NoOpInvokable.class);
		source1.setParallelism(2);

		final JobVertex source2 = new JobVertex("source2");
		source2.setInvokableClass(NoOpInvokable.class);
		source2.setParallelism(2);

		final JobGraph jobGraph = new JobGraph("test job", source1, source2);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion sourceRegion11 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source1.getID()).getTaskVertices()[0]);
		FailoverRegion sourceRegion12 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source1.getID()).getTaskVertices()[1]);
		FailoverRegion targetRegion21 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source2.getID()).getTaskVertices()[0]);
		FailoverRegion targetRegion22 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source2.getID()).getTaskVertices()[1]);

		assertTrue(sourceRegion11 != sourceRegion12);
		assertTrue(sourceRegion12 != targetRegion21);
		assertTrue(targetRegion21 != targetRegion22);
	}

	/**
	 * Tests that validates that embarrassingly parallel chains of vertices work correctly.
	 * 
	 * <pre>
	 *     (a1) --> (b1)
	 *
	 *     (a2) --> (b2)
	 *
	 *     (a3) --> (b3)
	 *
	 *     ...
	 * </pre>
	 */
	@Test
	public void testEmbarrassinglyParallelCase() throws Exception {
		int parallelism = 10000;
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(parallelism);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(parallelism);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(parallelism);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion preRegion1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[0]);
		FailoverRegion preRegion2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[0]);
		FailoverRegion preRegion3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0]);

		assertTrue(preRegion1 == preRegion2);
		assertTrue(preRegion2 == preRegion3);

		for (int i = 1; i < parallelism; ++i) {
			FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[i]);
			FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[i]);
			FailoverRegion region3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[i]);

			assertTrue(region1 == region2);
			assertTrue(region2 == region3);

			assertTrue(preRegion1 != region1);
		}
	}

	/**
	 * Tests that validates that a single pipelined component via a sequence of all-to-all
	 * connections works correctly.
	 * 
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1) 
	 *           X         X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *           X         X
	 *     (a3) -+-> (b3) -+-> (c3)
	 *
	 *     ...
	 * </pre>
	 */
	@Test
	public void testOneComponentViaTwoExchanges() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(3);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(5);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[1]);
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[4]);
		FailoverRegion region3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0]);

		assertTrue(region1 == region2);
		assertTrue(region2 == region3);
	}

	/**
	 * Tests that validates that a single pipelined component via a cascade of joins
	 * works correctly.
	 * 
	 * <p>Non-parallelized view:
	 * <pre>
	 *     (1)--+
	 *          +--(5)-+
	 *     (2)--+      |
	 *                 +--(7)
	 *     (3)--+      |
	 *          +--(6)-+
	 *     (4)--+
	 *     ...
	 * </pre>
	 */
	@Test
	public void testOneComponentViaCascadeOfJoins() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		final JobVertex vertex5 = new JobVertex("vertex5");
		vertex5.setInvokableClass(NoOpInvokable.class);
		vertex5.setParallelism(4);

		final JobVertex vertex6 = new JobVertex("vertex6");
		vertex6.setInvokableClass(NoOpInvokable.class);
		vertex6.setParallelism(4);

		final JobVertex vertex7 = new JobVertex("vertex7");
		vertex7.setInvokableClass(NoOpInvokable.class);
		vertex7.setParallelism(2);

		vertex5.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex5.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex6.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex6.connectNewDataSetAsInput(vertex4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex7.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex7.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3, vertex4, vertex5, vertex6, vertex7);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		Iterator<ExecutionVertex> evs = eg.getAllExecutionVertices().iterator();

		FailoverRegion preRegion = failoverStrategy.getFailoverRegion(evs.next());

		while (evs.hasNext()) {
			FailoverRegion region = failoverStrategy.getFailoverRegion(evs.next());
			assertTrue(preRegion == region);
		}
	}

	/**
	 * Tests that validates that a single pipelined component instance from one source
	 * works correctly.
	 * 
	 * <p>Non-parallelized view:
	 * <pre>
	 *                 +--(1)
	 *          +--(5)-+
	 *          |      +--(2)
	 *     (7)--+
	 *          |      +--(3)
	 *          +--(6)-+
	 *                 +--(4)
	 *     ...
	 * </pre>
	 */
	@Test
	public void testOneComponentInstanceFromOneSource() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		final JobVertex vertex5 = new JobVertex("vertex5");
		vertex5.setInvokableClass(NoOpInvokable.class);
		vertex5.setParallelism(4);

		final JobVertex vertex6 = new JobVertex("vertex6");
		vertex6.setInvokableClass(NoOpInvokable.class);
		vertex6.setParallelism(4);

		final JobVertex vertex7 = new JobVertex("vertex7");
		vertex7.setInvokableClass(NoOpInvokable.class);
		vertex7.setParallelism(2);

		vertex1.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex2.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex3.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex4.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex5.connectNewDataSetAsInput(vertex7, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex6.connectNewDataSetAsInput(vertex7, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex7, vertex5, vertex6, vertex1, vertex2, vertex3, vertex4);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		Iterator<ExecutionVertex> evs = eg.getAllExecutionVertices().iterator();

		FailoverRegion preRegion = failoverStrategy.getFailoverRegion(evs.next());

		while (evs.hasNext()) {
			FailoverRegion region = failoverStrategy.getFailoverRegion(evs.next());
			assertTrue(preRegion == region);
		}
	}

	/**
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1) 
	 *           X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *           X
	 *     (a3) -+-> (b3) -+-> (c3)
	 *
	 *           ^         ^
	 *           |         |
	 *     (pipelined) (blocking)
	 *
	 * </pre>
	 */
	@Test
	public void testTwoComponentsViaBlockingExchange() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(3);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[1]);
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[0]);
		FailoverRegion region31 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0]);
		FailoverRegion region32 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[1]);

		assertTrue(region1 == region2);
		assertTrue(region2 != region31);
		assertTrue(region32 != region31);
	}

	/**
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1) 
	 *           X         X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *           X         X
	 *     (a3) -+-> (b3) -+-> (c3)
	 *
	 *           ^         ^
	 *           |         |
	 *     (pipelined) (blocking)
	 * </pre>
	 */
	@Test
	public void testTwoComponentsViaBlockingExchange2() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(3);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[1]);
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[0]);
		FailoverRegion region31 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0]);
		FailoverRegion region32 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[1]);

		assertTrue(region1 == region2);
		assertTrue(region2 != region31);
		assertTrue(region32 != region31);
	}

	/**
	 * Cascades of joins with partially blocking, partially pipelined exchanges:
	 * <pre>
	 *     (1)--+
	 *          +--(5)-+
	 *     (2)--+      |
	 *              (block)
	 *                 |
	 *                 +--(7)
	 *                 |
	 *              (block)
	 *     (3)--+      |
	 *          +--(6)-+
	 *     (4)--+
	 *     ...
	 * </pre>
	 *
	 * Component 1: 1, 2, 5; component 2: 3,4,6; component 3: 7 
	 */
	@Test
	public void testMultipleComponentsViaCascadeOfJoins() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		final JobVertex vertex5 = new JobVertex("vertex5");
		vertex5.setInvokableClass(NoOpInvokable.class);
		vertex5.setParallelism(4);

		final JobVertex vertex6 = new JobVertex("vertex6");
		vertex6.setInvokableClass(NoOpInvokable.class);
		vertex6.setParallelism(4);

		final JobVertex vertex7 = new JobVertex("vertex7");
		vertex7.setInvokableClass(NoOpInvokable.class);
		vertex7.setParallelism(2);

		vertex5.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex5.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex6.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex6.connectNewDataSetAsInput(vertex4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex7.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		vertex7.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3, vertex4, vertex5, vertex6, vertex7);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[0]);
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[5]);
		FailoverRegion region5 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex5.getID()).getTaskVertices()[2]);

		assertTrue(region1 == region2);
		assertTrue(region1 == region5);

		FailoverRegion region3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0]);
		FailoverRegion region4 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex4.getID()).getTaskVertices()[5]);
		FailoverRegion region6 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex6.getID()).getTaskVertices()[2]);

		assertTrue(region3 == region4);
		assertTrue(region3 == region6);

		FailoverRegion region71 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex7.getID()).getTaskVertices()[0]);
		FailoverRegion region72 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex7.getID()).getTaskVertices()[1]);

		assertTrue(region71 != region72);
		assertTrue(region1 != region71);
		assertTrue(region1 != region72);
		assertTrue(region3 != region71);
		assertTrue(region3 != region72);
	}

	@Test
	public void testDiamondWithMixedPipelinedAndBlockingExchanges() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		vertex3.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex4.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex4.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3, vertex4);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		Iterator<ExecutionVertex> evs = eg.getAllExecutionVertices().iterator();

		FailoverRegion preRegion = failoverStrategy.getFailoverRegion(evs.next());

		while (evs.hasNext()) {
			FailoverRegion region = failoverStrategy.getFailoverRegion(evs.next());
			assertTrue(preRegion == region);
		}
	}

	/**
	 * This test checks that are strictly co-located vertices are in the same failover region,
	 * even through they are connected via a blocking pattern.
	 * This is currently an assumption / limitation of the scheduler.
	 */
	@Test
	public void testBlockingAllToAllTopologyWithCoLocation() throws Exception {
		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(10);

		final JobVertex target = new JobVertex("target");
		target.setInvokableClass(NoOpInvokable.class);
		target.setParallelism(13);

		target.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final SlotSharingGroup sharingGroup = new SlotSharingGroup();
		source.setSlotSharingGroup(sharingGroup);
		target.setSlotSharingGroup(sharingGroup);

		source.setStrictlyCoLocatedWith(target);

		final JobGraph jobGraph = new JobGraph("test job", source, target);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source.getID()).getTaskVertices()[0]);
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(target.getID()).getTaskVertices()[0]);

		// we use 'assertTrue' here rather than 'assertEquals' because we want to test
		// for referential equality, to be on the safe side
		assertTrue(region1 == region2);
	}

	/**
	 * This test checks that are strictly co-located vertices are in the same failover region,
	 * even through they are connected via a blocking pattern.
	 * This is currently an assumption / limitation of the scheduler.
	 */
	@Test
	public void testPipelinedOneToOneTopologyWithCoLocation() throws Exception {
		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(10);

		final JobVertex target = new JobVertex("target");
		target.setInvokableClass(NoOpInvokable.class);
		target.setParallelism(10);

		target.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final SlotSharingGroup sharingGroup = new SlotSharingGroup();
		source.setSlotSharingGroup(sharingGroup);
		target.setSlotSharingGroup(sharingGroup);

		source.setStrictlyCoLocatedWith(target);

		final JobGraph jobGraph = new JobGraph("test job", source, target);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();
		FailoverRegion sourceRegion1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source.getID()).getTaskVertices()[0]);
		FailoverRegion sourceRegion2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source.getID()).getTaskVertices()[1]);
		FailoverRegion targetRegion1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(target.getID()).getTaskVertices()[0]);
		FailoverRegion targetRegion2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(target.getID()).getTaskVertices()[1]);

		// we use 'assertTrue' here rather than 'assertEquals' because we want to test
		// for referential equality, to be on the safe side
		assertTrue(sourceRegion1 == sourceRegion2);
		assertTrue(sourceRegion2 == targetRegion1);
		assertTrue(targetRegion1 == targetRegion2);
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
			1000,
			VoidBlobWriter.getInstance(),
			timeout,
			log);
	}
}
