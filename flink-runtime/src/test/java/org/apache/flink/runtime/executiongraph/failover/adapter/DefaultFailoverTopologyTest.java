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

package org.apache.flink.runtime.executiongraph.failover.adapter;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.TestRestartStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverEdge;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverTopology;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSimpleTestGraph;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DefaultFailoverTopology}.
 */
public class DefaultFailoverTopologyTest extends TestLogger {

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

	/**
	 * Tests that the generated failover topology is strictly matched with the given ExecutionGraph.
	 */
	@Test
	public void testTopology() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraph();
		DefaultFailoverTopology adapter = new DefaultFailoverTopology(executionGraph);
		assertGraphEquals(executionGraph, adapter);
	}

	/**
	 * Tests the case that the graph has collocation constraints.
	 */
	@Test
	public void testWithCollocationConstraints() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraph(true);
		DefaultFailoverTopology adapter = new DefaultFailoverTopology(executionGraph);
		assertTrue(adapter.containsCoLocationConstraints());
	}

	/**
	 * Tests the case that the graph has no collocation constraint.
	 */
	@Test
	public void testWithoutCollocationConstraints() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraph(false);
		DefaultFailoverTopology adapter = new DefaultFailoverTopology(executionGraph);
		assertFalse(adapter.containsCoLocationConstraints());
	}

	private ExecutionGraph createExecutionGraph() throws Exception {
		return createExecutionGraph(false);
	}

	private ExecutionGraph createExecutionGraph(boolean addCollocationConstraints) throws Exception {
		JobVertex[] jobVertices = new JobVertex[3];
		int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[2] = createNoOpVertex("v3", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, BLOCKING);
		jobVertices[2].connectNewDataSetAsInput(jobVertices[1], POINTWISE, PIPELINED);

		if (addCollocationConstraints) {
			SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
			jobVertices[1].setSlotSharingGroup(slotSharingGroup);
			jobVertices[2].setSlotSharingGroup(slotSharingGroup);

			CoLocationGroup coLocationGroup = new CoLocationGroup();
			coLocationGroup.addVertex(jobVertices[1]);
			coLocationGroup.addVertex(jobVertices[2]);
			jobVertices[1].updateCoLocationGroup(coLocationGroup);
			jobVertices[2].updateCoLocationGroup(coLocationGroup);
		}

		return createSimpleTestGraph(
			new JobID(),
			taskManagerGateway,
			triggeredRestartStrategy,
			jobVertices);
	}

	private static void assertGraphEquals(ExecutionGraph originalGraph, FailoverTopology adaptedTopology) {
		List<ExecutionVertex> originalVertices = StreamSupport.stream(
			originalGraph.getAllExecutionVertices().spliterator(),
			false).collect(Collectors.toList());
		List<FailoverVertex> adaptedVertices = StreamSupport.stream(
			adaptedTopology.getFailoverVertices().spliterator(),
			false).collect(Collectors.toList());

		assertEquals(originalVertices.size(), adaptedVertices.size());

		// the vertices should be topologically sorted, i.e. in the same order
		for (int i = 0; i < originalVertices.size(); i++) {
			ExecutionVertex originalVertex = originalVertices.get(i);
			FailoverVertex adaptedVertex = adaptedVertices.get(i);
			assertVertexEquals(originalVertex, adaptedVertex);
		}
	}

	private static void assertVertexEquals(ExecutionVertex originalVertex, FailoverVertex adaptedVertex) {
		// compare vertex internal properties
		assertTrue(compareVertexInternalProperties(originalVertex, adaptedVertex));

		// compare input edges
		List<ExecutionEdge> originalInputEdges = IntStream.range(0, originalVertex.getNumberOfInputs())
			.mapToObj(originalVertex::getInputEdges)
			.flatMap(Arrays::stream)
			.collect(Collectors.toList());
		List<FailoverEdge> adaptedInputEdges = StreamSupport.stream(
			adaptedVertex.getInputEdges().spliterator(),
			false).collect(Collectors.toList());
		assertEdgesEquals(originalInputEdges, adaptedInputEdges);

		// compare output edges
		List<ExecutionEdge> originalOutputEdges = originalVertex.getProducedPartitions().values().stream()
			.map(IntermediateResultPartition::getConsumers)
			.flatMap(Collection::stream)
			.flatMap(Collection::stream)
			.collect(Collectors.toList());
		List<FailoverEdge> adaptedOutputEdges = StreamSupport.stream(
			adaptedVertex.getOutputEdges().spliterator(),
			false).collect(Collectors.toList());
		assertEdgesEquals(originalOutputEdges, adaptedOutputEdges);
	}

	private static boolean compareVertexInternalProperties(ExecutionVertex originalVertex, FailoverVertex adaptedVertex) {
		return originalVertex.getJobvertexId().equals(adaptedVertex.getExecutionVertexID().getJobVertexId()) &&
			originalVertex.getParallelSubtaskIndex() == adaptedVertex.getExecutionVertexID().getSubtaskIndex() &&
			originalVertex.getTaskNameWithSubtaskIndex().equals(adaptedVertex.getExecutionVertexName());
	}

	private static void assertEdgesEquals(
		Collection<ExecutionEdge> originalEdges,
		Collection<FailoverEdge> adaptedEdges) {

		assertEquals(originalEdges.size(), adaptedEdges.size());

		for (ExecutionEdge originalEdge : originalEdges) {
			List<FailoverEdge> matchedAdaptedEdges = adaptedEdges.stream()
				.filter(adapted -> compareEdge(originalEdge, adapted))
				.collect(Collectors.toList());

			// there should be exactly 1 matched edge
			// this ensures originalEdges and adaptedEdges elements have a one-to-one mapping
			// it also helps to ensure there is no duplicated edge
			assertEquals(1,  matchedAdaptedEdges.size());
		}
	}

	private static boolean compareEdge(ExecutionEdge originalEdge, FailoverEdge adaptedEdge) {
		return originalEdge.getSource().getPartitionId().equals(adaptedEdge.getResultPartitionID()) &&
			originalEdge.getSource().getResultType().equals(adaptedEdge.getResultPartitionType()) &&
			compareVertexInternalProperties(originalEdge.getSource().getProducer(), adaptedEdge.getSourceVertex()) &&
			compareVertexInternalProperties(originalEdge.getTarget(), adaptedEdge.getTargetVertex());
	}
}
