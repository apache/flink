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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.TestRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.api.common.InputDependencyConstraint.ANY;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSimpleTestGraph;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link ExecutionGraphToSchedulingTopologyAdapter}.
 */
public class ExecutionGraphToSchedulingTopologyAdapterTest extends TestLogger {

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

	private ExecutionGraph executionGraph;

	private ExecutionGraphToSchedulingTopologyAdapter adapter;

	@Before
	public void setUp() throws Exception {
		JobVertex[] jobVertices = new JobVertex[2];
		int parallelism = 3;
		jobVertices[0] = createNoOpVertex(parallelism);
		jobVertices[1] = createNoOpVertex(parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, BLOCKING);
		jobVertices[0].setInputDependencyConstraint(ALL);
		jobVertices[1].setInputDependencyConstraint(ANY);
		executionGraph = createSimpleTestGraph(
			new JobID(),
			taskManagerGateway,
			triggeredRestartStrategy,
			jobVertices);
		adapter = new ExecutionGraphToSchedulingTopologyAdapter(executionGraph);
	}

	@Test
	public void testConstructor() {
		// implicitly tests order constraint of getVertices()
		assertGraphEquals(executionGraph, adapter);
	}

	@Test
	public void testGetResultPartition() {
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			for (Map.Entry<IntermediateResultPartitionID, IntermediateResultPartition> entry : vertex.getProducedPartitions().entrySet()) {
				IntermediateResultPartition partition = entry.getValue();
				SchedulingResultPartition schedulingResultPartition = adapter.getResultPartition(entry.getKey())
					.orElseThrow(() -> new IllegalArgumentException("can not find partition " + entry.getKey()));

				assertPartitionEquals(partition, schedulingResultPartition);
			}
		}
	}

	@Test
	public void testGetVertexOrThrow() {
		try {
			adapter.getVertexOrThrow(new ExecutionVertexID(new JobVertexID(), 0));
			fail("get not exist vertex");
		} catch (IllegalArgumentException exception) {
			// expected
		}
	}

	@Test
	public void testResultPartitionOrThrow() {
		try {
			adapter.getResultPartitionOrThrow(new IntermediateResultPartitionID());
			fail("get not exist result partition");
		} catch (IllegalArgumentException exception) {
			// expected
		}
	}

	private static void assertGraphEquals(
		ExecutionGraph originalGraph,
		SchedulingTopology adaptedTopology) {

		Iterator<ExecutionVertex> originalVertices = originalGraph.getAllExecutionVertices().iterator();
		Iterator<SchedulingExecutionVertex> adaptedVertices = adaptedTopology.getVertices().iterator();

		while (originalVertices.hasNext()) {
			ExecutionVertex originalVertex = originalVertices.next();
			SchedulingExecutionVertex adaptedVertex = adaptedVertices.next();

			assertVertexEquals(originalVertex, adaptedVertex);

			List<IntermediateResultPartition> originalConsumedPartitions = IntStream.range(0, originalVertex.getNumberOfInputs())
				.mapToObj(originalVertex::getInputEdges)
				.flatMap(Arrays::stream)
				.map(ExecutionEdge::getSource)
				.collect(Collectors.toList());
			Collection<SchedulingResultPartition> adaptedConsumedPartitions = adaptedVertex.getConsumedResultPartitions();

			assertPartitionsEquals(originalConsumedPartitions, adaptedConsumedPartitions);

			Collection<IntermediateResultPartition> originalProducedPartitions = originalVertex.getProducedPartitions().values();
			Collection<SchedulingResultPartition> adaptedProducedPartitions = adaptedVertex.getProducedResultPartitions();

			assertPartitionsEquals(originalProducedPartitions, adaptedProducedPartitions);
		}

		assertFalse("Number of adapted vertices exceeds number of original vertices.", adaptedVertices.hasNext());
	}

	private static void assertPartitionsEquals(
		Collection<IntermediateResultPartition> originalPartitions,
		Collection<SchedulingResultPartition> adaptedPartitions) {

		assertEquals(originalPartitions.size(), adaptedPartitions.size());

		for (IntermediateResultPartition originalPartition : originalPartitions) {
			SchedulingResultPartition adaptedPartition = adaptedPartitions.stream()
				.filter(adapted -> adapted.getId().equals(originalPartition.getPartitionId()))
				.findAny()
				.orElseThrow(() -> new AssertionError("Could not find matching adapted partition for " + originalPartition));

			assertPartitionEquals(originalPartition, adaptedPartition);

			List<ExecutionVertex> originalConsumers = originalPartition.getConsumers().stream()
				.flatMap(Collection::stream)
				.map(ExecutionEdge::getTarget)
				.collect(Collectors.toList());
			Collection<SchedulingExecutionVertex> adaptedConsumers = adaptedPartition.getConsumers();

			for (ExecutionVertex originalConsumer : originalConsumers) {
				// it is sufficient to verify that some vertex exists with the correct ID here,
				// since deep equality is verified later in the main loop
				// this DOES rely on an implicit assumption that the vertices objects returned by the topology are
				// identical to those stored in the partition
				ExecutionVertexID originalId = new ExecutionVertexID(originalConsumer.getJobvertexId(), originalConsumer.getParallelSubtaskIndex());
				assertTrue(adaptedConsumers.stream().anyMatch(adaptedConsumer -> adaptedConsumer.getId().equals(originalId)));
			}
		}
	}

	private static void assertPartitionEquals(
		IntermediateResultPartition originalPartition,
		SchedulingResultPartition adaptedPartition) {

		assertEquals(originalPartition.getPartitionId(), adaptedPartition.getId());
		assertEquals(originalPartition.getIntermediateResult().getId(), adaptedPartition.getResultId());
		assertEquals(originalPartition.getResultType(), adaptedPartition.getPartitionType());
		assertVertexEquals(
			originalPartition.getProducer(),
			adaptedPartition.getProducer());
	}

	private static void assertVertexEquals(
		ExecutionVertex originalVertex,
		SchedulingExecutionVertex adaptedVertex) {
		assertEquals(
			new ExecutionVertexID(originalVertex.getJobvertexId(), originalVertex.getParallelSubtaskIndex()),
			adaptedVertex.getId());
		assertEquals(originalVertex.getInputDependencyConstraint(), adaptedVertex.getInputDependencyConstraint());
	}
}
