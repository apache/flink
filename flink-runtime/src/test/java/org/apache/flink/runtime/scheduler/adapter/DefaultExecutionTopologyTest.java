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

import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.TestRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

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
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link DefaultExecutionTopology}.
 */
public class DefaultExecutionTopologyTest extends TestLogger {

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

	private ExecutionGraph executionGraph;

	private DefaultExecutionTopology adapter;

	@Before
	public void setUp() throws Exception {
		JobVertex[] jobVertices = new JobVertex[2];
		int parallelism = 3;
		jobVertices[0] = createNoOpVertex(parallelism);
		jobVertices[1] = createNoOpVertex(parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);
		jobVertices[0].setInputDependencyConstraint(ALL);
		jobVertices[1].setInputDependencyConstraint(ANY);
		executionGraph = createSimpleTestGraph(
			taskManagerGateway,
			triggeredRestartStrategy,
			jobVertices);
		adapter = new DefaultExecutionTopology(executionGraph);
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
				DefaultResultPartition schedulingResultPartition = adapter.getResultPartition(entry.getKey())
					.orElseThrow(() -> new IllegalArgumentException("can not find partition " + entry.getKey()));

				assertPartitionEquals(partition, schedulingResultPartition);
			}
		}
	}

	@Test
	public void testResultPartitionStateSupplier() {
		final IntermediateResultPartition intermediateResultPartition = IterableUtils
			.toStream(executionGraph.getAllExecutionVertices())
			.flatMap(v -> v.getProducedPartitions().values().stream())
			.findAny()
			.get();

		final DefaultResultPartition schedulingResultPartition = adapter
			.getResultPartition(intermediateResultPartition.getPartitionId())
			.get();

		assertEquals(ResultPartitionState.CREATED, schedulingResultPartition.getState());

		intermediateResultPartition.markDataProduced();
		assertEquals(ResultPartitionState.CONSUMABLE, schedulingResultPartition.getState());
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

	@Test
	public void testWithCoLocationConstraints() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraphWithCoLocationConstraint();
		adapter = new DefaultExecutionTopology(executionGraph);
		assertTrue(adapter.containsCoLocationConstraints());
	}

	@Test
	public void testWithoutCoLocationConstraints() {
		assertFalse(adapter.containsCoLocationConstraints());
	}

	private ExecutionGraph createExecutionGraphWithCoLocationConstraint() throws Exception {
		JobVertex[] jobVertices = new JobVertex[2];
		int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		jobVertices[0].setSlotSharingGroup(slotSharingGroup);
		jobVertices[1].setSlotSharingGroup(slotSharingGroup);

		CoLocationGroup coLocationGroup = new CoLocationGroup();
		coLocationGroup.addVertex(jobVertices[0]);
		coLocationGroup.addVertex(jobVertices[1]);
		jobVertices[0].updateCoLocationGroup(coLocationGroup);
		jobVertices[1].updateCoLocationGroup(coLocationGroup);

		return createSimpleTestGraph(
			taskManagerGateway,
			triggeredRestartStrategy,
			jobVertices);
	}

	private static void assertGraphEquals(
		ExecutionGraph originalGraph,
		DefaultExecutionTopology adaptedTopology) {

		Iterator<ExecutionVertex> originalVertices = originalGraph.getAllExecutionVertices().iterator();
		Iterator<DefaultExecutionVertex> adaptedVertices = adaptedTopology.getVertices().iterator();

		while (originalVertices.hasNext()) {
			ExecutionVertex originalVertex = originalVertices.next();
			DefaultExecutionVertex adaptedVertex = adaptedVertices.next();

			assertVertexEquals(originalVertex, adaptedVertex);

			List<IntermediateResultPartition> originalConsumedPartitions = IntStream.range(0, originalVertex.getNumberOfInputs())
				.mapToObj(originalVertex::getInputEdges)
				.flatMap(Arrays::stream)
				.map(ExecutionEdge::getSource)
				.collect(Collectors.toList());
			Iterable<DefaultResultPartition> adaptedConsumedPartitions = adaptedVertex.getConsumedResults();

			assertPartitionsEquals(originalConsumedPartitions, adaptedConsumedPartitions);

			Collection<IntermediateResultPartition> originalProducedPartitions = originalVertex.getProducedPartitions().values();
			Iterable<DefaultResultPartition> adaptedProducedPartitions = adaptedVertex.getProducedResults();

			assertPartitionsEquals(originalProducedPartitions, adaptedProducedPartitions);
		}

		assertFalse("Number of adapted vertices exceeds number of original vertices.", adaptedVertices.hasNext());
	}

	private static void assertPartitionsEquals(
		Iterable<IntermediateResultPartition> originalResultPartitions,
		Iterable<DefaultResultPartition> adaptedResultPartitions) {

		assertEquals(Iterables.size(originalResultPartitions), Iterables.size(adaptedResultPartitions));

		for (IntermediateResultPartition originalPartition : originalResultPartitions) {
			DefaultResultPartition adaptedPartition = IterableUtils.toStream(adaptedResultPartitions)
				.filter(adapted -> adapted.getId().equals(originalPartition.getPartitionId()))
				.findAny()
				.orElseThrow(() -> new AssertionError("Could not find matching adapted partition for " + originalPartition));

			assertPartitionEquals(originalPartition, adaptedPartition);

			List<ExecutionVertex> originalConsumers = originalPartition.getConsumers().stream()
				.flatMap(Collection::stream)
				.map(ExecutionEdge::getTarget)
				.collect(Collectors.toList());
			Iterable<DefaultExecutionVertex> adaptedConsumers = adaptedPartition.getConsumers();

			for (ExecutionVertex originalConsumer : originalConsumers) {
				// it is sufficient to verify that some vertex exists with the correct ID here,
				// since deep equality is verified later in the main loop
				// this DOES rely on an implicit assumption that the vertices objects returned by the topology are
				// identical to those stored in the partition
				ExecutionVertexID originalId = originalConsumer.getID();
				assertTrue(IterableUtils.toStream(adaptedConsumers).anyMatch(adaptedConsumer -> adaptedConsumer.getId().equals(originalId)));
			}
		}
	}

	private static void assertPartitionEquals(
		IntermediateResultPartition originalPartition,
		DefaultResultPartition adaptedPartition) {

		assertEquals(originalPartition.getPartitionId(), adaptedPartition.getId());
		assertEquals(originalPartition.getIntermediateResult().getId(), adaptedPartition.getResultId());
		assertEquals(originalPartition.getResultType(), adaptedPartition.getResultType());
		assertVertexEquals(
			originalPartition.getProducer(),
			adaptedPartition.getProducer());
	}

	private static void assertVertexEquals(
		ExecutionVertex originalVertex,
		DefaultExecutionVertex adaptedVertex) {

		assertEquals(
			originalVertex.getID(),
			adaptedVertex.getId());
		assertEquals(originalVertex.getInputDependencyConstraint(), adaptedVertex.getInputDependencyConstraint());
	}
}
