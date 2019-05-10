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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.TestRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.api.common.InputDependencyConstraint.ANY;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSimpleTestGraph;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexState;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.DONE;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.EMPTY;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.PRODUCING;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.RELEASED;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link DefaultResultPartition}.
 */
public class DefaultResultPartitionTest {

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

	private ExecutionGraph executionGraph;

	private ExecutionGraphToSchedulingTopologyAdapter adapter;

	private List<IntermediateResultPartition> intermediateResultPartitions;

	private List<SchedulingResultPartition> schedulingResultPartitions;

	@Before
	public void setUp() throws Exception {
		final int parallelism = 3;
		JobVertex[] jobVertices = new JobVertex[2];
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

		intermediateResultPartitions = new ArrayList<>();
		schedulingResultPartitions = new ArrayList<>();

		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			for (Map.Entry<IntermediateResultPartitionID, IntermediateResultPartition> entry
				: vertex.getProducedPartitions().entrySet()) {
				intermediateResultPartitions.add(entry.getValue());
				schedulingResultPartitions.add(adapter.getResultPartition(entry.getKey())
					.orElseThrow(() -> new IllegalArgumentException("can not find partition" + entry.getKey())));
			}
		}
		assertEquals(parallelism, intermediateResultPartitions.size());
	}

	@Test
	public void testBasicInfo() {
		for (int idx = 0; idx < intermediateResultPartitions.size(); idx++) {
			final IntermediateResultPartition partition = intermediateResultPartitions.get(idx);
			final SchedulingResultPartition schedulingResultPartition = schedulingResultPartitions.get(idx);
			assertEquals(partition.getPartitionId(), schedulingResultPartition.getId());
			assertEquals(partition.getIntermediateResult().getId(), schedulingResultPartition.getResultId());
			assertEquals(partition.getResultType(), schedulingResultPartition.getPartitionType());
		}
	}

	@Test
	public void testGetConsumers() {
		for (int idx = 0; idx < intermediateResultPartitions.size(); idx++) {
			Collection<ExecutionVertexID> schedulingConsumers = schedulingResultPartitions.get(idx).getConsumers()
				.stream().map(SchedulingExecutionVertex::getId).collect(Collectors.toList());

			Set<ExecutionVertexID> executionConsumers = new HashSet<>();
			for (List<ExecutionEdge> list :intermediateResultPartitions.get(idx).getConsumers()) {
				for (ExecutionEdge edge : list) {
					final ExecutionVertex vertex = edge.getTarget();
					executionConsumers.add(new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()));
				}
			}
			assertThat(schedulingConsumers, containsInAnyOrder(executionConsumers.toArray()));
		}
	}

	@Test
	public void testGetProducer() {
		for (int idx = 0; idx < intermediateResultPartitions.size(); idx++) {
			final ExecutionVertex vertex = intermediateResultPartitions.get(idx).getProducer();
			assertEquals(schedulingResultPartitions.get(idx).getProducer().getId(),
				new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()));
		}
	}

	@Test
	public void testGetPartitionState() {
		List<SchedulingExecutionVertex> schedulingExecutionVertices = new ArrayList<>();
		executionGraph.getAllExecutionVertices().forEach(
			vertex -> schedulingExecutionVertices.add(new DefaultExecutionVertex(vertex)));

		final ExecutionState[] states = ExecutionState.values();
		Random random = new Random();
		for (ExecutionVertex executionVertex: executionGraph.getAllExecutionVertices()) {
			setVertexState(executionVertex, states[Math.abs(random.nextInt()) % states.length]);
		}

		int idx = 0;
		for (ExecutionVertex executionVertex: executionGraph.getAllExecutionVertices()) {
			for (IntermediateResultPartitionID partitionId : executionVertex.getProducedPartitions().keySet()) {
				SchedulingResultPartition.ResultPartitionState partitionState = adapter.getResultPartition(partitionId)
					.orElseThrow(() -> new IllegalArgumentException("can not find partition" + partitionId))
					.getState();
				switch (schedulingExecutionVertices.get(idx++).getState()) {
					case FINISHED:
						assertEquals(DONE, partitionState);
						break;
					case RUNNING:
						assertEquals(PRODUCING, partitionState);
						break;
					case DEPLOYING:
					case CREATED:
					case SCHEDULED:
						assertEquals(EMPTY, partitionState);
						break;
					default:
						assertEquals(RELEASED, partitionState);
						break;
				}
			}
		}
	}
}
