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
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link DefaultExecutionVertex}.
 */
public class DefaultExecutionVertexTest {

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

	private final int parallelism = 3;

	private List<SchedulingExecutionVertex> schedulingExecutionVertices;

	private List<ExecutionVertex> executionVertices;

	@Before
	public void setUp() throws Exception {
		JobVertex[] jobVertices = new JobVertex[2];
		jobVertices[0] = createNoOpVertex(parallelism);
		jobVertices[1] = createNoOpVertex(parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, BLOCKING);
		jobVertices[0].setInputDependencyConstraint(ALL);
		jobVertices[1].setInputDependencyConstraint(ANY);
		ExecutionGraph executionGraph = createSimpleTestGraph(
			new JobID(),
			taskManagerGateway,
			triggeredRestartStrategy,
			jobVertices);
		ExecutionGraphToSchedulingTopologyAdapter adapter = new ExecutionGraphToSchedulingTopologyAdapter(executionGraph);

		schedulingExecutionVertices = new ArrayList<>();
		adapter.getVertices().forEach(vertex -> schedulingExecutionVertices.add(vertex));
		executionVertices = new ArrayList<>();
		executionGraph.getAllExecutionVertices().forEach(vertex -> executionVertices.add(vertex));
	}

	@Test
	public void testGetId() {
		for (int idx = 0; idx < schedulingExecutionVertices.size(); idx++){
			assertEquals(schedulingExecutionVertices.get(idx).getId().getJobVertexId(),
				executionVertices.get(idx).getJobvertexId());
			assertEquals(schedulingExecutionVertices.get(idx).getId().getSubtaskIndex(),
				executionVertices.get(idx).getParallelSubtaskIndex());
		}
	}

	@Test
	public void testGetExecutionState() {
		final ExecutionState[] states = ExecutionState.values();
		Random random = new Random();
		for (ExecutionVertex executionVertex: executionVertices) {
			setVertexState(executionVertex, states[Math.abs(random.nextInt()) % states.length]);
		}

		for (int idx = 0; idx < schedulingExecutionVertices.size(); idx++) {
			assertEquals(schedulingExecutionVertices.get(idx).getState(), executionVertices.get(idx).getExecutionState());
		}
	}

	@Test
	public void testGetInputDependencyConstraint() {
		for (int idx = 0; idx < schedulingExecutionVertices.size(); idx++) {
			assertEquals(schedulingExecutionVertices.get(idx).getInputDependencyConstraint(),
				executionVertices.get(idx).getJobVertex().getInputDependencyConstraint());
		}
	}

	@Test
	public void testGetConsumedResultPartitions() {
		for (int idx = 0; idx < schedulingExecutionVertices.size(); idx++) {
			Collection<IntermediateResultPartitionID> partitionIds1 =  schedulingExecutionVertices
				.get(idx).getProducedResultPartitions().stream().map(SchedulingResultPartition::getId)
				.collect(Collectors.toList());
			Set<IntermediateResultPartitionID> partitionIds2 = executionVertices
				.get(idx).getProducedPartitions().keySet();
			assertThat(partitionIds1, containsInAnyOrder(partitionIds2.toArray()));
		}
	}

	@Test
	public void testGetProducedResultPartitions() {
		for (int idx = parallelism; idx < schedulingExecutionVertices.size(); idx++) {
			Collection<IntermediateResultPartitionID> partitionIds1 = schedulingExecutionVertices
				.get(idx).getConsumedResultPartitions().stream().map(SchedulingResultPartition::getId)
				.collect(Collectors.toList());

			Set<IntermediateResultPartitionID> partitionIds2 = Arrays.stream(executionVertices
				.get(idx).getInputEdges(0)).map(ExecutionEdge::getSource).map(IntermediateResultPartition::getPartitionId)
				.collect(Collectors.toSet());
			assertThat(partitionIds1, containsInAnyOrder(partitionIds2.toArray()));
		}
	}
}
