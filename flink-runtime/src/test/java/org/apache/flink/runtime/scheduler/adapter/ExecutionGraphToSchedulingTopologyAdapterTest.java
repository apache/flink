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
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.api.common.InputDependencyConstraint.ANY;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSimpleTestGraph;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexState;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ExecutionGraphToSchedulingTopologyAdapter}.
 */
public class ExecutionGraphToSchedulingTopologyAdapterTest extends TestLogger {

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

	private final int parallelism = 3;

	private ExecutionGraph executionGraph;

	private JobVertex[] jobVertices;

	private ExecutionGraphToSchedulingTopologyAdapter adapter;

	@Before
	public void setUp() throws Exception {
		jobVertices = new JobVertex[2];
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
	public void testGetVertex() {
		int idx = 0;
		for (SchedulingExecutionVertex vertex : adapter.getVertices()) {
			assertEquals(new ExecutionVertexID(jobVertices[idx / parallelism].getID(), idx % parallelism), vertex.getId());
			idx++;
		}
	}

	@Test
	public void testGetResultPartition() {
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			for (Map.Entry<IntermediateResultPartitionID, IntermediateResultPartition> entry
					: vertex.getProducedPartitions().entrySet()) {
				IntermediateResultPartition partition = entry.getValue();
				SchedulingResultPartition schedulingResultPartition = adapter.getResultPartition(entry.getKey())
					.orElseThrow(() -> new IllegalArgumentException("can not find partition" + entry.getKey()));

				assertEquals(partition.getPartitionId(), schedulingResultPartition.getId());
				assertEquals(partition.getIntermediateResult().getId(), schedulingResultPartition.getResultId());
				assertEquals(partition.getResultType(), schedulingResultPartition.getPartitionType());
			}
		}
	}

	@Test
	public void getExecutionState() {
		final ExecutionState[] executionStates = ExecutionState.values();
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			for (ExecutionState state : executionStates) {
				setVertexState(vertex, state);
				assertEquals(
					state,
					adapter.getVertex(new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()))
						.orElseThrow(() -> new IllegalArgumentException("can not find vertex"))
						.getState());
			}
		}
	}
}
