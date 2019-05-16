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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.Provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link DefaultExecutionVertex}.
 */
public class DefaultExecutionVertexTest extends TestLogger {

	private final ExecutionStateProviderTest stateProvider = new ExecutionStateProviderTest();

	private List<SchedulingExecutionVertex> schedulingExecutionVertices;

	private IntermediateResultPartitionID intermediateResultPartitionId;

	@Before
	public void setUp() throws Exception {

		schedulingExecutionVertices = new ArrayList<>(2);
		intermediateResultPartitionId = new IntermediateResultPartitionID();

		DefaultResultPartition schedulingResultPartition = new DefaultResultPartition(
			intermediateResultPartitionId,
			new IntermediateDataSetID(),
			BLOCKING);
		DefaultExecutionVertex schedulingVertex1 = new DefaultExecutionVertex(
			new ExecutionVertexID(new JobVertexID(), 0),
			Collections.singletonList(schedulingResultPartition),
			ALL,
			stateProvider);
		schedulingResultPartition.setProducer(schedulingVertex1);
		DefaultExecutionVertex schedulingVertex2 = new DefaultExecutionVertex(
			new ExecutionVertexID(new JobVertexID(), 0),
			Collections.emptyList(),
			ALL,
			stateProvider);
		schedulingVertex2.addConsumedPartition(schedulingResultPartition);
		schedulingExecutionVertices.add(schedulingVertex1);
		schedulingExecutionVertices.add(schedulingVertex2);
	}

	@Test
	public void testGetExecutionState() {
		final ExecutionState[] states = ExecutionState.values();
		for (ExecutionState state : states) {
			for (SchedulingExecutionVertex srp : schedulingExecutionVertices) {
				stateProvider.setExecutionState(state);
				assertEquals(state, srp.getState());
			}
		}
	}

	@Test
	public void testGetProducedResultPartitions() {
		Collection<IntermediateResultPartitionID> partitionIds1 =  schedulingExecutionVertices
			.get(0).getProducedResultPartitions().stream().map(SchedulingResultPartition::getId)
			.collect(Collectors.toList());
		List<IntermediateResultPartitionID> partitionIds2 = Collections.singletonList(intermediateResultPartitionId);
		assertThat(partitionIds1, containsInAnyOrder(partitionIds2.toArray()));
	}

	@Test
	public void testGetConsumedResultPartitions() {
			Collection<IntermediateResultPartitionID> partitionIds1 = schedulingExecutionVertices
				.get(1).getConsumedResultPartitions().stream().map(SchedulingResultPartition::getId)
				.collect(Collectors.toList());
			List<IntermediateResultPartitionID> partitionIds2 = Collections.singletonList(intermediateResultPartitionId);
			assertThat(partitionIds1, containsInAnyOrder(partitionIds2.toArray()));
	}

	/**
	 * A simple implementation of {@link Provider} for testing.
	 */
	public static class ExecutionStateProviderTest implements Provider<ExecutionState> {

		private ExecutionState executionState;

		void setExecutionState(ExecutionState state) {
			executionState = state;
		}

		@Override
		public ExecutionState invoke(ExecutionState request) {
			return executionState;
		}
	}
}
