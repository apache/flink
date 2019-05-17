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

import java.util.Collections;

import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.DONE;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.EMPTY;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.PRODUCING;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.RELEASED;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DefaultResultPartition}.
 */
public class DefaultResultPartitionTest extends TestLogger {

	private final DefaultExecutionVertexTest.TestExecutionStateSupplier stateProvider =
		new DefaultExecutionVertexTest.TestExecutionStateSupplier();

	private SchedulingExecutionVertex producerVertex;

	private SchedulingExecutionVertex consumerVertex;

	private DefaultResultPartition resultPartition;

	@Before
	public void setUp() {
		resultPartition = new DefaultResultPartition(
			new IntermediateResultPartitionID(),
			new IntermediateDataSetID(),
			BLOCKING);

		producerVertex = new DefaultExecutionVertex(
			new ExecutionVertexID(new JobVertexID(), 0),
			Collections.singletonList(resultPartition),
			ALL,
			stateProvider);
		resultPartition.setProducer(producerVertex);
		consumerVertex = new DefaultExecutionVertex(
			new ExecutionVertexID(new JobVertexID(), 0),
			java.util.Collections.emptyList(),
			ALL,
			stateProvider);
		resultPartition.addConsumer(consumerVertex);
	}

	@Test
	public void testGetConsumers() {
		ExecutionVertexID schedulingConsumers = resultPartition.getConsumers()
			.stream().findAny().map(SchedulingExecutionVertex::getId)
			.orElseThrow(() -> new IllegalArgumentException("can not find vertex"));
		assertEquals(schedulingConsumers, consumerVertex.getId());
	}

	@Test
	public void testGetProducer() {
		assertEquals(resultPartition.getProducer().getId(), producerVertex.getId());
	}

	@Test
	public void testGetPartitionState() {
		for (ExecutionState state : ExecutionState.values()) {
			stateProvider.setExecutionState(state);
			SchedulingResultPartition.ResultPartitionState partitionState = resultPartition.getState();
			switch (state) {
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
