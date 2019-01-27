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

package org.apache.flink.runtime.preaggregatedaccumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests the aggregating and querying actions of AccumulatorAggregationCoordinator.
 */
public class AccumulatorAggregationCoordinatorTest {

	@Test
	public void testCommission() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraph(4);

		// Get the id of the unique job vertex.
		final JobVertexID jobVertexID = executionGraph.getVerticesTopologically().iterator().next().getJobVertexId();
		final String name = "test";

		AccumulatorAggregationCoordinator coordinator = new AccumulatorAggregationCoordinator();

		// The first commit contains the aggregated value from part of tasks.
		CommitAccumulator commitAccumulator = new CommitAccumulator(
			jobVertexID,
			name,
			new IntCounter(2),
			new HashSet<>(Arrays.asList(1, 3)));
		coordinator.commitPreAggregatedAccumulator(executionGraph, commitAccumulator);

		Map<String, AccumulatorAggregationCoordinator.GlobalAggregatedAccumulator> aggregatedAccumulatorMap =
			coordinator.getAggregatedAccumulators();
		assertTrue("There should be exactly one accumulator with the name " + name,
			aggregatedAccumulatorMap.size() == 1 && aggregatedAccumulatorMap.containsKey(name));
		assertFalse("Not all tasks committed.", aggregatedAccumulatorMap.get(name).isAllCommitted());
		assertEquals(2, aggregatedAccumulatorMap.get(name).getAggregatedValue().getLocalValue());
		assertEquals(new HashSet<>(Arrays.asList(1, 3)), aggregatedAccumulatorMap.get(name).getCommittedTasks());

		// The second commit contains the repeat value from the task 3, the value will also be merged, but
		// the accumulator remains incomplete since the task 0 has not committed yet.
		commitAccumulator = new CommitAccumulator(
			jobVertexID,
			name,
			new IntCounter(2),
			new HashSet<>(Arrays.asList(2, 3))
		);
		coordinator.commitPreAggregatedAccumulator(executionGraph, commitAccumulator);
		assertTrue("There should be exactly one accumulator with the name " + name,
			aggregatedAccumulatorMap.size() == 1 && aggregatedAccumulatorMap.containsKey(name));
		assertFalse("Not all tasks committed.", aggregatedAccumulatorMap.get(name).isAllCommitted());
		assertEquals(4, aggregatedAccumulatorMap.get(name).getAggregatedValue().getLocalValue());
		assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), aggregatedAccumulatorMap.get(name).getCommittedTasks());

		// The third commit completes the accumulator aggregation.
		commitAccumulator = new CommitAccumulator(
			jobVertexID,
			name,
			new IntCounter(3),
			new HashSet<>(Arrays.asList(0, 2)));
		coordinator.commitPreAggregatedAccumulator(executionGraph, commitAccumulator);
		assertTrue("There should be exactly one accumulator with the name " + name,
			aggregatedAccumulatorMap.size() == 1 && aggregatedAccumulatorMap.containsKey(name));
		assertTrue("All tasks have already committed.", aggregatedAccumulatorMap.get(name).isAllCommitted());
		assertEquals(7, aggregatedAccumulatorMap.get(name).getAggregatedValue().getLocalValue());
		assertEquals(new HashSet<>(Arrays.asList(0, 1, 2, 3)), aggregatedAccumulatorMap.get(name).getCommittedTasks());
	}

	/**
	 * Tests querying on an accumulator which has not finished aggregation yet. The queries will be
	 * added to the unfulfilled list first and after the accumulator finishes aggregation, these queries
	 * will be completed.
	 */
	@Test
	public void testQueryingIncompleteAccumulator() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraph(4);

		// Get the id of the unique job vertex.
		final JobVertexID jobVertexID = executionGraph.getVerticesTopologically().iterator().next().getJobVertexId();
		final String name = "test";

		AccumulatorAggregationCoordinator coordinator = new AccumulatorAggregationCoordinator();

		// Part of tasks commits.
		CommitAccumulator commitAccumulator = new CommitAccumulator(
			jobVertexID,
			name,
			new IntCounter(2),
			new HashSet<>(Arrays.asList(1, 3)));
		coordinator.commitPreAggregatedAccumulator(executionGraph, commitAccumulator);

		// Trigger a couple of queries.
		final int numberOfQueries = 2;
		Accumulator[] queryResults = new Accumulator[numberOfQueries];

		for (int i = 0; i < numberOfQueries; ++i) {
			CompletableFuture<Accumulator<Object, Serializable>> queryFuture = coordinator.queryPreAggregatedAccumulator(name);
			assertFalse("The query should not finished since not all tasks commit.", queryFuture.isDone());

			final int myIndex = i;
			queryFuture.whenComplete((accumulator, throwable) -> {
				queryResults[myIndex] = accumulator;
			});
		}

		commitAccumulator = new CommitAccumulator(
			jobVertexID,
			name,
			new IntCounter(3),
			new HashSet<>(Arrays.asList(0, 2)));
		coordinator.commitPreAggregatedAccumulator(executionGraph, commitAccumulator);

		for (int i = 0; i < numberOfQueries; ++i) {
			assertNotNull("The query should complete after all tasks commit.", queryResults[i]);
			assertEquals(5, queryResults[i].getLocalValue());
		}
	}

	/**
	 * Tests querying on an accumulator which has finished aggregation. The queries will be completed
	 * directly.
	 */
	@Test
	public void testQueryingCompleteAccumulator() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraph(4);

		// Get the id of the unique job vertex.
		final JobVertexID jobVertexID = executionGraph.getVerticesTopologically().iterator().next().getJobVertexId();
		final String name = "test";

		AccumulatorAggregationCoordinator coordinator = new AccumulatorAggregationCoordinator();

		// All tasks commit their values and complete the accumulator.
		CommitAccumulator commitAccumulator = new CommitAccumulator(
			jobVertexID,
			name,
			new IntCounter(5),
			new HashSet<>(Arrays.asList(0, 1, 2, 3)));
		coordinator.commitPreAggregatedAccumulator(executionGraph, commitAccumulator);

		// Trigger a couple of queries.
		final int numberOfQueries = 2;
		Accumulator[] queryResults = new Accumulator[numberOfQueries];

		for (int i = 0; i < numberOfQueries; ++i) {
			CompletableFuture<Accumulator<Object, Serializable>> queryFuture = coordinator.queryPreAggregatedAccumulator(name);
			assertTrue("The query should finished since all tasks commit.", queryFuture.isDone());

			final int myIndex = i;
			queryFuture.whenComplete((accumulator, throwable) -> {
				queryResults[myIndex] = accumulator;
			});
		}

		for (int i = 0; i < numberOfQueries; ++i) {
			assertNotNull("The query should complete after all tasks commit.", queryResults[i]);
			assertEquals(5, queryResults[i].getLocalValue());
		}
	}

	private ExecutionGraph createExecutionGraph(int parallelism) throws Exception {
		JobVertex jobVertex = ExecutionGraphTestUtils.createJobVertex("map", parallelism, NoOpInvokable.class);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			mock(SlotProvider.class),
			mock(RestartStrategy.class),
			TestingUtils.defaultExecutor(),
			jobVertex
		);

		return executionGraph;
	}
}
