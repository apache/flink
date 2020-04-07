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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionConnectionException;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

/**
 * Tests the failure handling logic of the {@link RestartPipelinedRegionFailoverStrategy}.
 */
public class RestartPipelinedRegionFailoverStrategyTest extends TestLogger {

	/**
	 * Tests for scenes that a task fails for its own error, in which case the
	 * region containing the failed task and its consumer regions should be restarted.
	 * <pre>
	 *     (v1) -+-> (v4)
	 *           x
	 *     (v2) -+-> (v5)
	 *
	 *     (v3) -+-> (v6)
	 *
	 *           ^
	 *           |
	 *       (blocking)
	 * </pre>
	 * Each vertex is in an individual region.
	 */
	@Test
	public void testRegionFailoverForRegionInternalErrors() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex();

		topology.connect(v1, v4, ResultPartitionType.BLOCKING);
		topology.connect(v1, v5, ResultPartitionType.BLOCKING);
		topology.connect(v2, v4, ResultPartitionType.BLOCKING);
		topology.connect(v2, v5, ResultPartitionType.BLOCKING);
		topology.connect(v3, v6, ResultPartitionType.BLOCKING);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		// when v1 fails, {v1,v4,v5} should be restarted
		HashSet<ExecutionVertexID> expectedResult = new HashSet<>();
		expectedResult.add(v1.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")));

		// when v2 fails, {v2,v4,v5} should be restarted
		expectedResult.clear();
		expectedResult.add(v2.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v2.getId(), new Exception("Test failure")));

		// when v3 fails, {v3,v6} should be restarted
		expectedResult.clear();
		expectedResult.add(v3.getId());
		expectedResult.add(v6.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v3.getId(), new Exception("Test failure")));

		// when v4 fails, {v4} should be restarted
		expectedResult.clear();
		expectedResult.add(v4.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v4.getId(), new Exception("Test failure")));

		// when v5 fails, {v5} should be restarted
		expectedResult.clear();
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v5.getId(), new Exception("Test failure")));

		// when v6 fails, {v6} should be restarted
		expectedResult.clear();
		expectedResult.add(v6.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v6.getId(), new Exception("Test failure")));
	}

	/**
	 * Tests for scenes that a task fails for data consumption error, in which case the
	 * region containing the failed task, the region containing the unavailable result partition
	 * and all their consumer regions should be restarted.
	 * <pre>
	 *     (v1) -+-> (v4)
	 *           x
	 *     (v2) -+-> (v5)
	 *
	 *     (v3) -+-> (v6)
	 *
	 *           ^
	 *           |
	 *       (blocking)
	 * </pre>
	 * Each vertex is in an individual region.
	 */
	@Test
	public void testRegionFailoverForDataConsumptionErrors() throws Exception {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex();

		topology.connect(v1, v4, ResultPartitionType.BLOCKING);
		topology.connect(v1, v5, ResultPartitionType.BLOCKING);
		topology.connect(v2, v4, ResultPartitionType.BLOCKING);
		topology.connect(v2, v5, ResultPartitionType.BLOCKING);
		topology.connect(v3, v6, ResultPartitionType.BLOCKING);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		// when v4 fails to consume data from v1, {v1,v4,v5} should be restarted
		HashSet<ExecutionVertexID> expectedResult = new HashSet<>();
		Iterator<TestingSchedulingResultPartition> v4InputEdgeIterator = v4.getConsumedResults().iterator();
		expectedResult.add(v1.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v4.getId(),
				new PartitionConnectionException(
					new ResultPartitionID(
						v4InputEdgeIterator.next().getId(),
						new ExecutionAttemptID()),
					new Exception("Test failure"))));

		// when v4 fails to consume data from v2, {v2,v4,v5} should be restarted
		expectedResult.clear();
		expectedResult.add(v2.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v4.getId(),
				new PartitionNotFoundException(
					new ResultPartitionID(
						v4InputEdgeIterator.next().getId(),
						new ExecutionAttemptID()))));

		// when v5 fails to consume data from v1, {v1,v4,v5} should be restarted
		expectedResult.clear();
		Iterator<TestingSchedulingResultPartition> v5InputEdgeIterator = v5.getConsumedResults().iterator();
		expectedResult.add(v1.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v5.getId(),
				new PartitionConnectionException(
					new ResultPartitionID(
						v5InputEdgeIterator.next().getId(),
						new ExecutionAttemptID()),
					new Exception("Test failure"))));

		// when v5 fails to consume data from v2, {v2,v4,v5} should be restarted
		expectedResult.clear();
		expectedResult.add(v2.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v5.getId(),
				new PartitionNotFoundException(
					new ResultPartitionID(
						v5InputEdgeIterator.next().getId(),
						new ExecutionAttemptID()))));

		// when v6 fails to consume data from v3, {v3,v6} should be restarted
		expectedResult.clear();
		Iterator<TestingSchedulingResultPartition> v6InputEdgeIterator = v6.getConsumedResults().iterator();
		expectedResult.add(v3.getId());
		expectedResult.add(v6.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v6.getId(),
				new PartitionConnectionException(
					new ResultPartitionID(
						v6InputEdgeIterator.next().getId(),
						new ExecutionAttemptID()),
					new Exception("Test failure"))));
	}

	/**
	 * Tests to verify region failover results regarding different input result partition availability combinations.
	 * <pre>
	 *     (v1) --rp1--\
	 *                 (v3)
	 *     (v2) --rp2--/
	 *
	 *             ^
	 *             |
	 *         (blocking)
	 * </pre>
	 * Each vertex is in an individual region.
	 * rp1, rp2 are result partitions.
	 */
	@Test
	public void testRegionFailoverForVariousResultPartitionAvailabilityCombinations() throws Exception {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();

		topology.connect(v1, v3, ResultPartitionType.BLOCKING);
		topology.connect(v2, v3, ResultPartitionType.BLOCKING);

		TestResultPartitionAvailabilityChecker availabilityChecker = new TestResultPartitionAvailabilityChecker();
		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology, availabilityChecker);

		IntermediateResultPartitionID rp1ID = v1.getProducedResults().iterator().next().getId();
		IntermediateResultPartitionID rp2ID = v2.getProducedResults().iterator().next().getId();

		// -------------------------------------------------
		// Combination1: (rp1 == available, rp == available)
		// -------------------------------------------------
		availabilityChecker.failedPartitions.clear();

		// when v1 fails, {v1,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v3.getId()));

		// when v2 fails, {v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v2.getId(), new Exception("Test failure")),
			containsInAnyOrder(v2.getId(), v3.getId()));

		// when v3 fails, {v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v3.getId(), new Exception("Test failure")),
			containsInAnyOrder(v3.getId()));

		// -------------------------------------------------
		// Combination2: (rp1 == unavailable, rp == available)
		// -------------------------------------------------
		availabilityChecker.failedPartitions.clear();
		availabilityChecker.markResultPartitionFailed(rp1ID);

		// when v1 fails, {v1,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v3.getId()));

		// when v2 fails, {v1,v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v2.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v2.getId(), v3.getId()));

		// when v3 fails, {v1,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v3.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v3.getId()));

		// -------------------------------------------------
		// Combination3: (rp1 == available, rp == unavailable)
		// -------------------------------------------------
		availabilityChecker.failedPartitions.clear();
		availabilityChecker.markResultPartitionFailed(rp2ID);

		// when v1 fails, {v1,v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v2.getId(), v3.getId()));

		// when v2 fails, {v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v2.getId(), new Exception("Test failure")),
			containsInAnyOrder(v2.getId(), v3.getId()));

		// when v3 fails, {v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v3.getId(), new Exception("Test failure")),
			containsInAnyOrder(v2.getId(), v3.getId()));

		// -------------------------------------------------
		// Combination4: (rp1 == unavailable, rp == unavailable)
		// -------------------------------------------------
		availabilityChecker.failedPartitions.clear();
		availabilityChecker.markResultPartitionFailed(rp1ID);
		availabilityChecker.markResultPartitionFailed(rp2ID);

		// when v1 fails, {v1,v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v2.getId(), v3.getId()));

		// when v2 fails, {v1,v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v2.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v2.getId(), v3.getId()));

		// when v3 fails, {v1,v2,v3} should be restarted
		assertThat(
			strategy.getTasksNeedingRestart(v3.getId(), new Exception("Test failure")),
			containsInAnyOrder(v1.getId(), v2.getId(), v3.getId()));
	}

	/**
	 * Tests region failover scenes for topology with multiple vertices.
	 * <pre>
	 *     (v1) ---> (v2) --|--> (v3) ---> (v4) --|--> (v5) ---> (v6)
	 *
	 *           ^          ^          ^          ^          ^
	 *           |          |          |          |          |
	 *     (pipelined) (blocking) (pipelined) (blocking) (pipelined)
	 * </pre>
	 * Component 1: 1,2; component 2: 3,4; component 3: 5,6
	 */
	@Test
	public void testRegionFailoverForMultipleVerticesRegions() throws Exception {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex();

		topology.connect(v1, v2, ResultPartitionType.PIPELINED);
		topology.connect(v2, v3, ResultPartitionType.BLOCKING);
		topology.connect(v3, v4, ResultPartitionType.PIPELINED);
		topology.connect(v4, v5, ResultPartitionType.BLOCKING);
		topology.connect(v5, v6, ResultPartitionType.PIPELINED);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		// when v3 fails due to internal error, {v3,v4,v5,v6} should be restarted
		HashSet<ExecutionVertexID> expectedResult = new HashSet<>();
		expectedResult.add(v3.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		expectedResult.add(v6.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v3.getId(), new Exception("Test failure")));

		// when v3 fails to consume from v2, {v1,v2,v3,v4,v5,v6} should be restarted
		expectedResult.clear();
		expectedResult.add(v1.getId());
		expectedResult.add(v2.getId());
		expectedResult.add(v3.getId());
		expectedResult.add(v4.getId());
		expectedResult.add(v5.getId());
		expectedResult.add(v6.getId());
		assertEquals(expectedResult,
			strategy.getTasksNeedingRestart(v3.getId(),
				new PartitionConnectionException(
					new ResultPartitionID(
						v3.getConsumedResults().iterator().next().getId(),
						new ExecutionAttemptID()),
					new Exception("Test failure"))));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static class TestResultPartitionAvailabilityChecker implements ResultPartitionAvailabilityChecker {

		private final HashSet<IntermediateResultPartitionID> failedPartitions;

		public TestResultPartitionAvailabilityChecker() {
			this.failedPartitions = new HashSet<>();
		}

		@Override
		public boolean isAvailable(IntermediateResultPartitionID resultPartitionID) {
			return !failedPartitions.contains(resultPartitionID);
		}

		public void markResultPartitionFailed(IntermediateResultPartitionID resultPartitionID) {
			failedPartitions.add(resultPartitionID);
		}

		public void removeResultPartitionFromFailedState(IntermediateResultPartitionID resultPartitionID) {
			failedPartitions.remove(resultPartitionID);
		}
	}
}
