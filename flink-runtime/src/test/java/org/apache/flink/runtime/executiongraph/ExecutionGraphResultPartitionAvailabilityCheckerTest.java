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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.io.network.partition.PartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ExecutionGraphResultPartitionAvailabilityChecker}.
 */
public class ExecutionGraphResultPartitionAvailabilityCheckerTest extends TestLogger {

	@Test
	public void testPartitionAvailabilityCheck() throws Exception {

		final TestingPartitionTracker partitionTracker = new TestingPartitionTracker();
		final ExecutionGraph eg = createExecutionGraph(partitionTracker);
		final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker = eg.getResultPartitionAvailabilityChecker();

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();

		final IntermediateResultPartitionID irp1ID = ev11.getProducedPartitions().keySet().iterator().next();
		final IntermediateResultPartitionID irp2ID = ev12.getProducedPartitions().keySet().iterator().next();

		final Map<IntermediateResultPartitionID, Boolean> expectedAvailability =
			new HashMap<IntermediateResultPartitionID, Boolean>() {{
				put(irp1ID, true);
				put(irp2ID, false);
			}};

		// let the partition tracker respect the expected availability result
		partitionTracker.setIsPartitionTrackedFunction(rpID -> expectedAvailability.get(rpID.getPartitionId()));

		for (IntermediateResultPartitionID irpID : expectedAvailability.keySet()) {
			assertEquals(expectedAvailability.get(irpID), resultPartitionAvailabilityChecker.isAvailable(irpID));
		}
	}

	// ------------------------------- Test Utils -----------------------------------------

	/**
	 * Creating an execution graph as below.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *        (blocking)
	 * </pre>
	 */
	private ExecutionGraph createExecutionGraph(final PartitionTracker partitionTracker) throws Exception {

		final JobVertex v1 = new JobVertex("vertex1");
		v1.setInvokableClass(NoOpInvokable.class);
		v1.setParallelism(2);

		final JobVertex v2 = new JobVertex("vertex2");
		v2.setInvokableClass(NoOpInvokable.class);
		v2.setParallelism(2);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(new JobID(), "testjob", v1, v2);

		final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(jobGraph)
			.setPartitionTracker(partitionTracker)
			.build();

		return eg;
	}
}
