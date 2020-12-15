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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ExecutionGraphVariousFailuesTest extends TestLogger {

	/**
	 * Tests that a failing scheduleOrUpdateConsumers call with a non-existing execution attempt
	 * id, will not fail the execution graph.
	 */
	@Test
	public void testFailingScheduleOrUpdateConsumers() throws Exception {
		final SchedulerBase scheduler = SchedulerTestingUtils.newSchedulerBuilder(new JobGraph()).build();
		scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();

		final ExecutionGraph eg = scheduler.getExecutionGraph();

		assertEquals(JobStatus.RUNNING, eg.getState());
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		IntermediateResultPartitionID intermediateResultPartitionId = new IntermediateResultPartitionID();
		ExecutionAttemptID producerId = new ExecutionAttemptID();
		ResultPartitionID resultPartitionId = new ResultPartitionID(intermediateResultPartitionId, producerId);

		// The execution attempt id does not exist and thus the scheduleOrUpdateConsumers call
		// should fail

		try {
			scheduler.scheduleOrUpdateConsumers(resultPartitionId);
			fail("Expected ExecutionGraphException.");
		} catch (RuntimeException e) {
			// we've expected this exception to occur
			assertThat(e.getCause(), instanceOf(ExecutionGraphException.class));
		}

		assertEquals(JobStatus.RUNNING, eg.getState());
	}
}
