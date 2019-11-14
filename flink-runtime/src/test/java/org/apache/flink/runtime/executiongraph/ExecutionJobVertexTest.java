/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import org.junit.Assert;
import org.junit.Test;

public class ExecutionJobVertexTest {

	private static final int NOT_CONFIGURED = -1;

	@Test
	public void testMaxParallelismDefaulting() throws Exception {

		// default minimum
		ExecutionJobVertex executionJobVertex = createExecutionJobVertex(1, NOT_CONFIGURED);
		Assert.assertEquals(128, executionJobVertex.getMaxParallelism());

		// test round up part 1
		executionJobVertex = createExecutionJobVertex(171, NOT_CONFIGURED);
		Assert.assertEquals(256, executionJobVertex.getMaxParallelism());

		// test round up part 2
		executionJobVertex = createExecutionJobVertex(172, NOT_CONFIGURED);
		Assert.assertEquals(512, executionJobVertex.getMaxParallelism());

		// test round up limit
		executionJobVertex = createExecutionJobVertex(1 << 15, NOT_CONFIGURED);
		Assert.assertEquals(1 << 15, executionJobVertex.getMaxParallelism());

		// test upper bound
		try {
			executionJobVertex = createExecutionJobVertex(1 + (1 << 15), NOT_CONFIGURED);
			executionJobVertex.getMaxParallelism();
			Assert.fail();
		} catch (IllegalArgumentException ignore) {
		}

		// parallelism must be smaller than the max parallelism
		try {
			createExecutionJobVertex(172, 4);
			Assert.fail("We should not be able to create an ExecutionJobVertex which " +
				"has a smaller max parallelism than parallelism.");
		} catch (JobException ignored) {
			// expected
		}


		// test configured / trumps computed default
		executionJobVertex = createExecutionJobVertex(4, 1 << 15);
		Assert.assertEquals(1 << 15, executionJobVertex.getMaxParallelism());

		// test upper bound configured
		try {
			executionJobVertex = createExecutionJobVertex(4, 1 + (1 << 15));
			Assert.fail(String.valueOf(executionJobVertex.getMaxParallelism()));
		} catch (IllegalArgumentException ignore) {
		}

		// test lower bound configured
		try {
			executionJobVertex = createExecutionJobVertex(4, 0);
			Assert.fail(String.valueOf(executionJobVertex.getMaxParallelism()));
		} catch (IllegalArgumentException ignore) {
		}

		// test override trumps test configured 2
		executionJobVertex = createExecutionJobVertex(4, NOT_CONFIGURED);
		executionJobVertex.setMaxParallelism(7);
		Assert.assertEquals(7, executionJobVertex.getMaxParallelism());

		// test lower bound with derived value
		executionJobVertex = createExecutionJobVertex(4, NOT_CONFIGURED);
		try {
			executionJobVertex.setMaxParallelism(0);
			Assert.fail(String.valueOf(executionJobVertex.getMaxParallelism()));
		} catch (IllegalArgumentException ignore) {
		}

		// test upper bound with derived value
		executionJobVertex = createExecutionJobVertex(4, NOT_CONFIGURED);
		try {
			executionJobVertex.setMaxParallelism(1 + (1 << 15));
			Assert.fail(String.valueOf(executionJobVertex.getMaxParallelism()));
		} catch (IllegalArgumentException ignore) {
		}

		// test complain on setting derived value in presence of a configured value
		executionJobVertex = createExecutionJobVertex(4, 16);
		try {
			executionJobVertex.setMaxParallelism(7);
			Assert.fail(String.valueOf(executionJobVertex.getMaxParallelism()));
		} catch (IllegalStateException ignore) {
		}

	}

	//------------------------------------------------------------------------------------------------------

	public static ExecutionJobVertex createExecutionJobVertex(
			int parallelism,
			int preconfiguredMaxParallelism) throws JobException, JobExecutionException {
		JobVertex jobVertex = new JobVertex("testVertex");
		jobVertex.setInvokableClass(AbstractInvokable.class);
		jobVertex.setParallelism(parallelism);

		if (NOT_CONFIGURED != preconfiguredMaxParallelism) {
			jobVertex.setMaxParallelism(preconfiguredMaxParallelism);
		}

		ExecutionGraph executionGraph = createExecutionGraph();
		return new ExecutionJobVertex(executionGraph, jobVertex, 1, Time.seconds(10));
	}

	private static ExecutionGraph createExecutionGraph() throws JobException, JobExecutionException {
		final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
			.newBuilder()
			.setFutureExecutor(new DirectScheduledExecutorService())
			.build();
		executionGraph.transitionToRunning();
		return executionGraph;
	}
}
