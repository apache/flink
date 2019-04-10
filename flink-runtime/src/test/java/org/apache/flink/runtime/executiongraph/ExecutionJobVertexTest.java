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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

	/**
	 * Tests for FLINK-12053.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-12053">FLINK-12053</a>
	 *
	 * Vertex#0	Vertex#1
	 * 	  PW\	/ATA
	 * 		 \ /
	 * 		Vertex#2
	 * 		  |PW
	 * 		  |
	 * 		Vertex#3
	 */
	@Test
	public void testAdaptivelyAdjustParallelism() throws Exception {
		JobVertex[] vertices = new JobVertex[4];
		for (int i = 0; i < vertices.length; i++) {
			vertices[i] = new JobVertex("Vertex#" + i);
			vertices[i].setParallelism(5);
			vertices[i].setInvokableClass(NoOpInvokable.class);
		}
		vertices[1].setParallelism(1);

		vertices[2].connectNewDataSetAsInput(vertices[0], DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
		vertices[2].connectNewDataSetAsInput(vertices[1], DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		vertices[3].connectNewDataSetAsInput(vertices[2], DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		final CompletableFuture<LogicalSlot> slotFuture = new CompletableFuture<>();
		slotProvider.addSlot(vertices[1].getID(), 0, slotFuture);
		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ENABLE_ADAPTIVE_PARALLELISM.key(), "true");

		final ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(new JobID(), slotProvider,
			new NoRestartStrategy(), configuration, vertices);

		ExecutionJobVertex[] executionJobVertices = new ExecutionJobVertex[4];
		for (int i = 0; i < executionJobVertices.length; i++) {
			executionJobVertices[i] = executionGraph.getJobVertex(vertices[i].getID());
		}
		executionJobVertices[2].computeAdaptiveParallelism();
		Assert.assertEquals(5, executionJobVertices[0].getParallelism());
		Assert.assertEquals(1, executionJobVertices[2].getParallelism());
		Assert.assertEquals(1, executionJobVertices[3].getParallelism());
	}

	//------------------------------------------------------------------------------------------------------

	private static ExecutionJobVertex createExecutionJobVertex(
			int parallelism,
			int preconfiguredMaxParallelism) throws JobException {

		JobVertex jobVertex = new JobVertex("testVertex");
		jobVertex.setInvokableClass(AbstractInvokable.class);
		jobVertex.setParallelism(parallelism);

		if (NOT_CONFIGURED != preconfiguredMaxParallelism) {
			jobVertex.setMaxParallelism(preconfiguredMaxParallelism);
		}

		ExecutionGraph executionGraphMock = mock(ExecutionGraph.class);
		when(executionGraphMock.getFutureExecutor()).thenReturn(Executors.directExecutor());
		ExecutionJobVertex executionJobVertex =
				new ExecutionJobVertex(executionGraphMock, jobVertex, 1, Time.seconds(10));

		return executionJobVertex;
	}
}
