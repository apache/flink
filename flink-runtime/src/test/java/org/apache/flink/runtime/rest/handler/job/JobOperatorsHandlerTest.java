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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobOperators;
import org.apache.flink.runtime.rest.messages.JobOperatorsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;


/**
 * Tests of {@link JobOperatorsHandler}.
 */
public class JobOperatorsHandlerTest extends TestLogger {

	@Test
	public void testHandleRequest() throws Exception {

		// Prepare the execution graph.
		final JobID jobID = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();
		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final JobOperatorsHandler handler = new JobOperatorsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			() -> null,
			Time.milliseconds(100),
			Collections.emptyMap(),
			JobOperatorsHeaders.getInstance(),
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			TestingUtils.defaultExecutor());

		final HashMap<String, String> receivedPathParameters = new HashMap<>(1);
		receivedPathParameters.put(JobIDPathParameter.KEY, jobID.toString());

		final HandlerRequest<EmptyRequestBody, JobMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskMessageParameters(),
			receivedPathParameters,
			Collections.emptyMap());

		final StringifiedAccumulatorResult[] emptyAccumulators = new StringifiedAccumulatorResult[0];
		final long bytesInLocal = 1L;
		final long bytesInRemote = 2L;
		final long bytesOut = 10L;
		final long recordsIn = 20L;
		final long recordsOut = 30L;

		final IOMetrics ioMetrics = new IOMetrics(
			bytesInLocal,
			bytesInRemote,
			bytesOut,
			recordsIn,
			recordsOut,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0);
		final ExecutionState expectedState = ExecutionState.FINISHED;
		List<OperatorDescriptor> operatorDescriptors = new ArrayList<>();
		OperatorDescriptor  operatorDescriptor = new OperatorDescriptor("o1", new OperatorID());
		operatorDescriptors.add(operatorDescriptor);
		JobOperators.JobOperator testJobOperator = new JobOperators.JobOperator(jobVertexId,
			operatorDescriptor.getOperatorID(), operatorDescriptor.getOperatorName(), new ArrayList<>());
		List<JobOperators.JobOperator> testJobOperators = new ArrayList<>();
		testJobOperators.add(testJobOperator);
		JobOperators testJobOperatorsInfo = new JobOperators(testJobOperators);
		final ArchivedExecutionJobVertex archivedExecutionJobVertex = new ArchivedExecutionJobVertex(
			new ArchivedExecutionVertex[]{
				null, // the first subtask won't be queried
				new ArchivedExecutionVertex(
					1,
					"test task",
					new ArchivedExecution(
						emptyAccumulators,
						ioMetrics,
						new ExecutionAttemptID(),
						0,
						expectedState,
						null,
						null,
						null,
						1,
						new long[ExecutionState.values().length]),
					new EvictingBoundedList<>(0)
				)
			},
			jobVertexId,
			"test",
			1,
			1,
			emptyAccumulators,
			operatorDescriptors);
		final List<ArchivedExecutionJobVertex> verticesInCreationOrder = new ArrayList<>();
		verticesInCreationOrder.add(archivedExecutionJobVertex);
		final ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder().setJobID(jobID).setVerticesInCreationOrder(verticesInCreationOrder).build();
		// Handle request.
		final JobOperators operators = handler.handleRequest(request, executionGraph);
		assertEquals(testJobOperatorsInfo, operators);
	}
}
