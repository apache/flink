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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link SubtaskCurrentAttemptDetailsHandler}.
 */
public class SubtaskCurrentAttemptDetailsHandlerTest extends TestLogger {

	@SuppressWarnings("unchecked")
	@Test
	public void testHandleRequest() throws Exception {

		// Prepare the execution graph.
		final JobID jobID = new JobID();
		final JobVertexID jobVertexID = new JobVertexID();

		// The testing subtask.
		final int subtaskIndex = 1;
		final int attempt = 2;
		final ExecutionState expectedState = ExecutionState.FINISHED;
		final long deployingTs = System.currentTimeMillis() - 1024;
		final long finishedTs = System.currentTimeMillis();

		// Mock execution since instancing a Execution is too complicated.
		// And mocking is much shorter than implementing a fake class implemented the AccessExecution interface.
		final Execution execution = mock(Execution.class);
		final ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 9527);

		when(executionVertex.getCurrentExecutionAttempt()).thenReturn(execution);
		when(execution.getState()).thenReturn(expectedState);
		when(execution.getAssignedResourceLocation()).thenReturn(taskManagerLocation);
		when(execution.getStateTimestamp(ExecutionState.DEPLOYING)).thenReturn(deployingTs);
		when(execution.getStateTimestamp(expectedState)).thenReturn(finishedTs);
		when(execution.getParallelSubtaskIndex()).thenReturn(subtaskIndex);
		when(execution.getAttemptNumber()).thenReturn(attempt);

		final long bytesInLocal = 1;
		final long bytesInRemote = 2;
		final long bytesOut = 10;
		final long recordsIn = 20;
		final long recordsOut = 30;

		when(execution.getIOMetrics()).thenReturn(
			new IOMetrics(
				bytesInLocal,
				bytesInRemote,
				bytesOut,
				recordsIn,
				recordsOut,
				0,
				0,
				0,
				0,
				0));

		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final SubtaskCurrentAttemptDetailsHandler handler = new SubtaskCurrentAttemptDetailsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			mock(GatewayRetriever.class),
			Time.milliseconds(100),
			restHandlerConfiguration.getResponseHeaders(),
			null,
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			mock(Executor.class),
			mock(MetricFetcher.class)
		);

		final HandlerRequest<EmptyRequestBody, SubtaskMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskMessageParameters(),
			new HashMap<String, String>() {{
				put(JobIDPathParameter.KEY, jobID.toString());
				put(JobVertexIdPathParameter.KEY, jobVertexID.toString());
			}},
			Collections.emptyMap()
		);

		// Handle request.
		final SubtaskExecutionAttemptDetailsInfo detailsInfo = handler.handleRequest(request, executionVertex);

		// Verify
		final IOMetricsInfo ioMetricsInfo = new IOMetricsInfo(
			bytesInLocal + bytesInRemote,
			true,
			bytesOut,
			true,
			recordsIn,
			true,
			recordsOut,
			true
		);

		final SubtaskExecutionAttemptDetailsInfo expectedDetailsInfo = new SubtaskExecutionAttemptDetailsInfo(
			subtaskIndex,
			expectedState,
			attempt,
			taskManagerLocation.getHostname(),
			deployingTs,
			finishedTs,
			finishedTs - deployingTs,
			ioMetricsInfo
		);

		assertEquals(expectedDetailsInfo, detailsInfo);
	}
}
