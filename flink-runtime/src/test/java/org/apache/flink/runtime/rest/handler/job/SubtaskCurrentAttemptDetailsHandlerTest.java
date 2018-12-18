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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskCurrentAttemptDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link SubtaskCurrentAttemptDetailsHandler}.
 */
public class SubtaskCurrentAttemptDetailsHandlerTest extends TestLogger {

	@Test
	public void testHandleRequest() throws Exception {

		// Prepare the execution graph.
		final JobID jobID = new JobID();
		final JobVertexID jobVertexID = new JobVertexID();

		// The testing subtask.
		final long deployingTs = System.currentTimeMillis() - 1024;
		final long finishedTs = System.currentTimeMillis();

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

		final long[] timestamps = new long[ExecutionState.values().length];
		timestamps[ExecutionState.DEPLOYING.ordinal()] = deployingTs;
		final ExecutionState expectedState = ExecutionState.FINISHED;

		timestamps[expectedState.ordinal()] = finishedTs;

		final LocalTaskManagerLocation assignedResourceLocation = new LocalTaskManagerLocation();
		final AllocationID allocationID = new AllocationID();

		final int subtaskIndex = 1;
		final int attempt = 2;
		final ArchivedExecution execution = new ArchivedExecution(
			new StringifiedAccumulatorResult[0],
			ioMetrics,
			new ExecutionAttemptID(),
			attempt,
			expectedState,
			null,
			assignedResourceLocation,
			allocationID,
			subtaskIndex,
			timestamps);

		final ArchivedExecutionVertex executionVertex = new ArchivedExecutionVertex(
			subtaskIndex,
			"Test archived execution vertex",
			execution,
			new EvictingBoundedList<>(0));

		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final MetricFetcher<?> metricFetcher = new MetricFetcher<>(
			() -> null,
			path -> null,
			TestingUtils.defaultExecutor(),
			Time.milliseconds(1000L));

		final SubtaskCurrentAttemptDetailsHandler handler = new SubtaskCurrentAttemptDetailsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			() -> null,
			Time.milliseconds(100),
			Collections.emptyMap(),
			SubtaskCurrentAttemptDetailsHeaders.getInstance(),
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			TestingUtils.defaultExecutor(),
			metricFetcher);

		final HashMap<String, String> receivedPathParameters = new HashMap<>(2);
		receivedPathParameters.put(JobIDPathParameter.KEY, jobID.toString());
		receivedPathParameters.put(JobVertexIdPathParameter.KEY, jobVertexID.toString());

		final HandlerRequest<EmptyRequestBody, SubtaskMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskMessageParameters(),
			receivedPathParameters,
			Collections.emptyMap());

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
			assignedResourceLocation.getHostname(),
			deployingTs,
			finishedTs,
			finishedTs - deployingTs,
			ioMetricsInfo
		);

		assertEquals(expectedDetailsInfo, detailsInfo);
	}
}
