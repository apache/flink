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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
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
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link SubtaskExecutionAttemptDetailsHandler}.
 */
public class SubtaskExecutionAttemptDetailsHandlerTest extends TestLogger {

	@Test
	public void testHandleRequest() throws Exception {

		final JobID jobID = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();

		// The testing subtask.
		final int subtaskIndex = 1;
		final ExecutionState expectedState = ExecutionState.FINISHED;
		final int attempt = 0;

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

		final ArchivedExecutionJobVertex archivedExecutionJobVertex = new ArchivedExecutionJobVertex(
			new ArchivedExecutionVertex[]{
				null, // the first subtask won't be queried
				new ArchivedExecutionVertex(
					subtaskIndex,
					"test task",
					new ArchivedExecution(
						emptyAccumulators,
						ioMetrics,
						new ExecutionAttemptID(),
						attempt,
						expectedState,
						null,
						null,
						null,
						subtaskIndex,
						new long[ExecutionState.values().length]),
					new EvictingBoundedList<>(0)
				)
			},
			jobVertexId,
			"test",
			1,
			1,
			emptyAccumulators);

		// Change some fields so we can make it different from other sub tasks.
		final MetricFetcher<?> metricFetcher = new MetricFetcher<>(
			() -> null,
			path -> null,
			TestingUtils.defaultExecutor(),
			Time.milliseconds(1000L));

		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final SubtaskExecutionAttemptDetailsHandler handler = new SubtaskExecutionAttemptDetailsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			() -> null,
			Time.milliseconds(100L),
			Collections.emptyMap(),
			SubtaskExecutionAttemptDetailsHeaders.getInstance(),
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			TestingUtils.defaultExecutor(),
			metricFetcher);

		final HashMap<String, String> receivedPathParameters = new HashMap<>(4);
		receivedPathParameters.put(JobIDPathParameter.KEY, jobID.toString());
		receivedPathParameters.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
		receivedPathParameters.put(SubtaskIndexPathParameter.KEY, Integer.toString(subtaskIndex));
		receivedPathParameters.put(SubtaskAttemptPathParameter.KEY, Integer.toString(attempt));

		final HandlerRequest<EmptyRequestBody, SubtaskAttemptMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskAttemptMessageParameters(),
			receivedPathParameters,
			Collections.emptyMap()
		);

		// Handle request.
		final SubtaskExecutionAttemptDetailsInfo detailsInfo = handler.handleRequest(
			request,
			archivedExecutionJobVertex);

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
			"(unassigned)",
			-1L,
			0L,
			-1L,
			ioMetricsInfo
		);

		assertEquals(expectedDetailsInfo, detailsInfo);
	}
}
