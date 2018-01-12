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
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link SubtaskExecutionAttemptDetailsHandler}.
 */
public class SubtaskExecutionAttemptDetailsHandlerTest extends TestLogger {

	@SuppressWarnings("unchecked")
	@Test
	public void testHandleRequest() throws Exception {

		// Prepare the execution graph.
		final JobID jobID = new JobID();

		final ExecutionGraph executionGraph = new ExecutionGraph(
			new DummyJobInformation(jobID, "job name"),
			mock(ScheduledExecutorService.class),
			mock(Executor.class),
			Time.milliseconds(100),
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			mock(SlotProvider.class),
			ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance()
		);

		final JobVertex jobVertex = new JobVertex("MockVertex");
		jobVertex.setParallelism(128);
		jobVertex.setInvokableClass(AbstractInvokable.class);

		executionGraph.attachJobGraph(Collections.singletonList(jobVertex));

		// The testing subtask.
		final int subtaskIndex = 1;
		final ExecutionState expectedState = ExecutionState.SCHEDULED;

		// Change some fields so we can make it different from other sub tasks.
		final Execution execution = executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[subtaskIndex].getCurrentExecutionAttempt();
		Whitebox.setInternalState(execution, "state", expectedState);

		// Mock the metric fetcher.
		final MetricFetcher metricFetcher = mock(MetricFetcher.class);
		final MetricStore metricStore = mock(MetricStore.class);
		final MetricStore.ComponentMetricStore componentMetricStore = mock(MetricStore.ComponentMetricStore.class);

		final long bytesInLocal = 1;
		final long bytesInRemote = 2;
		final long bytesOut = 10;
		final long recordsIn = 20;
		final long recordsOut = 30;

		when(componentMetricStore.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL)).thenReturn(Long.toString(bytesInLocal));
		when(componentMetricStore.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE)).thenReturn(Long.toString(bytesInRemote));
		when(componentMetricStore.getMetric(MetricNames.IO_NUM_BYTES_OUT)).thenReturn(Long.toString(bytesOut));
		when(componentMetricStore.getMetric(MetricNames.IO_NUM_RECORDS_IN)).thenReturn(Long.toString(recordsIn));
		when(componentMetricStore.getMetric(MetricNames.IO_NUM_RECORDS_OUT)).thenReturn(Long.toString(recordsOut));

		when(metricStore.getSubtaskMetricStore(jobID.toString(), jobVertex.getID().toString(), subtaskIndex))
			.thenReturn(componentMetricStore);
		when(metricFetcher.getMetricStore()).thenReturn(metricStore);

		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final SubtaskExecutionAttemptDetailsHandler handler = new SubtaskExecutionAttemptDetailsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			mock(GatewayRetriever.class),
			Time.milliseconds(100),
			restHandlerConfiguration.getResponseHeaders(),
			null,
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			mock(Executor.class),
			metricFetcher
		);

		final int attempt = 0;

		final HandlerRequest<EmptyRequestBody, SubtaskAttemptMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskAttemptMessageParameters(),
			new HashMap<String, String>() {{
				put(JobIDPathParameter.KEY, jobID.toString());
				put(JobVertexIdPathParameter.KEY, jobVertex.getID().toString());
				put(SubtaskIndexPathParameter.KEY, Integer.toString(subtaskIndex));
				put(SubtaskAttemptPathParameter.KEY, Integer.toString(attempt));
			}},
			Collections.emptyMap()
		);

		// Handle request.
		final SubtaskExecutionAttemptDetailsInfo detailsInfo = handler.handleRequest(
			request,
			executionGraph.getJobVertex(jobVertex.getID()));

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
			-1,
			-1,
			-1,
			ioMetricsInfo
		);

		assertEquals(expectedDetailsInfo, detailsInfo);
	}
}
