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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link JobVertexMetricsHandler}.
 */
public class JobVertexMetricsHandlerTest extends TestLogger {

	private static final String TEST_METRIC_NAME = "test_counter";

	private static final int TEST_METRIC_VALUE = 1000;

	@Mock
	private MetricFetcher mockMetricFetcher;

	@Mock
	private DispatcherGateway mockDispatcherGateway;


	private JobVertexMetricsHandler jobVertexMetricsHandler;

	private Map<String, String> pathParameters;

	private int subtaskIndex = 1;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);

		final String jobId = new JobID().toString();
		final String vertexId = new JobVertexID().toString();

		final MetricStore metricStore = new MetricStore();
		metricStore.add(new MetricDump.CounterDump(
			new QueryScopeInfo.TaskQueryScopeInfo(jobId, vertexId, subtaskIndex),
			TEST_METRIC_NAME,
			TEST_METRIC_VALUE));

		when(mockMetricFetcher.getMetricStore()).thenReturn(metricStore);

		jobVertexMetricsHandler = new JobVertexMetricsHandler(
			CompletableFuture.completedFuture("localhost:1234"),
			new GatewayRetriever<DispatcherGateway>() {
				@Override
				public CompletableFuture<DispatcherGateway> getFuture() {
					return CompletableFuture.completedFuture(mockDispatcherGateway);
				}
			},
			Time.milliseconds(50),
			Collections.emptyMap(),
			mockMetricFetcher);

		pathParameters = new HashMap<>();
		pathParameters.put("jobid", jobId);
		pathParameters.put("vertexid", vertexId);
	}

	@Test
	public void testGetMetricsFromTaskMetricStore() throws Exception {
		final CompletableFuture<MetricCollectionResponseBody> completableFuture =
			jobVertexMetricsHandler.handleRequest(
				new HandlerRequest<>(
					EmptyRequestBody.getInstance(),
					new JobVertexMetricsMessageParameters(),
					pathParameters,
					Collections.emptyMap()),
				mockDispatcherGateway);

		assertTrue(completableFuture.isDone());

		final MetricCollectionResponseBody metricCollectionResponseBody = completableFuture.get();
		assertThat(metricCollectionResponseBody.getMetrics(), hasSize(1));

		final Metric metric = metricCollectionResponseBody.getMetrics().iterator().next();
		assertThat(metric.getId(), equalTo(subtaskIndex + "." + TEST_METRIC_NAME));
	}

}
