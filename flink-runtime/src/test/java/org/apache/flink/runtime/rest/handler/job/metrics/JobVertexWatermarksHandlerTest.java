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
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link JobVertexWatermarksHandler}.
 */
public class JobVertexWatermarksHandlerTest {

	private static final JobID TEST_JOB_ID = new JobID();

	private static final JobVertexID TEST_VERTEX_ID = new JobVertexID();

	private MetricFetcher metricFetcher;
	private MetricStore.TaskMetricStore taskMetricStore;
	private JobVertexWatermarksHandler watermarkHandler;
	private HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request;
	private AccessExecutionJobVertex vertex;

	@Before
	public void before() throws Exception {
		taskMetricStore = Mockito.mock(MetricStore.TaskMetricStore.class);

		MetricStore metricStore = Mockito.mock(MetricStore.class);
		Mockito.when(metricStore.getTaskMetricStore(TEST_JOB_ID.toString(), TEST_VERTEX_ID.toString()))
			.thenReturn(taskMetricStore);

		metricFetcher = Mockito.mock(MetricFetcher.class);
		Mockito.when(metricFetcher.getMetricStore()).thenReturn(metricStore);

		watermarkHandler = new JobVertexWatermarksHandler(
			Mockito.mock(LeaderGatewayRetriever.class),
			Time.seconds(1),
			Collections.emptyMap(),
			metricFetcher,
			Mockito.mock(ExecutionGraphCache.class),
			Mockito.mock(Executor.class));

		final Map<String, String> pathParameters = new HashMap<>();
		pathParameters.put(JobIDPathParameter.KEY, TEST_JOB_ID.toString());
		pathParameters.put(JobVertexIdPathParameter.KEY, TEST_VERTEX_ID.toString());

		request = new HandlerRequest<>(EmptyRequestBody.getInstance(), new JobVertexMessageParameters(),
				pathParameters, Collections.emptyMap());

		vertex = Mockito.mock(AccessExecutionJobVertex.class);
		Mockito.when(vertex.getJobVertexId()).thenReturn(TEST_VERTEX_ID);

		AccessExecutionVertex firstTask = Mockito.mock(AccessExecutionVertex.class);
		AccessExecutionVertex secondTask = Mockito.mock(AccessExecutionVertex.class);
		Mockito.when(firstTask.getParallelSubtaskIndex()).thenReturn(0);
		Mockito.when(secondTask.getParallelSubtaskIndex()).thenReturn(1);

		AccessExecutionVertex[] accessExecutionVertices = {firstTask, secondTask};
		Mockito.when(vertex.getTaskVertices()).thenReturn(accessExecutionVertices);
	}

	@After
	public void after() {
		Mockito.verify(metricFetcher).update();
	}

	@Test
	public void testWatermarksRetrieval() throws Exception {
		Mockito.when(taskMetricStore.getMetric("0.currentInputWatermark")).thenReturn("23");
		Mockito.when(taskMetricStore.getMetric("1.currentInputWatermark")).thenReturn("42");

		MetricCollectionResponseBody response = watermarkHandler.handleRequest(request, vertex);

		assertThat(response.getMetrics(),
			containsInAnyOrder(
				new MetricMatcher("0.currentInputWatermark", "23"),
				new MetricMatcher("1.currentInputWatermark", "42")));
	}

	@Test
	public void testPartialWatermarksAvailable() throws Exception {
		Mockito.when(taskMetricStore.getMetric("0.currentInputWatermark")).thenReturn("23");
		Mockito.when(taskMetricStore.getMetric("1.currentInputWatermark")).thenReturn(null);

		MetricCollectionResponseBody response = watermarkHandler.handleRequest(request, vertex);

		assertThat(response.getMetrics(),
			contains(
				new MetricMatcher("0.currentInputWatermark", "23")));
	}

	@Test
	public void testNoWatermarksAvailable() throws Exception {
		Mockito.when(taskMetricStore.getMetric("0.currentInputWatermark")).thenReturn(null);
		Mockito.when(taskMetricStore.getMetric("1.currentInputWatermark")).thenReturn(null);

		MetricCollectionResponseBody response = watermarkHandler.handleRequest(request, vertex);

		assertThat(response.getMetrics(), is(empty()));
	}

	private static class MetricMatcher extends BaseMatcher<Metric> {

		private String id;
		@Nullable private String value;

		MetricMatcher(String id, @Nullable String value) {
			this.id = id;
			this.value = value;
		}

		@Override
		public boolean matches(Object o) {
			if (!(o instanceof Metric)) {
				return false;
			}
			Metric actual = (Metric) o;
			return actual.getId().equals(id) && Objects.equals(value, actual.getValue());
		}

		@Override
		public void describeTo(Description description) {
			description.appendValue(new Metric(id, value));
		}
	}
}
