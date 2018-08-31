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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AbstractMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractMetricsHandler}.
 */
public class AbstractMetricsHandlerTest extends TestLogger {

	private static final String TEST_METRIC_NAME = "test_counter";

	private static final int TEST_METRIC_VALUE = 1000;

	private static final String METRICS_FILTER_QUERY_PARAM = "get";

	@Mock
	private MetricFetcher mockMetricFetcher;

	@Mock
	private DispatcherGateway mockDispatcherGateway;

	private TestMetricsHandler testMetricsHandler;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);

		final MetricStore metricStore = new MetricStore();
		metricStore.add(new MetricDump.CounterDump(
			new QueryScopeInfo.JobManagerQueryScopeInfo(),
			TEST_METRIC_NAME,
			TEST_METRIC_VALUE));

		when(mockMetricFetcher.getMetricStore()).thenReturn(metricStore);

		testMetricsHandler = new TestMetricsHandler(
			CompletableFuture.completedFuture("localhost:1234"),
			new GatewayRetriever<DispatcherGateway>() {
				@Override
				public CompletableFuture<DispatcherGateway> getFuture() {
					return CompletableFuture.completedFuture(mockDispatcherGateway);
				}
			},
			Time.milliseconds(50),
			Collections.emptyMap(),
			new TestMetricsHeaders(),
			mockMetricFetcher);
	}

	@Test
	public void testListMetrics() throws Exception {
		final CompletableFuture<MetricCollectionResponseBody> completableFuture =
			testMetricsHandler.handleRequest(
				new HandlerRequest<>(
					EmptyRequestBody.getInstance(),
					new TestMessageParameters(),
					Collections.emptyMap(),
					Collections.emptyMap()),
				mockDispatcherGateway);

		assertTrue(completableFuture.isDone());

		final MetricCollectionResponseBody metricCollectionResponseBody = completableFuture.get();
		assertThat(metricCollectionResponseBody.getMetrics(), hasSize(1));

		final Metric metric = metricCollectionResponseBody.getMetrics().iterator().next();
		assertThat(metric.getId(), equalTo(TEST_METRIC_NAME));
		assertThat(metric.getValue(), equalTo(null));
	}

	@Test
	public void testReturnEmptyListIfNoComponentMetricStore() throws Exception {
		testMetricsHandler.returnComponentMetricStore = false;

		final CompletableFuture<MetricCollectionResponseBody> completableFuture =
			testMetricsHandler.handleRequest(
				new HandlerRequest<>(
					EmptyRequestBody.getInstance(),
					new TestMessageParameters(),
					Collections.emptyMap(),
					Collections.emptyMap()),
				mockDispatcherGateway);

		assertTrue(completableFuture.isDone());

		final MetricCollectionResponseBody metricCollectionResponseBody = completableFuture.get();
		assertThat(metricCollectionResponseBody.getMetrics(), empty());
	}

	@Test
	public void testGetMetrics() throws Exception {
		final CompletableFuture<MetricCollectionResponseBody> completableFuture =
			testMetricsHandler.handleRequest(
				new HandlerRequest<>(
					EmptyRequestBody.getInstance(),
					new TestMessageParameters(),
					Collections.emptyMap(),
					Collections.singletonMap(METRICS_FILTER_QUERY_PARAM, Collections.singletonList(TEST_METRIC_NAME))),
				mockDispatcherGateway);

		assertTrue(completableFuture.isDone());

		final MetricCollectionResponseBody metricCollectionResponseBody = completableFuture.get();
		assertThat(metricCollectionResponseBody.getMetrics(), hasSize(1));

		final Metric metric = metricCollectionResponseBody.getMetrics().iterator().next();
		assertThat(metric.getId(), equalTo(TEST_METRIC_NAME));
		assertThat(metric.getValue(), equalTo(Integer.toString(TEST_METRIC_VALUE)));
	}

	@Test
	public void testReturnEmptyListIfRequestedMetricIsUnknown() throws Exception {
		final CompletableFuture<MetricCollectionResponseBody> completableFuture =
			testMetricsHandler.handleRequest(
				new HandlerRequest<>(
					EmptyRequestBody.getInstance(),
					new TestMessageParameters(),
					Collections.emptyMap(),
					Collections.singletonMap(METRICS_FILTER_QUERY_PARAM, Collections.singletonList("unknown_metric"))),
				mockDispatcherGateway);

		assertTrue(completableFuture.isDone());

		final MetricCollectionResponseBody metricCollectionResponseBody = completableFuture.get();
		assertThat(metricCollectionResponseBody.getMetrics(), empty());
	}

	private static class TestMetricsHandler extends AbstractMetricsHandler<TestMessageParameters> {

		private boolean returnComponentMetricStore = true;

		private TestMetricsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MessageHeaders<EmptyRequestBody,
				MetricCollectionResponseBody,
				TestMessageParameters> messageHeaders,
			MetricFetcher metricFetcher) {

			super(localRestAddress, leaderRetriever, timeout, headers, messageHeaders, metricFetcher);
		}

		@Nullable
		@Override
		protected MetricStore.ComponentMetricStore getComponentMetricStore(
			HandlerRequest<EmptyRequestBody,
				TestMessageParameters> request,
			MetricStore metricStore) {
			return returnComponentMetricStore ? metricStore.getJobManager() : null;
		}
	}

	private static class TestMetricsHeaders extends
		AbstractMetricsHeaders<TestMessageParameters> {

		@Override
		public TestMessageParameters getUnresolvedMessageParameters() {
			return new TestMessageParameters();
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/";
		}

		@Override
		public String getDescription() {
			return "";
		}
	}

	private static class TestMessageParameters extends MessageParameters {

		@Override
		public Collection<MessagePathParameter<?>> getPathParameters() {
			return Collections.emptyList();
		}

		@Override
		public Collection<MessageQueryParameter<?>> getQueryParameters() {
			return Collections.singletonList(new MetricsFilterParameter());
		}
	}

}
