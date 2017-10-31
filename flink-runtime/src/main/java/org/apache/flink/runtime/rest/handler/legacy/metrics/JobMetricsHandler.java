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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.LegacyRestHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.metrics.MetricEntry;
import org.apache.flink.runtime.rest.messages.metrics.MetricMessageParameters;
import org.apache.flink.runtime.rest.messages.metrics.MetricNameParameter;
import org.apache.flink.runtime.rest.messages.metrics.MetricsOverview;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Request handler that returns for a given job a list of all available metrics or the values for a set of metrics.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code {"available": [ { "name" : "X", "id" : "X" } ] } }
 *
 * <p>If the query parameters do contain a "get" parameter, a comma-separated list of metric names is expected as a value.
 * {@code /metrics?get=X,Y}
 *
 * The handler will then return a list containing the values of the requested metrics.
 * {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y", "value" : "T" } ] }
 */
public class JobMetricsHandler extends AbstractMetricsHandler
		implements LegacyRestHandler<DispatcherGateway, MetricsOverview, MetricMessageParameters> {

	public static final String PARAMETER_JOB_ID = "jobid";

	private static final String JOB_METRICS_REST_PATH = "/jobs/:jobid/metrics";

	public JobMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor, fetcher);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_METRICS_REST_PATH};
	}

	@Override
	protected Map<String, String> getMapFor(Map<String, String> pathParams, MetricStore metrics) {
		MetricStore.ComponentMetricStore job = metrics.getJobMetricStore(pathParams.get(PARAMETER_JOB_ID));
		return job != null ? job.metrics : null;
	}

	@Override
	public CompletableFuture<MetricsOverview> handleRequest(HandlerRequest<EmptyRequestBody, MetricMessageParameters> request, DispatcherGateway gateway) {
		return CompletableFuture.supplyAsync(
				() -> {
					fetcher.update();
					JobID jobID = request.getPathParameter(JobIDPathParameter.class);
					List<String> requestedMetrics = request.getQueryParameter(MetricNameParameter.class);
					return getMetricsOverview(jobID, requestedMetrics);
				},
				executor);
	}

	protected MetricsOverview getMetricsOverview(JobID jobID, List<String> requestedMetrics) {
		Map<String, String> metricsMap = getMetricsMapByJobId(jobID, fetcher.getMetricStore());
		if (metricsMap == null) {
			return new MetricsOverview();
		}

		if (requestedMetrics == null || requestedMetrics.isEmpty()) {
			return new MetricsOverview(
					metricsMap.entrySet().stream()
							.map(e -> new MetricEntry(e.getKey(), e.getValue()))
							.collect(Collectors.toList()));
		} else {
			return new MetricsOverview(
					requestedMetrics.stream()
							.filter(e -> metricsMap.get(e) != null)
							.map(e -> new MetricEntry(e, metricsMap.get(e)))
							.collect(Collectors.toList()));
		}
	}

	private Map<String, String> getMetricsMapByJobId(JobID jobID, MetricStore metrics) {
		MetricStore.ComponentMetricStore metricStore = metrics.getJobMetricStore(jobID);
		return metricStore != null ? metricStore.metrics : null;
	}
}
