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
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Request handler that returns for a given task a list of all available metrics or the values for a set of metrics.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code {"available": [ { "name" : "X", "id" : "X" } ] } }
 *
 * <p>If the query parameters do contain a "get" parameter, a comma-separated list of metric names is expected as a value.
 * {@code /metrics?get=X,Y}
 * The handler will then return a list containing the values of the requested metrics.
 * {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y", "value" : "T" } ] }
 *
 * @param <M> Type of the concrete {@link MessageParameters}
 */
public abstract class AbstractMetricsHandler<M extends MessageParameters> extends
	AbstractRestHandler<RestfulGateway, EmptyRequestBody, MetricCollectionResponseBody, M> {

	private final MetricFetcher metricFetcher;

	public AbstractMetricsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MessageHeaders<EmptyRequestBody, MetricCollectionResponseBody, M> messageHeaders,
			MetricFetcher metricFetcher) {
		super(leaderRetriever, timeout, headers, messageHeaders);
		this.metricFetcher = requireNonNull(metricFetcher, "metricFetcher must not be null");
	}

	@Override
	protected final CompletableFuture<MetricCollectionResponseBody> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody, M> request,
			@Nonnull RestfulGateway gateway) throws RestHandlerException {
		metricFetcher.update();

		final MetricStore.ComponentMetricStore componentMetricStore = getComponentMetricStore(
			request,
			metricFetcher.getMetricStore());

		if (componentMetricStore == null || componentMetricStore.metrics == null) {
			return CompletableFuture.completedFuture(
				new MetricCollectionResponseBody(Collections.emptyList()));
		}

		final Set<String> requestedMetrics = new HashSet<>(request.getQueryParameter(
			MetricsFilterParameter.class));

		if (requestedMetrics.isEmpty()) {
			return CompletableFuture.completedFuture(
				new MetricCollectionResponseBody(getAvailableMetrics(componentMetricStore)));
		} else {
			final List<Metric> metrics = getRequestedMetrics(componentMetricStore, requestedMetrics);
			return CompletableFuture.completedFuture(new MetricCollectionResponseBody(metrics));
		}
	}

	/**
	 * Returns the {@link MetricStore.ComponentMetricStore} that should be queried for metrics.
	 */
	@Nullable
	protected abstract MetricStore.ComponentMetricStore getComponentMetricStore(
		HandlerRequest<EmptyRequestBody, M> request,
		MetricStore metricStore);

	private static List<Metric> getAvailableMetrics(MetricStore.ComponentMetricStore componentMetricStore) {
		return componentMetricStore.metrics
			.keySet()
			.stream()
			.map(Metric::new)
			.collect(Collectors.toList());
	}

	private static List<Metric> getRequestedMetrics(
			MetricStore.ComponentMetricStore componentMetricStore,
			Set<String> requestedMetrics) throws RestHandlerException {

		final List<Metric> metrics = new ArrayList<>(requestedMetrics.size());
		for (final String requestedMetric : requestedMetrics) {
			final String value = componentMetricStore.getMetric(requestedMetric, null);
			if (value != null) {
				metrics.add(new Metric(requestedMetric, value));
			}
		}
		return metrics;
	}

}
