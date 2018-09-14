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

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJsonRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Abstract request handler that returns a list of all available metrics or the values for a set of metrics.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code [ { "id" : "X" } ] }
 *
 * <p>If the query parameters do contain a "get" parameter, a comma-separated list of metric names is expected as a value.
 * {@code /metrics?get=X,Y}
 * The handler will then return a list containing the values of the requested metrics.
 * {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y", "value" : "T" } ] }
 */
public abstract class AbstractMetricsHandler extends AbstractJsonRequestHandler {

	public static final String PARAMETER_METRICS = "get";

	private final MetricFetcher fetcher;

	public AbstractMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor);
		this.fetcher = Preconditions.checkNotNull(fetcher);
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.supplyAsync(
			() -> {
				fetcher.update();
				String requestedMetricsList = queryParams.get(PARAMETER_METRICS);
				try {
					return requestedMetricsList != null
						? getMetricsValues(pathParams, requestedMetricsList)
						: getAvailableMetricsList(pathParams);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not retrieve metrics.", e));
				}
			},
			executor);

	}

	/**
	 * Returns a Map containing the metrics belonging to the entity pointed to by the path parameters.
	 *
	 * @param pathParams REST path parameters
	 * @param metrics MetricStore containing all metrics
	 * @return Map containing metrics, or null if no metric exists
	 */
	protected abstract Map<String, String> getMapFor(Map<String, String> pathParams, MetricStore metrics);

	private String getMetricsValues(Map<String, String> pathParams, String requestedMetricsList) throws IOException {
		if (requestedMetricsList.isEmpty()) {
			/*
			 * The WebInterface doesn't check whether the list of available metrics was empty. This can lead to a
			 * request for which the "get" parameter is an empty string.
			 */
			return "";
		}
		Map<String, String> metrics = getMapFor(pathParams, fetcher.getMetricStore());
		if (metrics == null) {
			return "";
		}
		String[] requestedMetrics = requestedMetricsList.split(",");

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartArray();
		for (String requestedMetric : requestedMetrics) {
			Object metricValue = metrics.get(requestedMetric);
			if (metricValue != null) {
				gen.writeStartObject();
				gen.writeStringField("id", requestedMetric);
				gen.writeStringField("value", metricValue.toString());
				gen.writeEndObject();
			}
		}
		gen.writeEndArray();

		gen.close();
		return writer.toString();
	}

	private String getAvailableMetricsList(Map<String, String> pathParams) throws IOException {
		Map<String, String> metrics = getMapFor(pathParams, fetcher.getMetricStore());
		if (metrics == null) {
			return "";
		}
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartArray();
		for (String m : metrics.keySet()) {
			gen.writeStartObject();
			gen.writeStringField("id", m);
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.close();
		return writer.toString();
	}
}
