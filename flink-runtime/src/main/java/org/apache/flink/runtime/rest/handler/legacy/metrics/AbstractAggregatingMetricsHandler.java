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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Abstract request handler for querying aggregated metrics. Subclasses return either a list of all available metrics
 * or the aggregated values of them across all/selected entities.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code [ { "id" : "X" } ] }
 *
 * <p>If the query parameters do contain a "get" parameter, a comma-separated list of metric names is expected as a value.
 * {@code /metrics?get=X,Y}
 * The handler will then return a list containing the values of the requested metrics.
 * {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y", "value" : "T" } ] }
 *
 * <p>The "agg" query parameter is used to define which aggregates should be calculated. Available aggregations are
 * "sum", "max", "min" and "avg". If the parameter is not specified, all aggregations will be returned.
 * {@code /metrics?get=X,Y&agg=min,max}
 * The handler will then return a list of objects containing the aggregations for the requested metrics.
 * {@code [ { "id" : "X", "min", "1", "max", "2" }, { "id" : "Y", "min", "4", "max", "10"}]}
 */
abstract class AbstractAggregatingMetricsHandler extends AbstractJsonRequestHandler {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final String PARAMETER_AGGREGATION = "agg";

	private final MetricFetcher fetcher;

	AbstractAggregatingMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor);
		this.fetcher = Preconditions.checkNotNull(fetcher);
	}

	protected abstract Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, Map<String, String> pathParameters, Map<String, String> queryParameters);

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					fetcher.update();
					String requestedMetricsList = queryParams.get(AbstractMetricsHandler.PARAMETER_METRICS);
					String aggTypeList = queryParams.get(PARAMETER_AGGREGATION);
					MetricStore store = fetcher.getMetricStore();

					Collection<? extends MetricStore.ComponentMetricStore> stores = getStores(store, pathParams, queryParams);
					if (stores == null){
						return "[]";
					}

					if (requestedMetricsList == null) {
						Collection<String> list = getAvailableMetrics(stores);
						return mapMetricListToJson(list);
					}

					if (requestedMetricsList.isEmpty()) {
						/*
						 * The WebInterface doesn't check whether the list of available metrics was empty. This can lead to a
						 * request for which the "get" parameter is an empty string.
						 */
						return "[]";
					}

					String[] requestedMetrics = requestedMetricsList.split(",");

					List<DoubleAccumulator.DoubleAccumulatorFactory<?>> requestedAggregationsFactories = new ArrayList<>();
					// by default we return all aggregations
					if (aggTypeList == null || aggTypeList.isEmpty()) {
						requestedAggregationsFactories.add(DoubleAccumulator.DoubleMinimumFactory.get());
						requestedAggregationsFactories.add(DoubleAccumulator.DoubleMaximumFactory.get());
						requestedAggregationsFactories.add(DoubleAccumulator.DoubleSumFactory.get());
						requestedAggregationsFactories.add(DoubleAccumulator.DoubleAverageFactory.get());
					} else {
						for (String aggregation : aggTypeList.split(",")) {
							switch (aggregation.toLowerCase()) {
								case DoubleAccumulator.DoubleMinimum.NAME:
									requestedAggregationsFactories.add(DoubleAccumulator.DoubleMinimumFactory.get());
									break;
								case DoubleAccumulator.DoubleMaximum.NAME:
									requestedAggregationsFactories.add(DoubleAccumulator.DoubleMaximumFactory.get());
									break;
								case DoubleAccumulator.DoubleSum.NAME:
									requestedAggregationsFactories.add(DoubleAccumulator.DoubleSumFactory.get());
									break;
								case DoubleAccumulator.DoubleAverage.NAME:
									requestedAggregationsFactories.add(DoubleAccumulator.DoubleAverageFactory.get());
									break;
								default:
									log.warn("Invalid aggregation specified: {}", aggregation.toLowerCase());
							}
						}
					}

					return getAggregatedMetricValues(stores, requestedMetrics, requestedAggregationsFactories);
				} catch (Exception e) {
					throw new CompletionException(new FlinkException("Could not retrieve metrics.", e));
				}
			},
			executor);
	}

	/**
	 * Returns a JSON string containing a list of all available metrics in the given stores. Effectively this method maps
	 * the union of all key-sets to JSON.
	 *
	 * @param stores metrics
	 * @return JSON string containing a list of all available metrics
	 */
	private static Collection<String> getAvailableMetrics(Collection<? extends MetricStore.ComponentMetricStore> stores) {
		Set<String> uniqueMetrics = new HashSet<>();
		for (MetricStore.ComponentMetricStore store : stores) {
			uniqueMetrics.addAll(store.metrics.keySet());
		}
		return uniqueMetrics;
	}

	private static String mapMetricListToJson(Collection<String> metrics) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartArray();
		for (String m : metrics) {
			gen.writeStartObject();
			gen.writeStringField("id", m);
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.close();
		return writer.toString();
	}

	/**
	 * Extracts and aggregates all requested metrics from the given metric stores, and maps the result to a JSON string.
	 *
	 * @param stores available metrics
	 * @param requestedMetrics ids of requested metrics
	 * @param requestedAggregationsFactories requested aggregations
	 * @return JSON string containing the requested metrics
	 * @throws IOException
	 */
	private String getAggregatedMetricValues(
			Collection<? extends MetricStore.ComponentMetricStore> stores,
			String[] requestedMetrics,
			List<DoubleAccumulator.DoubleAccumulatorFactory<?>> requestedAggregationsFactories) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartArray();
		for (String requestedMetric : requestedMetrics) {
			final Collection<Double> values = new ArrayList<>();
			try {
				for (MetricStore.ComponentMetricStore store : stores) {
					String stringValue = store.metrics.get(requestedMetric);
					if (stringValue != null) {
						values.add(Double.valueOf(stringValue));
					}
				}
			} catch (NumberFormatException nfe) {
				log.warn("The metric {} is not numeric and can't be aggregated.", requestedMetric, nfe);
				// metric is not numeric so we can't perform aggregations => ignore it
				continue;
			}
			if (!values.isEmpty()) {

				gen.writeStartObject();
				gen.writeStringField("id", requestedMetric);
				for (DoubleAccumulator.DoubleAccumulatorFactory<?> accFactory : requestedAggregationsFactories) {
					Iterator<Double> valuesIterator = values.iterator();
					DoubleAccumulator acc = accFactory.get(valuesIterator.next());
					valuesIterator.forEachRemaining(acc::add);

					gen.writeStringField(acc.getName(), String.valueOf(acc.getValue()));
				}
				gen.writeEndObject();
			}
		}
		gen.writeEndArray();

		gen.close();
		return writer.toString();
	}
}
