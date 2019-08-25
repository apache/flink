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
import org.apache.flink.runtime.rest.messages.job.metrics.AbstractAggregatedMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AbstractAggregatedMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

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
public abstract class AbstractAggregatingMetricsHandler<P extends AbstractAggregatedMetricsParameters<?>> extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, AggregatedMetricsResponseBody, P> {

	private final Executor executor;
	private final MetricFetcher fetcher;

	protected AbstractAggregatingMetricsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			AbstractAggregatedMetricsHeaders<P> messageHeaders,
			Executor executor,
			MetricFetcher fetcher) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.executor = Preconditions.checkNotNull(executor);
		this.fetcher = Preconditions.checkNotNull(fetcher);
	}

	@Nonnull
	abstract Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, HandlerRequest<EmptyRequestBody, P> request);

	@Override
	protected CompletableFuture<AggregatedMetricsResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, P> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					fetcher.update();
					List<String> requestedMetrics = request.getQueryParameter(MetricsFilterParameter.class);
					List<MetricsAggregationParameter.AggregationMode> requestedAggregations = request.getQueryParameter(MetricsAggregationParameter.class);
					MetricStore store = fetcher.getMetricStore();

					Collection<? extends MetricStore.ComponentMetricStore> stores = getStores(store, request);

					if (requestedMetrics.isEmpty()) {
						Collection<String> list = getAvailableMetrics(stores);
						return new AggregatedMetricsResponseBody(
							list.stream()
								.map(AggregatedMetric::new)
								.collect(Collectors.toList())
						);
					}

					DoubleAccumulator.DoubleMinimumFactory minimumFactory = null;
					DoubleAccumulator.DoubleMaximumFactory maximumFactory = null;
					DoubleAccumulator.DoubleAverageFactory averageFactory = null;
					DoubleAccumulator.DoubleSumFactory sumFactory = null;
					// by default we return all aggregations
					if (requestedAggregations.isEmpty()) {
						minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
						maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
						averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
						sumFactory = DoubleAccumulator.DoubleSumFactory.get();
					} else {
						for (MetricsAggregationParameter.AggregationMode aggregation : requestedAggregations) {
							switch (aggregation) {
								case MIN:
									minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
									break;
								case MAX:
									maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
									break;
								case AVG:
									averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
									break;
								case SUM:
									sumFactory = DoubleAccumulator.DoubleSumFactory.get();
									break;
								default:
									log.warn("Unsupported aggregation specified: {}", aggregation);
							}
						}
					}
					MetricAccumulatorFactory metricAccumulatorFactory = new MetricAccumulatorFactory(minimumFactory, maximumFactory, averageFactory, sumFactory);

					return getAggregatedMetricValues(stores, requestedMetrics, metricAccumulatorFactory);
				} catch (Exception e) {
					log.warn("Could not retrieve metrics.", e);
					throw new CompletionException(new RestHandlerException("Could not retrieve metrics.", HttpResponseStatus.INTERNAL_SERVER_ERROR));
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
		Set<String> uniqueMetrics = new HashSet<>(32);
		for (MetricStore.ComponentMetricStore store : stores) {
			uniqueMetrics.addAll(store.metrics.keySet());
		}
		return uniqueMetrics;
	}

	/**
	 * Extracts and aggregates all requested metrics from the given metric stores, and maps the result to a JSON string.
	 *
	 * @param stores available metrics
	 * @param requestedMetrics ids of requested metrics
	 * @param requestedAggregationsFactories requested aggregations
	 * @return JSON string containing the requested metrics
	 */
	private AggregatedMetricsResponseBody getAggregatedMetricValues(
			Collection<? extends MetricStore.ComponentMetricStore> stores,
			List<String> requestedMetrics,
			MetricAccumulatorFactory requestedAggregationsFactories) {

		Collection<AggregatedMetric> aggregatedMetrics = new ArrayList<>(requestedMetrics.size());
		for (String requestedMetric : requestedMetrics) {
			final Collection<Double> values = new ArrayList<>(stores.size());
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

				Iterator<Double> valuesIterator = values.iterator();
				MetricAccumulator acc = requestedAggregationsFactories.get(requestedMetric, valuesIterator.next());
				valuesIterator.forEachRemaining(acc::add);

				aggregatedMetrics.add(acc.get());
			} else {
				return new AggregatedMetricsResponseBody(Collections.emptyList());
			}
		}
		return new AggregatedMetricsResponseBody(aggregatedMetrics);
	}

	private static class MetricAccumulatorFactory {

		@Nullable
		private final DoubleAccumulator.DoubleMinimumFactory minimumFactory;

		@Nullable
		private final DoubleAccumulator.DoubleMaximumFactory maximumFactory;

		@Nullable
		private final DoubleAccumulator.DoubleAverageFactory averageFactory;

		@Nullable
		private final DoubleAccumulator.DoubleSumFactory sumFactory;

		private MetricAccumulatorFactory(
				@Nullable DoubleAccumulator.DoubleMinimumFactory minimumFactory,
				@Nullable DoubleAccumulator.DoubleMaximumFactory maximumFactory,
				@Nullable DoubleAccumulator.DoubleAverageFactory averageFactory,
				@Nullable DoubleAccumulator.DoubleSumFactory sumFactory) {
			this.minimumFactory = minimumFactory;
			this.maximumFactory = maximumFactory;
			this.averageFactory = averageFactory;
			this.sumFactory = sumFactory;
		}

		MetricAccumulator get(String metricName, double init) {
			return new MetricAccumulator(
				metricName,
				minimumFactory == null ? null : minimumFactory.get(init),
				maximumFactory == null ? null : maximumFactory.get(init),
				averageFactory == null ? null : averageFactory.get(init),
				sumFactory == null ? null : sumFactory.get(init)
			);
		}
	}

	private static class MetricAccumulator {
		private final String metricName;

		@Nullable
		private final DoubleAccumulator min;
		@Nullable
		private final DoubleAccumulator max;
		@Nullable
		private final DoubleAccumulator avg;
		@Nullable
		private final DoubleAccumulator sum;

		private MetricAccumulator(
				String metricName,
				@Nullable DoubleAccumulator min,
				@Nullable DoubleAccumulator max,
				@Nullable DoubleAccumulator avg,
				@Nullable DoubleAccumulator sum) {
			this.metricName = Preconditions.checkNotNull(metricName);
			this.min = min;
			this.max = max;
			this.avg = avg;
			this.sum = sum;
		}

		void add(double value) {
			if (min != null) {
				min.add(value);
			}
			if (max != null) {
				max.add(value);
			}
			if (avg != null) {
				avg.add(value);
			}
			if (sum != null) {
				sum.add(value);
			}
		}

		AggregatedMetric get() {
			return new AggregatedMetric(
				metricName,
				min == null ? null : min.getValue(),
				max == null ? null : max.getValue(),
				avg == null ? null : avg.getValue(),
				sum == null ? null : sum.getValue()
			);
		}
	}
}
