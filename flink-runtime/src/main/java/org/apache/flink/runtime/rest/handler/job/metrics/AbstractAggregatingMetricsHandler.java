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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Abstract request handler for querying aggregated metrics. Subclasses return either a list of all
 * available metrics or the aggregated values of them across all/selected entities.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code [ { "id" : "X" } ] }
 *
 * <p>If the query parameters do contain a "get" parameter, a comma-separated list of metric names
 * is expected as a value. {@code /metrics?get=X,Y} The handler will then return a list containing
 * the values of the requested metrics. {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y",
 * "value" : "T" } ] }
 *
 * <p>The "agg" query parameter is used to define which aggregates should be calculated. Available
 * aggregations are "sum", "max", "min" and "avg". If the parameter is not specified, all
 * aggregations will be returned. {@code /metrics?get=X,Y&agg=min,max} The handler will then return
 * a list of objects containing the aggregations for the requested metrics. {@code [ { "id" : "X",
 * "min", "1", "max", "2" }, { "id" : "Y", "min", "4", "max", "10"}]}
 */
public abstract class AbstractAggregatingMetricsHandler<
                P extends AbstractAggregatedMetricsParameters<?>>
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, AggregatedMetricsResponseBody, P> {

    private final Executor executor;
    private final MetricFetcher fetcher;

    protected AbstractAggregatingMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            AbstractAggregatedMetricsHeaders<P> messageHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.executor = Preconditions.checkNotNull(executor);
        this.fetcher = Preconditions.checkNotNull(fetcher);
    }

    @Nonnull
    abstract Collection<? extends MetricStore.ComponentMetricStore> getStores(
            MetricStore store, HandlerRequest<EmptyRequestBody> request);

    @Override
    protected CompletableFuture<AggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        fetcher.update();
                        List<String> requestedMetrics =
                                request.getQueryParameter(MetricsFilterParameter.class);
                        List<MetricsAggregationParameter.AggregationMode> requestedAggregations =
                                request.getQueryParameter(MetricsAggregationParameter.class);
                        MetricStore store = fetcher.getMetricStore();

                        Collection<? extends MetricStore.ComponentMetricStore> stores =
                                getStores(store, request);

                        if (requestedMetrics.isEmpty()) {
                            Collection<String> list =
                                    AggregatedMetricsStoreHelper.getAvailableMetrics(stores);
                            return new AggregatedMetricsResponseBody(
                                    list.stream()
                                            .map(AggregatedMetric::new)
                                            .collect(Collectors.toList()));
                        }

                        return AggregatedMetricsStoreHelper.getAggregatedMetricValues(
                                stores, requestedMetrics, requestedAggregations);
                    } catch (Exception e) {
                        log.warn("Could not retrieve metrics.", e);
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Could not retrieve metrics.",
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR));
                    }
                },
                executor);
    }
}
