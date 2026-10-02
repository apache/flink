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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsBatchParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsBatchResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsNamesHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsNamesParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsValuesHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsValuesParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVerticesFilterQueryParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsRegexFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/** Batch handlers for aggregated subtask metrics. */
public class AggregatingSubtasksMetricsBatchHandler {

    private AggregatingSubtasksMetricsBatchHandler() {}

    /** Handler for batch aggregated subtask metric name discovery. */
    public static class NamesHandler
            extends AbstractBatchSubtasksMetricsHandler<AggregatedSubtaskMetricsNamesParameters> {

        public NamesHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Duration timeout,
                Map<String, String> responseHeaders,
                Executor executor,
                MetricFetcher fetcher) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    AggregatedSubtaskMetricsNamesHeaders.getInstance(),
                    executor,
                    fetcher);
        }

        @Override
        AggregatedSubtaskMetricsBatchResponseBody handleBatchRequest(
                MetricStore store, JobID jobId, HandlerRequest<EmptyRequestBody> request)
                throws RestHandlerException {
            final List<Pattern> patterns =
                    compilePatterns(request.getQueryParameter(MetricsRegexFilterParameter.class));
            final List<JobVertexID> vertexIds = getVertexIds(request);
            final Collection<AggregatedSubtaskMetricsBatchResponseBody.VertexAggregatedMetrics>
                    metricsByVertex = new ArrayList<>(vertexIds.size());

            for (JobVertexID vertexId : vertexIds) {
                final Collection<AggregatedMetric> metrics =
                        AggregatedMetricsStoreHelper.getAvailableMetrics(
                                        getSubtaskMetricStores(store, jobId, vertexId))
                                .stream()
                                .filter(metric -> matchesAny(metric, patterns))
                                .sorted()
                                .map(AggregatedMetric::new)
                                .collect(Collectors.toList());
                metricsByVertex.add(
                        new AggregatedSubtaskMetricsBatchResponseBody.VertexAggregatedMetrics(
                                vertexId, metrics));
            }

            return new AggregatedSubtaskMetricsBatchResponseBody(metricsByVertex);
        }

        private static List<Pattern> compilePatterns(Collection<String> regex)
                throws RestHandlerException {
            try {
                return regex.stream().map(Pattern::compile).collect(Collectors.toList());
            } catch (PatternSyntaxException e) {
                throw new RestHandlerException(
                        "Invalid metric name regex.", HttpResponseStatus.BAD_REQUEST, e);
            }
        }

        private static boolean matchesAny(String metric, List<Pattern> patterns) {
            return patterns.isEmpty()
                    || patterns.stream().anyMatch(pattern -> pattern.matcher(metric).matches());
        }
    }

    /** Handler for batch aggregated subtask metric values. */
    public static class ValuesHandler
            extends AbstractBatchSubtasksMetricsHandler<AggregatedSubtaskMetricsValuesParameters> {

        public ValuesHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Duration timeout,
                Map<String, String> responseHeaders,
                Executor executor,
                MetricFetcher fetcher) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    AggregatedSubtaskMetricsValuesHeaders.getInstance(),
                    executor,
                    fetcher);
        }

        @Override
        AggregatedSubtaskMetricsBatchResponseBody handleBatchRequest(
                MetricStore store, JobID jobId, HandlerRequest<EmptyRequestBody> request)
                throws RestHandlerException {
            final List<MetricsAggregationParameter.AggregationMode> aggregations =
                    request.getQueryParameter(MetricsAggregationParameter.class);
            final List<String> requestedMetrics =
                    request.getQueryParameter(MetricsFilterParameter.class);
            final List<JobVertexID> vertexIds = getVertexIds(request);
            final Collection<AggregatedSubtaskMetricsBatchResponseBody.VertexAggregatedMetrics>
                    metricsByVertex = new ArrayList<>(vertexIds.size());

            for (JobVertexID vertexId : vertexIds) {
                final AggregatedMetricsResponseBody aggregatedMetrics =
                        AggregatedMetricsStoreHelper.getAggregatedMetricValues(
                                getSubtaskMetricStores(store, jobId, vertexId),
                                requestedMetrics,
                                aggregations);
                metricsByVertex.add(
                        new AggregatedSubtaskMetricsBatchResponseBody.VertexAggregatedMetrics(
                                vertexId, aggregatedMetrics.getMetrics()));
            }

            return new AggregatedSubtaskMetricsBatchResponseBody(metricsByVertex);
        }
    }

    private abstract static class AbstractBatchSubtasksMetricsHandler<
                    P extends AggregatedSubtaskMetricsBatchParameters>
            extends AbstractRestHandler<
                    RestfulGateway,
                    EmptyRequestBody,
                    AggregatedSubtaskMetricsBatchResponseBody,
                    P> {

        private final Executor executor;
        private final MetricFetcher fetcher;

        private AbstractBatchSubtasksMetricsHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Duration timeout,
                Map<String, String> responseHeaders,
                RuntimeMessageHeaders<
                                EmptyRequestBody, AggregatedSubtaskMetricsBatchResponseBody, P>
                        messageHeaders,
                Executor executor,
                MetricFetcher fetcher) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
            this.executor = Preconditions.checkNotNull(executor);
            this.fetcher = Preconditions.checkNotNull(fetcher);
        }

        @Override
        protected CompletableFuture<AggregatedSubtaskMetricsBatchResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            fetcher.update();
                            return handleBatchRequest(
                                    fetcher.getMetricStore(),
                                    request.getPathParameter(JobIDPathParameter.class),
                                    request);
                        } catch (RestHandlerException e) {
                            throw new CompletionException(e);
                        } catch (Exception e) {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            "Could not retrieve metrics.",
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            e));
                        }
                    },
                    executor);
        }

        abstract AggregatedSubtaskMetricsBatchResponseBody handleBatchRequest(
                MetricStore store, JobID jobId, HandlerRequest<EmptyRequestBody> request)
                throws RestHandlerException;

        final List<JobVertexID> getVertexIds(HandlerRequest<EmptyRequestBody> request)
                throws RestHandlerException {
            final List<JobVertexID> vertexIds =
                    request.getQueryParameter(JobVerticesFilterQueryParameter.class);
            if (vertexIds.isEmpty()) {
                throw new RestHandlerException(
                        "At least one job vertex must be specified.",
                        HttpResponseStatus.BAD_REQUEST);
            }
            return vertexIds;
        }
    }

    private static Collection<? extends MetricStore.ComponentMetricStore> getSubtaskMetricStores(
            MetricStore store, JobID jobId, JobVertexID vertexId) {
        MetricStore.TaskMetricStore taskMetricStore =
                store.getTaskMetricStore(jobId.toString(), vertexId.toString());
        if (taskMetricStore == null) {
            return Collections.emptyList();
        }
        return taskMetricStore.getAllSubtaskMetricStores().values();
    }
}
