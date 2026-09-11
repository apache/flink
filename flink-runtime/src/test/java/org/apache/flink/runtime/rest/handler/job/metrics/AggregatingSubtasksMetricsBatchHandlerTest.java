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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsBatchResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsNamesHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsValuesHeaders;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowableOfType;
import static org.assertj.core.api.AssertionsForClassTypes.within;

/** Tests for {@link AggregatingSubtasksMetricsBatchHandler}. */
class AggregatingSubtasksMetricsBatchHandlerTest {

    private static final JobID JOB_ID = JobID.generate();
    private static final JobVertexID VERTEX_ID_1 = new JobVertexID();
    private static final JobVertexID VERTEX_ID_2 = new JobVertexID();
    private static final Duration TIMEOUT = Duration.ofMillis(50);
    private static final Map<String, String> TEST_HEADERS = Collections.emptyMap();

    private static final DispatcherGateway MOCK_DISPATCHER_GATEWAY = new TestingDispatcherGateway();

    private static final GatewayRetriever<DispatcherGateway> LEADER_RETRIEVER =
            new GatewayRetriever<DispatcherGateway>() {
                @Override
                public CompletableFuture<DispatcherGateway> getFuture() {
                    return CompletableFuture.completedFuture(MOCK_DISPATCHER_GATEWAY);
                }
            };

    private MetricFetcher fetcher;
    private Map<String, String> pathParameters;
    private Map<String, List<String>> queryParameters;

    @BeforeEach
    void setUp() {
        fetcher =
                new MetricFetcherImpl<>(
                        () -> null,
                        rpcServiceAddress -> null,
                        Executors.directExecutor(),
                        TestingUtils.TIMEOUT,
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue().toMillis());

        fetcher.getMetricStore().add(counter(VERTEX_ID_1, 0, "busyTimeMsPerSecond", 1));
        fetcher.getMetricStore().add(counter(VERTEX_ID_1, 1, "busyTimeMsPerSecond", 3));
        fetcher.getMetricStore().add(counter(VERTEX_ID_1, 0, "numRecordsInPerSecond", 5));
        fetcher.getMetricStore().add(counter(VERTEX_ID_2, 0, "busyTimeMsPerSecond", 2));
        fetcher.getMetricStore().add(counter(VERTEX_ID_2, 1, "numRecordsInPerSecond", 6));

        pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
        queryParameters = new HashMap<>();
    }

    @Test
    void testListMetricNamesForMultipleVertices() throws Exception {
        final AggregatingSubtasksMetricsBatchHandler.NamesHandler handler =
                new AggregatingSubtasksMetricsBatchHandler.NamesHandler(
                        LEADER_RETRIEVER,
                        TIMEOUT,
                        TEST_HEADERS,
                        Executors.directExecutor(),
                        fetcher);

        queryParameters.put("vertices", Collections.singletonList(VERTEX_ID_1 + "," + VERTEX_ID_2));
        queryParameters.put("regex", Collections.singletonList(".*busyTime.*"));

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        AggregatedSubtaskMetricsNamesHeaders.getInstance()
                                .getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParameters,
                        Collections.emptyList());

        final AggregatedSubtaskMetricsBatchResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        assertThat(getMetricIds(response, VERTEX_ID_1)).containsExactly("abc.busyTimeMsPerSecond");
        assertThat(getMetricIds(response, VERTEX_ID_2)).containsExactly("abc.busyTimeMsPerSecond");
    }

    @Test
    void testInvalidMetricNameRegexFailsWithBadRequest() throws Exception {
        final AggregatingSubtasksMetricsBatchHandler.NamesHandler handler =
                new AggregatingSubtasksMetricsBatchHandler.NamesHandler(
                        LEADER_RETRIEVER,
                        TIMEOUT,
                        TEST_HEADERS,
                        Executors.directExecutor(),
                        fetcher);

        queryParameters.put("vertices", Collections.singletonList(VERTEX_ID_1.toString()));
        queryParameters.put("regex", Collections.singletonList("["));

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        AggregatedSubtaskMetricsNamesHeaders.getInstance()
                                .getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParameters,
                        Collections.emptyList());

        final ExecutionException exception =
                catchThrowableOfType(
                        () -> handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get(),
                        ExecutionException.class);

        assertBadRequest(exception);
    }

    @Test
    void testMissingVerticesFailsWithBadRequest() throws Exception {
        final AggregatingSubtasksMetricsBatchHandler.NamesHandler handler =
                new AggregatingSubtasksMetricsBatchHandler.NamesHandler(
                        LEADER_RETRIEVER,
                        TIMEOUT,
                        TEST_HEADERS,
                        Executors.directExecutor(),
                        fetcher);

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        AggregatedSubtaskMetricsNamesHeaders.getInstance()
                                .getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParameters,
                        Collections.emptyList());

        final ExecutionException exception =
                catchThrowableOfType(
                        () -> handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get(),
                        ExecutionException.class);

        assertBadRequest(exception);
    }

    @Test
    void testAggregateMetricValuesForMultipleVertices() throws Exception {
        final AggregatingSubtasksMetricsBatchHandler.ValuesHandler handler =
                new AggregatingSubtasksMetricsBatchHandler.ValuesHandler(
                        LEADER_RETRIEVER,
                        TIMEOUT,
                        TEST_HEADERS,
                        Executors.directExecutor(),
                        fetcher);

        queryParameters.put("vertices", Collections.singletonList(VERTEX_ID_1 + "," + VERTEX_ID_2));
        queryParameters.put("get", Collections.singletonList("abc.busyTimeMsPerSecond"));
        queryParameters.put("agg", Collections.singletonList("min,max,avg"));

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        AggregatedSubtaskMetricsValuesHeaders.getInstance()
                                .getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParameters,
                        Collections.emptyList());

        final AggregatedSubtaskMetricsBatchResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        final AggregatedMetric vertex1Metric = getOnlyMetric(response, VERTEX_ID_1);
        assertThat(vertex1Metric.getId()).isEqualTo("abc.busyTimeMsPerSecond");
        assertThat(vertex1Metric.getMin()).isCloseTo(1.0, within(0.1));
        assertThat(vertex1Metric.getMax()).isCloseTo(3.0, within(0.1));
        assertThat(vertex1Metric.getAvg()).isCloseTo(2.0, within(0.1));
        assertThat(vertex1Metric.getSum()).isNull();

        final AggregatedMetric vertex2Metric = getOnlyMetric(response, VERTEX_ID_2);
        assertThat(vertex2Metric.getId()).isEqualTo("abc.busyTimeMsPerSecond");
        assertThat(vertex2Metric.getMin()).isCloseTo(2.0, within(0.1));
        assertThat(vertex2Metric.getMax()).isCloseTo(2.0, within(0.1));
        assertThat(vertex2Metric.getAvg()).isCloseTo(2.0, within(0.1));
        assertThat(vertex2Metric.getSum()).isNull();
    }

    @Test
    void testInvalidAggregationFailsDuringRequestParameterResolution() {
        queryParameters.put("vertices", Collections.singletonList(VERTEX_ID_1.toString()));
        queryParameters.put("get", Collections.singletonList("abc.busyTimeMsPerSecond"));
        queryParameters.put("agg", Collections.singletonList("median"));

        assertThatThrownBy(
                        () ->
                                HandlerRequest.resolveParametersAndCreate(
                                        EmptyRequestBody.getInstance(),
                                        AggregatedSubtaskMetricsValuesHeaders.getInstance()
                                                .getUnresolvedMessageParameters(),
                                        pathParameters,
                                        queryParameters,
                                        Collections.emptyList()))
                .isInstanceOf(HandlerRequestException.class);
    }

    private static MetricDump.CounterDump counter(
            JobVertexID vertexId, int subtaskIndex, String name, long value) {
        return new MetricDump.CounterDump(
                new QueryScopeInfo.TaskQueryScopeInfo(
                        JOB_ID.toString(), vertexId.toString(), subtaskIndex, 0, "abc"),
                name,
                value);
    }

    private static List<String> getMetricIds(
            AggregatedSubtaskMetricsBatchResponseBody response, JobVertexID vertexId) {
        return getMetrics(response, vertexId).stream()
                .map(AggregatedMetric::getId)
                .collect(Collectors.toList());
    }

    private static AggregatedMetric getOnlyMetric(
            AggregatedSubtaskMetricsBatchResponseBody response, JobVertexID vertexId) {
        return getMetrics(response, vertexId).iterator().next();
    }

    private static Collection<AggregatedMetric> getMetrics(
            AggregatedSubtaskMetricsBatchResponseBody response, JobVertexID vertexId) {
        return response.getMetricsByVertex().stream()
                .filter(vertexMetrics -> vertexMetrics.getVertexId().equals(vertexId))
                .findFirst()
                .orElseThrow(AssertionError::new)
                .getMetrics();
    }

    private static void assertBadRequest(ExecutionException exception) {
        assertThat(exception).isNotNull();
        assertThat(exception.getCause()).isInstanceOf(RestHandlerException.class);
        assertThat(((RestHandlerException) exception.getCause()).getHttpResponseStatus())
                .isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }
}
