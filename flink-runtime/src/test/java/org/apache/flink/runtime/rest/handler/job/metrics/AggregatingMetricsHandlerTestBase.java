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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AbstractAggregatedMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.powermock.api.mockito.PowerMockito.mock;

/** Test base for handlers that extend {@link AbstractAggregatingMetricsHandler}. */
public abstract class AggregatingMetricsHandlerTestBase<
                H extends AbstractAggregatingMetricsHandler<P>,
                P extends AbstractAggregatedMetricsParameters<?>>
        extends TestLogger {

    private static final CompletableFuture<String> TEST_REST_ADDRESS;
    private static final DispatcherGateway MOCK_DISPATCHER_GATEWAY;
    private static final GatewayRetriever<DispatcherGateway> LEADER_RETRIEVER;
    private static final Time TIMEOUT = Time.milliseconds(50);
    private static final Map<String, String> TEST_HEADERS = Collections.emptyMap();
    private static final Executor EXECUTOR = TestingUtils.defaultExecutor();

    static {
        TEST_REST_ADDRESS = CompletableFuture.completedFuture("localhost:12345");

        MOCK_DISPATCHER_GATEWAY = mock(DispatcherGateway.class);

        LEADER_RETRIEVER =
                new GatewayRetriever<DispatcherGateway>() {
                    @Override
                    public CompletableFuture<DispatcherGateway> getFuture() {
                        return CompletableFuture.completedFuture(MOCK_DISPATCHER_GATEWAY);
                    }
                };
    }

    private H handler;
    private MetricStore store;
    private Map<String, String> pathParameters;

    @Before
    public void setUp() throws Exception {
        MetricFetcher fetcher =
                new MetricFetcherImpl<RestfulGateway>(
                        mock(GatewayRetriever.class),
                        mock(MetricQueryServiceRetriever.class),
                        Executors.directExecutor(),
                        TestingUtils.TIMEOUT(),
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue());
        store = fetcher.getMetricStore();

        Collection<MetricDump> metricDumps = getMetricDumps();
        for (MetricDump dump : metricDumps) {
            store.add(dump);
        }

        handler = getHandler(LEADER_RETRIEVER, TIMEOUT, TEST_HEADERS, EXECUTOR, fetcher);
        pathParameters = getPathParameters();
    }

    protected Map<String, String> getPathParameters() {
        return Collections.emptyMap();
    }

    protected abstract Tuple2<String, List<String>> getFilter();

    protected abstract Collection<MetricDump> getMetricDumps();

    protected abstract H getHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher);

    @Test
    public void getStores() throws Exception {
        { // test without filter
            HandlerRequest<EmptyRequestBody, P> request =
                    new HandlerRequest<>(
                            EmptyRequestBody.getInstance(),
                            handler.getMessageHeaders().getUnresolvedMessageParameters(),
                            pathParameters,
                            Collections.emptyMap());
            Collection<? extends MetricStore.ComponentMetricStore> subStores =
                    handler.getStores(store, request);

            assertEquals(3, subStores.size());

            List<String> sortedMetrics1 =
                    subStores.stream()
                            .map(subStore -> subStore.getMetric("abc.metric1"))
                            .filter(Objects::nonNull)
                            .sorted()
                            .collect(Collectors.toList());

            assertEquals(2, sortedMetrics1.size());

            assertEquals("1", sortedMetrics1.get(0));
            assertEquals("3", sortedMetrics1.get(1));

            List<String> sortedMetrics2 =
                    subStores.stream()
                            .map(subStore -> subStore.getMetric("abc.metric2"))
                            .filter(Objects::nonNull)
                            .sorted()
                            .collect(Collectors.toList());

            assertEquals(1, sortedMetrics2.size());

            assertEquals("5", sortedMetrics2.get(0));
        }

        { // test with filter
            Tuple2<String, List<String>> filter = getFilter();
            Map<String, List<String>> queryParameters = new HashMap<>(4);
            queryParameters.put(filter.f0, filter.f1);
            HandlerRequest<EmptyRequestBody, P> request =
                    new HandlerRequest<>(
                            EmptyRequestBody.getInstance(),
                            handler.getMessageHeaders().getUnresolvedMessageParameters(),
                            pathParameters,
                            queryParameters);
            Collection<? extends MetricStore.ComponentMetricStore> subStores =
                    handler.getStores(store, request);

            assertEquals(2, subStores.size());

            List<String> sortedMetrics1 =
                    subStores.stream()
                            .map(subStore -> subStore.getMetric("abc.metric1"))
                            .filter(Objects::nonNull)
                            .sorted()
                            .collect(Collectors.toList());

            assertEquals(1, sortedMetrics1.size());

            assertEquals("1", sortedMetrics1.get(0));

            List<String> sortedMetrics2 =
                    subStores.stream()
                            .map(subStore -> subStore.getMetric("abc.metric2"))
                            .filter(Objects::nonNull)
                            .sorted()
                            .collect(Collectors.toList());

            assertEquals(1, sortedMetrics2.size());

            assertEquals("5", sortedMetrics2.get(0));
        }
    }

    @Test
    public void testListMetrics() throws Exception {
        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        Collections.emptyMap());

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        List<String> availableMetrics =
                response.getMetrics().stream()
                        .map(AggregatedMetric::getId)
                        .sorted()
                        .collect(Collectors.toList());

        assertEquals(2, availableMetrics.size());
        assertEquals("abc.metric1", availableMetrics.get(0));
        assertEquals("abc.metric2", availableMetrics.get(1));
    }

    @Test
    public void testMinAggregation() throws Exception {
        Map<String, List<String>> queryParams = new HashMap<>(4);
        queryParams.put("get", Collections.singletonList("abc.metric1"));
        queryParams.put("agg", Collections.singletonList("min"));

        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParams);

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        Collection<AggregatedMetric> aggregatedMetrics = response.getMetrics();

        assertEquals(1, aggregatedMetrics.size());
        AggregatedMetric aggregatedMetric = aggregatedMetrics.iterator().next();

        assertEquals("abc.metric1", aggregatedMetric.getId());
        assertEquals(1.0, aggregatedMetric.getMin(), 0.1);
        assertNull(aggregatedMetric.getMax());
        assertNull(aggregatedMetric.getSum());
        assertNull(aggregatedMetric.getAvg());
    }

    @Test
    public void testMaxAggregation() throws Exception {
        Map<String, List<String>> queryParams = new HashMap<>(4);
        queryParams.put("get", Collections.singletonList("abc.metric1"));
        queryParams.put("agg", Collections.singletonList("max"));

        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParams);

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        Collection<AggregatedMetric> aggregatedMetrics = response.getMetrics();

        assertEquals(1, aggregatedMetrics.size());
        AggregatedMetric aggregatedMetric = aggregatedMetrics.iterator().next();

        assertEquals("abc.metric1", aggregatedMetric.getId());
        assertEquals(3.0, aggregatedMetric.getMax(), 0.1);
        assertNull(aggregatedMetric.getMin());
        assertNull(aggregatedMetric.getSum());
        assertNull(aggregatedMetric.getAvg());
    }

    @Test
    public void testSumAggregation() throws Exception {
        Map<String, List<String>> queryParams = new HashMap<>(4);
        queryParams.put("get", Collections.singletonList("abc.metric1"));
        queryParams.put("agg", Collections.singletonList("sum"));

        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParams);

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        Collection<AggregatedMetric> aggregatedMetrics = response.getMetrics();

        assertEquals(1, aggregatedMetrics.size());
        AggregatedMetric aggregatedMetric = aggregatedMetrics.iterator().next();

        assertEquals("abc.metric1", aggregatedMetric.getId());
        assertEquals(4.0, aggregatedMetric.getSum(), 0.1);
        assertNull(aggregatedMetric.getMin());
        assertNull(aggregatedMetric.getMax());
        assertNull(aggregatedMetric.getAvg());
    }

    @Test
    public void testAvgAggregation() throws Exception {
        Map<String, List<String>> queryParams = new HashMap<>(4);
        queryParams.put("get", Collections.singletonList("abc.metric1"));
        queryParams.put("agg", Collections.singletonList("avg"));

        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParams);

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        Collection<AggregatedMetric> aggregatedMetrics = response.getMetrics();

        assertEquals(1, aggregatedMetrics.size());
        AggregatedMetric aggregatedMetric = aggregatedMetrics.iterator().next();

        assertEquals("abc.metric1", aggregatedMetric.getId());
        assertEquals(2.0, aggregatedMetric.getAvg(), 0.1);
        assertNull(aggregatedMetric.getMin());
        assertNull(aggregatedMetric.getMax());
        assertNull(aggregatedMetric.getSum());
    }

    @Test
    public void testMultipleAggregation() throws Exception {
        Map<String, List<String>> queryParams = new HashMap<>(4);
        queryParams.put("get", Collections.singletonList("abc.metric1"));
        queryParams.put("agg", Arrays.asList("min", "max", "avg"));

        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParams);

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        Collection<AggregatedMetric> aggregatedMetrics = response.getMetrics();

        assertEquals(1, aggregatedMetrics.size());
        AggregatedMetric aggregatedMetric = aggregatedMetrics.iterator().next();

        assertEquals("abc.metric1", aggregatedMetric.getId());
        assertEquals(1.0, aggregatedMetric.getMin(), 0.1);
        assertEquals(3.0, aggregatedMetric.getMax(), 0.1);
        assertEquals(2.0, aggregatedMetric.getAvg(), 0.1);
        assertNull(aggregatedMetric.getSum());
    }

    @Test
    public void testDefaultAggregation() throws Exception {
        Map<String, List<String>> queryParams = new HashMap<>(4);
        queryParams.put("get", Collections.singletonList("abc.metric1"));

        HandlerRequest<EmptyRequestBody, P> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        pathParameters,
                        queryParams);

        AggregatedMetricsResponseBody response =
                handler.handleRequest(request, MOCK_DISPATCHER_GATEWAY).get();

        Collection<AggregatedMetric> aggregatedMetrics = response.getMetrics();

        assertEquals(1, aggregatedMetrics.size());
        AggregatedMetric aggregatedMetric = aggregatedMetrics.iterator().next();

        assertEquals("abc.metric1", aggregatedMetric.getId());
        assertEquals(1.0, aggregatedMetric.getMin(), 0.1);
        assertEquals(3.0, aggregatedMetric.getMax(), 0.1);
        assertEquals(2.0, aggregatedMetric.getAvg(), 0.1);
        assertEquals(4.0, aggregatedMetric.getSum(), 0.1);
    }
}
