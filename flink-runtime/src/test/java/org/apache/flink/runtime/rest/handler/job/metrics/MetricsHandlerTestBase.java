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
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test base class for subclasses of {@link AbstractMetricsHandler}. */
abstract class MetricsHandlerTestBase<T extends AbstractMetricsHandler> {

    private static final String TEST_METRIC_NAME = "test_counter";

    private static final int TEST_METRIC_VALUE = 1000;

    static final Time TIMEOUT = Time.milliseconds(50);

    static final Map<String, String> TEST_HEADERS = Collections.emptyMap();

    MetricFetcher mockMetricFetcher;

    GatewayRetriever<DispatcherGateway> leaderRetriever;

    private final DispatcherGateway mockDispatcherGateway = new TestingDispatcherGateway();

    private T metricsHandler;

    private Map<String, String> pathParameters;

    @BeforeEach
    void setUp() {
        final MetricStore metricStore = new MetricStore();
        metricStore.add(
                new MetricDump.CounterDump(
                        getQueryScopeInfo(), TEST_METRIC_NAME, TEST_METRIC_VALUE));
        mockMetricFetcher =
                new MetricFetcher() {
                    @Override
                    public MetricStore getMetricStore() {
                        return metricStore;
                    }

                    @Override
                    public void update() {}

                    @Override
                    public long getLastUpdateTime() {
                        return 0;
                    }
                };
        this.leaderRetriever = () -> CompletableFuture.completedFuture(mockDispatcherGateway);
        this.pathParameters = getPathParameters();
        this.metricsHandler = getMetricsHandler();
    }

    /**
     * Tests that the metric with name defined under {@link #TEST_METRIC_NAME} can be retrieved from
     * the {@link MetricStore.ComponentMetricStore} returned from {@link
     * AbstractMetricsHandler#getComponentMetricStore(HandlerRequest, MetricStore)}.
     */
    @Test
    void testGetMetric() throws Exception {
        @SuppressWarnings("unchecked")
        final CompletableFuture<MetricCollectionResponseBody> completableFuture =
                metricsHandler.handleRequest(
                        HandlerRequest.resolveParametersAndCreate(
                                EmptyRequestBody.getInstance(),
                                metricsHandler.getMessageHeaders().getUnresolvedMessageParameters(),
                                pathParameters,
                                Collections.emptyMap(),
                                Collections.emptyList()),
                        mockDispatcherGateway);

        assertThat(completableFuture).isDone();

        final MetricCollectionResponseBody metricCollectionResponseBody = completableFuture.get();
        assertThat(metricCollectionResponseBody.getMetrics()).hasSize(1);

        final Metric metric = metricCollectionResponseBody.getMetrics().iterator().next();
        assertThat(metric.getId()).isEqualTo(getExpectedIdForMetricName(TEST_METRIC_NAME));
    }

    /** Returns instance under test. */
    abstract T getMetricsHandler();

    abstract QueryScopeInfo getQueryScopeInfo();

    abstract Map<String, String> getPathParameters();

    /**
     * Returns the expected metric id for a given metric name. By default the metric name without
     * any modifications is returned.
     *
     * @param metricName The metric name.
     * @return The id of the metric name possibly with additional information, e.g., subtask index
     *     as a prefix.
     */
    String getExpectedIdForMetricName(final String metricName) {
        return metricName;
    }
}
