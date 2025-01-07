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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionConfigBuilder;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobDetailsHandler}. */
class JobDetailsHandlerTest {
    private JobDetailsHandler jobDetailsHandler;
    private HandlerRequest<EmptyRequestBody> handlerRequest;
    private AccessExecutionGraph archivedExecutionGraph;
    private final String expectedStreamGraphJson =
            "{\"pending_operators:2,\":\"nodes\":[{\"id\":\"1\",\"parallelism\":1,\"operator\":\"Source: Sequence Source\",\"description\":\"Source: Sequence Source\",\"inputs\":[]},{\"id\":\"2\",\"parallelism\":1,\"operator\":\"Sink: Print to Std. Out\",\"description\":\"Sink: Print to Std. Out\",\"inputs\":[{\"num\":0,\"id\":\"1\",\"ship_strategy\":\"FORWARD\",\"exchange\":\"UNDEFINED\"}]}]}";

    private static HandlerRequest<EmptyRequestBody> createRequest(JobID jobId)
            throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new TaskManagerMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @BeforeEach
    void setUp() throws HandlerRequestException {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);
        final RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(new Configuration());
        final MetricFetcher metricFetcher =
                new MetricFetcherImpl<>(
                        () -> null,
                        address -> null,
                        Executors.directExecutor(),
                        Duration.ofMillis(1000L),
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue().toMillis());
        final ArchivedExecutionConfig archivedExecutionConfig =
                new ArchivedExecutionConfigBuilder().build();

        archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setArchivedExecutionConfig(archivedExecutionConfig)
                        .setStreamGraphJson(expectedStreamGraphJson)
                        .build();
        jobDetailsHandler =
                new JobDetailsHandler(
                        leaderRetriever,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobDetailsHeaders.getInstance(),
                        new DefaultExecutionGraphCache(
                                restHandlerConfiguration.getTimeout(),
                                Duration.ofMillis(restHandlerConfiguration.getRefreshInterval())),
                        Executors.directExecutor(),
                        metricFetcher);
        handlerRequest = createRequest(archivedExecutionGraph.getJobID());
    }

    @Test
    void testGetJobDetailsWithStreamGraphJson() throws RestHandlerException {
        JobDetailsInfo jobDetailsInfo =
                jobDetailsHandler.handleRequest(handlerRequest, archivedExecutionGraph);
        assertThat(jobDetailsInfo.getStreamGraphJson())
                .isEqualTo(new JobPlanInfo.RawJson(expectedStreamGraphJson).toString());
    }
}
