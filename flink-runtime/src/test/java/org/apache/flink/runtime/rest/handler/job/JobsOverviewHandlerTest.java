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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionConfigBuilder;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobsOverviewHandler}. */
class JobsOverviewHandlerTest {
    private JobsOverviewHandler jobsOverviewHandler;
    private HandlerRequest<EmptyRequestBody> handlerRequest;
    private AccessExecutionGraph archivedExecutionGraph;
    private TestingRestfulGateway testingRestfulGateway;

    private static HandlerRequest<EmptyRequestBody> createRequest() throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new TaskManagerMessageParameters(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @BeforeEach
    void setUp() throws HandlerRequestException {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);
        final ArchivedExecutionConfig archivedExecutionConfig =
                new ArchivedExecutionConfigBuilder().build();
        jobsOverviewHandler =
                new JobsOverviewHandler(
                        leaderRetriever,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobsOverviewHeaders.getInstance());
        handlerRequest = createRequest();

        archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setArchivedExecutionConfig(archivedExecutionConfig)
                        .setPendingOperatorCounts(1)
                        .build();

        testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setRequestMultipleJobDetailsSupplier(
                                () ->
                                        CompletableFuture.completedFuture(
                                                new MultipleJobsDetails(
                                                        Collections.singleton(
                                                                JobDetails.createDetailsForJob(
                                                                        archivedExecutionGraph)))))
                        .build();
    }

    @Test
    void testGetJobsOverviewWithPendingOperators()
            throws RestHandlerException, ExecutionException, InterruptedException {
        MultipleJobsDetails multiDetails =
                jobsOverviewHandler.handleRequest(handlerRequest, testingRestfulGateway).get();
        assertThat(multiDetails.getJobs()).hasSize(1);
        assertThat(multiDetails.getJobs().iterator().next().getPendingOperators())
                .isEqualTo(archivedExecutionGraph.getPendingOperatorCount());
    }
}
