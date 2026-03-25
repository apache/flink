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

package org.apache.flink.runtime.rest.handler.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleApplicationsDetails;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.ApplicationsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ApplicationsOverviewHandler}. */
class ApplicationsOverviewHandlerTest {

    private ApplicationsOverviewHandler handler;
    private HandlerRequest<EmptyRequestBody> handlerRequest;
    private ArchivedApplication archivedApplication;
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
        handler =
                new ApplicationsOverviewHandler(
                        leaderRetriever,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        ApplicationsOverviewHeaders.getInstance());

        handlerRequest = createRequest();

        archivedApplication =
                new ArchivedApplication(
                        ApplicationID.generate(),
                        "Test Application",
                        ApplicationState.FINISHED,
                        new long[] {1L, 1L, 1L, 1L, 1L, 1L, 1L},
                        Collections.emptyMap(),
                        Collections.emptyList());

        testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setRequestMultipleApplicationDetailsSupplier(
                                () ->
                                        CompletableFuture.completedFuture(
                                                new MultipleApplicationsDetails(
                                                        Collections.singleton(
                                                                ApplicationDetails
                                                                        .fromArchivedApplication(
                                                                                archivedApplication)))))
                        .build();
    }

    @Test
    void testGetApplicationsOverview() throws Exception {
        MultipleApplicationsDetails multiDetails =
                handler.handleRequest(handlerRequest, testingRestfulGateway).get();
        assertThat(multiDetails.getApplications()).hasSize(1);
        assertThat(multiDetails.getApplications().iterator().next())
                .isEqualTo(ApplicationDetails.fromArchivedApplication(archivedApplication));
    }
}
