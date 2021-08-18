/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Test for the {@link TaskManagerLogListHandler}. */
public class TaskManagerLogListHandlerTest extends TestLogger {

    private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();
    private TestingResourceManagerGateway resourceManagerGateway;
    private TaskManagerLogListHandler taskManagerLogListHandler;
    private HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> handlerRequest;

    @Before
    public void setUp() throws HandlerRequestException {
        resourceManagerGateway = new TestingResourceManagerGateway();
        taskManagerLogListHandler =
                new TaskManagerLogListHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        TaskManagerLogsHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway));
        handlerRequest = createRequest(EXPECTED_TASK_MANAGER_ID);
    }

    @Test
    public void testGetTaskManagerLogsList() throws Exception {
        List<LogInfo> logsList =
                Arrays.asList(
                        new LogInfo("taskmanager.log", 1024L),
                        new LogInfo("taskmanager.out", 1024L),
                        new LogInfo("taskmanager-2.out", 1024L));
        resourceManagerGateway.setRequestTaskManagerLogListFunction(
                EXPECTED_TASK_MANAGER_ID -> CompletableFuture.completedFuture(logsList));
        LogListInfo logListInfo =
                taskManagerLogListHandler
                        .handleRequest(handlerRequest, resourceManagerGateway)
                        .get();
        assertThat(logListInfo.getLogInfos(), hasSize(logsList.size()));
    }

    @Test
    public void testGetTaskManagerLogsListForUnknownTaskExecutorException() throws Exception {
        resourceManagerGateway.setRequestTaskManagerLogListFunction(
                EXPECTED_TASK_MANAGER_ID ->
                        FutureUtils.completedExceptionally(
                                new UnknownTaskExecutorException(EXPECTED_TASK_MANAGER_ID)));
        try {
            taskManagerLogListHandler.handleRequest(handlerRequest, resourceManagerGateway).get();
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            assertThat(cause, is(instanceOf(RestHandlerException.class)));

            final RestHandlerException restHandlerException = (RestHandlerException) cause;
            assertThat(
                    restHandlerException.getHttpResponseStatus(),
                    is(equalTo(HttpResponseStatus.NOT_FOUND)));
            assertThat(
                    restHandlerException.getMessage(),
                    containsString("Could not find TaskExecutor " + EXPECTED_TASK_MANAGER_ID));
        }
    }

    private static HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> createRequest(
            ResourceID taskManagerId) throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(TaskManagerIdPathParameter.KEY, taskManagerId.toString());
        Map<String, List<String>> queryParameters = Collections.emptyMap();

        return new HandlerRequest<>(
                EmptyRequestBody.getInstance(),
                new TaskManagerMessageParameters(),
                pathParameters,
                queryParameters);
    }
}
