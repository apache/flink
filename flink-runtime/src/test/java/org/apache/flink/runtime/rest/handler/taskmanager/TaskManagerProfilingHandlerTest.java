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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.cluster.ProfilingRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerProfilingHeaders;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link TaskManagerProfilingHandler}. */
class TaskManagerProfilingHandlerTest {

    private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();
    private TestingResourceManagerGateway resourceManagerGateway;
    private TaskManagerProfilingHandler taskManagerProfilingHandler;
    private HandlerRequest<ProfilingRequestBody> handlerRequest;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws HandlerRequestException {
        Configuration clusterConfiguration = new Configuration();
        clusterConfiguration.set(RestOptions.MAX_PROFILING_HISTORY_SIZE, 3);
        clusterConfiguration.set(RestOptions.PROFILING_RESULT_DIR, tempDir.toString());
        resourceManagerGateway = new TestingResourceManagerGateway();
        taskManagerProfilingHandler =
                new TaskManagerProfilingHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        TaskManagerProfilingHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway),
                        clusterConfiguration);
        handlerRequest = createRequest(EXPECTED_TASK_MANAGER_ID);
    }

    @Test
    void testGetTaskManagerProfiling() throws Exception {
        ProfilingInfo profilingInfo = ProfilingInfo.create(30, ProfilingInfo.ProfilingMode.ITIMER);
        resourceManagerGateway.setRequestProfilingFunction(
                EXPECTED_TASK_MANAGER_ID -> CompletableFuture.completedFuture(profilingInfo));
        ProfilingInfo profilingInfoResp =
                taskManagerProfilingHandler
                        .handleRequest(handlerRequest, resourceManagerGateway)
                        .get();
        assertThat(profilingInfoResp).isEqualTo(profilingInfo);
    }

    @Test
    void testGetTaskManagerProfilingForUnknownTaskExecutorException() throws Exception {
        resourceManagerGateway.setRequestProfilingListFunction(
                EXPECTED_TASK_MANAGER_ID ->
                        FutureUtils.completedExceptionally(
                                new UnknownTaskExecutorException(EXPECTED_TASK_MANAGER_ID)));
        try {
            taskManagerProfilingHandler.handleRequest(handlerRequest, resourceManagerGateway).get();
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            assertThat(cause).isInstanceOf(UnknownTaskExecutorException.class);

            final UnknownTaskExecutorException unknownTaskExecutorException =
                    (UnknownTaskExecutorException) cause;
            assertThat(unknownTaskExecutorException.getMessage())
                    .contains("No TaskExecutor registered under " + EXPECTED_TASK_MANAGER_ID);
        }
    }

    private static HandlerRequest<ProfilingRequestBody> createRequest(ResourceID taskManagerId)
            throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(TaskManagerIdPathParameter.KEY, taskManagerId.toString());
        Map<String, List<String>> queryParameters = Collections.emptyMap();
        ProfilingRequestBody requestBody =
                new ProfilingRequestBody(ProfilingInfo.ProfilingMode.ITIMER, 10);
        return HandlerRequest.resolveParametersAndCreate(
                requestBody,
                new TaskManagerMessageParameters(),
                pathParameters,
                queryParameters,
                Collections.emptyList());
    }
}
