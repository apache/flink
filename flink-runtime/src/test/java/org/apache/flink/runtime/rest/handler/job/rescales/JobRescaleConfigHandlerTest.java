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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleConfigHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleConfigInfo;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/*** Test for {@link JobRescaleConfigHandler}. */
class JobRescaleConfigHandlerTest {

    @Test
    void testGetJobRescaleConfigInfo() throws HandlerRequestException, RestHandlerException {
        JobRescaleConfigHandler testInstance =
                new JobRescaleConfigHandler(
                        CompletableFuture::new,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobRescaleConfigHeaders.getInstance(),
                        new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                        Executors.directExecutor());
        final JobID jobId = new JobID();
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new JobMessageParameters(),
                        pathParameters,
                        Collections.emptyMap(),
                        Collections.emptyList());

        // Test for null on JobRescaleConfigInfo.
        ExecutionGraphInfo executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().setJobRescaleConfigInfo(null).build());
        assertThat(
                        testInstance.handleRequest(
                                request, executionGraphInfo.getArchivedExecutionGraph()))
                .isNull();

        // Test for nonnull on JobRescaleConfigInfo.
        JobRescaleConfigInfo jobRescaleConfigInfo =
                new JobRescaleConfigInfo(
                        1, SchedulerExecutionMode.REACTIVE, 1L, 1L, 1L, 1L, 1L, 1L, 1);
        executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobRescaleConfigInfo(jobRescaleConfigInfo)
                                .build());
        assertThat(
                        testInstance.handleRequest(
                                request, executionGraphInfo.getArchivedExecutionGraph()))
                .isEqualTo(jobRescaleConfigInfo);
    }
}
