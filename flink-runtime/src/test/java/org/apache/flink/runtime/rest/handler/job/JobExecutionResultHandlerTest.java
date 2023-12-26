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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JobExecutionResultHandler}. */
class JobExecutionResultHandlerTest {

    private static final JobID TEST_JOB_ID = new JobID();

    private JobExecutionResultHandler jobExecutionResultHandler;

    private HandlerRequest<EmptyRequestBody> testRequest;

    @BeforeEach
    void setUp() throws Exception {
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder().build();

        jobExecutionResultHandler =
                new JobExecutionResultHandler(
                        () -> CompletableFuture.completedFuture(testingRestfulGateway),
                        Time.seconds(10),
                        Collections.emptyMap());

        testRequest =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new JobMessageParameters(),
                        Collections.singletonMap("jobid", TEST_JOB_ID.toString()),
                        Collections.emptyMap(),
                        Collections.emptyList());
    }

    @Test
    void testResultInProgress() throws Exception {
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setRequestJobStatusFunction(
                                jobId -> CompletableFuture.completedFuture(JobStatus.RUNNING))
                        .build();

        final JobExecutionResultResponseBody responseBody =
                jobExecutionResultHandler.handleRequest(testRequest, testingRestfulGateway).get();

        assertThat(responseBody.getStatus().getId()).isEqualTo(QueueStatus.Id.IN_PROGRESS);
    }

    @Test
    void testCompletedResult() throws Exception {
        final JobStatus jobStatus = JobStatus.FINISHED;
        final ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(TEST_JOB_ID)
                        .setState(jobStatus)
                        .build();

        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setRequestJobStatusFunction(
                                jobId -> {
                                    assertThat(jobId).isEqualTo(TEST_JOB_ID);
                                    return CompletableFuture.completedFuture(jobStatus);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    assertThat(jobId).isEqualTo(TEST_JOB_ID);
                                    return CompletableFuture.completedFuture(
                                            JobResult.createFrom(executionGraph));
                                })
                        .build();

        final JobExecutionResultResponseBody responseBody =
                jobExecutionResultHandler.handleRequest(testRequest, testingRestfulGateway).get();

        assertThat(responseBody.getStatus().getId()).isEqualTo(QueueStatus.Id.COMPLETED);
        assertThat(responseBody.getJobExecutionResult()).isNotNull();
    }

    @Test
    void testPropagateFlinkJobNotFoundExceptionAsRestHandlerException() throws Exception {
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setRequestJobStatusFunction(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new FlinkJobNotFoundException(jobId)))
                        .build();

        assertThatFuture(
                        jobExecutionResultHandler.handleRequest(testRequest, testingRestfulGateway))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestHandlerException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.NOT_FOUND));
    }
}
