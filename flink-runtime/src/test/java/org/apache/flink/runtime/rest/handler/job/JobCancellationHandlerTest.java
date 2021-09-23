/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.rest.RestMatchers;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobCancellationHeaders;
import org.apache.flink.runtime.rest.messages.JobCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the {@link JobCancellationHandler}. */
public class JobCancellationHandlerTest extends TestLogger {
    @Test
    public void testSuccessfulCancellation() throws Exception {
        testResponse(
                jobId -> CompletableFuture.completedFuture(Acknowledge.get()),
                CompletableFuture::get);
    }

    @Test
    public void testErrorCodeForNonCanceledTerminalJob() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                jobId ->
                        FutureUtils.completedExceptionally(
                                new FlinkJobTerminatedWithoutCancellationException(
                                        jobId, JobStatus.FINISHED)),
                HttpResponseStatus.CONFLICT);
    }

    @Test
    public void testErrorCodeForTimeout() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                jobId -> FutureUtils.completedExceptionally(new TimeoutException()),
                HttpResponseStatus.REQUEST_TIMEOUT);
    }

    @Test
    public void testErrorCodeForUnknownJob() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                jobId -> FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)),
                HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void testErrorCodeForUnknownError() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                jobId -> FutureUtils.completedExceptionally(new RuntimeException()),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private static void testResponseCodeOnFailedDispatcherCancellationResponse(
            Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
            HttpResponseStatus expectedErrorCode)
            throws Exception {

        testResponse(
                cancelJobFunction,
                cancellationFuture ->
                        assertThat(
                                cancellationFuture,
                                RestMatchers.respondsWithError(expectedErrorCode)));
    }

    private static void testResponse(
            Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
            ThrowingConsumer<CompletableFuture<EmptyResponseBody>, Exception> assertion)
            throws Exception {
        final RestfulGateway gateway = createGateway(cancelJobFunction);

        final JobCancellationHandler jobCancellationHandler = createHandler(gateway);

        final JobCancellationMessageParameters messageParameters =
                jobCancellationHandler
                        .getMessageHeaders()
                        .getUnresolvedMessageParameters()
                        .resolveJobId(new JobID());

        final CompletableFuture<EmptyResponseBody> cancellationFuture =
                jobCancellationHandler.handleRequest(
                        HandlerRequest.create(EmptyRequestBody.getInstance(), messageParameters),
                        gateway);

        assertion.accept(cancellationFuture);
    }

    private static RestfulGateway createGateway(
            Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction) {
        return new TestingRestfulGateway.Builder().setCancelJobFunction(cancelJobFunction).build();
    }

    private static JobCancellationHandler createHandler(RestfulGateway gateway) {
        return new JobCancellationHandler(
                () -> CompletableFuture.completedFuture(gateway),
                Time.hours(1),
                Collections.emptyMap(),
                JobCancellationHeaders.getInstance(),
                TerminationModeQueryParameter.TerminationMode.CANCEL);
    }
}
