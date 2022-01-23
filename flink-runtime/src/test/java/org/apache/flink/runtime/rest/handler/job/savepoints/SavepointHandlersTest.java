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

package org.apache.flink.runtime.rest.handler.job.savepoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.UnknownOperationKeyException;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.RestMatchers;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.rest.handler.job.savepoints.SavepointTestUtilities.getResultIfKeyMatches;
import static org.apache.flink.runtime.rest.handler.job.savepoints.SavepointTestUtilities.setReferenceToOperationKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Test for {@link SavepointHandlers}. */
public class SavepointHandlersTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10);

    private static final JobID JOB_ID = new JobID();

    private static final String COMPLETED_SAVEPOINT_EXTERNAL_POINTER =
            "/tmp/savepoint-0d2fb9-8d5e0106041a";

    private static final String DEFAULT_REQUESTED_SAVEPOINT_TARGET_DIRECTORY = "/tmp";

    private SavepointHandlers.SavepointTriggerHandler savepointTriggerHandler;

    private SavepointHandlers.SavepointStatusHandler savepointStatusHandler;

    private GatewayRetriever<RestfulGateway> leaderRetriever;

    @Before
    public void setUp() throws Exception {
        leaderRetriever = () -> CompletableFuture.completedFuture(null);

        final SavepointHandlers savepointHandlers = new SavepointHandlers(null);
        savepointTriggerHandler =
                savepointHandlers
                .new SavepointTriggerHandler(leaderRetriever, TIMEOUT, Collections.emptyMap());

        savepointStatusHandler =
                new SavepointHandlers.SavepointStatusHandler(
                        leaderRetriever, TIMEOUT, Collections.emptyMap());
    }

    @Test
    public void testSavepointCompletedSuccessfully() throws Exception {
        final OperationResult<String> successfulResult =
                OperationResult.success(COMPLETED_SAVEPOINT_EXTERNAL_POINTER);
        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerSavepointFunction(setReferenceToOperationKey(keyReference))
                        .setGetSavepointStatusFunction(
                                getResultIfKeyMatches(successfulResult, keyReference))
                        .build();

        final TriggerId triggerId =
                savepointTriggerHandler
                        .handleRequest(triggerSavepointRequest(), testingRestfulGateway)
                        .get()
                        .getTriggerId();

        AsynchronousOperationResult<SavepointInfo> savepointResponseBody;
        savepointResponseBody =
                savepointStatusHandler
                        .handleRequest(savepointStatusRequest(triggerId), testingRestfulGateway)
                        .get();

        assertThat(savepointResponseBody.queueStatus().getId(), equalTo(QueueStatus.Id.COMPLETED));
        assertThat(savepointResponseBody.resource(), notNullValue());
        assertThat(
                savepointResponseBody.resource().getLocation(),
                equalTo(COMPLETED_SAVEPOINT_EXTERNAL_POINTER));
    }

    @Test
    public void testTriggerSavepointWithDefaultDirectory() throws Exception {
        final CompletableFuture<String> targetDirectoryFuture = new CompletableFuture<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerSavepointFunction(
                                (AsynchronousJobOperationKey operationKey,
                                        String targetDirectory) -> {
                                    targetDirectoryFuture.complete(targetDirectory);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();
        final String defaultSavepointDir = "/other/dir";
        final SavepointHandlers savepointHandlers = new SavepointHandlers(defaultSavepointDir);
        final SavepointHandlers.SavepointTriggerHandler savepointTriggerHandler =
                savepointHandlers
                .new SavepointTriggerHandler(leaderRetriever, TIMEOUT, Collections.emptyMap());

        savepointTriggerHandler
                .handleRequest(triggerSavepointRequestWithDefaultDirectory(), testingRestfulGateway)
                .get();

        assertThat(targetDirectoryFuture.get(), equalTo(defaultSavepointDir));
    }

    @Test
    public void testTriggerSavepointNoDirectory() throws Exception {
        TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerSavepointFunction(
                                (AsynchronousJobOperationKey operationKey, String directory) ->
                                        CompletableFuture.completedFuture(Acknowledge.get()))
                        .build();

        try {
            savepointTriggerHandler
                    .handleRequest(
                            triggerSavepointRequestWithDefaultDirectory(), testingRestfulGateway)
                    .get();
            fail("Expected exception not thrown.");
        } catch (RestHandlerException rhe) {
            assertThat(
                    rhe.getMessage(),
                    equalTo(
                            "Config key [state.savepoints.dir] is not set. "
                                    + "Property [target-directory] must be provided."));
            assertThat(rhe.getHttpResponseStatus(), equalTo(HttpResponseStatus.BAD_REQUEST));
        }
    }

    @Test
    public void testSavepointCompletedWithException() throws Exception {
        final OperationResult<String> failedResult =
                OperationResult.failure(new RuntimeException("expected"));
        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerSavepointFunction(setReferenceToOperationKey(keyReference))
                        .setGetSavepointStatusFunction(
                                getResultIfKeyMatches(failedResult, keyReference))
                        .build();

        final TriggerId triggerId =
                savepointTriggerHandler
                        .handleRequest(triggerSavepointRequest(), testingRestfulGateway)
                        .get()
                        .getTriggerId();

        final AsynchronousOperationResult<SavepointInfo> savepointResponseBody =
                savepointStatusHandler
                        .handleRequest(savepointStatusRequest(triggerId), testingRestfulGateway)
                        .get();

        assertThat(savepointResponseBody.queueStatus().getId(), equalTo(QueueStatus.Id.COMPLETED));
        assertThat(savepointResponseBody.resource(), notNullValue());
        assertThat(savepointResponseBody.resource().getFailureCause(), notNullValue());
        final Throwable savepointError =
                savepointResponseBody
                        .resource()
                        .getFailureCause()
                        .deserializeError(ClassLoader.getSystemClassLoader());
        assertThat(savepointError.getMessage(), equalTo("expected"));
        assertThat(savepointError, instanceOf(RuntimeException.class));
    }

    @Test
    public void testProvidedTriggerId() throws Exception {
        final OperationResult<String> successfulResult =
                OperationResult.success(COMPLETED_SAVEPOINT_EXTERNAL_POINTER);
        AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerSavepointFunction(setReferenceToOperationKey(keyReference))
                        .setGetSavepointStatusFunction(
                                getResultIfKeyMatches(successfulResult, keyReference))
                        .build();

        final TriggerId providedTriggerId = new TriggerId();

        final TriggerId returnedTriggerId =
                savepointTriggerHandler
                        .handleRequest(
                                triggerSavepointRequest(
                                        DEFAULT_REQUESTED_SAVEPOINT_TARGET_DIRECTORY,
                                        providedTriggerId),
                                testingRestfulGateway)
                        .get()
                        .getTriggerId();

        assertEquals(providedTriggerId, returnedTriggerId);

        AsynchronousOperationResult<SavepointInfo> savepointResponseBody;
        savepointResponseBody =
                savepointStatusHandler
                        .handleRequest(
                                savepointStatusRequest(providedTriggerId), testingRestfulGateway)
                        .get();

        assertThat(savepointResponseBody.queueStatus().getId(), equalTo(QueueStatus.Id.COMPLETED));
        assertThat(savepointResponseBody.resource(), notNullValue());
        assertThat(
                savepointResponseBody.resource().getLocation(),
                equalTo(COMPLETED_SAVEPOINT_EXTERNAL_POINTER));
    }

    @Test
    public void testQueryStatusOfUnknownOperationReturnsError()
            throws HandlerRequestException, RestHandlerException {
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setGetSavepointStatusFunction(
                                key ->
                                        FutureUtils.completedExceptionally(
                                                new UnknownOperationKeyException(key)))
                        .build();

        final CompletableFuture<AsynchronousOperationResult<SavepointInfo>> statusFuture =
                savepointStatusHandler.handleRequest(
                        savepointStatusRequest(new TriggerId()), testingRestfulGateway);

        assertThat(statusFuture, RestMatchers.respondsWithError(HttpResponseStatus.NOT_FOUND));
    }

    private static HandlerRequest<SavepointTriggerRequestBody> triggerSavepointRequest()
            throws HandlerRequestException {
        return triggerSavepointRequest(DEFAULT_REQUESTED_SAVEPOINT_TARGET_DIRECTORY, null);
    }

    private static HandlerRequest<SavepointTriggerRequestBody>
            triggerSavepointRequestWithDefaultDirectory() throws HandlerRequestException {
        return triggerSavepointRequest(null, null);
    }

    private static HandlerRequest<SavepointTriggerRequestBody> triggerSavepointRequest(
            @Nullable final String targetDirectory, @Nullable TriggerId triggerId)
            throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                new SavepointTriggerRequestBody(targetDirectory, false, triggerId),
                new SavepointTriggerMessageParameters(),
                Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private static HandlerRequest<EmptyRequestBody> savepointStatusRequest(
            final TriggerId triggerId) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
        pathParameters.put(TriggerIdPathParameter.KEY, triggerId.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new SavepointStatusMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
