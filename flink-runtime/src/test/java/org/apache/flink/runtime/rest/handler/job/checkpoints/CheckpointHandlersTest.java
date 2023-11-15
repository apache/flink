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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.CheckpointType;
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
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CheckpointHandlers}. */
class CheckpointHandlersTest {

    private static final Time TIMEOUT = Time.seconds(10);

    private static final JobID JOB_ID = new JobID();

    private static final Long COMPLETED_CHECKPOINT_ID = 123456L;

    private static CheckpointHandlers.CheckpointTriggerHandler checkpointTriggerHandler;

    private static CheckpointHandlers.CheckpointStatusHandler checkpointStatusHandler;

    @BeforeAll
    static void setUp() throws Exception {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);

        checkpointTriggerHandler =
                new CheckpointHandlers.CheckpointTriggerHandler(
                        leaderRetriever, TIMEOUT, Collections.emptyMap());

        checkpointStatusHandler =
                new CheckpointHandlers.CheckpointStatusHandler(
                        leaderRetriever, TIMEOUT, Collections.emptyMap());
    }

    @Test
    void testCheckpointTriggerCompletedSuccessfully() throws Exception {
        final OperationResult<Long> successfulResult =
                OperationResult.success(COMPLETED_CHECKPOINT_ID);
        final CompletableFuture<CheckpointType> checkpointPropertiesFuture =
                new CompletableFuture<>();

        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerCheckpointFunction(
                                (AsynchronousJobOperationKey key,
                                        CheckpointType checkpointType) -> {
                                    keyReference.set(key);
                                    checkpointPropertiesFuture.complete(checkpointType);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setGetCheckpointStatusFunction(
                                (AsynchronousJobOperationKey operationKey) -> {
                                    if (operationKey.equals(keyReference.get())) {
                                        return CompletableFuture.completedFuture(successfulResult);
                                    }
                                    throw new RuntimeException(
                                            "Expected operation key "
                                                    + keyReference.get()
                                                    + ", but received "
                                                    + operationKey);
                                })
                        .build();

        final CheckpointType checkpointType = CheckpointType.FULL;

        final TriggerId triggerId =
                checkpointTriggerHandler
                        .handleRequest(
                                triggerCheckpointRequest(checkpointType, null),
                                testingRestfulGateway)
                        .get()
                        .getTriggerId();

        final AsynchronousOperationResult<CheckpointInfo> checkpointTriggerResponseBody =
                checkpointStatusHandler
                        .handleRequest(
                                checkpointTriggerStatusRequest(triggerId), testingRestfulGateway)
                        .get();

        assertThat(checkpointTriggerResponseBody.queueStatus().getId())
                .isEqualTo(QueueStatus.Id.COMPLETED);
        assertThat(checkpointTriggerResponseBody.resource()).isNotNull();
        assertThat(checkpointTriggerResponseBody.resource().getCheckpointId())
                .isEqualTo(COMPLETED_CHECKPOINT_ID);
        assertThat(checkpointPropertiesFuture.get()).isEqualTo(CheckpointType.FULL);
    }

    @Test
    void testTriggerCheckpointNoCheckpointType() throws Exception {
        final OperationResult<Long> successfulResult =
                OperationResult.success(COMPLETED_CHECKPOINT_ID);
        final CompletableFuture<CheckpointType> checkpointTypeFuture = new CompletableFuture<>();

        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerCheckpointFunction(
                                (AsynchronousJobOperationKey key,
                                        CheckpointType checkpointType) -> {
                                    keyReference.set(key);
                                    checkpointTypeFuture.complete(checkpointType);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setGetCheckpointStatusFunction(
                                (AsynchronousJobOperationKey operationKey) -> {
                                    if (operationKey.equals(keyReference.get())) {
                                        return CompletableFuture.completedFuture(successfulResult);
                                    }
                                    throw new RuntimeException(
                                            "Expected operation key "
                                                    + keyReference.get()
                                                    + ", but received "
                                                    + operationKey);
                                })
                        .build();

        final TriggerId triggerId =
                checkpointTriggerHandler
                        .handleRequest(triggerCheckpointRequest(null, null), testingRestfulGateway)
                        .get()
                        .getTriggerId();

        AsynchronousOperationResult<CheckpointInfo> checkpointTriggerResponseBody;
        checkpointTriggerResponseBody =
                checkpointStatusHandler
                        .handleRequest(
                                checkpointTriggerStatusRequest(triggerId), testingRestfulGateway)
                        .get();

        assertThat(checkpointTriggerResponseBody.queueStatus().getId())
                .isEqualTo(QueueStatus.Id.COMPLETED);
        assertThat(checkpointTriggerResponseBody.resource()).isNotNull();
        assertThat(checkpointTriggerResponseBody.resource().getCheckpointId())
                .isEqualTo(COMPLETED_CHECKPOINT_ID);
        assertThat(checkpointTypeFuture.get()).isEqualTo(CheckpointType.DEFAULT);
    }

    @Test
    void testCheckpointCompletedWithException() throws Exception {
        final OperationResult<Long> failedResult =
                OperationResult.failure(new RuntimeException("expected"));

        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerCheckpointFunction(
                                (AsynchronousJobOperationKey key,
                                        CheckpointType checkpointType) -> {
                                    keyReference.set(key);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setGetCheckpointStatusFunction(
                                (AsynchronousJobOperationKey operationKey) -> {
                                    if (operationKey.equals(keyReference.get())) {
                                        return CompletableFuture.completedFuture(failedResult);
                                    }
                                    throw new RuntimeException(
                                            "Expected operation key "
                                                    + keyReference.get()
                                                    + ", but received "
                                                    + operationKey);
                                })
                        .build();

        final TriggerId triggerId =
                checkpointTriggerHandler
                        .handleRequest(triggerCheckpointRequest(null, null), testingRestfulGateway)
                        .get()
                        .getTriggerId();

        AsynchronousOperationResult<CheckpointInfo> checkpointTriggerResponseBody;
        checkpointTriggerResponseBody =
                checkpointStatusHandler
                        .handleRequest(
                                checkpointTriggerStatusRequest(triggerId), testingRestfulGateway)
                        .get();

        assertThat(checkpointTriggerResponseBody.queueStatus().getId())
                .isEqualTo(QueueStatus.Id.COMPLETED);
        assertThat(checkpointTriggerResponseBody.resource()).isNotNull();
        assertThat(checkpointTriggerResponseBody.resource().getFailureCause()).isNotNull();

        final Throwable checkpointError =
                checkpointTriggerResponseBody
                        .resource()
                        .getFailureCause()
                        .deserializeError(ClassLoader.getSystemClassLoader());
        assertThat(checkpointError.getMessage()).matches("expected");
        assertThat(checkpointError).isInstanceOf(RuntimeException.class);
    }

    @Test
    void testProvidedTriggerId() throws Exception {
        final OperationResult<Long> successfulResult =
                OperationResult.success(COMPLETED_CHECKPOINT_ID);
        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerCheckpointFunction(
                                (AsynchronousJobOperationKey key,
                                        CheckpointType checkpointType) -> {
                                    keyReference.set(key);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setGetCheckpointStatusFunction(
                                (AsynchronousJobOperationKey operationKey) -> {
                                    if (operationKey.equals(keyReference.get())) {
                                        return CompletableFuture.completedFuture(successfulResult);
                                    }
                                    throw new RuntimeException(
                                            "Expected operation key "
                                                    + keyReference.get()
                                                    + ", but received "
                                                    + operationKey);
                                })
                        .build();

        final TriggerId providedTriggerId = new TriggerId();

        final TriggerId returnedTriggerId =
                checkpointTriggerHandler
                        .handleRequest(
                                triggerCheckpointRequest(CheckpointType.FULL, providedTriggerId),
                                testingRestfulGateway)
                        .get()
                        .getTriggerId();

        assertThat(providedTriggerId).isEqualTo(returnedTriggerId);

        AsynchronousOperationResult<CheckpointInfo> checkpointTriggerResponseBody;
        checkpointTriggerResponseBody =
                checkpointStatusHandler
                        .handleRequest(
                                checkpointTriggerStatusRequest(providedTriggerId),
                                testingRestfulGateway)
                        .get();

        assertThat(checkpointTriggerResponseBody.queueStatus().getId())
                .isEqualTo(QueueStatus.Id.COMPLETED);
        assertThat(checkpointTriggerResponseBody.resource()).isNotNull();
        assertThat(checkpointTriggerResponseBody.resource().getCheckpointId())
                .isEqualTo(COMPLETED_CHECKPOINT_ID);
    }

    @Test
    void testQueryStatusOfUnknownOperationReturnsError()
            throws HandlerRequestException, RestHandlerException {

        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setGetCheckpointStatusFunction(
                                key ->
                                        FutureUtils.completedExceptionally(
                                                new UnknownOperationKeyException(key)))
                        .build();

        final CompletableFuture<AsynchronousOperationResult<CheckpointInfo>> statusFuture =
                checkpointStatusHandler.handleRequest(
                        checkpointTriggerStatusRequest(new TriggerId()), testingRestfulGateway);

        assertThat(statusFuture)
                .matches(RestMatchers.respondsWithError(HttpResponseStatus.NOT_FOUND)::matches);
    }

    private static HandlerRequest<CheckpointTriggerRequestBody> triggerCheckpointRequest(
            final CheckpointType checkpointType, @Nullable final TriggerId triggerId)
            throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                new CheckpointTriggerRequestBody(checkpointType, triggerId),
                new CheckpointTriggerMessageParameters(),
                Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private static HandlerRequest<EmptyRequestBody> checkpointTriggerStatusRequest(
            final TriggerId triggerId) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
        pathParameters.put(TriggerIdPathParameter.KEY, triggerId.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new CheckpointStatusMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
