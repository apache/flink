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

package org.apache.flink.runtime.rest.handler.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkApplicationNotFoundException;
import org.apache.flink.runtime.messages.FlinkApplicationTerminatedWithoutCancellationException;
import org.apache.flink.runtime.rest.RestMatchers;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.ApplicationCancellationHeaders;
import org.apache.flink.runtime.rest.messages.ApplicationCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for the {@link ApplicationCancellationHandler}. */
class ApplicationCancellationHandlerTest {
    @Test
    void testSuccessfulCancellation() throws Exception {
        testResponse(
                applicationId -> CompletableFuture.completedFuture(Acknowledge.get()),
                CompletableFuture::get);
    }

    @Test
    void testErrorCodeForNonCanceledTerminalApplication() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                applicationId ->
                        FutureUtils.completedExceptionally(
                                new FlinkApplicationTerminatedWithoutCancellationException(
                                        applicationId, ApplicationState.FINISHED)),
                HttpResponseStatus.CONFLICT);
    }

    @Test
    void testErrorCodeForTimeout() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                applicationId -> FutureUtils.completedExceptionally(new TimeoutException()),
                HttpResponseStatus.REQUEST_TIMEOUT);
    }

    @Test
    void testErrorCodeForUnknownApplication() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                applicationId ->
                        FutureUtils.completedExceptionally(
                                new FlinkApplicationNotFoundException(applicationId)),
                HttpResponseStatus.NOT_FOUND);
    }

    @Test
    void testErrorCodeForUnknownError() throws Exception {
        testResponseCodeOnFailedDispatcherCancellationResponse(
                applicationId -> FutureUtils.completedExceptionally(new RuntimeException()),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private static void testResponseCodeOnFailedDispatcherCancellationResponse(
            Function<ApplicationID, CompletableFuture<Acknowledge>> cancelApplicationFunction,
            HttpResponseStatus expectedErrorCode)
            throws Exception {

        testResponse(
                cancelApplicationFunction,
                cancellationFuture ->
                        assertThat(cancellationFuture)
                                .satisfies(
                                        matching(
                                                RestMatchers.respondsWithError(
                                                        expectedErrorCode))));
    }

    private static void testResponse(
            Function<ApplicationID, CompletableFuture<Acknowledge>> cancelApplicationFunction,
            ThrowingConsumer<CompletableFuture<EmptyResponseBody>, Exception> assertion)
            throws Exception {
        final RestfulGateway gateway = createGateway(cancelApplicationFunction);

        final ApplicationCancellationHandler applicationCancellationHandler =
                createHandler(gateway);

        final ApplicationCancellationMessageParameters messageParameters =
                applicationCancellationHandler
                        .getMessageHeaders()
                        .getUnresolvedMessageParameters()
                        .resolveApplicationId(new ApplicationID());

        final CompletableFuture<EmptyResponseBody> cancellationFuture =
                applicationCancellationHandler.handleRequest(
                        HandlerRequest.create(EmptyRequestBody.getInstance(), messageParameters),
                        gateway);

        assertion.accept(cancellationFuture);
    }

    private static RestfulGateway createGateway(
            Function<ApplicationID, CompletableFuture<Acknowledge>> cancelApplicationFunction) {
        return new TestingRestfulGateway.Builder()
                .setCancelApplicationFunction(cancelApplicationFunction)
                .build();
    }

    private static ApplicationCancellationHandler createHandler(RestfulGateway gateway) {
        return new ApplicationCancellationHandler(
                () -> CompletableFuture.completedFuture(gateway),
                TestingUtils.TIMEOUT,
                Collections.emptyMap(),
                ApplicationCancellationHeaders.getInstance());
    }
}
