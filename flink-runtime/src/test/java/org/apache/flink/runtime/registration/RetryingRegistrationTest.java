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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the generic retrying registration class, validating the failure, retry, and back-off
 * behavior.
 */
class RetryingRegistrationTest {

    @RegisterExtension
    public static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private TestingRpcService rpcService;

    @BeforeEach
    void setup() {
        rpcService = new TestingRpcService();
    }

    @AfterEach
    void tearDown() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.closeAsync().get();
        }
    }

    @Test
    void testSimpleSuccessfulRegistration() throws Exception {
        final String testId = "laissez les bon temps roulez";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        // an endpoint that immediately returns success
        ManualResponseTestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(new TestRegistrationSuccess(testId));

        try {
            rpcService.registerGateway(testEndpointAddress, testGateway);

            TestRetryingRegistration registration =
                    new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);
            registration.startRegistration();

            CompletableFuture<
                            RetryingRegistration.RetryingRegistrationResult<
                                    TestRegistrationGateway,
                                    TestRegistrationSuccess,
                                    TestRegistrationRejection>>
                    future = registration.getFuture();
            assertThat(future).isNotNull();

            // multiple accesses return the same future
            assertThat(registration.getFuture()).isEqualTo(future);

            RetryingRegistration.RetryingRegistrationResult<
                            TestRegistrationGateway,
                            TestRegistrationSuccess,
                            TestRegistrationRejection>
                    registrationResponse = future.get(10L, TimeUnit.SECONDS);

            // validate correct invocation and result
            assertThat(registrationResponse.getSuccess().getCorrelationId()).isEqualTo(testId);
            assertThat(testGateway.getInvocations().take().leaderId()).isEqualTo(leaderId);
        } finally {
            testGateway.stop();
        }
    }

    @Test
    void testPropagateFailures() throws Exception {
        final String testExceptionMessage = "testExceptionMessage";

        // RPC service that fails with exception upon the connection
        RpcService rpc = mock(RpcService.class);
        when(rpc.connect(anyString(), any(Class.class)))
                .thenThrow(new RuntimeException(testExceptionMessage));

        TestRetryingRegistration registration =
                new TestRetryingRegistration(rpc, "testaddress", UUID.randomUUID());
        registration.startRegistration();

        CompletableFuture<?> future = registration.getFuture();
        assertThat(future).isDone();

        assertThatThrownBy(future::get)
                .withFailMessage("We expected an ExecutionException.")
                .isInstanceOf(ExecutionException.class)
                .cause()
                .hasMessage(testExceptionMessage);
    }

    @Test
    void testRetryConnectOnFailure() throws Exception {
        final String testId = "laissez les bon temps roulez";
        final UUID leaderId = UUID.randomUUID();

        ScheduledExecutorServiceAdapter executor =
                new ScheduledExecutorServiceAdapter(EXECUTOR_EXTENSION.getExecutor());
        ManualResponseTestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(new TestRegistrationSuccess(testId));

        try {
            // RPC service that fails upon the first connection, but succeeds on the second
            RpcService rpc = mock(RpcService.class);
            when(rpc.connect(anyString(), any(Class.class)))
                    .thenReturn(
                            FutureUtils.completedExceptionally(
                                    new Exception(
                                            "test connect failure")), // first connection attempt
                            // fails
                            CompletableFuture.completedFuture(
                                    testGateway) // second connection attempt succeeds
                            );
            when(rpc.getScheduledExecutor()).thenReturn(executor);

            TestRetryingRegistration registration =
                    new TestRetryingRegistration(rpc, "foobar address", leaderId);

            long start = System.currentTimeMillis();

            registration.startRegistration();

            RetryingRegistration.RetryingRegistrationResult<
                            TestRegistrationGateway,
                            TestRegistrationSuccess,
                            TestRegistrationRejection>
                    registrationResponse = registration.getFuture().get(10L, TimeUnit.SECONDS);

            // measure the duration of the registration --> should be longer than the error delay
            long duration = System.currentTimeMillis() - start;

            assertThat(duration)
                    .withFailMessage(
                            "The registration should have failed the first time. Thus the duration should be longer than at least a single error delay.")
                    .isGreaterThan(TestRetryingRegistration.DELAY_ON_ERROR);

            // validate correct invocation and result
            assertThat(registrationResponse.getSuccess().getCorrelationId()).isEqualTo(testId);
            assertThat(testGateway.getInvocations().take().leaderId()).isEqualTo(leaderId);
        } finally {
            testGateway.stop();
        }
    }

    @Test
    @Timeout(10000)
    void testRetriesOnTimeouts() throws Exception {
        final String testId = "rien ne va plus";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        // an endpoint that immediately returns futures with timeouts before returning a successful
        // future
        ManualResponseTestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(
                        null, // timeout
                        null, // timeout
                        new TestRegistrationSuccess(testId) // success
                        );

        try {
            rpcService.registerGateway(testEndpointAddress, testGateway);

            final long initialTimeout = 20L;
            TestRetryingRegistration registration =
                    new TestRetryingRegistration(
                            rpcService,
                            testEndpointAddress,
                            leaderId,
                            new RetryingRegistrationConfiguration(
                                    initialTimeout,
                                    1000L,
                                    15000L, // make sure that we timeout in case of an error
                                    15000L));

            long started = System.nanoTime();
            registration.startRegistration();

            CompletableFuture<
                            RetryingRegistration.RetryingRegistrationResult<
                                    TestRegistrationGateway,
                                    TestRegistrationSuccess,
                                    TestRegistrationRejection>>
                    future = registration.getFuture();
            RetryingRegistration.RetryingRegistrationResult<
                            TestRegistrationGateway,
                            TestRegistrationSuccess,
                            TestRegistrationRejection>
                    registrationResponse = future.get(10L, TimeUnit.SECONDS);

            long finished = System.nanoTime();
            long elapsedMillis = (finished - started) / 1000000;

            // validate correct invocation and result
            assertThat(registrationResponse.getSuccess().getCorrelationId()).isEqualTo(testId);
            assertThat(testGateway.getInvocations().take().leaderId()).isEqualTo(leaderId);

            // validate that some retry-delay / back-off behavior happened
            assertThat(elapsedMillis)
                    .withFailMessage("retries did not properly back off")
                    .isGreaterThanOrEqualTo(3 * initialTimeout);
        } finally {
            testGateway.stop();
        }
    }

    @Test
    void testFailure() throws Exception {
        final String testId = "qui a coupe le fromage";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        ManualResponseTestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(
                        null, // timeout
                        new RegistrationResponse.Failure(new FlinkException("no reason")),
                        null, // timeout
                        new TestRegistrationSuccess(testId) // success
                        );

        try {
            rpcService.registerGateway(testEndpointAddress, testGateway);

            TestRetryingRegistration registration =
                    new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);

            long started = System.nanoTime();
            registration.startRegistration();

            CompletableFuture<
                            RetryingRegistration.RetryingRegistrationResult<
                                    TestRegistrationGateway,
                                    TestRegistrationSuccess,
                                    TestRegistrationRejection>>
                    future = registration.getFuture();
            RetryingRegistration.RetryingRegistrationResult<
                            TestRegistrationGateway,
                            TestRegistrationSuccess,
                            TestRegistrationRejection>
                    registrationResponse = future.get(10L, TimeUnit.SECONDS);

            long finished = System.nanoTime();
            long elapsedMillis = (finished - started) / 1000000;

            // validate correct invocation and result
            assertThat(registrationResponse.getSuccess().getCorrelationId()).isEqualTo(testId);
            assertThat(testGateway.getInvocations().take().leaderId()).isEqualTo(leaderId);

            // validate that some retry-delay / back-off behavior happened
            assertThat(elapsedMillis)
                    .withFailMessage("retries did not properly back off")
                    .isGreaterThanOrEqualTo(
                            2 * TestRetryingRegistration.INITIAL_TIMEOUT
                                    + TestRetryingRegistration.DELAY_ON_FAILURE);
        } finally {
            testGateway.stop();
        }
    }

    @Test
    void testRegistrationRejection() {
        final TestRegistrationGateway testRegistrationGateway =
                new ManualResponseTestRegistrationGateway(
                        new TestRegistrationRejection(
                                TestRegistrationRejection.RejectionReason.REJECTED));

        rpcService.registerGateway(testRegistrationGateway.getAddress(), testRegistrationGateway);

        final TestRetryingRegistration testRetryingRegistration =
                new TestRetryingRegistration(
                        rpcService, testRegistrationGateway.getAddress(), UUID.randomUUID());

        testRetryingRegistration.startRegistration();

        final RetryingRegistration.RetryingRegistrationResult<
                        TestRegistrationGateway, TestRegistrationSuccess, TestRegistrationRejection>
                response = testRetryingRegistration.getFuture().join();

        assertThat(response.isRejection()).isTrue();
        assertThat(response.getRejection().getRejectionReason())
                .isEqualTo(TestRegistrationRejection.RejectionReason.REJECTED);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRetryOnError() throws Exception {
        final String testId = "Petit a petit, l'oiseau fait son nid";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        // gateway that upon calls first responds with a failure, then with a success
        final Queue<CompletableFuture<RegistrationResponse>> responses = new ArrayDeque<>(2);
        responses.add(FutureUtils.completedExceptionally(new Exception("test exception")));
        responses.add(CompletableFuture.completedFuture(new TestRegistrationSuccess(testId)));

        TestRegistrationGateway testGateway =
                DefaultTestRegistrationGateway.newBuilder()
                        .setRegistrationFunction((uuid, aLong) -> responses.poll())
                        .build();

        rpcService.registerGateway(testEndpointAddress, testGateway);

        TestRetryingRegistration registration =
                new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);

        long started = System.nanoTime();
        registration.startRegistration();

        CompletableFuture<
                        RetryingRegistration.RetryingRegistrationResult<
                                TestRegistrationGateway,
                                TestRegistrationSuccess,
                                TestRegistrationRejection>>
                future = registration.getFuture();
        RetryingRegistration.RetryingRegistrationResult<
                        TestRegistrationGateway, TestRegistrationSuccess, TestRegistrationRejection>
                registrationResponse = future.get(10, TimeUnit.SECONDS);

        long finished = System.nanoTime();
        long elapsedMillis = (finished - started) / 1000000;

        assertThat(registrationResponse.getSuccess().getCorrelationId()).isEqualTo(testId);

        // validate that some retry-delay / back-off behavior happened
        assertThat(elapsedMillis)
                .withFailMessage("retries did not properly back off")
                .isGreaterThanOrEqualTo(TestRetryingRegistration.DELAY_ON_ERROR);
    }

    @Test
    void testCancellation() throws Exception {
        final String testEndpointAddress = "my-test-address";
        final UUID leaderId = UUID.randomUUID();

        CompletableFuture<RegistrationResponse> result = new CompletableFuture<>();
        AtomicInteger registrationCallCounter = new AtomicInteger(0);

        TestRegistrationGateway testGateway =
                DefaultTestRegistrationGateway.newBuilder()
                        .setRegistrationFunction(
                                (uuid, aLong) -> {
                                    registrationCallCounter.incrementAndGet();
                                    return result;
                                })
                        .build();

        rpcService.registerGateway(testEndpointAddress, testGateway);

        TestRetryingRegistration registration =
                new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);
        registration.startRegistration();

        // cancel and fail the current registration attempt
        registration.cancel();
        result.completeExceptionally(new TimeoutException());

        // there should not be a second registration attempt
        assertThat(registrationCallCounter).hasValueLessThanOrEqualTo(1);
    }

    // ------------------------------------------------------------------------
    //  test registration
    // ------------------------------------------------------------------------

    static class TestRegistrationSuccess extends RegistrationResponse.Success {
        private static final long serialVersionUID = 5542698790917150604L;

        private final String correlationId;

        public TestRegistrationSuccess(String correlationId) {
            this.correlationId = correlationId;
        }

        public String getCorrelationId() {
            return correlationId;
        }
    }

    static class TestRegistrationRejection extends RegistrationResponse.Rejection {

        private final RejectionReason rejectionReason;

        /**
         * Creates a new rejection message.
         *
         * @param rejectionReason
         */
        public TestRegistrationRejection(RejectionReason rejectionReason) {
            this.rejectionReason = rejectionReason;
        }

        public RejectionReason getRejectionReason() {
            return rejectionReason;
        }

        enum RejectionReason {
            REJECTED;
        }
    }

    static class TestRetryingRegistration
            extends RetryingRegistration<
                    UUID,
                    TestRegistrationGateway,
                    TestRegistrationSuccess,
                    TestRegistrationRejection> {

        // we use shorter timeouts here to speed up the tests
        static final long INITIAL_TIMEOUT = 20;
        static final long MAX_TIMEOUT = 200;
        static final long DELAY_ON_ERROR = 200;
        static final long DELAY_ON_FAILURE = 200;
        static final RetryingRegistrationConfiguration RETRYING_REGISTRATION_CONFIGURATION =
                new RetryingRegistrationConfiguration(
                        INITIAL_TIMEOUT, MAX_TIMEOUT, DELAY_ON_ERROR, DELAY_ON_FAILURE);

        public TestRetryingRegistration(RpcService rpc, String targetAddress, UUID leaderId) {
            this(rpc, targetAddress, leaderId, RETRYING_REGISTRATION_CONFIGURATION);
        }

        public TestRetryingRegistration(
                RpcService rpc,
                String targetAddress,
                UUID leaderId,
                RetryingRegistrationConfiguration retryingRegistrationConfiguration) {
            super(
                    LoggerFactory.getLogger(RetryingRegistrationTest.class),
                    rpc,
                    "TestEndpoint",
                    TestRegistrationGateway.class,
                    targetAddress,
                    leaderId,
                    retryingRegistrationConfiguration);
        }

        @Override
        protected CompletableFuture<RegistrationResponse> invokeRegistration(
                TestRegistrationGateway gateway, UUID leaderId, long timeoutMillis) {
            return gateway.registrationCall(leaderId, timeoutMillis);
        }
    }
}
