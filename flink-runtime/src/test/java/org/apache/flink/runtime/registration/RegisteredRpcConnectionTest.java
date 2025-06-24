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

import org.apache.flink.runtime.registration.RetryingRegistrationTest.TestRegistrationRejection;
import org.apache.flink.runtime.registration.RetryingRegistrationTest.TestRegistrationSuccess;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.types.Either;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for RegisteredRpcConnection, validating the successful, failure and close behavior. */
class RegisteredRpcConnectionTest {

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
    void testSuccessfulRpcConnection() throws Exception {
        final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
        final UUID leaderId = UUID.randomUUID();
        final String connectionID = "Test RPC Connection ID";

        // an endpoint that immediately returns success
        ManualResponseTestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(
                        new RetryingRegistrationTest.TestRegistrationSuccess(connectionID));

        try {
            rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

            TestRpcConnection connection =
                    new TestRpcConnection(
                            testRpcConnectionEndpointAddress,
                            leaderId,
                            rpcService.getScheduledExecutor(),
                            rpcService);
            connection.start();

            // wait for connection established
            final Either<TestRegistrationSuccess, TestRegistrationRejection> connectionResult =
                    connection.getConnectionFuture().get();

            assertThat(connectionResult.isLeft()).isTrue();

            final String actualConnectionId = connectionResult.left().getCorrelationId();

            // validate correct invocation and result
            assertThat(connection.isConnected()).isTrue();
            assertThat(connection.getTargetAddress()).isEqualTo(testRpcConnectionEndpointAddress);
            assertThat(connection.getTargetLeaderId()).isEqualTo(leaderId);
            assertThat(connection.getTargetGateway()).isEqualTo(testGateway);
            assertThat(actualConnectionId).isEqualTo(connectionID);
        } finally {
            testGateway.stop();
        }
    }

    @Test
    void testRpcConnectionFailures() throws Exception {
        final String connectionFailureMessage = "Test RPC Connection failure";
        final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
        final UUID leaderId = UUID.randomUUID();

        final RuntimeException registrationException =
                new RuntimeException(connectionFailureMessage);

        // gateway that upon registration calls throws an exception
        TestRegistrationGateway testGateway =
                DefaultTestRegistrationGateway.newBuilder()
                        .setRegistrationFunction(
                                (uuid, aLong) -> {
                                    throw registrationException;
                                })
                        .build();

        rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

        TestRpcConnection connection =
                new TestRpcConnection(
                        testRpcConnectionEndpointAddress,
                        leaderId,
                        rpcService.getScheduledExecutor(),
                        rpcService);
        connection.start();

        // wait for connection failure
        assertThatThrownBy(() -> connection.getConnectionFuture().get())
                .withFailMessage("expected failure.")
                .isInstanceOf(ExecutionException.class)
                .hasCause(registrationException);

        // validate correct invocation and result
        assertThat(connection.isConnected()).isFalse();
        assertThat(connection.getTargetAddress()).isEqualTo(testRpcConnectionEndpointAddress);
        assertThat(connection.getTargetLeaderId()).isEqualTo(leaderId);
        assertThat(connection.getTargetGateway()).isNull();
    }

    @Test
    void testRpcConnectionRejectionCallsOnRegistrationRejection() {
        TestRegistrationGateway testRegistrationGateway =
                DefaultTestRegistrationGateway.newBuilder()
                        .setRegistrationFunction(
                                (uuid, aLong) ->
                                        CompletableFuture.completedFuture(
                                                new TestRegistrationRejection(
                                                        TestRegistrationRejection.RejectionReason
                                                                .REJECTED)))
                        .build();

        rpcService.registerGateway(testRegistrationGateway.getAddress(), testRegistrationGateway);

        TestRpcConnection connection =
                new TestRpcConnection(
                        testRegistrationGateway.getAddress(),
                        UUID.randomUUID(),
                        rpcService.getScheduledExecutor(),
                        rpcService);
        connection.start();

        final Either<TestRegistrationSuccess, TestRegistrationRejection> connectionResult =
                connection.getConnectionFuture().join();

        assertThat(connectionResult.isRight()).isTrue();
        final TestRegistrationRejection registrationRejection = connectionResult.right();

        assertThat(registrationRejection.getRejectionReason())
                .isEqualTo(TestRegistrationRejection.RejectionReason.REJECTED);
    }

    @Test
    void testRpcConnectionClose() throws Exception {
        final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
        final UUID leaderId = UUID.randomUUID();
        final String connectionID = "Test RPC Connection ID";

        ManualResponseTestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(
                        new RetryingRegistrationTest.TestRegistrationSuccess(connectionID));

        try {
            rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

            TestRpcConnection connection =
                    new TestRpcConnection(
                            testRpcConnectionEndpointAddress,
                            leaderId,
                            rpcService.getScheduledExecutor(),
                            rpcService);
            connection.start();
            // close the connection
            connection.close();

            // validate connection is closed
            assertThat(connection.getTargetAddress()).isEqualTo(testRpcConnectionEndpointAddress);
            assertThat(connection.getTargetLeaderId()).isEqualTo(leaderId);
            assertThat(connection.isClosed()).isTrue();
        } finally {
            testGateway.stop();
        }
    }

    @Test
    void testReconnect() throws Exception {
        final String connectionId1 = "Test RPC Connection ID 1";
        final String connectionId2 = "Test RPC Connection ID 2";
        final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
        final UUID leaderId = UUID.randomUUID();
        final TestRegistrationGateway testGateway =
                new ManualResponseTestRegistrationGateway(
                        new RetryingRegistrationTest.TestRegistrationSuccess(connectionId1),
                        new RetryingRegistrationTest.TestRegistrationSuccess(connectionId2));

        rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

        TestRpcConnection connection =
                new TestRpcConnection(
                        testRpcConnectionEndpointAddress,
                        leaderId,
                        rpcService.getScheduledExecutor(),
                        rpcService);
        connection.start();

        final Either<TestRegistrationSuccess, TestRegistrationRejection> firstConnectionResult =
                connection.getConnectionFuture().get();

        assertThat(firstConnectionResult.isLeft()).isTrue();

        final String actualConnectionId1 = firstConnectionResult.left().getCorrelationId();

        assertThat(actualConnectionId1).isEqualTo(connectionId1);

        assertThat(connection.tryReconnect()).isTrue();

        final Either<TestRegistrationSuccess, TestRegistrationRejection> secondConnectionResult =
                connection.getConnectionFuture().get();

        assertThat(secondConnectionResult.isLeft()).isTrue();

        final String actualConnectionId2 = secondConnectionResult.left().getCorrelationId();

        assertThat(actualConnectionId2).isEqualTo(connectionId2);
    }

    // ------------------------------------------------------------------------
    //  test RegisteredRpcConnection
    // ------------------------------------------------------------------------

    private static class TestRpcConnection
            extends RegisteredRpcConnection<
                    UUID,
                    TestRegistrationGateway,
                    TestRegistrationSuccess,
                    TestRegistrationRejection> {

        private final Object lock = new Object();

        private final RpcService rpcService;

        private CompletableFuture<Either<TestRegistrationSuccess, TestRegistrationRejection>>
                connectionFuture;

        public TestRpcConnection(
                String targetAddress,
                UUID targetLeaderId,
                Executor executor,
                RpcService rpcService) {
            super(
                    LoggerFactory.getLogger(RegisteredRpcConnectionTest.class),
                    targetAddress,
                    targetLeaderId,
                    executor);
            this.rpcService = rpcService;
            this.connectionFuture = new CompletableFuture<>();
        }

        @Override
        protected RetryingRegistration<
                        UUID,
                        TestRegistrationGateway,
                        RetryingRegistrationTest.TestRegistrationSuccess,
                        RetryingRegistrationTest.TestRegistrationRejection>
                generateRegistration() {
            return new RetryingRegistrationTest.TestRetryingRegistration(
                    rpcService, getTargetAddress(), getTargetLeaderId());
        }

        @Override
        protected void onRegistrationSuccess(
                RetryingRegistrationTest.TestRegistrationSuccess success) {
            synchronized (lock) {
                connectionFuture.complete(Either.Left(success));
            }
        }

        @Override
        protected void onRegistrationRejection(TestRegistrationRejection rejection) {
            synchronized (lock) {
                connectionFuture.complete(Either.Right(rejection));
            }
        }

        @Override
        protected void onRegistrationFailure(Throwable failure) {
            synchronized (lock) {
                connectionFuture.completeExceptionally(failure);
            }
        }

        @Override
        public boolean tryReconnect() {
            synchronized (lock) {
                connectionFuture.cancel(false);
                connectionFuture = new CompletableFuture<>();
            }
            return super.tryReconnect();
        }

        public CompletableFuture<Either<TestRegistrationSuccess, TestRegistrationRejection>>
                getConnectionFuture() {
            synchronized (lock) {
                return connectionFuture;
            }
        }
    }
}
