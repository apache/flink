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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.utils.CollectTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CollectSinkOperatorCoordinator}. */
class CollectSinkOperatorCoordinatorTest {

    private static final TypeSerializer<Row> serializer =
            new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                    .createSerializer(new ExecutionConfig());

    @Test
    void testNoAddress() throws Exception {
        try (CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator()) {
            coordinator.start();

            final String requestVersion = "version";
            final CompletableFuture<CoordinationResponse> response =
                    coordinator.handleCoordinationRequest(
                            createRequestForCoordinatorGeneratedResponse(requestVersion));
            assertEmptyResponseGeneratedFromCoordinator(response, requestVersion);
        }
    }

    @Test
    void testClosingTheCoordinatorAfterRequestWasReceivedBySinkFunction() throws Exception {
        try (final TestingSinkFunction sinkFunction = new TestingSinkFunction()) {
            final String expectedVersion = "version";
            final CompletableFuture<CoordinationResponse> responseFuture;

            final CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator();
            coordinator.start();
            sinkFunction.registerSinkFunctionWith(coordinator);

            final CompletableFuture<Void> sinkFunctionProcessing =
                    sinkFunction.handleRequestWithoutResponse();
            assertThat(sinkFunctionProcessing)
                    .as("The SocketServer waits for the request to be sent.")
                    .isNotDone();

            responseFuture =
                    coordinator.handleCoordinationRequest(
                            createRequestForCoordinatorGeneratedResponse(expectedVersion));
            assertThat(responseFuture)
                    .as(
                            "The response shouldn't complete because the SinkFunction doesn't send any response.")
                    .isNotDone();

            FlinkAssertions.assertThatFuture(sinkFunctionProcessing)
                    .as(
                            "The SocketServer should eventually have handled the request without sending a response back.")
                    .eventuallySucceeds();

            // closing the coordinator should have resulted in an empty response being returned
            coordinator.close();
            assertEmptyResponseGeneratedFromCoordinator(responseFuture, expectedVersion);
        }
    }

    @Test
    void testSuccessfulResponse() throws Exception {
        try (CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator();
                final TestingSinkFunction sinkFunction =
                        TestingSinkFunction.createSinkFunctionAndInitializeCoordinator(
                                coordinator)) {
            coordinator.start();

            final List<Row> expectedData = Arrays.asList(Row.of(1, "aaa"), Row.of(2, "bbb"));
            final CompletableFuture<CoordinationResponse> responseFuture =
                    coordinator.handleCoordinationRequest(
                            createRequestForSinkFunctionGeneratedResponse());
            assertThat(responseFuture).isNotDone();

            sinkFunction.handleRequest(expectedData);

            assertResponseWithDefaultMetadataFromSinkFunction(responseFuture, expectedData);
        }
    }

    @Test
    void testClosingTheServerSocketOfTheSinkFunction() throws Exception {
        try (CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator()) {
            coordinator.start();

            final TestingSinkFunction sinkFunction =
                    TestingSinkFunction.createSinkFunctionAndInitializeCoordinator(coordinator);
            final String version = "version";
            final CompletableFuture<CoordinationResponse> responseFuture =
                    coordinator.handleCoordinationRequest(
                            createRequestForCoordinatorGeneratedResponse(version));
            assertThat(responseFuture).isNotDone();

            FlinkAssertions.assertThatFuture(sinkFunction.handleRequestWithoutResponse())
                    .eventuallySucceeds();

            // closing the ServerSocket on the SinkFunction side should result in an empty response
            sinkFunction.close();
            assertEmptyResponseGeneratedFromSinkFunction(responseFuture);
        }
    }

    @Test
    void testClosingTheListeningSocketInTheSinkFunction() throws Exception {
        try (CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator()) {
            coordinator.start();

            try (final TestingSinkFunction sinkFunction =
                    TestingSinkFunction.createSinkFunctionAndInitializeCoordinator(coordinator)) {
                final String version = "version";
                final CompletableFuture<CoordinationResponse> responseFuture =
                        coordinator.handleCoordinationRequest(
                                createRequestForCoordinatorGeneratedResponse(version));
                assertThat(responseFuture).isNotDone();

                sinkFunction.handleRequestWithoutResponse();

                // wait for the connection to be established
                sinkFunction.waitForConnectionToBeEstablished();
                // in order to close the connection from the SinkFunction's side
                sinkFunction.closeAcceptingSocket();

                // closing the accepting socket of the SinkFunction should result in an empty
                // response on the coordinator side
                assertEmptyResponseGeneratedFromCoordinator(responseFuture, version);
            }
        }
    }

    @Test
    void testCoordinatorNotConnectingToTheSinkFunctionSocket() throws Exception {
        try (final TestingSinkFunction sinkFunction =
                TestingSinkFunction.createTestingSinkFunctionWithoutConnection()) {
            final CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator();
            coordinator.start();
            sinkFunction.registerSinkFunctionWith(coordinator);

            final String expectedVersion = "version";
            final CompletableFuture<CoordinationResponse> responseFuture =
                    coordinator.handleCoordinationRequest(
                            createRequestForCoordinatorGeneratedResponse(expectedVersion));
            assertThat(responseFuture).isNotDone();

            // closing th coordinator before the request is sent should result in an empty response
            coordinator.close();
            assertEmptyResponseGeneratedFromCoordinator(responseFuture, expectedVersion);
        }
    }

    @Test
    void testReconnectAfterSinkFunctionSocketDisconnect() throws Exception {
        try (CollectSinkOperatorCoordinator coordinator = new CollectSinkOperatorCoordinator()) {
            coordinator.start();

            final TestingSinkFunction sinkFunction =
                    TestingSinkFunction.createSinkFunctionAndInitializeCoordinator(coordinator);

            final String expectedVersion = "version";
            final CompletableFuture<CoordinationResponse> responseFuture =
                    coordinator.handleCoordinationRequest(
                            createRequestForCoordinatorGeneratedResponse(expectedVersion));

            sinkFunction.waitForConnectionToBeEstablished();

            // simulation a failure while the request is handled by the SinkFunction
            coordinator.executionAttemptFailed(0, 0, null);
            sinkFunction.closeAcceptingSocket();

            // the client generates an empty response after the accepting socket was closed within
            // the sinkFunction
            assertEmptyResponseGeneratedFromCoordinator(responseFuture, expectedVersion);

            // the coordinator returns empty responses as long as there is no new connection
            // established
            final String anotherVersion = "another-version";
            assertEmptyResponseGeneratedFromCoordinator(
                    coordinator.handleCoordinationRequest(
                            createRequestForCoordinatorGeneratedResponse(anotherVersion)),
                    anotherVersion);

            // the next request is properly handled
            final TestingSinkFunction anotherSinkFunction =
                    TestingSinkFunction.createSinkFunctionAndInitializeCoordinator(coordinator);
            final CompletableFuture<CoordinationResponse> anotherResponseFuture =
                    coordinator.handleCoordinationRequest(
                            createRequestForSinkFunctionGeneratedResponse());
            final List<Row> expectedData = Arrays.asList(Row.of(1, "aaa"), Row.of(2, "bbb"));
            anotherSinkFunction.handleRequest(expectedData);

            assertResponseWithDefaultMetadataFromSinkFunction(anotherResponseFuture, expectedData);
            anotherSinkFunction.close();
        }
    }

    private static CoordinationRequest createRequestForSinkFunctionGeneratedResponse() {
        final String unusedVersion = "random-version";
        return createRequestForCoordinatorGeneratedResponse(unusedVersion);
    }

    private static CoordinationRequest createRequestForCoordinatorGeneratedResponse(
            String version) {
        final int unusedOffset = 123;
        return new CollectCoordinationRequest(version, unusedOffset);
    }

    private static void assertEmptyResponseGeneratedFromSinkFunction(
            CompletableFuture<CoordinationResponse> responseFuture) throws Exception {
        assertEmptyResponseGeneratedFromCoordinator(
                responseFuture, TestingSinkFunction.DEFAULT_SINK_FUNCTION_RESPONSE_VERSION);
    }

    private static void assertEmptyResponseGeneratedFromCoordinator(
            CompletableFuture<CoordinationResponse> responseFuture, String expectedVersion)
            throws Exception {
        assertResponse(responseFuture, expectedVersion, -1, Collections.emptyList());
    }

    private static void assertResponseWithDefaultMetadataFromSinkFunction(
            CompletableFuture<CoordinationResponse> responseFuture, List<Row> expectedData)
            throws Exception {
        assertResponse(
                responseFuture,
                TestingSinkFunction.DEFAULT_SINK_FUNCTION_RESPONSE_VERSION,
                TestingSinkFunction.DEFAULT_SINK_FUNCTION_RESPONSE_OFFSET,
                expectedData);
    }

    private static void assertResponse(
            CompletableFuture<CoordinationResponse> responseFuture,
            String expectedVersion,
            long expectedOffset,
            List<Row> expectedResults)
            throws Exception {
        final CollectCoordinationResponse response =
                (CollectCoordinationResponse) responseFuture.get();

        assertThat(response.getVersion()).isEqualTo(expectedVersion);
        assertThat(response.getLastCheckpointedOffset()).isEqualTo(expectedOffset);

        final List<Row> actualResult = response.getResults(serializer);
        assertThat(actualResult).hasSize(expectedResults.size());
        for (int rowId = 0; rowId < actualResult.size(); rowId++) {
            final Row expectedRow = expectedResults.get(rowId);
            final Row actualRow = actualResult.get(rowId);
            assertThat(actualRow.getArity()).isEqualTo(expectedRow.getArity());
            for (int columnId = 0; columnId < actualRow.getArity(); columnId++) {
                assertThat(actualRow.getField(columnId)).isEqualTo(expectedRow.getField(columnId));
            }
        }
    }

    /**
     * {@code TestingSinkFunction} simulates the {@link CollectSinkFunction} side of the {@link
     * CollectSinkOperatorCoordinator} communication.
     */
    private static class TestingSinkFunction implements AutoCloseable {

        static final String DEFAULT_SINK_FUNCTION_RESPONSE_VERSION = "version";
        static final int DEFAULT_SINK_FUNCTION_RESPONSE_OFFSET = 2;

        private final ServerSocket serverSocket;

        // null indicates that the socket was closed
        @Nullable private CompletableFuture<SocketConnection> connectionFuture;

        /**
         * Creates a {@code TestingSinkFunction} and connects it with the passed {@link
         * CollectSinkOperatorCoordinator}.
         */
        public static TestingSinkFunction createSinkFunctionAndInitializeCoordinator(
                CollectSinkOperatorCoordinator coordinator) throws Exception {
            final TestingSinkFunction socketServer = new TestingSinkFunction();
            socketServer.registerSinkFunctionWith(coordinator);

            return socketServer;
        }

        /**
         * Creates a {@code TestingSinKFunction} that doesn't listen on the configured. Sending
         * requests to this function will block forever when trying to connect to it.
         */
        public static TestingSinkFunction createTestingSinkFunctionWithoutConnection()
                throws IOException {
            return new TestingSinkFunction(ignoredServerSocket -> null);
        }

        /**
         * Instantiates a {@code TestingSinkFunction} that starts listening for incoming connections
         * right-away.
         */
        public TestingSinkFunction() throws IOException {
            this(TestingSinkFunction::acceptSocketAsync);
        }

        private TestingSinkFunction(
                Function<ServerSocket, CompletableFuture<SocketConnection>> socketListenerFactory)
                throws IOException {
            this.serverSocket = new ServerSocket(0);

            this.connectionFuture = socketListenerFactory.apply(serverSocket);
        }

        private static CompletableFuture<SocketConnection> acceptSocketAsync(
                ServerSocket serverSocket) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return new SocketConnection(
                                    NetUtils.acceptWithoutTimeout(serverSocket));
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    });
        }

        private CompletableFuture<SocketConnection> getConnectionFuture() {
            Preconditions.checkState(
                    connectionFuture != null,
                    "The accepting Socket is already closed. The calling operation is not possible anymore.");

            return connectionFuture;
        }

        /** Returns the {@link InetSocketAddress} of the {@code TestingSinkFunction}. */
        private InetSocketAddress getSocketAddress() {
            return new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), serverSocket.getLocalPort());
        }

        /** Registers the {@code TestingSinkFunction} with the passed {@code coordinator}. */
        public void registerSinkFunctionWith(CollectSinkOperatorCoordinator coordinator)
                throws Exception {
            coordinator.handleEventFromOperator(
                    0, 0, new CollectSinkAddressEvent(getSocketAddress()));
        }

        @Override
        public void close() throws Exception {
            closeAcceptingSocket();
            this.serverSocket.close();
        }

        /** Closes the established connection from the {@code SinkFunction}'s side. */
        public void closeAcceptingSocket() throws Exception {
            if (connectionFuture != null) {
                connectionFuture.get().close();
                connectionFuture = null;
            }
        }

        /**
         * Waits for the connection to be established between the {@code coordinator} and the {@code
         * SinkFunction}. This method will block until a request is sent by the coordinator and a
         * {@code handle*} call is initiated by this instance.
         */
        public void waitForConnectionToBeEstablished()
                throws ExecutionException, InterruptedException {
            getConnectionFuture().get();
        }

        /**
         * Handles a request with the given data in a synchronous fashion. The {@code
         * TestingSinkFunction}'s default meta information is attached to the response.
         *
         * @see #DEFAULT_SINK_FUNCTION_RESPONSE_VERSION
         * @see #DEFAULT_SINK_FUNCTION_RESPONSE_OFFSET
         */
        public void handleRequest(List<Row> actualData) {
            handleRequest(
                    DEFAULT_SINK_FUNCTION_RESPONSE_VERSION,
                    DEFAULT_SINK_FUNCTION_RESPONSE_OFFSET,
                    actualData);
        }

        /**
         * Handles the next request synchronously. The passed {@code actualData} will be forwarded
         * to the response.
         */
        public void handleRequest(String actualVersion, int actualOffset, List<Row> actualData) {
            handleRequestAsync(
                            actualVersion,
                            actualOffset,
                            CompletableFuture.completedFuture(actualData))
                    .join();
        }

        /**
         * Waits for the socket connection to be established and handles an incoming request. This
         * call will block until a request is sent that can be handled.
         */
        public CompletableFuture<Void> handleRequestWithoutResponse() {
            return internalConnectWithRequestHandlingAsync().thenApply(socketConnection -> null);
        }

        private CompletableFuture<SocketConnection> internalConnectWithRequestHandlingAsync() {
            return getConnectionFuture()
                    .thenApply(
                            socketConnection -> {
                                try {
                                    // parsing the request to ensure correct format of input message
                                    new CollectCoordinationRequest(
                                            socketConnection.getDataInputView());
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }

                                return socketConnection;
                            });
        }

        /**
         * Handles the request by sending a {@link CollectCoordinationResponse} via the socket.
         *
         * @return {@code CompleteFuture} that indicates whether the asynchronous processing on the
         *     {@code SinkFunction}'s side finished.
         */
        public CompletableFuture<Void> handleRequestAsync(
                String actualVersion,
                int actualOffset,
                CompletableFuture<List<Row>> actualDataAsync) {
            return internalConnectWithRequestHandlingAsync()
                    .thenCombineAsync(
                            actualDataAsync,
                            (socketConnection, data) -> {
                                if (socketConnection == null) {
                                    throw new CompletionException(
                                            new IllegalStateException(
                                                    "No SocketConnection established."));
                                }

                                try {
                                    // serialize generic response (only the data is relevant)
                                    new CollectCoordinationResponse(
                                                    actualVersion,
                                                    actualOffset,
                                                    CollectTestUtils.toBytesList(data, serializer))
                                            .serialize(socketConnection.getDataOutputView());
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }

                                return null;
                            });
        }
    }
}
