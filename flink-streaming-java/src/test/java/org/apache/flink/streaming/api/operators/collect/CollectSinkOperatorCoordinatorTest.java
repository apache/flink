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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.utils.CollectTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.NetUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CollectSinkOperatorCoordinator}. */
class CollectSinkOperatorCoordinatorTest {

    private static final int SOCKET_TIMEOUT_MILLIS = 1000;

    private static final TypeSerializer<Row> serializer =
            new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                    .createSerializer(new ExecutionConfig());

    @Test
    void testNoAddress() throws Exception {
        CollectSinkOperatorCoordinator coordinator =
                new CollectSinkOperatorCoordinator(SOCKET_TIMEOUT_MILLIS);
        coordinator.start();

        final String expectedVersion = "version";
        final CompletableFuture<CoordinationResponse> response =
                coordinator.handleCoordinationRequest(
                        createRequestForClientGeneratedResponse(expectedVersion));
        assertEmptyResponseGeneratedFromClient(response, expectedVersion);

        coordinator.close();
    }

    @Test
    void testServerFailure() throws Exception {
        CollectSinkOperatorCoordinator coordinator =
                new CollectSinkOperatorCoordinator(SOCKET_TIMEOUT_MILLIS);
        coordinator.start();

        List<List<Row>> expected =
                Arrays.asList(
                        Arrays.asList(Row.of(1, "aaa"), Row.of(2, "bbb")),
                        Arrays.asList(Row.of(3, "ccc"), Row.of(4, "ddd"), Row.of(5, "eee")));
        ServerThread server = new ServerThread(expected, 3);
        server.start();
        coordinator.handleEventFromOperator(
                0, 0, new CollectSinkAddressEvent(server.getServerAddress()));

        // a normal response
        CompletableFuture<CoordinationResponse> response =
                coordinator.handleCoordinationRequest(createRequestForServerGeneratedResponse());
        assertResponseWithDefaultMetadataFromServer(response, expected.get(0));

        // a normal response
        response = coordinator.handleCoordinationRequest(createRequestForServerGeneratedResponse());
        assertResponseWithDefaultMetadataFromServer(response, expected.get(1));

        // server closes here
        final String expectedVersion = "version3";
        CompletableFuture<CoordinationResponse> responseFuture =
                coordinator.handleCoordinationRequest(
                        createRequestForClientGeneratedResponse(expectedVersion));
        coordinator.executionAttemptFailed(0, 0, null);

        // new server comes
        expected = Collections.singletonList(Arrays.asList(Row.of(6, "fff"), Row.of(7, "ggg")));
        server = new ServerThread(expected, 2);
        server.start();
        coordinator.handleEventFromOperator(
                0, 0, new CollectSinkAddressEvent(server.getServerAddress()));

        // check failed request
        assertEmptyResponseGeneratedFromClient(responseFuture, expectedVersion);

        // a normal response
        response = coordinator.handleCoordinationRequest(createRequestForServerGeneratedResponse());
        assertResponseWithDefaultMetadataFromServer(response, expected.get(0));

        server.close();
        coordinator.close();
    }

    private static CoordinationRequest createRequestForServerGeneratedResponse() {
        final String unusedVersion = "random-version";
        return createRequestForClientGeneratedResponse(unusedVersion);
    }

    private static CoordinationRequest createRequestForClientGeneratedResponse(String version) {
        final int unusedOffset = 123;
        return new CollectCoordinationRequest(version, unusedOffset);
    }

    private static void assertEmptyResponseGeneratedFromServer(
            CompletableFuture<CoordinationResponse> responseFuture) throws Exception {
        assertEmptyResponseGeneratedFromClient(
                responseFuture, ServerThread.DEFAULT_SERVER_RESPONSE_VERSION);
    }

    private static void assertEmptyResponseGeneratedFromClient(
            CompletableFuture<CoordinationResponse> responseFuture, String expectedVersion)
            throws Exception {
        assertResponse(responseFuture, expectedVersion, -1, Collections.emptyList());
    }

    private static void assertResponseWithDefaultMetadataFromServer(
            CompletableFuture<CoordinationResponse> responseFuture, List<Row> expectedData)
            throws Exception {
        assertResponse(
                responseFuture,
                ServerThread.DEFAULT_SERVER_RESPONSE_VERSION,
                ServerThread.DEFAULT_SERVER_RESPONSE_OFFSET,
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

    private static class ServerThread extends Thread {

        static final String DEFAULT_SERVER_RESPONSE_VERSION = "server-response-version";
        static final int DEFAULT_SERVER_RESPONSE_OFFSET = 2;

        private final LinkedList<List<Row>> data;
        private final int closeRequestNum;

        private final ServerSocket server;
        private boolean running;

        private ServerThread(List<List<Row>> data, int closeRequestNum) throws IOException {
            this.data = new LinkedList<>(data);
            this.closeRequestNum = closeRequestNum;

            this.server = new ServerSocket(0);
        }

        @Override
        public void run() {
            running = true;

            int requestNum = 0;
            Socket socket = null;
            DataInputViewStreamWrapper inStream = null;
            DataOutputViewStreamWrapper outStream = null;

            try {
                while (running) {
                    if (socket == null) {
                        socket = NetUtils.acceptWithoutTimeout(server);
                        inStream = new DataInputViewStreamWrapper(socket.getInputStream());
                        outStream = new DataOutputViewStreamWrapper(socket.getOutputStream());
                    }

                    // parsing the request to ensure correct format of input message
                    new CollectCoordinationRequest(inStream);

                    requestNum++;
                    if (requestNum >= closeRequestNum) {
                        // server close abruptly
                        running = false;
                        break;
                    }

                    // serialize generic response (only the data is relevant)
                    new CollectCoordinationResponse(
                                    DEFAULT_SERVER_RESPONSE_VERSION,
                                    DEFAULT_SERVER_RESPONSE_OFFSET,
                                    CollectTestUtils.toBytesList(data.removeFirst(), serializer))
                            .serialize(outStream);
                }

                socket.close();
                server.close();
            } catch (IOException e) {
                // ignore
            }
        }

        public void close() {
            running = false;
        }

        public InetSocketAddress getServerAddress() {
            return new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort());
        }
    }
}
