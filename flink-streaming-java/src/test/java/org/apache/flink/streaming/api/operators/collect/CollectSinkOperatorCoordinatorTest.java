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
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.utils.CollectTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.NetUtils;

import org.junit.Assert;
import org.junit.Test;

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

/** Tests for {@link CollectSinkOperatorCoordinator}. */
public class CollectSinkOperatorCoordinatorTest {

    private static final int SOCKET_TIMEOUT_MILLIS = 1000;

    private static final TypeSerializer<Row> serializer =
            new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                    .createSerializer(new ExecutionConfig());

    @Test
    public void testNoAddress() throws Exception {
        CollectSinkOperatorCoordinator coordinator =
                new CollectSinkOperatorCoordinator(SOCKET_TIMEOUT_MILLIS);
        coordinator.start();

        CollectCoordinationRequest request = new CollectCoordinationRequest("version", 123);
        CollectCoordinationResponse response =
                (CollectCoordinationResponse) coordinator.handleCoordinationRequest(request).get();
        assertResponseEquals(request, response, -1, Collections.emptyList());

        coordinator.close();
    }

    @Test
    public void testServerFailure() throws Exception {
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
                0, new CollectSinkAddressEvent(server.getServerAddress()));

        // a normal response
        CollectCoordinationRequest request = new CollectCoordinationRequest("version1", 123);
        CollectCoordinationResponse response =
                (CollectCoordinationResponse) coordinator.handleCoordinationRequest(request).get();
        assertResponseEquals(request, response, 0, expected.get(0));

        // a normal response
        request = new CollectCoordinationRequest("version2", 456);
        response =
                (CollectCoordinationResponse) coordinator.handleCoordinationRequest(request).get();
        assertResponseEquals(request, response, 0, expected.get(1));

        // server closes here
        request = new CollectCoordinationRequest("version3", 789);
        CompletableFuture<CoordinationResponse> responseFuture =
                coordinator.handleCoordinationRequest(request);
        coordinator.subtaskFailed(0, null);

        // new server comes
        expected = Collections.singletonList(Arrays.asList(Row.of(6, "fff"), Row.of(7, "ggg")));
        server = new ServerThread(expected, 2);
        server.start();
        coordinator.handleEventFromOperator(
                0, new CollectSinkAddressEvent(server.getServerAddress()));

        // check failed request
        response = (CollectCoordinationResponse) responseFuture.get();
        assertResponseEquals(request, response, -1, Collections.emptyList());

        // a normal response
        request = new CollectCoordinationRequest("version4", 101112);
        response =
                (CollectCoordinationResponse) coordinator.handleCoordinationRequest(request).get();
        assertResponseEquals(request, response, 0, expected.get(0));

        server.close();
        coordinator.close();
    }

    private void assertResponseEquals(
            CollectCoordinationRequest request,
            CollectCoordinationResponse response,
            long expectedLastCheckpointedOffset,
            List<Row> expectedResults)
            throws Exception {
        Assert.assertEquals(request.getVersion(), response.getVersion());
        Assert.assertEquals(expectedLastCheckpointedOffset, response.getLastCheckpointedOffset());
        List<Row> results = response.getResults(serializer);
        Assert.assertEquals(expectedResults.size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            Row expectedRow = expectedResults.get(i);
            Row actualRow = results.get(i);
            Assert.assertEquals(expectedRow.getArity(), actualRow.getArity());
            for (int j = 0; j < actualRow.getArity(); j++) {
                Assert.assertEquals(expectedRow.getField(j), actualRow.getField(j));
            }
        }
    }

    private static class ServerThread extends Thread {

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

                    CollectCoordinationRequest request = new CollectCoordinationRequest(inStream);

                    requestNum++;
                    if (requestNum >= closeRequestNum) {
                        // server close abruptly
                        running = false;
                        break;
                    }

                    CollectCoordinationResponse response =
                            new CollectCoordinationResponse(
                                    request.getVersion(),
                                    0,
                                    CollectTestUtils.toBytesList(data.removeFirst(), serializer));
                    response.serialize(outStream);
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
