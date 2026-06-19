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

package org.apache.flink.queryablestate.network;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.AtomicKvStateRequestStats;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests general behavior of the {@link AbstractServerBase}. */
class AbstractServerTest {

    /**
     * Tests that in case of port collision, a FlinkRuntimeException is thrown with a specific
     * message.
     */
    @Test
    void testServerInitializationFailure() throws Throwable {

        List<Integer> portList = Collections.singletonList(0);

        try (TestServer server1 =
                new TestServer(
                        "Test Server 1", new DisabledKvStateRequestStats(), portList.iterator())) {
            server1.start();

            try (TestServer server2 =
                    new TestServer(
                            "Test Server 2",
                            new DisabledKvStateRequestStats(),
                            Collections.singletonList(server1.getServerAddress().getPort())
                                    .iterator())) {
                // the expected exception along with the adequate message
                assertThatThrownBy(() -> server2.start())
                        .hasMessage(
                                "Unable to start Test Server 2. All ports in provided range are occupied.");
            }
        }
    }

    /**
     * Tests that in case of port collision and big enough port range, the server will try to bind
     * to the next port in the range.
     */
    @Test
    void testPortRangeSuccess() throws Throwable {

        AtomicKvStateRequestStats serverStats1 = new AtomicKvStateRequestStats();
        AtomicKvStateRequestStats serverStats2 = new AtomicKvStateRequestStats();
        AtomicKvStateRequestStats clientStats = new AtomicKvStateRequestStats();

        final int portRangeStart = 7777;
        final int portRangeEnd = 7900;
        List<Integer> portList =
                IntStream.range(portRangeStart, portRangeEnd + 1)
                        .boxed()
                        .collect(Collectors.toList());

        try (TestServer server1 =
                        new TestServer("Test Server 1", serverStats1, portList.iterator());
                TestServer server2 =
                        new TestServer("Test Server 2", serverStats2, portList.iterator());
                TestClient client =
                        new TestClient(
                                "Test Client",
                                1,
                                new MessageSerializer<>(
                                        new TestMessage.TestMessageDeserializer(),
                                        new TestMessage.TestMessageDeserializer()),
                                clientStats)) {
            server1.start();
            assertThat(server1.getServerAddress().getPort()).isGreaterThanOrEqualTo(portRangeStart);
            assertThat(server1.getServerAddress().getPort()).isLessThanOrEqualTo(portRangeEnd);

            server2.start();
            assertThat(server2.getServerAddress().getPort()).isGreaterThanOrEqualTo(portRangeStart);
            assertThat(server2.getServerAddress().getPort()).isLessThanOrEqualTo(portRangeEnd);

            TestMessage response1 =
                    client.sendRequest(server1.getServerAddress(), new TestMessage("ping")).join();
            assertThat(response1.getMessage()).isEqualTo(server1.getServerName() + "-ping");

            TestMessage response2 =
                    client.sendRequest(server2.getServerAddress(), new TestMessage("pong")).join();
            assertThat(response2.getMessage()).isEqualTo(server2.getServerName() + "-pong");

            assertThat(serverStats1.getNumConnections()).isEqualTo(1L);
            assertThat(serverStats2.getNumConnections()).isEqualTo(1L);

            assertThat(clientStats.getNumConnections()).isEqualTo(2L);
            assertThat(clientStats.getNumFailed()).isEqualTo(0L);
            assertThat(clientStats.getNumSuccessful()).isEqualTo(2L);
            assertThat(clientStats.getNumRequests()).isEqualTo(2L);
        }

        assertThat(serverStats1.getNumConnections()).isEqualTo(0L);
        assertThat(serverStats2.getNumConnections()).isEqualTo(0L);

        assertThat(clientStats.getNumConnections()).isEqualTo(0L);
        assertThat(clientStats.getNumFailed()).isEqualTo(0L);
        assertThat(clientStats.getNumSuccessful()).isEqualTo(2L);
        assertThat(clientStats.getNumRequests()).isEqualTo(2L);
    }

    private static class TestClient extends Client<TestMessage, TestMessage>
            implements AutoCloseable {

        TestClient(
                String clientName,
                int numEventLoopThreads,
                MessageSerializer<TestMessage, TestMessage> serializer,
                KvStateRequestStats stats) {
            super(clientName, numEventLoopThreads, serializer, stats);
        }

        @Override
        public void close() throws Exception {
            shutdown().join();
            assertThat(isEventGroupShutdown()).isTrue();
        }
    }

    /**
     * A server that receives a {@link TestMessage test message} and returns another test message
     * containing the same string as the request with the name of the server prepended.
     */
    private static class TestServer extends AbstractServerBase<TestMessage, TestMessage>
            implements AutoCloseable {

        private final KvStateRequestStats requestStats;

        TestServer(String name, KvStateRequestStats stats, Iterator<Integer> bindPort)
                throws UnknownHostException {
            super(name, InetAddress.getLocalHost().getHostName(), bindPort, 1, 1);
            this.requestStats = stats;
        }

        @Override
        public AbstractServerHandler<TestMessage, TestMessage> initializeHandler() {
            return new AbstractServerHandler<TestMessage, TestMessage>(
                    this,
                    new MessageSerializer<>(
                            new TestMessage.TestMessageDeserializer(),
                            new TestMessage.TestMessageDeserializer()),
                    requestStats) {

                @Override
                public CompletableFuture<TestMessage> handleRequest(
                        long requestId, TestMessage request) {
                    TestMessage response =
                            new TestMessage(getServerName() + '-' + request.getMessage());
                    return CompletableFuture.completedFuture(response);
                }

                @Override
                public CompletableFuture<Void> shutdown() {
                    return CompletableFuture.completedFuture(null);
                }
            };
        }

        @Override
        public void close() throws Exception {
            shutdownServer().get();
            assertThat(getQueryExecutor().isTerminated()).isTrue();
            assertThat(isEventGroupShutdown()).isTrue();
        }
    }

    /** Message with a string as payload. */
    private static class TestMessage extends MessageBody {

        private final String message;

        TestMessage(String message) {
            this.message = Preconditions.checkNotNull(message);
        }

        public String getMessage() {
            return message;
        }

        @Override
        public byte[] serialize() {
            byte[] content = message.getBytes(ConfigConstants.DEFAULT_CHARSET);

            // message size + 4 for the length itself
            return ByteBuffer.allocate(content.length + Integer.BYTES)
                    .putInt(content.length)
                    .put(content)
                    .array();
        }

        /** The deserializer for our {@link TestMessage test messages}. */
        public static class TestMessageDeserializer implements MessageDeserializer<TestMessage> {

            @Override
            public TestMessage deserializeMessage(ByteBuf buf) {
                int length = buf.readInt();
                String message = "";
                if (length > 0) {
                    byte[] name = new byte[length];
                    buf.readBytes(name);
                    message = new String(name, ConfigConstants.DEFAULT_CHARSET);
                }
                return new TestMessage(message);
            }
        }
    }
}
