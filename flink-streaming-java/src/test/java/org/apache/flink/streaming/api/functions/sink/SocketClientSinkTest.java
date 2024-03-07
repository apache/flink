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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.NetUtils;

import org.apache.commons.io.IOUtils;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link org.apache.flink.streaming.api.functions.sink.SocketClientSink}. */
class SocketClientSinkTest {

    private static final String TEST_MESSAGE = "testSocketSinkInvoke";

    private static final String EXCEPTION_MESSGAE =
            "Failed to send message '" + TEST_MESSAGE + "\n'";

    private static final String host = "127.0.0.1";

    private SerializationSchema<String> simpleSchema =
            new SerializationSchema<String>() {
                @Override
                public byte[] serialize(String element) {
                    return element.getBytes(ConfigConstants.DEFAULT_CHARSET);
                }
            };

    @Test
    void testSocketSink() throws Exception {
        final ServerSocket server = new ServerSocket(0);
        final int port = server.getLocalPort();

        CheckedThread sinkRunner =
                new CheckedThread("Test sink runner") {
                    @Override
                    public void go() throws Exception {
                        SocketClientSink<String> simpleSink =
                                new SocketClientSink<>(host, port, simpleSchema, 0);
                        simpleSink.open(DefaultOpenContext.INSTANCE);
                        simpleSink.invoke(TEST_MESSAGE + '\n', SinkContextUtil.forTimestamp(0));
                        simpleSink.close();
                    }
                };

        sinkRunner.start();

        Socket sk = NetUtils.acceptWithoutTimeout(server);
        BufferedReader rdr = new BufferedReader(new InputStreamReader(sk.getInputStream()));

        String value = rdr.readLine();

        sinkRunner.sync();
        server.close();

        assertThat(value).isEqualTo(TEST_MESSAGE);
    }

    @Test
    void testSinkAutoFlush() throws Exception {
        final ServerSocket server = new ServerSocket(0);
        final int port = server.getLocalPort();

        final SocketClientSink<String> simpleSink =
                new SocketClientSink<>(host, port, simpleSchema, 0, true);
        simpleSink.open(DefaultOpenContext.INSTANCE);

        CheckedThread sinkRunner =
                new CheckedThread("Test sink runner") {
                    @Override
                    public void go() throws Exception {
                        // need two messages here: send a fin to cancel the client
                        // state:FIN_WAIT_2 while the server is CLOSE_WAIT
                        simpleSink.invoke(TEST_MESSAGE + '\n', SinkContextUtil.forTimestamp(0));
                    }
                };

        sinkRunner.start();

        Socket sk = NetUtils.acceptWithoutTimeout(server);
        BufferedReader rdr = new BufferedReader(new InputStreamReader(sk.getInputStream()));
        String value = rdr.readLine();

        sinkRunner.sync();
        simpleSink.close();
        server.close();

        assertThat(value).isEqualTo(TEST_MESSAGE);
    }

    @Test
    void testSocketSinkNoRetry() throws Exception {
        final ServerSocket server = new ServerSocket(0);
        final int port = server.getLocalPort();

        try {
            CheckedThread serverRunner =
                    new CheckedThread("Test server runner") {

                        @Override
                        public void go() throws Exception {
                            Socket sk = NetUtils.acceptWithoutTimeout(server);
                            sk.close();
                        }
                    };
            serverRunner.start();

            SocketClientSink<String> simpleSink =
                    new SocketClientSink<>(host, port, simpleSchema, 0, true);
            simpleSink.open(DefaultOpenContext.INSTANCE);

            // wait socket server to close
            serverRunner.sync();

            assertThatThrownBy(
                            () -> {
                                // socket should be closed, so this should trigger a re-try
                                // need two messages here: send a fin to cancel the client
                                // state:FIN_WAIT_2 while
                                // the server is CLOSE_WAIT
                                while (true) { // we have to do this more often as the server side
                                    // closed is not
                                    // guaranteed to be noticed immediately
                                    simpleSink.invoke(
                                            TEST_MESSAGE + '\n', SinkContextUtil.forTimestamp(0));
                                }
                            })
                    // check whether throw a exception that reconnect failed.
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(EXCEPTION_MESSGAE);

            assertThat(simpleSink.getCurrentNumberOfRetries()).isZero();
        } finally {
            IOUtils.closeQuietly(server);
        }
    }

    @Test
    void testRetry() throws Exception {

        final ServerSocket[] serverSocket = new ServerSocket[1];
        final ExecutorService[] executor = new ExecutorService[1];

        try {
            serverSocket[0] = new ServerSocket(0);
            executor[0] = Executors.newCachedThreadPool();

            int port = serverSocket[0].getLocalPort();

            Callable<Void> serverTask =
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            Socket socket = NetUtils.acceptWithoutTimeout(serverSocket[0]);

                            BufferedReader reader =
                                    new BufferedReader(
                                            new InputStreamReader(socket.getInputStream()));

                            String value = reader.readLine();
                            assertThat(value).isEqualTo("0");

                            socket.close();
                            return null;
                        }
                    };

            Future<Void> serverFuture = executor[0].submit(serverTask);

            final SocketClientSink<String> sink =
                    new SocketClientSink<>(
                            host, serverSocket[0].getLocalPort(), simpleSchema, -1, true);

            // Create the connection
            sink.open(DefaultOpenContext.INSTANCE);

            // Initial payload => this will be received by the server an then the socket will be
            // closed.
            sink.invoke("0\n", SinkContextUtil.forTimestamp(0));

            // Get future an make sure there was no problem. This will rethrow any Exceptions from
            // the server.
            serverFuture.get();

            // Shutdown the server socket
            serverSocket[0].close();
            assertThat(serverSocket[0].isClosed()).isTrue();

            // No retries expected at this point
            assertThat(sink.getCurrentNumberOfRetries()).isZero();

            final CountDownLatch retryLatch = new CountDownLatch(1);
            final CountDownLatch again = new CountDownLatch(1);

            Callable<Void> sinkTask =
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            // Send next payload => server is down, should try to reconnect.

                            // We need to send more than just one packet to notice the closed
                            // connection.
                            while (retryLatch.getCount() != 0) {
                                sink.invoke("1\n");
                            }

                            return null;
                        }
                    };

            Future<Void> sinkFuture = executor[0].submit(sinkTask);

            while (sink.getCurrentNumberOfRetries() == 0) {
                // Wait for a retry
                Thread.sleep(100);
            }

            // OK the poor guy retried to write
            retryLatch.countDown();

            // Restart the server
            try {
                serverSocket[0] = new ServerSocket(port);
            } catch (BindException be) {
                // some other process may be using this port now
                throw new AssumptionViolatedException(
                        "Could not bind server to previous port.", be);
            }
            Socket socket = NetUtils.acceptWithoutTimeout(serverSocket[0]);

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Wait for the reconnect
            String value = reader.readLine();

            assertThat(value).isEqualTo("1");

            // OK the sink re-connected. :)
        } finally {
            if (serverSocket[0] != null) {
                serverSocket[0].close();
            }

            if (executor[0] != null) {
                executor[0].shutdown();
            }
        }
    }
}
