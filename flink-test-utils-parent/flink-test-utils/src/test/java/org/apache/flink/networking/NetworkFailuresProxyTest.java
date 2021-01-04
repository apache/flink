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

package org.apache.flink.networking;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

import static org.junit.Assert.assertEquals;

/** Tests for NetworkFailuresProxy. */
public class NetworkFailuresProxyTest {
    public static final int SOCKET_TIMEOUT = 500_000;

    @Test
    public void testProxy() throws Exception {
        try (EchoServer echoServer = new EchoServer(SOCKET_TIMEOUT);
                NetworkFailuresProxy proxy =
                        new NetworkFailuresProxy(0, "localhost", echoServer.getLocalPort());
                EchoClient echoClient =
                        new EchoClient("localhost", proxy.getLocalPort(), SOCKET_TIMEOUT)) {
            echoServer.start();

            assertEquals("42", echoClient.write("42"));
            assertEquals("Ala ma kota!", echoClient.write("Ala ma kota!"));
        }
    }

    @Test
    public void testMultipleConnections() throws Exception {
        try (EchoServer echoServer = new EchoServer(SOCKET_TIMEOUT);
                NetworkFailuresProxy proxy =
                        new NetworkFailuresProxy(0, "localhost", echoServer.getLocalPort());
                EchoClient echoClient1 =
                        new EchoClient("localhost", proxy.getLocalPort(), SOCKET_TIMEOUT);
                EchoClient echoClient2 =
                        new EchoClient("localhost", proxy.getLocalPort(), SOCKET_TIMEOUT)) {
            echoServer.start();

            assertEquals("42", echoClient1.write("42"));
            assertEquals("Ala ma kota!", echoClient2.write("Ala ma kota!"));
            assertEquals("Ala hat eine Katze!", echoClient1.write("Ala hat eine Katze!"));
        }
    }

    @Test
    public void testBlockTraffic() throws Exception {
        try (EchoServer echoServer = new EchoServer(SOCKET_TIMEOUT);
                NetworkFailuresProxy proxy =
                        new NetworkFailuresProxy(0, "localhost", echoServer.getLocalPort())) {
            echoServer.start();

            try (EchoClient echoClient =
                    new EchoClient("localhost", proxy.getLocalPort(), SOCKET_TIMEOUT)) {
                assertEquals("42", echoClient.write("42"));
                proxy.blockTraffic();
                try {
                    echoClient.write("Ala ma kota!");
                } catch (SocketException ex) {
                    assertEquals("Connection reset", ex.getMessage());
                }
            }

            try (EchoClient echoClient =
                    new EchoClient("localhost", proxy.getLocalPort(), SOCKET_TIMEOUT)) {
                assertEquals(null, echoClient.write("42"));
            } catch (SocketException ex) {
                assertEquals("Connection reset", ex.getMessage());
            }

            proxy.unblockTraffic();
            try (EchoClient echoClient =
                    new EchoClient("localhost", proxy.getLocalPort(), SOCKET_TIMEOUT)) {
                assertEquals("42", echoClient.write("42"));
                assertEquals("Ala ma kota!", echoClient.write("Ala ma kota!"));
            }
        }
    }

    /** Simple echo client that sends a message over the network and waits for the answer. */
    public static class EchoClient implements AutoCloseable {
        private final Socket socket;
        private final PrintWriter output;
        private final BufferedReader input;

        public EchoClient(String hostName, int portNumber, int socketTimeout) throws IOException {
            socket = new Socket(hostName, portNumber);
            socket.setSoTimeout(socketTimeout);
            output = new PrintWriter(socket.getOutputStream(), true);
            input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        public String write(String message) throws IOException {
            output.println(message);
            return input.readLine();
        }

        @Override
        public void close() throws Exception {
            input.close();
            output.close();
            socket.close();
        }
    }
}
