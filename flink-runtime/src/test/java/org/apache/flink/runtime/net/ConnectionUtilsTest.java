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

package org.apache.flink.runtime.net;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for the network utilities. */
@RunWith(PowerMockRunner.class)
public class ConnectionUtilsTest {

    @Test
    public void testReturnLocalHostAddressUsingHeuristics() throws Exception {
        // instead of using a unstable localhost:port as "unreachable" to cause Test fails unstably
        // using a Absolutely unreachable outside ip:port
        InetSocketAddress unreachable = new InetSocketAddress("8.8.8.8", 0xFFFF);

        final long start = System.nanoTime();
        InetAddress add = ConnectionUtils.findConnectingAddress(unreachable, 2000, 400);

        // check that it did not take forever (max 30 seconds)
        // this check can unfortunately not be too tight, or it will be flaky on some CI
        // infrastructure
        assertTrue(System.nanoTime() - start < 30_000_000_000L);

        // we should have found a heuristic address
        assertNotNull(add);

        // make sure that we returned the InetAddress.getLocalHost as a heuristic
        assertEquals(InetAddress.getLocalHost(), add);
    }

    @Test
    public void testFindConnectingAddressWhenGetLocalHostThrows() throws Exception {
        PowerMockito.mockStatic(InetAddress.class);
        Mockito.when(InetAddress.getLocalHost())
                .thenThrow(new UnknownHostException())
                .thenCallRealMethod();

        final InetAddress loopbackAddress = Inet4Address.getByName("127.0.0.1");
        Thread socketServerThread;
        try (ServerSocket socket = new ServerSocket(0, 1, loopbackAddress)) {
            // Make sure that the thread will eventually die even if something else goes wrong
            socket.setSoTimeout(10_000);
            socketServerThread =
                    new Thread(
                            new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        socket.accept();
                                    } catch (IOException e) {
                                        // ignore
                                    }
                                }
                            });
            socketServerThread.start();

            final InetSocketAddress socketAddress =
                    new InetSocketAddress(loopbackAddress, socket.getLocalPort());
            final InetAddress address =
                    ConnectionUtils.findConnectingAddress(socketAddress, 2000, 400);

            // Make sure we got an address via alternative means
            assertNotNull(address);
        }
    }
}
