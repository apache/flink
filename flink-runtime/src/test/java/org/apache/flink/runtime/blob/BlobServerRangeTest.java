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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests to ensure that the BlobServer properly starts on a specified range of available ports. */
class BlobServerRangeTest {

    @TempDir private Path tempDir;

    /** Start blob server on 0 = pick an ephemeral port. */
    @Test
    void testOnEphemeralPort() throws IOException {
        Configuration conf = new Configuration();
        conf.setString(BlobServerOptions.PORT, "0");

        BlobServer server = TestingBlobUtils.createServer(tempDir, conf);
        server.start();
        server.close();
    }

    /** Try allocating on an unavailable port. */
    @Test
    void testPortUnavailable() throws IOException {
        // allocate on an ephemeral port
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
        } catch (IOException e) {
            e.printStackTrace();
            fail("An exception was thrown while preparing the test " + e.getMessage());
        }

        Configuration conf = new Configuration();
        conf.setString(BlobServerOptions.PORT, String.valueOf(socket.getLocalPort()));

        // this thing is going to throw an exception
        try {
            assertThatThrownBy(() -> TestingBlobUtils.createServer(tempDir, conf))
                    .isInstanceOf(IOException.class)
                    .hasMessageStartingWith("Unable to open BLOB Server in specified port range: ");
        } finally {
            socket.close();
        }
    }

    /** Give the BlobServer a choice of three ports, where two of them are allocated. */
    @Test
    void testOnePortAvailable() throws IOException {
        int numAllocated = 2;
        ServerSocket[] sockets = new ServerSocket[numAllocated];
        for (int i = 0; i < numAllocated; i++) {
            try {
                sockets[i] = new ServerSocket(0);
            } catch (IOException e) {
                e.printStackTrace();
                fail("An exception was thrown while preparing the test " + e.getMessage());
            }
        }
        Configuration conf = new Configuration();
        conf.setString(
                BlobServerOptions.PORT,
                sockets[0].getLocalPort() + "," + sockets[1].getLocalPort() + ",50000-50050");

        // this thing is going to throw an exception
        try {
            BlobServer server = TestingBlobUtils.createServer(tempDir, conf);
            server.start();
            assertThat(server.getPort()).isBetween(50000, 50050);
            server.close();
        } finally {
            for (int i = 0; i < numAllocated; ++i) {
                sockets[i].close();
            }
        }
    }
}
