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

package org.apache.flink.connector.pulsar.testutils.runtime.mock;

import javax.net.ServerSocketFactory;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class is used for finding a available socket port. Use for running multiple instances of the
 * bookkeeper tests at once.
 */
public final class PortBindingUtils {

    private static final int MIN_PORT = 1024;
    private static final int MAX_PORT = 65535;

    private PortBindingUtils() {
        // No public constructor
    }

    public static int findAvailablePort() {
        return findAvailablePort(MIN_PORT, MAX_PORT);
    }

    public static int findAvailablePort(int minPort, int maxPort) {
        int port;
        do {
            port = randomPort(minPort, maxPort);
        } while (!isPortAvailable(port));

        return port;
    }

    private static int randomPort(int minPort, int maxPort) {
        int delta = maxPort - minPort;
        return ThreadLocalRandom.current().nextInt(delta) + minPort;
    }

    public static boolean isPortAvailable(int port) {
        try (ServerSocket serverSocket =
                ServerSocketFactory.getDefault()
                        .createServerSocket(port, 1, InetAddress.getByName("localhost"))) {
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
