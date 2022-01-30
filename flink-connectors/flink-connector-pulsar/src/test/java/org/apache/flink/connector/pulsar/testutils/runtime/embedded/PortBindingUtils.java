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

package org.apache.flink.connector.pulsar.testutils.runtime.embedded;

import javax.net.ServerSocketFactory;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This util is used for finding a available socket port. Use for running instance of the bookkeeper
 * and zookeeper at once.
 *
 * <p>This util isn't thread-safe. Don't use it in multi-thread environment.
 */
public final class PortBindingUtils {

    private static final int MIN_PORT = 1024;
    private static final int MAX_PORT = 65535;
    private static final Set<Integer> USED_PORTS = new HashSet<>();

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

        USED_PORTS.add(port);
        return port;
    }

    public static void releasePorts() {
        USED_PORTS.clear();
    }

    private static int randomPort(int minPort, int maxPort) {
        int delta = maxPort - minPort;
        return ThreadLocalRandom.current().nextInt(delta) + minPort;
    }

    public static boolean isPortAvailable(int port) {
        try (ServerSocket ignored =
                ServerSocketFactory.getDefault()
                        .createServerSocket(port, 1, InetAddress.getByName("localhost"))) {
            return !USED_PORTS.contains(port);
        } catch (Exception ex) {
            return false;
        }
    }
}
