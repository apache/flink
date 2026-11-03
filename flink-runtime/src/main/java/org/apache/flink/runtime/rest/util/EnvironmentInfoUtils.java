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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.annotation.Internal;

import java.util.Map;
import java.util.Optional;

/**
 * Helper utility class to retrieve the YARN container environment context. This relies on the
 * standard environment variables that YARN's NodeManager exports into every launched container, so
 * it intentionally avoids adding a dependency on the YARN client libraries.
 */
@Internal
public final class EnvironmentInfoUtils {

    private static final String ENV_CONTAINER_ID = "CONTAINER_ID";
    private static final String ENV_NM_HOST = "NM_HOST";
    private static final String ENV_NM_HTTP_PORT = "NM_HTTP_PORT";
    private static final String ENV_USER = "USER";

    private EnvironmentInfoUtils() {}

    /** Class that holds the application environment context. */
    public static final class EnvironmentContext {

        public final String containerId;

        public final String nodeManagerHostName;

        public final String nodeManagerHttpPort;

        public final String user;

        public EnvironmentContext(
                String containerId,
                String nodeManagerHostName,
                String nodeManagerHttpPort,
                String user) {
            this.containerId = containerId;
            this.nodeManagerHostName = nodeManagerHostName;
            this.nodeManagerHttpPort = nodeManagerHttpPort;
            this.user = user;
        }
    }

    /**
     * Reads the YARN container context from the process environment.
     *
     * @return the context, or empty if any of the required variables is missing or blank (e.g. when
     *     not running inside a YARN container)
     */
    public static Optional<EnvironmentContext> getEnvironmentContext() {
        return getEnvironmentContext(System.getenv());
    }

    /**
     * Reads the YARN container context from the given environment.
     *
     * @return the context, or empty if any of the required variables is missing or blank
     */
    public static Optional<EnvironmentContext> getEnvironmentContext(Map<String, String> env) {
        final String containerId = env.get(ENV_CONTAINER_ID);
        final String nodeManagerHost = env.get(ENV_NM_HOST);
        final String nodeManagerHttpPort = env.get(ENV_NM_HTTP_PORT);
        final String user = env.get(ENV_USER);

        if (isBlank(containerId)
                || isBlank(nodeManagerHost)
                || isBlank(nodeManagerHttpPort)
                || isBlank(user)) {
            return Optional.empty();
        }
        return Optional.of(
                new EnvironmentContext(containerId, nodeManagerHost, nodeManagerHttpPort, user));
    }

    /**
     * Builds the YARN NodeManager web URL of a container's logs. The scheme is always {@code http},
     * so this does not work if the NodeManagers are configured with {@code
     * yarn.http.policy=HTTPS_ONLY}.
     */
    public static String createNodeManagerLogUrl(
            String nodeManagerHost, String nodeManagerHttpPort, String containerId, String user) {
        return String.format(
                "http://%s:%s/node/containerlogs/%s/%s",
                nodeManagerHost, nodeManagerHttpPort, containerId, user);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
