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

/**
 * Helper utility class to retrieve the YARN container environment context. This relies on the
 * standard environment variables that YARN's NodeManager exports into every launched container,
 * so it intentionally avoids adding a dependency on the YARN client libraries.
 */
public class EnvironmentInfoUtils {

    private static final String ENV_CONTAINER_ID = "CONTAINER_ID";
    private static final String ENV_NM_HOST = "NM_HOST";
    private static final String ENV_NM_HTTP_PORT = "NM_HTTP_PORT";
    private static final String ENV_USER = "USER";

    /** Class that holds the application environment context. */
    public static class EnvironmentContext {

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

    public static EnvironmentContext getEnvironmentContext() {
        return new EnvironmentContext(
                getContainerId(),
                getNodeManagerHostName(),
                getNodeManagerHttpPort(),
                getUserInfo());
    }

    private static String getContainerId() {
        return System.getenv(ENV_CONTAINER_ID);
    }

    private static String getNodeManagerHostName() {
        return System.getenv(ENV_NM_HOST);
    }

    private static String getNodeManagerHttpPort() {
        return System.getenv(ENV_NM_HTTP_PORT);
    }

    private static String getUserInfo() {
        return System.getenv(ENV_USER);
    }
}
